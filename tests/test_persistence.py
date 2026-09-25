"""进程重启后：任务状态、租约接管、执行历史与幂等性均保持一致。"""
from __future__ import annotations

import pytest

from scheduler.config import SchedulerConfig
from scheduler.errors import InvalidStateTransitionError
from scheduler.models import TaskState
from scheduler.service import SchedulerService


def _make(db_path: str, clock):
    return SchedulerService(
        SchedulerConfig(db_path=db_path, lease_seconds=10.0, max_attempts=3,
                        backoff_base_seconds=1.0, backoff_max_seconds=60.0),
        now_fn=clock.now,
    )


def test_state_history_and_lease_survive_restart(tmp_path, clock):
    db = str(tmp_path / "scheduler.db")
    svc = _make(db, clock)

    running, _ = svc.submit("persist-running", {})
    clock.advance(0.1)
    done, _ = svc.submit("persist-done", {})
    clock.advance(0.1)
    failed, _ = svc.submit("persist-failed", {})
    clock.advance(0.1)
    cancelled, _ = svc.submit("persist-cancel", {})
    clock.advance(0.1)
    pending, _ = svc.submit("persist-pending", {})

    c = svc.claim("old-worker")
    assert c is not None and c.task_id == running.task_id
    c = svc.claim("w")
    assert c is not None and c.task_id == done.task_id
    svc.complete(done.task_id, "w", {"persisted": True})
    c = svc.claim("w")
    assert c is not None and c.task_id == failed.task_id
    svc.fail(failed.task_id, "w", "fatal", permanent=True)
    svc.cancel(cancelled.task_id)

    svc.close()

    # ---- 模拟进程重启：新建 service 指向同一数据库 ----
    svc2 = _make(db, clock)

    # 终态/待处理任务完整恢复
    assert svc2.get(done.task_id).result == {"persisted": True}
    assert svc2.get(failed.task_id).state is TaskState.FAILED
    assert svc2.get(cancelled.task_id).state is TaskState.CANCELLED
    print("restored terminal states: SUCCEEDED/FAILED/CANCELLED intact")

    # 执行历史保留
    events = svc2.history(done.task_id)
    reasons = [e.reason for e in events]
    assert reasons == ["submitted", "claimed", "completed"]
    print("restored history task=%s events=%s" % (done.task_id, reasons))

    # 未过期租约：其他执行者不能领取该 RUNNING 任务；PENDING 仍可领取
    assert svc2.claim("new-worker") is not None  # 领到 pending
    running_task = svc2.get(running.task_id, requester="new-worker")
    assert running_task.state is TaskState.RUNNING
    assert running_task.owner == "old-worker"

    # 租约超时后安全接管，attempt 续增
    clock.advance(11)
    taken = svc2.claim("new-worker")
    assert taken is not None and taken.task_id == running.task_id
    assert taken.owner == "new-worker" and taken.attempt == 2
    print("lease takeover after restart task=%s attempt=2" % running.task_id)
    finished = svc2.complete(running.task_id, "new-worker", {"recovered": True})
    assert finished.state is TaskState.SUCCEEDED
    assert finished.result == {"recovered": True}

    # 幂等键跨重启仍然生效
    dup, created = svc2.submit("persist-pending", {"x": 1})
    assert created is False and dup.task_id == pending.task_id
    print("idempotency across restart key=persist-pending created=False")

    # 终态任务重启后仍拒绝重复取消
    with pytest.raises(InvalidStateTransitionError):
        svc2.cancel(cancelled.task_id)

    svc2.close()


def test_retry_schedule_survives_restart(tmp_path, clock):
    db = str(tmp_path / "scheduler.db")
    svc = _make(db, clock)
    t, _ = svc.submit("retry-restart", {})
    svc.claim("w")
    svc.fail(t.task_id, "w", "transient", permanent=False)
    svc.close()

    svc2 = _make(db, clock)
    # 退避未到不可领取
    assert svc2.claim("w") is None
    clock.advance(1)
    claimed = svc2.claim("w")
    assert claimed is not None and claimed.attempt == 2
    print("retry due after restart task=%s attempt=2" % t.task_id)
    svc2.close()
