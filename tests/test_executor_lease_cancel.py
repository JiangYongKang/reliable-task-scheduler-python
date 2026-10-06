"""租约续约、执行中取消打断、取消/完成竞争、执行者失联接管与重启恢复。"""
from __future__ import annotations

import threading
import time

import pytest

from scheduler.config import SchedulerConfig
from scheduler.executor import (
    ExecutionContext,
    TaskCancelledError,
    TaskExecutor,
)
from scheduler.models import Task, TaskState
from scheduler.service import SchedulerService
from tests.test_executor import wait_until


@pytest.fixture
def exec_env(tmp_path):
    config = SchedulerConfig(
        db_path=str(tmp_path / "scheduler.db"),
        lease_seconds=1.0,
        max_attempts=3,
        backoff_base_seconds=0.1,
        backoff_max_seconds=1.0,
        renew_interval_seconds=0.2,
    )
    svc = SchedulerService(config)
    env = {"service": svc, "config": config, "executors": []}

    def make_executor(handler, concurrency=2, worker_prefix="executor"):
        ex = TaskExecutor(svc, handler, concurrency=concurrency,
                          poll_interval=0.05,
                          renew_interval=config.renew_interval_seconds,
                          worker_prefix=worker_prefix)
        env["executors"].append(ex)
        return ex

    env["make_executor"] = make_executor
    yield env
    for ex in env["executors"]:
        ex.stop(wait=False)
    svc.close()


def test_long_task_held_by_renewal_not_taken_over(exec_env):
    """执行时间(2.5s)远超租约(1s)：续约保证不被接管，只执行一次。"""
    svc = exec_env["service"]
    runs: list[int] = []

    def handler(task: Task, ctx: ExecutionContext):
        runs.append(task.attempt)
        deadline = time.monotonic() + 2.5
        while time.monotonic() < deadline:
            ctx.check_cancelled()
            time.sleep(0.05)
        return {"long": "done"}

    ex = exec_env["make_executor"](handler)
    ex.start()
    t, _ = svc.submit("long-task", {})
    print("input task sleep=2.5s lease=1.0s renew=0.2s")

    # 执行到一半时任务必须仍归原执行者持有，且租约被续到未来
    wait_until(lambda: len(runs) == 1)
    time.sleep(1.2)  # 已超过原始租约时长
    mid = svc.get(t.task_id)
    print("mid-execution state=%s owner=%s lease_expires_at-now=%.2fs"
          % (mid.state.value, mid.owner,
             (mid.lease_expires_at or 0) - time.time()))
    assert mid.state is TaskState.RUNNING
    assert mid.lease_expires_at is not None
    assert mid.lease_expires_at > time.time()  # 租约被续约，不可被抢走

    wait_until(lambda: svc.get(t.task_id).state is TaskState.SUCCEEDED,
               timeout=10, msg="long task should finish under renewal")
    final = svc.get(t.task_id)
    print("final attempt=%d handler_runs=%s" % (final.attempt, runs))
    assert final.attempt == 1 and runs == [1]  # 没有被接管重跑
    reasons = [e.reason for e in svc.history(t.task_id)]
    assert "lease_expired_takeover" not in reasons


def test_cancel_interrupts_running_task(exec_env):
    """执行中取消：执行体在可观察时间内停下，不写结果，终态 CANCELLED。"""
    svc = exec_env["service"]
    interrupted_at: list[float] = []
    started = threading.Event()

    def handler(task: Task, ctx: ExecutionContext):
        started.set()
        try:
            while True:
                ctx.check_cancelled()
                time.sleep(0.02)
        except TaskCancelledError:
            interrupted_at.append(time.monotonic())
            raise

    ex = exec_env["make_executor"](handler)
    ex.start()
    t, _ = svc.submit("cancel-me", {"work": "long"})
    assert started.wait(timeout=5)
    print("input cancel task=%s while RUNNING" % t.task_id)
    cancel_at = time.monotonic()
    cancelled = svc.cancel(t.task_id)
    assert cancelled.state is TaskState.CANCELLED

    wait_until(lambda: bool(interrupted_at),
               msg="handler should observe cancellation promptly")
    latency = interrupted_at[0] - cancel_at
    print("interrupted %.3fs after cancel (renew_interval=0.2s)" % latency)
    assert latency < 1.0  # 在可观察到的一小段时间内停下

    time.sleep(0.3)
    final = svc.get(t.task_id)
    assert final.state is TaskState.CANCELLED
    assert final.result is None  # 不残留半成品数据
    reasons = [e.reason for e in svc.history(t.task_id)]
    print("history events=%s" % reasons)
    assert reasons == ["submitted", "claimed", "cancelled"]


def test_cancel_complete_race_has_single_outcome(exec_env):
    """取消与完成几乎同时发生：最终只有一种结局，状态与结果一致。"""
    svc = exec_env["service"]
    release = threading.Event()
    entered = threading.Event()

    def handler(task: Task, ctx: ExecutionContext):
        entered.set()
        release.wait(timeout=5)
        return {"race": "result"}

    ex = exec_env["make_executor"](handler)
    ex.start()
    t, _ = svc.submit("race", {})
    assert entered.wait(timeout=5)

    # 取消与执行体返回几乎同时发生
    release.set()
    from scheduler.errors import InvalidStateTransitionError
    try:
        svc.cancel(t.task_id)
        print("cancel submitted concurrently with completion")
    except InvalidStateTransitionError as err:
        print("reject cancel code=%s current=SUCCEEDED" % err.code)

    wait_until(lambda: svc.get(t.task_id).state.terminal)
    final = svc.get(t.task_id)
    print("race outcome state=%s result=%r" % (final.state.value, final.result))
    if final.state is TaskState.CANCELLED:
        assert final.result is None  # 取消获胜 -> 结果不得落库
    else:
        assert final.state is TaskState.SUCCEEDED
        assert final.result == {"race": "result"}  # 完成获胜 -> 结果完整


def test_dead_executor_task_taken_over_and_finished_once(exec_env):
    """执行者失联（领取后不再续约）：租约超时后被接管重跑，结果唯一。"""
    svc = exec_env["service"]
    runs: list[str] = []

    def handler(task: Task, ctx: ExecutionContext):
        runs.append(task.owner or "?")
        return {"by": task.owner}

    # 模拟一个崩溃的执行者：领取后进程消失，永不续约也不回报
    t, _ = svc.submit("orphan-task", {"x": 1})
    ghost = svc.claim("ghost-worker")
    assert ghost is not None and ghost.task_id == t.task_id
    print("ghost-worker claimed task=%s then crashed (no renew, no report)"
          % ghost.task_id)

    ex = exec_env["make_executor"](handler, worker_prefix="rescuer")
    ex.start()
    wait_until(lambda: svc.get(ghost.task_id).state is TaskState.SUCCEEDED,
               timeout=10, msg="orphaned task should be taken over")
    final = svc.get(ghost.task_id)
    print("takeover final owner_runs=%s attempt=%d result=%s"
          % (runs, final.attempt, final.result))
    assert final.attempt == 2  # 第一次是 ghost 领取，第二次是接管
    assert len(runs) == 1 and runs[0].startswith("rescuer")
    assert final.result == {"by": runs[0]}

    events = svc.history(ghost.task_id)
    reasons = [e.reason for e in events]
    print("history events=%s" % reasons)
    assert "lease_expired_takeover" in reasons
    assert reasons[-1] == "completed"


def test_takeover_after_service_restart(tmp_path):
    """进程重启后：失联任务被新进程的执行器接管，历史与重启前对得上。"""
    db = str(tmp_path / "scheduler.db")
    config = SchedulerConfig(db_path=db, lease_seconds=1.0, max_attempts=3,
                             backoff_base_seconds=0.1, backoff_max_seconds=1.0,
                             renew_interval_seconds=0.2)

    # 旧进程：提交并领取后“崩溃”（直接关闭，不续约不回报）
    svc1 = SchedulerService(config)
    t, _ = svc1.submit("restart-task", {"x": 1})
    claimed = svc1.claim("old-process-worker")
    assert claimed is not None
    svc1.close()
    print("process restart: task=%s left RUNNING by old-process-worker"
          % t.task_id)

    # 新进程：同一数据库，执行器接管失联任务
    svc2 = SchedulerService(config)
    restored = svc2.get(t.task_id)
    assert restored.state is TaskState.RUNNING  # 重启前状态完整恢复
    assert restored.owner == "old-process-worker"

    runs: list[str] = []

    def handler(task: Task, ctx: ExecutionContext):
        runs.append(task.owner or "?")
        return {"ok": True}

    ex = TaskExecutor(svc2, handler, concurrency=1, poll_interval=0.05,
                      renew_interval=0.2, worker_prefix="new-process")
    ex.start()
    try:
        wait_until(lambda: svc2.get(t.task_id).state is TaskState.SUCCEEDED,
                   timeout=10)
    finally:
        ex.stop()
    final = svc2.get(t.task_id)
    reasons = [e.reason for e in svc2.history(t.task_id)]
    print("after restart: state=%s attempt=%d events=%s"
          % (final.state.value, final.attempt, reasons))
    assert final.result == {"ok": True}
    assert len(runs) == 1  # 最终结果唯一，无重复成功
    assert reasons == ["submitted", "claimed", "lease_expired_takeover",
                       "completed"]
    svc2.close()
