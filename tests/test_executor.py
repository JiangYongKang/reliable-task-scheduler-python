"""服务自驱执行：自动跑完、续约防误接管、执行中取消、失联接管重跑。

日志中可看到：提交输入、领取/续约/状态流转、取消打断与拒绝依据。
"""
from __future__ import annotations

import threading
import time

import pytest

from scheduler.config import SchedulerConfig
from scheduler.errors import (
    InvalidStateTransitionError,
    LeaseConflictError,
)
from scheduler.executor import (
    ExecutionEngine,
    PermanentTaskError,
    RetryTaskError,
)
from scheduler.models import TaskState
from scheduler.service import SchedulerService


def _wait_state(svc, task_id, states, timeout=5.0, requester=None):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = svc.get(task_id, requester=requester)
        if last.state in states:
            return last
        time.sleep(0.01)
    raise AssertionError(
        f"task {task_id} never reached {states}; last={last and last.state}")


@pytest.fixture
def env(tmp_path):
    """真实时钟 + 短租约/短退避的服务与引擎。"""
    cfg = SchedulerConfig(
        db_path=str(tmp_path / "executor.db"),
        lease_seconds=1.0,
        max_attempts=3,
        backoff_base_seconds=0.02,
        backoff_max_seconds=0.1,
        worker_concurrency=4,
        lease_renew_interval_seconds=0.3,
        poll_interval_seconds=0.01,
    )
    svc = SchedulerService(cfg)
    eng = ExecutionEngine(svc, worker_id="engine-main")
    yield svc, eng
    eng.stop(timeout=2)
    svc.close()


def _start(env, handlers):
    svc, eng = env
    for name, fn in handlers.items():
        eng.register_handler(name, fn)
    eng.start()
    return svc, eng


def test_engine_runs_submitted_tasks_to_success_without_external_claims(env):
    """场景1a：提交后无需外部 claim/complete，服务自动把任务跑完。"""
    svc, _engine = _start(env, {
        "default": lambda payload, ctx: {"doubled": payload["n"] * 2},
    })
    t, created = svc.submit("auto-1", {"n": 21})
    print("input submit key=auto-1 payload={'n': 21} created=%s" % created)

    done = _wait_state(svc, t.task_id,
                       {TaskState.SUCCEEDED, TaskState.FAILED})
    assert done.state is TaskState.SUCCEEDED
    assert done.result == {"doubled": 42}
    print("transition auto: PENDING -> RUNNING -> SUCCEEDED result=%s"
          % done.result)

    reasons = [e.reason for e in svc.history(t.task_id)]
    assert reasons == ["submitted", "claimed", "completed"]
    assert svc.stats()[TaskState.SUCCEEDED.value] == 1


def test_idempotent_key_executed_exactly_once_even_with_duplicate_submits(env):
    """场景1b：同一幂等键被提交多次，只真正执行一次。"""
    calls = []

    def handler(payload, ctx):
        calls.append(payload.get("v"))
        return {"ran": len(calls)}

    svc, _engine = _start(env, {"default": handler})
    t1, c1 = svc.submit("once", {"v": 1})
    t2, c2 = svc.submit("once", {"v": 2})
    t3, c3 = svc.submit("once", {"v": 3})
    print("duplicate submits created=%s,%s,%s same_task=%s"
          % (c1, c2, c3, t1.task_id == t2.task_id == t3.task_id))

    done = _wait_state(svc, t1.task_id, {TaskState.SUCCEEDED})
    assert done.result == {"ran": 1}
    assert calls == [1]  # 只执行一次，且用的是首次提交的 payload
    assert t1.task_id == t2.task_id == t3.task_id
    print("idempotency: handler invoked exactly once, payload not overwritten")


def test_oversubscribed_tasks_queue_instead_of_error_or_loss(env):
    """场景1c：任务数远超并发度时排队等待，最终一个不漏全部完成。"""
    running = threading.Semaphore(0)
    finish = threading.Event()

    def handler(payload, ctx):
        running.release()
        finish.wait(timeout=2)
        return {"n": payload["n"]}

    svc, _engine = _start(env, {"default": handler})
    total = 20
    tasks = [svc.submit(f"q-{i}", {"n": i})[0] for i in range(total)]

    # 恰好并发度（4）个在执行，其余全部排队为 PENDING
    for _ in range(4):
        assert running.acquire(timeout=2)
    time.sleep(0.05)
    stats = svc.stats()
    print("backlog observed RUNNING=%s PENDING=%s"
          % (stats["RUNNING"], stats["PENDING"]))
    assert stats["RUNNING"] == 4
    assert stats["PENDING"] == total - 4

    finish.set()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        stats = svc.stats()
        if stats[TaskState.SUCCEEDED.value] == total:
            break
        time.sleep(0.02)
    stats = svc.stats()
    print("drained stats=%s" % stats)
    assert stats[TaskState.SUCCEEDED.value] == total
    assert stats[TaskState.PENDING.value] == 0
    for t in tasks:
        assert svc.get(t.task_id).result == {"n": int(
            t.idempotency_key.split("-")[1])}


def test_transient_failure_retried_then_succeeds_automatically(env):
    """场景1d：执行体抛临时失败 -> 退避重试 -> 成功，全程自动。"""
    attempts = []

    def flaky(payload, ctx):
        attempts.append(ctx.attempt)
        print("handler invoked attempt=%s" % ctx.attempt)
        if ctx.attempt < 3:
            raise RetryTaskError("temporary blip")
        return {"ok_after": ctx.attempt}

    svc, _engine = _start(env, {"default": flaky})
    t, _ = svc.submit("retry-auto", {})
    done = _wait_state(svc, t.task_id, {TaskState.SUCCEEDED})
    assert done.result == {"ok_after": 3}
    assert attempts == [1, 2, 3]
    reasons = [e.reason for e in svc.history(t.task_id)]
    assert reasons == ["submitted", "claimed", "transient_failure_retry",
                       "claimed", "transient_failure_retry", "claimed",
                       "completed"]
    print("auto retry history=%s" % reasons)


def test_permanent_failure_terminates_without_retry(env):
    """场景1e：永久失败立即进入 FAILED，不再重试。"""
    attempts = []

    def doomed(payload, ctx):
        attempts.append(ctx.attempt)
        raise PermanentTaskError("unrecoverable")

    svc, _engine = _start(env, {"default": doomed})
    t, _ = svc.submit("perm-auto", {})
    failed = _wait_state(svc, t.task_id, {TaskState.FAILED})
    assert failed.error == "unrecoverable"
    assert attempts == [1]
    assert svc.stats()[TaskState.FAILED.value] == 1
    print("permanent failure recorded, error=%r" % failed.error)


def test_unknown_exception_is_retried_not_crash_engine(env):
    """场景1f：执行体抛出未声明异常按临时失败处理，引擎不崩溃。"""
    attempts = []

    def buggy(payload, ctx):
        attempts.append(ctx.attempt)
        if ctx.attempt < 2:
            raise ValueError("unexpected!")
        return {"recovered": True}

    svc, _engine = _start(env, {"default": buggy})
    t, _ = svc.submit("bug", {})
    done = _wait_state(svc, t.task_id, {TaskState.SUCCEEDED, TaskState.FAILED})
    assert done.state is TaskState.SUCCEEDED
    assert done.result == {"recovered": True}
    print("unexpected exception classified as transient, recovered on attempt 2")


def test_non_serializable_result_is_permanent_failure(env):
    """场景1g：返回无法持久化的结果时永久失败，不做无意义重试。"""
    attempts = []

    def bad_result(payload, ctx):
        attempts.append(ctx.attempt)
        return {"obj": object()}

    svc, _engine = _start(env, {"default": bad_result})
    t, _ = svc.submit("bad-result", {})
    failed = _wait_state(svc, t.task_id, {TaskState.FAILED})
    assert attempts == [1]
    assert "non-JSON-serializable" in failed.error
    print("non-serializable result -> FAILED immediately: %r" % failed.error)


def test_long_running_task_keeps_lease_and_is_not_taken_over(env):
    """场景2：执行时间远超租约，续约保证任务始终由原执行者持有。"""
    started = threading.Event()

    def long_task(payload, ctx):
        started.set()
        # 执行时长为租约（1s）的 3 倍；依赖续约保住租约
        interrupted = ctx.sleep_interruptible(3.0)
        assert not interrupted, "healthy executor must not be interrupted"
        return {"long": True}

    svc, _engine = _start(env, {"default": long_task})
    t, _ = svc.submit("long-renew", {})
    assert started.wait(timeout=2)
    running = _wait_state(svc, t.task_id, {TaskState.RUNNING})
    assert running.owner == "engine-main" and running.attempt == 1

    # 整个执行期间，外部反复尝试接管，必须全部空手而归
    deadline = time.monotonic() + 2.2
    while time.monotonic() < deadline:
        stolen = svc.claim("intruder")
        assert stolen is None, (
            "healthy long task must not be taken over; got %s"
            % (stolen and stolen.task_id))
        cur = svc.get(t.task_id, requester=None)
        assert cur.owner == "engine-main" and cur.attempt == 1
        time.sleep(0.03)

    done = _wait_state(svc, t.task_id, {TaskState.SUCCEEDED})
    assert done.result == {"long": True}
    assert [e.reason for e in svc.history(t.task_id)] == [
        "submitted", "claimed", "completed"]  # 无接管事件
    print("lease renewed throughout 3x lease duration; no takeover, attempt=1")


def test_cancel_interrupts_running_task_without_partial_result(env):
    """场景3：执行中的任务被取消后在短时间内停下，结果不落库。"""
    started = threading.Event()
    cancelled_observed = threading.Event()
    side_effects = []

    def cancellable(payload, ctx):
        started.set()
        # 分段执行并在每段检查取消信号
        for step in range(100):
            if ctx.sleep_interruptible(0.02):
                cancelled_observed.set()
                print("handler observed cancel at step=%s, stopping" % step)
                return {"half": "written"}  # 模拟早退；该返回值不得落库
            side_effects.append(step)  # 取消后不再产生后续步骤
        return {"done": True}

    svc, _engine = _start(env, {"default": cancellable})
    t, _ = svc.submit("cancel-running", {})
    assert started.wait(timeout=2)

    t0 = time.monotonic()
    cancelled = svc.cancel(t.task_id)
    assert cancelled.state is TaskState.CANCELLED
    assert cancelled_observed.wait(timeout=2)
    elapsed = time.monotonic() - t0
    print("interrupt observed within %.3fs" % elapsed)
    assert elapsed < 1.0  # 可观察的一小段时间内停下

    final = _wait_state(svc, t.task_id, {TaskState.CANCELLED})
    assert final.result is None and final.owner is None
    # 给工作线程一点时间，确认它没有把取消后的返回值写进去
    time.sleep(0.1)
    assert svc.get(t.task_id, requester=None).state is TaskState.CANCELLED
    assert svc.get(t.task_id, requester=None).result is None

    reasons = [e.reason for e in svc.history(t.task_id)]
    assert reasons == ["submitted", "claimed", "cancelled"]
    print("cancel history=%s, no completion, no partial result" % reasons)


def test_cancel_queued_task_never_runs(env):
    """场景3b：排队中的任务被取消后不会再被执行。"""
    blockers = threading.Semaphore(0)
    release = threading.Event()
    ran_queued = []

    def blocker(payload, ctx):
        blockers.release()
        release.wait(timeout=2)

    def queued(payload, ctx):
        ran_queued.append(1)
        return {"should": "not run"}

    svc, _engine = _start(env, {"blocker": blocker, "queued": queued})
    for i in range(4):
        svc.submit(f"block-{i}", {"type": "blocker"})
    for _ in range(4):
        assert blockers.acquire(timeout=2)
    q, _ = svc.submit("queued-cancel", {"type": "queued"})
    time.sleep(0.05)
    assert svc.get(q.task_id).state is TaskState.PENDING  # 仍在排队

    assert svc.cancel(q.task_id).state is TaskState.CANCELLED
    release.set()
    time.sleep(0.15)
    assert ran_queued == []
    assert svc.get(q.task_id).result is None
    print("queued task cancelled before execution, handler never invoked")


def test_dead_executor_taken_over_and_completed_once(env):
    """场景4：执行者失联（停止续约）后被接管重跑；最终唯一成功，僵尸回报被拒。"""
    # 先让一个「会崩溃」的执行者领取任务，且从不续约
    svc, _eng_unused = env
    t, _ = svc.submit("dead-man", {})
    claimed = svc.claim("dead-exec")
    assert claimed is not None and claimed.attempt == 1
    print("dead-exec claimed task=%s then crashed (no renewals)" % t.task_id)

    runs = []

    def survivor(payload, ctx):
        runs.append(ctx.attempt)
        return {"finished_by_attempt": ctx.attempt}

    eng2 = ExecutionEngine(svc, worker_id="survivor")
    eng2.register_handler("default", survivor)
    eng2.start()
    try:
        done = _wait_state(svc, t.task_id, {TaskState.SUCCEEDED}, timeout=5)
        assert done.result == {"finished_by_attempt": 2}
        assert runs == [2]
        print("takeover: survivor re-ran task at attempt=2, single success")

        # 失联执行者「复活」后的迟到回报必须被拒，结果唯一
        for action in ("complete",):
            with pytest.raises((LeaseConflictError,
                                InvalidStateTransitionError)) as ei:
                svc.complete(t.task_id, "dead-exec", {"zombie": True})
            print("reject zombie %s code=%s" % (action, ei.value.code))
        with pytest.raises(InvalidStateTransitionError) as ei:
            svc.fail(t.task_id, "dead-exec", "zombie", permanent=True)
        print("reject zombie fail code=%s" % ei.value.code)
        assert svc.get(t.task_id).result == {"finished_by_attempt": 2}

        reasons = [(e.reason, e.actor)
                   for e in svc.history(t.task_id)]
        assert reasons == [
            ("submitted", "api"),
            ("claimed", "dead-exec"),
            ("lease_expired_takeover", "survivor"),
            ("completed", "survivor"),
        ]
        print("takeover history consistent=%s" % reasons)
    finally:
        eng2.stop(timeout=2)


def test_takeover_after_restart_completes_with_history_intact(tmp_path):
    """场景4b：进程重启后引擎接管遗留 RUNNING 任务，历史与最终结果对得上。"""
    db = str(tmp_path / "restart.db")
    cfg1 = SchedulerConfig(db_path=db, lease_seconds=0.3,
                           lease_renew_interval_seconds=0.1,
                           backoff_base_seconds=0.02)
    svc1 = SchedulerService(cfg1)
    t, _ = svc1.submit("restart-run", {})
    claimed = svc1.claim("old-process")
    assert claimed.attempt == 1
    print("old process crashed mid-execution task=%s" % t.task_id)
    svc1.close()

    time.sleep(0.4)  # 旧租约到期

    cfg2 = SchedulerConfig(db_path=db, lease_seconds=0.3,
                           lease_renew_interval_seconds=0.1,
                           backoff_base_seconds=0.02)
    svc2 = SchedulerService(cfg2)
    eng = ExecutionEngine(svc2, worker_id="new-process")
    eng.register_handler("default", lambda p, c: {"after_restart": True})
    eng.start()
    try:
        done = _wait_state(svc2, t.task_id, {TaskState.SUCCEEDED}, timeout=5)
        assert done.result == {"after_restart": True}
        assert done.attempt == 2
        reasons = [(e.from_state, e.to_state, e.reason, e.actor)
                   for e in svc2.history(t.task_id)]
        assert reasons == [
            (None, "PENDING", "submitted", "api"),
            ("PENDING", "RUNNING", "claimed", "old-process"),
            ("RUNNING", "RUNNING", "lease_expired_takeover", "new-process"),
            ("RUNNING", "SUCCEEDED", "completed", "new-process"),
        ]
        print("restart takeover history=%s" % reasons)
    finally:
        eng.stop(timeout=2)
        svc2.close()
