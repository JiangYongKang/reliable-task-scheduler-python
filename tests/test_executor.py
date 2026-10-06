"""服务自驱执行：自动跑完、并发排队、幂等一次执行、失败重试与永久失败。

日志可观察：请求输入、任务流转（dispatch/execute/renew/complete/fail）、
被拒绝或被打断的依据。
"""
from __future__ import annotations

import threading
import time

import pytest

from scheduler.config import SchedulerConfig
from scheduler.executor import (
    ExecutionContext,
    PermanentTaskError,
    TaskExecutor,
)
from scheduler.models import Task, TaskState
from scheduler.service import SchedulerService


def wait_until(pred, timeout: float = 10.0, interval: float = 0.02,
               msg: str = "condition not met") -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(interval)
    raise AssertionError(f"timeout: {msg}")


@pytest.fixture
def exec_env(tmp_path):
    """真实时钟 + 短租约的执行器环境。"""
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


def test_auto_runs_submitted_task_to_success(exec_env):
    svc = exec_env["service"]
    ran: list[str] = []

    def handler(task: Task, ctx: ExecutionContext):
        ran.append(task.task_id)
        return {"done": task.payload["n"]}

    ex = exec_env["make_executor"](handler)
    ex.start()
    print("input submit key=auto-1 payload={'n': 7}")
    t, created = svc.submit("auto-1", {"n": 7})
    assert created

    wait_until(lambda: svc.get(t.task_id).state is TaskState.SUCCEEDED,
               msg="task should be executed automatically")
    final = svc.get(t.task_id)
    print("transition task=%s -> %s result=%s"
          % (t.task_id, final.state.value, final.result))
    assert final.result == {"done": 7}
    assert ran == [t.task_id]

    reasons = [e.reason for e in svc.history(t.task_id)]
    print("history task=%s events=%s" % (t.task_id, reasons))
    assert reasons == ["submitted", "claimed", "completed"]
    stats = svc.stats()
    assert stats["SUCCEEDED"] == 1 and stats["PENDING"] == 0


def test_queueing_when_tasks_exceed_concurrency(exec_env):
    svc = exec_env["service"]
    n, concurrency = 10, 3
    active = 0
    peak = 0
    lock = threading.Lock()

    def handler(task: Task, ctx: ExecutionContext):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.15)
        with lock:
            active -= 1
        return {"ok": True}

    ex = exec_env["make_executor"](handler, concurrency=concurrency)
    ex.start()
    print("input submit %d tasks, concurrency=%d" % (n, concurrency))
    ids = [svc.submit(f"q-{i}", {"i": i})[0].task_id for i in range(n)]

    wait_until(lambda: svc.stats()["SUCCEEDED"] == n,
               msg="all queued tasks should eventually succeed")
    print("peak concurrency observed=%d (limit=%d)" % (peak, concurrency))
    assert 1 <= peak <= concurrency  # 排队等待而非报错/丢弃/漏掉
    assert svc.stats()["PENDING"] == 0
    for tid in ids:
        assert svc.get(tid).state is TaskState.SUCCEEDED


def test_same_idempotency_key_executes_exactly_once(exec_env):
    svc = exec_env["service"]
    calls: list[str] = []

    def handler(task: Task, ctx: ExecutionContext):
        calls.append(task.idempotency_key)
        return {"v": 1}

    ex = exec_env["make_executor"](handler)
    ex.start()
    t1, c1 = svc.submit("dup-key", {"v": 1})
    t2, c2 = svc.submit("dup-key", {"v": 2})
    t3, c3 = svc.submit("dup-key", {"v": 3})
    print("submit x3 same key -> created=%s,%s,%s" % (c1, c2, c3))
    assert (c1, c2, c3) == (True, False, False)
    assert t1.task_id == t2.task_id == t3.task_id

    wait_until(lambda: svc.get(t1.task_id).state is TaskState.SUCCEEDED)
    time.sleep(0.3)  # 确认没有额外执行
    print("handler invocations=%d (expect 1)" % len(calls))
    assert calls == ["dup-key"]
    assert svc.stats()["SUCCEEDED"] == 1


def test_transient_failure_retries_with_backoff_then_succeeds(exec_env):
    svc = exec_env["service"]
    attempts: list[int] = []

    def handler(task: Task, ctx: ExecutionContext):
        attempts.append(task.attempt)
        if task.attempt < 2:
            raise RuntimeError("boom")
        return {"recovered": True}

    ex = exec_env["make_executor"](handler)
    ex.start()
    t, _ = svc.submit("flaky", {})
    wait_until(lambda: svc.get(t.task_id).state is TaskState.SUCCEEDED,
               msg="transient failure should be retried automatically")
    final = svc.get(t.task_id)
    print("attempts=%s final attempt=%d state=%s"
          % (attempts, final.attempt, final.state.value))
    assert attempts == [1, 2]
    assert final.attempt == 2 and final.result == {"recovered": True}
    reasons = [e.reason for e in svc.history(t.task_id)]
    assert "transient_failure_retry" in reasons


def test_permanent_failure_goes_to_failed_without_retry(exec_env):
    svc = exec_env["service"]
    calls = 0

    def handler(task: Task, ctx: ExecutionContext):
        nonlocal calls
        calls += 1
        raise PermanentTaskError("no point retrying")

    ex = exec_env["make_executor"](handler)
    ex.start()
    t, _ = svc.submit("doomed", {})
    wait_until(lambda: svc.get(t.task_id).state is TaskState.FAILED)
    time.sleep(0.3)
    final = svc.get(t.task_id)
    print("state=%s error=%s handler_calls=%d"
          % (final.state.value, final.error, calls))
    assert final.error == "no point retrying"
    assert calls == 1  # 永久失败不重试
    assert svc.stats()["FAILED"] == 1
