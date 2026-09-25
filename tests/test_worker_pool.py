"""End-to-end execution through the real worker pool."""
from __future__ import annotations

import threading
import time

from scheduler.models import SubmitRequest, TaskState
from scheduler.worker import (
    PermanentTaskError,
    TransientTaskError,
    WorkerPool,
)


def _wait_state(service, tid, expected, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = service.get_task(tid, owner="o").state
        if state == expected:
            return service.get_task(tid, owner="o")
        time.sleep(0.02)
    raise AssertionError(f"task {tid} did not reach {expected}; got {state}")


def _drive_clock(clock, service, stop_event):
    """Advance the injected fake clock in real time so backoff/lease timers
    (persisted on that clock) elapse while real worker threads poll."""
    step = 0.01
    while not stop_event.wait(step):
        clock.advance(step)


def test_worker_executes_simple_task(service, settings):
    pool = WorkerPool(service, handlers={"echo": lambda p, ctx: {"got": p}}, concurrency=2)
    pool.start()
    try:
        tid = service.submit(
            SubmitRequest(task_type="echo", payload={"hello": "world"}, owner="o")
        ).task_id
        task = _wait_state(service, tid, TaskState.SUCCEEDED)
        assert task.result == {"got": {"hello": "world"}}
    finally:
        pool.stop()


def test_worker_retries_then_succeeds(service, settings, clock):
    attempts = {"n": 0}

    def flaky(payload, ctx):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TransientTaskError("not ready")
        return {"ok": True, "attempts": attempts["n"]}

    object.__setattr__(settings, "base_backoff_seconds", 0.05)
    object.__setattr__(settings, "max_backoff_seconds", 0.05)
    pool = WorkerPool(service, handlers={"flaky": flaky}, concurrency=1)
    stop = threading.Event()
    driver = threading.Thread(target=_drive_clock, args=(clock, service, stop), daemon=True)
    driver.start()
    pool.start()
    try:
        tid = service.submit(
            SubmitRequest(task_type="flaky", payload={}, owner="o", max_attempts=5)
        ).task_id
        task = _wait_state(service, tid, TaskState.SUCCEEDED)
        assert attempts["n"] == 3
        assert task.attempt == 3
        assert task.result["attempts"] == 3
    finally:
        stop.set()
        pool.stop()


def test_worker_permanent_failure_no_retry(service, settings):
    def bad(payload, ctx):
        raise PermanentTaskError("invalid argument")

    pool = WorkerPool(service, handlers={"bad": bad}, concurrency=1)
    pool.start()
    try:
        tid = service.submit(
            SubmitRequest(task_type="bad", payload={}, owner="o", max_attempts=9)
        ).task_id
        task = _wait_state(service, tid, TaskState.PERMANENTLY_FAILED)
        assert task.attempt == 1
        assert task.last_error_type == "PermanentTaskError"
    finally:
        pool.stop()


def test_worker_unknown_type_is_permanent(service, settings):
    pool = WorkerPool(service, handlers={}, concurrency=1)
    pool.start()
    try:
        tid = service.submit(
            SubmitRequest(task_type="not-registered", payload={}, owner="o")
        ).task_id
        task = _wait_state(service, tid, TaskState.PERMANENTLY_FAILED)
        assert task.last_error_type == "UnknownTaskType"
    finally:
        pool.stop()


def test_worker_cancel_during_execution_wins(service, settings):
    started = threading.Event()
    release = threading.Event()

    def slow(payload, ctx):
        started.set()
        # Wait cooperatively until either cancelled or released.
        for _ in range(200):
            if ctx.cancelled():
                ctx.raise_if_cancelled()
            if release.wait(0.02):
                break
        return {"finished": True}

    pool = WorkerPool(service, handlers={"slow": slow}, concurrency=1)
    pool.start()
    try:
        tid = service.submit(
            SubmitRequest(task_type="slow", payload={}, owner="o", max_attempts=5)
        ).task_id
        assert started.wait(5.0)
        service.cancel(tid)
        task = _wait_state(service, tid, TaskState.CANCELLED, timeout=8.0)
        assert task.result is None
        release.set()
    finally:
        release.set()
        pool.stop()
