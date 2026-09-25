"""并发领取互斥、取消竞争、重试退避与永久失败。"""
from __future__ import annotations

import random
import threading

import pytest

from scheduler.errors import (
    InvalidStateTransitionError,
    LeaseConflictError,
)
from scheduler.models import TaskState


def test_concurrent_claim_only_one_winner(service):
    n = 20
    for i in range(n):
        service.submit(f"key-{i}", {"i": i})

    winners: list[tuple[str, str]] = []
    lock = threading.Lock()

    def worker(wid: str) -> None:
        got = service.claim(wid)
        if got is not None:
            print("worker=%s claimed task=%s attempt=%s"
                  % (wid, got.task_id, got.attempt))
            with lock:
                winners.append((wid, got.task_id))

    threads = [threading.Thread(target=worker, args=(f"w{i % 5}",))
               for i in range(n * 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    claimed_task_ids = [tid for _, tid in winners]
    assert len(winners) == n
    assert len(set(claimed_task_ids)) == n  # 没有两个执行者领取同一任务


def test_cancel_wins_against_late_complete(service):
    """分支一：取消先提交，执行者的完成必须被拒，结果不写入。"""
    t, _ = service.submit("race-cancel-wins", {})
    claimed = service.claim("w")
    assert claimed is not None
    cancelled = service.cancel(t.task_id)
    assert cancelled.state is TaskState.CANCELLED
    with pytest.raises(InvalidStateTransitionError) as ei:
        service.complete(t.task_id, "w", {"should": "not persist"})
    print("reject late complete code=%s current=CANCELLED" % ei.value.code)
    final = service.get(t.task_id, requester=None)
    assert final.state is TaskState.CANCELLED
    assert final.result is None  # 失败方结果不得落库


def test_complete_wins_against_late_cancel(service):
    """分支二：完成先提交，重复取消必须被拒，终态为 SUCCEEDED。"""
    t, _ = service.submit("race-complete-wins", {})
    claimed = service.claim("w")
    assert claimed is not None
    done = service.complete(t.task_id, "w", {"ok": True})
    assert done.state is TaskState.SUCCEEDED
    with pytest.raises(InvalidStateTransitionError) as ei:
        service.cancel(t.task_id)
    print("reject late cancel code=%s current=SUCCEEDED" % ei.value.code)
    final = service.get(t.task_id, requester=None)
    assert final.state is TaskState.SUCCEEDED and final.result == {"ok": True}


def test_cancel_complete_race_consistency(service):
    """随机交错并发：任何一种结局都必须状态确定、结果一致、错误分类正确。"""
    rng = random.Random(42)

    def run_once(idx: int) -> None:
        t, _ = service.submit(f"race-{idx}", {})
        claimed = service.claim(f"w{idx}")
        assert claimed is not None
        errors: list[InvalidStateTransitionError] = []

        def completer() -> None:
            try:
                service.complete(t.task_id, f"w{idx}", {"ok": idx})
                print("task=%s complete won -> SUCCEEDED" % t.task_id)
            except InvalidStateTransitionError as err:
                print("task=%s complete rejected code=%s"
                      % (t.task_id, err.code))
                errors.append(err)

        def canceller() -> None:
            try:
                service.cancel(t.task_id)
                print("task=%s cancel won -> CANCELLED" % t.task_id)
            except InvalidStateTransitionError as err:
                print("task=%s cancel rejected code=%s"
                      % (t.task_id, err.code))
                errors.append(err)

        fns = [completer, canceller]
        rng.shuffle(fns)
        threads = [threading.Thread(target=f) for f in fns]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        final = service.get(t.task_id, requester=None)
        assert final.state in (TaskState.CANCELLED, TaskState.SUCCEEDED)
        assert len(errors) == 1
        assert errors[0].code == "invalid_state_transition"
        if final.state is TaskState.SUCCEEDED:
            assert final.result == {"ok": idx}
        else:
            assert final.result is None

    for i in range(20):
        run_once(i)


def test_transient_retry_with_backoff_then_success(service, clock):
    t, _ = service.submit("retry-1", {}, max_attempts=3)
    for expected_attempt in (1, 2, 3):
        claimed = service.claim("w")
        assert claimed is not None and claimed.attempt == expected_attempt
        if expected_attempt < 3:
            failed = service.fail(t.task_id, "w", "boom", permanent=False)
            assert failed.state is TaskState.PENDING
            # 退避期间不可领取
            clock.advance(0.5)
            assert service.claim("w") is None
            # 指数退避：1s, 2s
            clock.advance(2 ** (expected_attempt - 1) - 0.5)
            print("retry backoff elapsed, task re-claimable attempt->%d"
                  % (expected_attempt + 1))
    done = service.complete(t.task_id, "w", {"final": True})
    assert done.state is TaskState.SUCCEEDED and done.result == {"final": True}


def test_exhausting_attempts_fails_permanently(service, clock):
    t, _ = service.submit("retry-2", {}, max_attempts=2)
    for attempt in (1, 2):
        claimed = service.claim("w")
        assert claimed is not None and claimed.attempt == attempt
        failed = service.fail(t.task_id, "w", "err", permanent=False)
        print("attempt=%s -> %s error=%s"
              % (attempt, failed.state.value, failed.error))
        if attempt < 2:
            assert failed.state is TaskState.PENDING
            clock.advance(10)
        else:
            assert failed.state is TaskState.FAILED
    assert service.claim("w") is None  # 永久失败不再重试
    events = service.history(t.task_id)
    assert events[-1].reason == "permanent_failure"


def test_explicit_permanent_failure_no_retry(service):
    t, _ = service.submit("perm-1", {})
    claimed = service.claim("w")
    assert claimed is not None
    failed = service.fail(t.task_id, "w", "fatal", permanent=True)
    assert failed.state is TaskState.FAILED
    assert service.claim("w") is None
