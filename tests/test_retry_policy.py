"""Finite backoff retry; permanent failures must never be retried."""
from __future__ import annotations

from scheduler.models import SubmitRequest, TaskState
from scheduler.worker import (
    PermanentTaskError,
    TransientTaskError,
)


def _claim_and_fail(service, tid, error):
    claim = service.claim("w")
    assert claim is not None and claim.task.id == tid
    return service.fail(tid, claim.token, error), claim


def test_transient_failures_retry_with_capped_backoff(service, clock):
    tid = service.submit(
        SubmitRequest(task_type="flaky", payload={}, owner="o")
    ).task_id
    expected_delays = [1.0, 2.0]
    for i in range(2):
        task, _ = _claim_and_fail(service, tid, TransientTaskError("temp"))
        assert task.state == TaskState.RETRY_WAIT
        due_in = task.next_run_at - clock()
        assert abs(due_in - expected_delays[i]) < 1e-6
        # Not claimable before the backoff expires.
        clock.advance(expected_delays[i] - 0.01)
        assert service.claim("w") is None
        clock.advance(0.011)
    # Third transient failure exhausts max_attempts=3.
    task, _ = _claim_and_fail(service, tid, TransientTaskError("temp again"))
    assert task.state == TaskState.PERMANENTLY_FAILED
    assert "max_attempts_exhausted" in task.last_error_type
    assert service.claim("w") is None


def test_permanent_failure_never_retried(service):
    tid = service.submit(
        SubmitRequest(task_type="p", payload={}, owner="o", max_attempts=10)
    ).task_id
    task, _ = _claim_and_fail(service, tid, PermanentTaskError("bad config"))
    assert task.state == TaskState.PERMANENTLY_FAILED
    assert task.attempt == 1
    assert task.last_error_type == "PermanentTaskError"
    assert service.claim("w") is None


def test_unknown_exception_treated_as_transient_then_exhausts(service, clock):
    tid = service.submit(
        SubmitRequest(task_type="x", payload={}, owner="o", max_attempts=2)
    ).task_id
    task, _ = _claim_and_fail(service, tid, ValueError("unexpected"))
    assert task.state == TaskState.RETRY_WAIT
    clock.advance(1.0)
    task, _ = _claim_and_fail(service, tid, ValueError("unexpected again"))
    assert task.state == TaskState.PERMANENTLY_FAILED
    assert task.last_error_type == "ValueError(max_attempts_exhausted)"


def test_history_records_retry_chain(service, clock):
    tid = service.submit(
        SubmitRequest(task_type="flaky", payload={}, owner="o", max_attempts=2)
    ).task_id
    claim = service.claim("w")
    service.fail(tid, claim.token, TransientTaskError("temp"))
    clock.advance(1.0)
    claim = service.claim("w")
    service.complete(tid, claim.token, {"done": True})
    kinds = [e["event"] for e in service.get_history(tid, owner="o")]
    assert kinds == [
        "SUBMITTED",
        "CLAIMED",
        "RETRY_SCHEDULED",
        "RETRY_AVAILABLE",
        "CLAIMED",
        "COMPLETED",
    ]
