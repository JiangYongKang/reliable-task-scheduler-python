"""Cancellation rules and cancel-vs-complete/retry/takeover races."""
from __future__ import annotations

import pytest

from scheduler.errors import (
    AlreadyCancelled,
    CancelInProgress,
    TaskAlreadyCompleted,
    TaskNotFound,
)
from scheduler.models import SubmitRequest, TaskState
from scheduler.worker import TransientTaskError


def test_unknown_task_cancel_rejected(service):
    with pytest.raises(TaskNotFound):
        service.cancel("does-not-exist")


def test_cancel_pending_is_immediate(service):
    tid = service.submit(SubmitRequest(task_type="echo", payload={}, owner="o")).task_id
    task = service.cancel(tid)
    assert task.state == TaskState.CANCELLED
    assert task.cancel_requested is True
    events = [e["event"] for e in service.get_history(tid, owner="o")]
    assert events[-1] == "CANCELLED"


def test_repeat_cancel_rejected_typed(service):
    tid = service.submit(SubmitRequest(task_type="echo", payload={}, owner="o")).task_id
    service.cancel(tid)
    with pytest.raises(AlreadyCancelled) as exc:
        service.cancel(tid)
    assert exc.value.code == "already_cancelled"


def test_cancel_completed_task_rejected(service):
    tid = service.submit(SubmitRequest(task_type="echo", payload={}, owner="o")).task_id
    claim = service.claim("w")
    service.complete(tid, claim.token, {"r": 1})
    with pytest.raises(TaskAlreadyCompleted):
        service.cancel(tid)


def test_cancel_running_marks_request_then_completion_loses(service):
    tid = service.submit(SubmitRequest(task_type="slow", payload={}, owner="o")).task_id
    claim = service.claim("w")
    task = service.cancel(tid)
    # Still running until the holder observes it or the lease expires.
    assert task.state == TaskState.RUNNING
    assert task.cancel_requested is True

    # Repeated cancel while lease active is a typed, idempotent conflict.
    with pytest.raises(CancelInProgress):
        service.cancel(tid)

    # Holder nonetheless finishes: cancellation must win.
    task = service.complete(tid, claim.token, {"late": True})
    assert task.state == TaskState.CANCELLED
    assert task.result is None
    events = service.get_history(tid, owner="o")
    kinds = [e["event"] for e in events]
    assert "COMPLETION_REJECTED" in kinds
    assert kinds[-1] == "CANCELLED"


def test_cancel_running_then_retry_loses(service):
    tid = service.submit(
        SubmitRequest(task_type="flaky", payload={}, owner="o")
    ).task_id
    claim = service.claim("w")
    service.cancel(tid)
    task = service.fail(tid, claim.token, TransientTaskError("boom"))
    assert task.state == TaskState.CANCELLED
    events = [e["event"] for e in service.get_history(tid, owner="o")]
    assert "RETRY_SCHEDULED" not in events


def test_cancel_running_taken_over_after_lease_expiry(service, clock):
    tid = service.submit(
        SubmitRequest(task_type="slow", payload={}, owner="o", max_attempts=5)
    ).task_id
    first = service.claim("dead-worker")
    service.cancel(tid)
    clock.advance(service.settings.lease_seconds + 1)

    # A takeover attempt must finalize cancellation rather than execute again.
    claim = service.claim("other-worker")
    assert claim is None
    task = service.get_task(tid, owner="o")
    assert task.state == TaskState.CANCELLED
    assert task.attempt == 1  # no extra attempt after cancellation
    events = service.get_history(tid, owner="o")
    assert "TAKEN_OVER" not in [e["event"] for e in events]
    assert events[-1]["event"] == "CANCELLED"


def test_cancel_retry_wait_before_due(service, clock):
    tid = service.submit(
        SubmitRequest(task_type="flaky", payload={}, owner="o")
    ).task_id
    claim = service.claim("w")
    service.fail(tid, claim.token, TransientTaskError("temp"))
    task = service.get_task(tid, owner="o")
    assert task.state == TaskState.RETRY_WAIT
    task = service.cancel(tid)
    assert task.state == TaskState.CANCELLED
    clock.advance(1000)
    assert service.claim("w") is None
    assert service.get_task(tid, owner="o").state == TaskState.CANCELLED


def test_owner_cannot_cancel_other_owners_task(service):
    from scheduler.errors import Forbidden

    tid = service.submit(
        SubmitRequest(task_type="echo", payload={}, owner="alice")
    ).task_id
    with pytest.raises(Forbidden):
        service.cancel(tid, owner="bob")
    # Task untouched.
    assert service.get_task(tid, owner="alice").state == TaskState.PENDING


def test_maintenance_finalizes_cancelled_expired_lease_without_claim(service, clock):
    # RUNNING task with a cancel request and an expired lease.
    tid = service.submit(
        SubmitRequest(task_type="slow", payload={}, owner="o", max_attempts=5)
    ).task_id
    service.claim("dead-worker")
    service.cancel(tid)
    assert service.get_task(tid, owner="o").state == TaskState.RUNNING
    clock.advance(service.settings.lease_seconds + 1)
    service.maintenance()
    task = service.get_task(tid, owner="o")
    assert task.state == TaskState.CANCELLED
    assert task.attempt == 1


def test_maintenance_makes_due_retry_claimable(service, clock):
    tid = service.submit(
        SubmitRequest(task_type="flaky", payload={}, owner="o", max_attempts=4)
    ).task_id
    claim = service.claim("w")
    service.fail(tid, claim.token, TransientTaskError("temp"))
    assert service.claim("w") is None
    clock.advance(1.0)
    service.maintenance()
    claim = service.claim("w")
    assert claim is not None and claim.task.attempt == 2
