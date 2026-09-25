"""Lease expiry allows safe takeover; the old holder can no longer report."""
from __future__ import annotations

import pytest

from scheduler.errors import NotLeaseHolder
from scheduler.models import SubmitRequest, TaskState


def test_expired_lease_is_taken_over_and_attempt_increments(service, clock):
    tid = service.submit(
        SubmitRequest(task_type="echo", payload={}, owner="o", max_attempts=5)
    ).task_id
    first = service.claim("slow-worker")
    assert first.task.attempt == 1

    # Before expiry nobody else can take it.
    assert service.claim("fast-worker") is None

    clock.advance(service.settings.lease_seconds + 1)
    second = service.claim("fast-worker")
    assert second is not None
    assert second.task.id == tid
    assert second.task.lease_owner == "fast-worker"
    assert second.task.attempt == 2

    events = service.get_history(tid, owner="o")
    kinds = [e["event"] for e in events]
    assert kinds.count("CLAIMED") == 1
    assert kinds.count("TAKEN_OVER") == 1
    takeover = next(e for e in events if e["event"] == "TAKEN_OVER")
    assert takeover["detail"]["previous_owner"] == "slow-worker"


def test_old_lease_holder_completion_is_rejected_after_takeover(service, clock):
    tid = service.submit(SubmitRequest(task_type="echo", payload={}, owner="o")).task_id
    first = service.claim("slow-worker")
    clock.advance(service.settings.lease_seconds + 1)
    second = service.claim("fast-worker")
    assert second is not None

    with pytest.raises(NotLeaseHolder):
        service.complete(tid, first.token, {"stale": True})

    task = service.complete(tid, second.token, {"fresh": True})
    assert task.state == TaskState.SUCCEEDED
    assert task.result == {"fresh": True}


def test_takeover_can_repeat_until_max_attempts(service, clock):
    from scheduler.worker import TransientTaskError

    tid = service.submit(
        SubmitRequest(task_type="flaky", payload={}, owner="o", max_attempts=3)
    ).task_id
    claim = service.claim("w0")
    # Every holder dies right after claiming.
    for expected in range(2, 4):
        clock.advance(service.settings.lease_seconds + 1)
        claim = service.claim(f"w{expected}")
        assert claim is not None and claim.task.attempt == expected
    task = service.get_task(tid, owner="o")
    assert task.attempt == 3
    assert task.state == TaskState.RUNNING
