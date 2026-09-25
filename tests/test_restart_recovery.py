"""Tasks and history survive process restart (reopening the same database)."""
from __future__ import annotations

from scheduler.config import Settings
from scheduler.models import SubmitRequest, TaskState
from scheduler.service import SchedulerService
from scheduler.store import TaskStore
from scheduler.worker import TransientTaskError


def _reopen(settings, clock) -> SchedulerService:
    store = TaskStore(settings.db_path)
    return SchedulerService(settings, store, clock=clock)


def test_pending_task_claimable_after_restart(service, settings, store, clock):
    tid = service.submit(
        SubmitRequest(task_type="echo", payload={"v": 1}, owner="o")
    ).task_id
    store.close()

    restarted = _reopen(settings, clock)
    task = restarted.get_task(tid, owner="o")
    assert task.state == TaskState.PENDING
    claim = restarted.claim("new-worker")
    assert claim is not None and claim.task.id == tid
    done = restarted.complete(tid, claim.token, {"ok": True})
    assert done.state == TaskState.SUCCEEDED
    assert done.result == {"ok": True}
    assert done.payload == {"v": 1}
    restarted.close()


def test_history_survives_restart(service, settings, store, clock):
    tid = service.submit(
        SubmitRequest(task_type="echo", payload={}, owner="o")
    ).task_id
    claim = service.claim("w")
    service.complete(tid, claim.token, {"r": 1})
    store.close()

    restarted = _reopen(settings, clock)
    kinds = [e["event"] for e in restarted.get_history(tid, owner="o")]
    assert kinds == ["SUBMITTED", "CLAIMED", "COMPLETED"]
    completed = next(e for e in restarted.get_history(tid, owner="o") if e["event"] == "COMPLETED")
    assert completed["attempt"] == 1
    restarted.close()


def test_retry_wait_survives_restart_and_becomes_due(service, settings, store, clock):
    tid = service.submit(
        SubmitRequest(task_type="flaky", payload={}, owner="o", max_attempts=4)
    ).task_id
    claim = service.claim("w")
    service.fail(tid, claim.token, TransientTaskError("temp"))
    task = service.get_task(tid, owner="o")
    assert task.state == TaskState.RETRY_WAIT
    due_at = task.next_run_at
    store.close()

    # Restart after the scheduled time.
    clock.advance(10)
    assert clock() >= due_at
    restarted = _reopen(settings, clock)
    claim = restarted.claim("w")
    assert claim is not None
    assert claim.task.id == tid
    assert claim.task.attempt == 2
    restarted.close()


def test_running_lease_from_dead_process_is_reclaimed_after_restart(
    service, settings, store, clock
):
    tid = service.submit(
        SubmitRequest(task_type="echo", payload={}, owner="o", max_attempts=5)
    ).task_id
    claim = service.claim("dead-process")
    assert claim.task.attempt == 1
    assert claim.task.lease_owner == "dead-process"
    store.close()  # process dies without completing or heartbeating

    clock.advance(service.settings.lease_seconds + 1)
    restarted = _reopen(settings, clock)
    new_claim = restarted.claim("recovered-worker")
    assert new_claim is not None
    assert new_claim.task.id == tid
    assert new_claim.task.attempt == 2
    assert new_claim.task.lease_owner == "recovered-worker"

    # The dead process's token must not be able to complete anything.
    from scheduler.errors import NotLeaseHolder
    import pytest

    with pytest.raises(NotLeaseHolder):
        restarted.complete(tid, claim.token, {"zombie": True})

    done = restarted.complete(tid, new_claim.token, {"recovered": True})
    assert done.state == TaskState.SUCCEEDED
    restarted.close()


def test_cancel_request_survives_restart_and_is_finalized(service, settings, store, clock):
    tid = service.submit(
        SubmitRequest(task_type="slow", payload={}, owner="o", max_attempts=5)
    ).task_id
    claim = service.claim("dead-process")
    service.cancel(tid)
    store.close()

    clock.advance(service.settings.lease_seconds + 1)
    restarted = _reopen(settings, clock)
    claim2 = restarted.claim("any-worker")
    assert claim2 is None
    task = restarted.get_task(tid, owner="o")
    assert task.state == TaskState.CANCELLED
    restarted.close()


def test_stats_available_after_restart(service, settings, store, clock):
    for i in range(3):
        service.submit(SubmitRequest(task_type="echo", payload={"i": i}, owner="o"))
    claim = service.claim("w")
    service.complete(claim.task.id, claim.token, None)
    store.close()

    restarted = _reopen(settings, clock)
    stats = restarted.stats()
    assert stats["total"] == 3
    assert stats["backlog"] == 2
    assert stats["by_state"]["SUCCEEDED"] == 1
    restarted.close()
