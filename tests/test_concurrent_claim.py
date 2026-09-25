"""Multiple executors must never successfully claim the same task twice."""
from __future__ import annotations

import threading

from scheduler.models import SubmitRequest, TaskState


def test_only_one_worker_claims_each_task(service, clock):
    task_ids = [
        service.submit(SubmitRequest(task_type="echo", payload={"i": i}, owner="o")).task_id
        for i in range(20)
    ]

    claimed: list[tuple[str, str, int]] = []
    lock = threading.Lock()

    def worker(name: str) -> None:
        for _ in range(25):
            claim = service.claim(name)
            if claim is None:
                clock.advance(0.001)
                continue
            with lock:
                claimed.append((claim.task.id, name, claim.task.attempt))
            service.complete(claim.task.id, claim.token, {"by": name})

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ids = [c[0] for c in claimed]
    assert sorted(ids) == sorted(task_ids)
    assert len(ids) == len(set(ids)) == 20
    for tid in task_ids:
        task = service.get_task(tid, owner="o")
        assert task.state == TaskState.SUCCEEDED
        events = service.get_history(tid, owner="o")
        assert [e["event"] for e in events].count("CLAIMED") == 1


def test_two_rapid_claims_one_wins(service):
    tid = service.submit(SubmitRequest(task_type="echo", payload={}, owner="o")).task_id
    first = service.claim("w1")
    second = service.claim("w2")
    assert first is not None and second is None
    task = service.get_task(tid, owner="o")
    assert task.lease_owner == "w1"
    assert task.attempt == 1


def test_lease_token_required_to_complete(service):
    from scheduler.errors import NotLeaseHolder
    import pytest

    tid = service.submit(SubmitRequest(task_type="echo", payload={}, owner="o")).task_id
    claim = service.claim("w1")
    with pytest.raises(NotLeaseHolder):
        service.complete(tid, "forged-token", {"x": 1})
    task = service.get_task(tid, owner="o")
    assert task.state == TaskState.RUNNING
