"""Duplicate submission, idempotency-key semantics and typed rejections."""
from __future__ import annotations

import pytest

from scheduler.errors import IdempotencyKeyPayloadMismatch
from scheduler.models import SubmitRequest, TaskState


def test_submit_creates_pending_task(service):
    result = service.submit(
        SubmitRequest(task_type="echo", payload={"a": 1}, owner="alice")
    )
    assert result.created is True
    task = service.get_task(result.task_id, owner="alice")
    assert task.state == TaskState.PENDING
    assert task.attempt == 0
    assert task.max_attempts == 3
    events = service.get_history(result.task_id, owner="alice")
    assert [e["event"] for e in events] == ["SUBMITTED"]


def test_same_idempotency_key_is_deduplicated(service):
    body = dict(task_type="echo", payload={"a": 1}, idempotency_key="KEY-1", owner="alice")
    first = service.submit(SubmitRequest(**body))
    second = service.submit(SubmitRequest(**body))
    assert first.created is True
    assert second.created is False
    assert second.task_id == first.task_id
    # Only one task and one SUBMITTED event must exist.
    assert service.stats()["total"] == 1
    events = service.get_history(first.task_id, owner="alice")
    assert sum(e["event"] == "SUBMITTED" for e in events) == 1


def test_idempotency_key_with_different_payload_is_rejected(service):
    service.submit(
        SubmitRequest(task_type="echo", payload={"a": 1}, idempotency_key="KEY-2", owner="alice")
    )
    with pytest.raises(IdempotencyKeyPayloadMismatch) as exc:
        service.submit(
            SubmitRequest(task_type="echo", payload={"a": 2}, idempotency_key="KEY-2", owner="alice")
        )
    assert exc.value.code == "idempotency_key_payload_mismatch"
    assert exc.value.http_status == 409


def test_idempotency_key_with_different_type_is_rejected(service):
    service.submit(
        SubmitRequest(task_type="echo", payload={}, idempotency_key="KEY-3", owner="alice")
    )
    with pytest.raises(IdempotencyKeyPayloadMismatch):
        service.submit(
            SubmitRequest(task_type="other", payload={}, idempotency_key="KEY-3", owner="alice")
        )


def test_different_keys_create_different_tasks(service):
    a = service.submit(
        SubmitRequest(task_type="echo", payload={}, idempotency_key="A", owner="alice")
    )
    b = service.submit(
        SubmitRequest(task_type="echo", payload={}, idempotency_key="B", owner="alice")
    )
    assert a.task_id != b.task_id
    assert service.stats()["total"] == 2
