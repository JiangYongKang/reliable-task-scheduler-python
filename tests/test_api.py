"""HTTP behaviour: status codes, error classification, isolation and legacy."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from scheduler.api import create_app
from scheduler.models import TaskState


@pytest.fixture()
def client(service, settings):
    return TestClient(create_app(service))


def test_root_preserves_legacy_contract(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json() == {"Hello": "World"}


def test_submit_and_dedup_over_http(client):
    body = {"task_type": "echo", "payload": {"x": 1}, "idempotency_key": "K", "owner": "a"}
    r1 = client.post("/tasks", json=body)
    assert r1.status_code == 201
    data1 = r1.json()
    assert data1["created"] is True
    r2 = client.post("/tasks", json=body)
    assert r2.status_code == 201
    assert r2.json()["created"] is False
    assert r2.json()["task_id"] == data1["task_id"]


def test_submit_conflict_classification(client):
    client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"x": 1}, "idempotency_key": "K", "owner": "a"},
    )
    r = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {"x": 2}, "idempotency_key": "K", "owner": "a"},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "idempotency_key_payload_mismatch"


def test_unknown_task_is_404_typed(client):
    r = client.get("/tasks/nope", params={"owner": "a"})
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "task_not_found"


def test_owner_isolation_via_query_and_header(client):
    r = client.post(
        "/tasks",
        json={"task_type": "echo", "payload": {}, "idempotency_key": None, "owner": "alice"},
    )
    tid = r.json()["task_id"]
    assert client.get(f"/tasks/{tid}", params={"owner": "bob"}).status_code == 403
    assert client.get(f"/tasks/{tid}", headers={"X-Owner": "bob"}).status_code == 403
    ok = client.get(f"/tasks/{tid}", headers={"X-Owner": "alice"})
    assert ok.status_code == 200
    assert ok.json()["state"] == TaskState.PENDING.value
    # Internal lease token must never appear in API responses.
    assert "lease_token" not in ok.json()


def test_cancel_flow_and_repeat_cancel(client):
    tid = client.post(
        "/tasks", json={"task_type": "echo", "payload": {}, "owner": "a"}
    ).json()["task_id"]
    r = client.post(f"/tasks/{tid}/cancel", params={"owner": "a"})
    assert r.status_code == 200 and r.json()["state"] == "CANCELLED"
    r2 = client.post(f"/tasks/{tid}/cancel", params={"owner": "a"})
    assert r2.status_code == 409
    assert r2.json()["detail"]["code"] == "already_cancelled"


def test_cancel_completed_is_409(client, service):
    tid = client.post(
        "/tasks", json={"task_type": "echo", "payload": {}, "owner": "a"}
    ).json()["task_id"]
    claim = service.claim("w")
    service.complete(tid, claim.token, {"r": 1})
    r = client.post(f"/tasks/{tid}/cancel", params={"owner": "a"})
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "task_already_completed"


def test_management_endpoints_require_token(client, settings, service):
    from scheduler.models import SubmitRequest

    service.submit(SubmitRequest(task_type="echo", payload={}, owner="a"))
    assert client.get("/internal/stats").status_code == 401
    r = client.get("/internal/stats", headers={"X-Management-Token": settings.management_token})
    assert r.status_code == 200
    assert r.json()["total"] >= 1
    assert r.json()["backlog"] == 1
    maint = client.post(
        "/internal/maintenance",
        headers={"X-Management-Token": settings.management_token},
    )
    assert maint.status_code == 200 and maint.json() == {"ok": True}


def test_history_endpoint_is_owner_scoped(client):
    tid = client.post(
        "/tasks", json={"task_type": "echo", "payload": {}, "owner": "a"}
    ).json()["task_id"]
    assert client.get(f"/tasks/{tid}/history", params={"owner": "x"}).status_code == 403
    r = client.get(f"/tasks/{tid}/history", params={"owner": "a"})
    assert r.status_code == 200
    assert r.json()["events"][0]["event"] == "SUBMITTED"
