"""HTTP 层：请求/响应契约、错误分类状态码、既有接口不回归。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from scheduler.config import SchedulerConfig
from scheduler.service import SchedulerService


@pytest.fixture
def client(tmp_path):
    svc = SchedulerService(SchedulerConfig(
        db_path=str(tmp_path / "api.db"), lease_seconds=10.0))
    from scheduler.api import create_router
    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(create_router(svc))

    @app.get("/")
    def root():
        return {"Hello": "World"}

    with TestClient(app) as c:
        yield c
    svc.close()


def test_root_unchanged(client):
    assert client.get("/").json() == {"Hello": "World"}


def test_submit_duplicate_and_unknown_api(client):
    r1 = client.post("/tasks", json={"idempotency_key": "k1", "payload": {"a": 1}})
    assert r1.status_code == 200 and r1.json()["created"] is True
    r2 = client.post("/tasks", json={"idempotency_key": "k1", "payload": {"a": 2}})
    assert r2.status_code == 200 and r2.json()["created"] is False
    assert r2.json()["task"]["task_id"] == r1.json()["task"]["task_id"]
    print("api duplicate submit returns same task, created=false")

    r3 = client.get("/tasks/nope")
    assert r3.status_code == 404
    assert r3.json()["detail"]["code"] == "task_not_found"
    print("api reject unknown code=task_not_found http=404")

    r4 = client.post("/tasks", json={"idempotency_key": ""})
    assert r4.status_code == 422


def test_full_lifecycle_and_classification_api(client):
    tid = client.post("/tasks", json={"idempotency_key": "k2",
                                      "max_attempts": 1}).json()["task"]["task_id"]

    # 未领取先完成 -> 非法迁移
    r = client.post(f"/tasks/{tid}/complete", json={"owner": "x", "result": None})
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "invalid_state_transition"

    claim = client.post("/tasks/claim?worker_id=w1").json()["task"]
    assert claim["state"] == "RUNNING"

    # 其他 worker 完成 -> 租约冲突
    r = client.post(f"/tasks/{tid}/complete", json={"owner": "w2", "result": None})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "lease_conflict"
    print("api reject foreign owner code=lease_conflict http=409")

    r = client.post(f"/tasks/{tid}/fail",
                    json={"owner": "w1", "error": "boom", "permanent": False})
    # max_attempts=1 -> 直接 FAILED
    assert r.status_code == 200 and r.json()["task"]["state"] == "FAILED"

    # 终态取消 -> 409
    r = client.post(f"/tasks/{tid}/cancel", json={})
    assert r.status_code == 409
    print("api reject cancel terminal code=invalid_state_transition http=409")

    stats = client.get("/tasks").json()["stats"]
    assert stats["FAILED"] == 1 and stats["claimable"] == 0
    print("api stats=%s" % stats)

    history = client.get(f"/tasks/{tid}/history").json()["events"]
    assert [e["reason"] for e in history] == [
        "submitted", "claimed", "permanent_failure"]


def test_redaction_and_claim_empty_api(client):
    assert client.post("/tasks/claim?worker_id=w").json() == {"task": None}
    tid = client.post("/tasks", json={"idempotency_key": "sec",
                                      "payload": {"p": 1}}).json()["task"]["task_id"]
    client.post("/tasks/claim?worker_id=owner-a")
    other = client.get(f"/tasks/{tid}?requester=owner-b").json()["task"]
    assert other["payload"] == {} and other["result"] is None
    print("api redacted payload for unrelated requester")


def test_renew_endpoint_extends_lease_and_rejects_stale_owner(client):
    client.post("/tasks", json={"idempotency_key": "rn1", "payload": {}})
    claimed = client.post("/tasks/claim", params={"worker_id": "w1"}).json()["task"]
    tid = claimed["task_id"]
    old_expiry = claimed["lease_expires_at"]
    print("input POST /tasks/%s/renew owner=w1" % tid)

    r = client.post(f"/tasks/{tid}/renew", json={"owner": "w1"})
    assert r.status_code == 200
    new_expiry = r.json()["task"]["lease_expires_at"]
    print("renew ok lease_expires_at %s -> %s" % (old_expiry, new_expiry))
    assert new_expiry >= old_expiry

    # 非持有者续约被拒：409 lease_conflict
    r2 = client.post(f"/tasks/{tid}/renew", json={"owner": "w2"})
    assert r2.status_code == 409
    assert r2.json()["detail"]["reason"] == "lease_conflict"
    print("reject renew by w2 reason=%s" % r2.json()["detail"]["reason"])

    # 已取消任务续约被拒：409 invalid_state_transition
    client.post(f"/tasks/{tid}/cancel")
    r3 = client.post(f"/tasks/{tid}/renew", json={"owner": "w1"})
    assert r3.status_code == 409
    assert r3.json()["detail"]["reason"] == "invalid_state_transition"
    print("reject renew after cancel reason=%s" % r3.json()["detail"]["reason"])
