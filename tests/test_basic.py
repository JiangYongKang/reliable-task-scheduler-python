"""基础流程：重复提交幂等、非法操作拒绝与错误分类、查询脱敏。"""
from __future__ import annotations

import pytest

from scheduler.errors import (
    InvalidStateTransitionError,
    LeaseConflictError,
    TaskNotFoundError,
)
from scheduler.models import TaskState


def test_duplicate_submit_is_idempotent(service):
    print("input submit key=k1 payload={'v': 1}")
    t1, created1 = service.submit("k1", {"v": 1})
    print("transition -> %s created=%s" % (t1.state.value, created1))
    t2, created2 = service.submit("k1", {"v": 999})
    print("duplicate submit -> %s created=%s" % (t2.state.value, created2))

    assert created1 is True
    assert created2 is False
    assert t1.task_id == t2.task_id
    assert t2.payload == {"v": 1}  # 重复提交不覆盖

    # 只有一个任务，只能被领取并执行一次
    claimed = service.claim("w1")
    assert claimed is not None and claimed.task_id == t1.task_id
    assert service.claim("w1") is None
    done = service.complete(t1.task_id, "w1", {"ok": True})
    assert done.state is TaskState.SUCCEEDED and done.result == {"ok": True}


def test_unknown_and_illegal_transitions_are_classified(service):
    t, _ = service.submit("k2", {})
    with pytest.raises(TaskNotFoundError) as ei:
        service.cancel("does-not-exist")
    print("reject cancel unknown code=%s" % ei.value.code)
    assert ei.value.code == "task_not_found"

    service.cancel(t.task_id)
    with pytest.raises(InvalidStateTransitionError) as ei:
        service.cancel(t.task_id)  # 重复取消
    print("reject double-cancel code=%s current=CANCELLED" % ei.value.code)
    assert ei.value.code == "invalid_state_transition"

    # 终态完成/失败均被拒绝
    with pytest.raises(InvalidStateTransitionError):
        service.complete(t.task_id, "w1", None)


def test_owner_scoped_query_redaction(service):
    t, _ = service.submit("k3", {"secret": "abc"})
    claimed = service.claim("owner-a")
    assert claimed is not None
    other = service.get(t.task_id, requester="owner-b")
    print("requester=owner-b sees payload=%r result=%r" % (other.payload, other.result))
    assert other.payload == {} and other.result is None
    mine = service.get(t.task_id, requester="owner-a")
    assert mine.payload == {"secret": "abc"}


def test_stale_owner_complete_rejected_after_takeover(service, clock):
    t, _ = service.submit("k4", {})
    first = service.claim("w1")
    assert first is not None
    clock.advance(11)  # 超过 10s 租约
    second = service.claim("w2")
    assert second is not None and second.owner == "w2" and second.attempt == 2
    print("takeover task=%s w1->w2 attempt=2" % t.task_id)
    with pytest.raises(LeaseConflictError) as ei:
        service.complete(t.task_id, "w1", {"late": True})
    print("reject stale owner code=%s reason=owner mismatch" % ei.value.code)
    assert ei.value.code == "lease_conflict"
    done = service.complete(t.task_id, "w2", {"ok": True})
    assert done.state is TaskState.SUCCEEDED and done.result == {"ok": True}
