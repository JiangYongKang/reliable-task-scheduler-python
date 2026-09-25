"""Domain models: task states, task records, events, requests/results."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskState(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    PERMANENTLY_FAILED = "PERMANENTLY_FAILED"
    CANCELLED = "CANCELLED"


# States from which a cancel request still wins against a lease holder.
CANCELLABLE_STATES = frozenset(
    {TaskState.PENDING, TaskState.RUNNING, TaskState.RETRY_WAIT}
)
TERMINAL_STATES = frozenset(
    {TaskState.SUCCEEDED, TaskState.PERMANENTLY_FAILED, TaskState.CANCELLED}
)


class EventType(str, enum.Enum):
    SUBMITTED = "SUBMITTED"
    CLAIMED = "CLAIMED"
    HEARTBEAT = "HEARTBEAT"
    COMPLETED = "COMPLETED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    RETRY_AVAILABLE = "RETRY_AVAILABLE"
    TAKEN_OVER = "TAKEN_OVER"
    PERMANENT_FAILURE = "PERMANENT_FAILURE"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELLED = "CANCELLED"
    COMPLETION_REJECTED = "COMPLETION_REJECTED"


@dataclass
class Task:
    id: str
    idempotency_key: str | None
    owner: str
    task_type: str
    payload: dict[str, Any]
    state: TaskState
    attempt: int = 0
    max_attempts: int = 5
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at: float | None = None
    cancel_requested: bool = False
    created_at: float = 0.0
    updated_at: float = 0.0
    next_run_at: float = 0.0
    result: Any = None
    last_error_type: str | None = None
    last_error_message: str | None = None

    def to_public_dict(self) -> dict[str, Any]:
        """API-facing view. Internal lease token is never exposed."""
        return {
            "id": self.id,
            "idempotency_key": self.idempotency_key,
            "owner": self.owner,
            "task_type": self.task_type,
            "payload": self.payload,
            "state": self.state.value,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "lease_owner": self.lease_owner,
            "lease_expires_at": self.lease_expires_at,
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "next_run_at": self.next_run_at if self.state == TaskState.RETRY_WAIT else None,
            "result": self.result,
            "last_error_type": self.last_error_type,
            "last_error_message": self.last_error_message,
        }


@dataclass
class TaskEvent:
    task_id: str
    event_type: str
    at: float
    id: int | None = None
    attempt: int | None = None
    lease_owner: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass
class SubmitRequest:
    task_type: str
    payload: dict[str, Any]
    idempotency_key: str | None = None
    owner: str = "default"
    max_attempts: int | None = None


@dataclass
class SubmitResult:
    task_id: str
    created: bool


@dataclass
class Claim:
    task: Task
    token: str
