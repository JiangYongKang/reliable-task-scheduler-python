"""Typed error hierarchy for the scheduler."""
from __future__ import annotations


class SchedulerError(Exception):
    """Base class for all scheduler errors."""

    code: str = "scheduler_error"
    http_status: int = 400

    def __init__(self, message: str = "", *, task_id: str | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.task_id = task_id


class TaskNotFound(SchedulerError):
    code = "task_not_found"
    http_status = 404


class InvalidStateTransition(SchedulerError):
    code = "invalid_state_transition"
    http_status = 409


class AlreadyCancelled(SchedulerError):
    code = "already_cancelled"
    http_status = 409


class CancelInProgress(SchedulerError):
    """A cancel request already exists but the lease has not expired yet."""

    code = "cancel_in_progress"
    http_status = 409


class TaskAlreadyCompleted(SchedulerError):
    code = "task_already_completed"
    http_status = 409


class NotLeaseHolder(SchedulerError):
    code = "not_lease_holder"
    http_status = 409


class IdempotencyKeyPayloadMismatch(SchedulerError):
    code = "idempotency_key_payload_mismatch"
    http_status = 409


class Unauthorized(SchedulerError):
    code = "unauthorized"
    http_status = 401


class Forbidden(SchedulerError):
    code = "forbidden"
    http_status = 403
