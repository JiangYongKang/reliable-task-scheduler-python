"""SchedulerService: orchestrates store operations, logging and validation."""
from __future__ import annotations

import json
import logging
import secrets
import time
import uuid
from typing import Any, Callable

from .config import Settings
from .errors import (
    AlreadyCancelled,
    CancelInProgress,
    Forbidden,
    IdempotencyKeyPayloadMismatch,
    InvalidStateTransition,
    NotLeaseHolder,
    TaskAlreadyCompleted,
    TaskNotFound,
)
from .models import Claim, SubmitRequest, SubmitResult, Task, TaskEvent, TaskState
from .store import TaskStore

logger = logging.getLogger("scheduler")

# Backoff cap for attempts even beyond max_attempts is irrelevant, but keep a
# hard floor to avoid silly values.


class SchedulerService:
    def __init__(
        self,
        settings: Settings,
        store: TaskStore,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.store = store
        self._clock = clock

    def _wall_now(self) -> float:
        """Current time used for persistence and lease comparisons.

        Defaults to wall-clock epoch time; tests inject a controllable clock.
        """
        return self._clock()

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    @staticmethod
    def _fingerprint(req: SubmitRequest) -> str:
        return json.dumps(
            {"task_type": req.task_type, "payload": req.payload, "owner": req.owner},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        )

    def backoff_delay(self, attempt: int) -> float:
        """Exponential backoff with jitter-free deterministic cap."""
        s = self.settings
        delay = s.base_backoff_seconds * (2 ** max(0, attempt - 1))
        return min(delay, s.max_backoff_seconds)

    def _check_owner(self, task: Task, owner: str | None) -> None:
        if owner is not None and task.owner != owner:
            # Do not leak whether the task exists for another owner.
            raise Forbidden(
                f"task '{task.id}' is not accessible by owner '{owner}'",
                task_id=task.id,
            )

    def _load_owned_or_404(self, task_id: str, owner: str | None) -> Task:
        task = self.store.get_task(task_id)
        if task is None:
            raise TaskNotFound(f"task '{task_id}' does not exist", task_id=task_id)
        self._check_owner(task, owner)
        return task

    # ------------------------------------------------------------------ #
    # Submission
    # ------------------------------------------------------------------ #
    def submit(self, req: SubmitRequest) -> SubmitResult:
        logger.info(
            "submit request: task_type=%s owner=%s idempotency_key=%s payload=%s",
            req.task_type,
            req.owner,
            req.idempotency_key,
            req.payload,
        )
        fingerprint = self._fingerprint(req)
        max_attempts = (
            req.max_attempts
            if req.max_attempts is not None
            else self.settings.max_attempts
        )

        if req.idempotency_key:
            existing = self.store.get_task_by_idempotency_key(req.idempotency_key)
            if existing is not None:
                same = self._fingerprint(
                    SubmitRequest(
                        task_type=existing.task_type,
                        payload=existing.payload,
                        idempotency_key=existing.idempotency_key,
                        owner=existing.owner,
                    )
                )
                if same != fingerprint:
                    logger.info(
                        "submit rejected: idempotency_key=%s reused with a "
                        "different payload (existing task %s)",
                        req.idempotency_key,
                        existing.id,
                    )
                    raise IdempotencyKeyPayloadMismatch(
                        "idempotency key was already used with different parameters",
                        task_id=existing.id,
                    )
                logger.info(
                    "submit deduplicated: idempotency_key=%s -> existing task %s "
                    "(state=%s)",
                    req.idempotency_key,
                    existing.id,
                    existing.state.value,
                )
                return SubmitResult(task_id=existing.id, created=False)

        now = self._wall_now()
        task = Task(
            id=uuid.uuid4().hex,
            idempotency_key=req.idempotency_key,
            owner=req.owner,
            task_type=req.task_type,
            payload=req.payload,
            state=TaskState.PENDING,
            max_attempts=max_attempts,
            created_at=now,
            updated_at=now,
            next_run_at=now,
        )
        if not self.store.insert_task(task):
            # Race: another process inserted the same idempotency key.
            existing = self.store.get_task_by_idempotency_key(req.idempotency_key)
            if existing is not None:
                same = self._fingerprint(
                    SubmitRequest(
                        task_type=existing.task_type,
                        payload=existing.payload,
                        idempotency_key=existing.idempotency_key,
                        owner=existing.owner,
                    )
                )
                if same != fingerprint:
                    raise IdempotencyKeyPayloadMismatch(
                        "idempotency key was already used with different parameters",
                        task_id=existing.id,
                    )
                logger.info(
                    "submit deduplicated after collision: key=%s -> %s",
                    req.idempotency_key,
                    existing.id,
                )
                return SubmitResult(task_id=existing.id, created=False)
        self.store.append_event(
            TaskEvent(
                task_id=task.id,
                event_type="SUBMITTED",
                at=now,
                attempt=0,
                detail={"max_attempts": max_attempts, "payload": req.payload},
            )
        )
        logger.info(
            "task %s: created PENDING (task_type=%s, max_attempts=%s)",
            task.id,
            task.task_type,
            max_attempts,
        )
        return SubmitResult(task_id=task.id, created=True)

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #
    def get_task(self, task_id: str, owner: str | None = None) -> Task:
        task = self._load_owned_or_404(task_id, owner)
        logger.info(
            "get task %s by owner=%s -> state=%s attempt=%s cancel_requested=%s",
            task_id,
            owner,
            task.state.value,
            task.attempt,
            task.cancel_requested,
        )
        return task

    def get_history(
        self, task_id: str, owner: str | None = None
    ) -> list[dict[str, Any]]:
        task = self._load_owned_or_404(task_id, owner)
        events = self.store.list_events(task.id)
        logger.info(
            "get history task=%s by owner=%s -> %d events",
            task_id,
            owner,
            len(events),
        )
        return [
            {
                "seq": e.id,
                "event": e.event_type,
                "at": e.at,
                "attempt": e.attempt,
                "lease_owner": e.lease_owner,
                "detail": e.detail,
            }
            for e in events
        ]

    # ------------------------------------------------------------------ #
    # Cancellation
    # ------------------------------------------------------------------ #
    def cancel(self, task_id: str, owner: str | None = None) -> Task:
        task = self._load_owned_or_404(task_id, owner)
        logger.info(
            "cancel request: task=%s by owner=%s current_state=%s "
            "cancel_requested=%s lease_owner=%s",
            task_id,
            owner,
            task.state.value,
            task.cancel_requested,
            task.lease_owner,
        )
        status, _effective = self.store.request_cancel(task_id, self._wall_now())
        if status == "ok":
            result = self.store.get_task(task_id)
            logger.info(
                "cancel accepted: task=%s -> state=%s",
                task_id,
                result.state.value if result else "?",
            )
            return result  # type: ignore[return-value]
        reason = {
            "not_found": None,  # handled above
            "already_cancelled": AlreadyCancelled(
                f"task '{task_id}' is already CANCELLED", task_id=task_id
            ),
            "cancel_in_progress": CancelInProgress(
                f"cancel for task '{task_id}' is already in progress, "
                "waiting for lease expiry",
                task_id=task_id,
            ),
        }.get(status)
        if reason is not None:
            logger.info("cancel rejected task=%s reason=%s", task_id, reason.code)
            raise reason
        if status.startswith("terminal:"):
            state = status.split(":", 1)[1]
            if state == TaskState.SUCCEEDED.value:
                err = TaskAlreadyCompleted(
                    f"task '{task_id}' already SUCCEEDED and cannot be cancelled",
                    task_id=task_id,
                )
            else:
                err = InvalidStateTransition(
                    f"task '{task_id}' is PERMANENTLY_FAILED and cannot be cancelled",
                    task_id=task_id,
                )
            logger.info("cancel rejected task=%s reason=%s", task_id, err.code)
            raise err
        raise InvalidStateTransition(status, task_id=task_id)

    # ------------------------------------------------------------------ #
    # Worker-facing lease operations
    # ------------------------------------------------------------------ #
    def claim(self, worker_id: str) -> Claim | None:
        now = self._wall_now()
        token = secrets.token_hex(24)
        task = self.store.claim_next(
            worker_id, now=now, token=token, expires_at=now + self.settings.lease_seconds
        )
        if task is None:
            return None
        logger.info(
            "lease granted: task=%s worker=%s attempt=%s expires_in=%ss",
            task.id,
            worker_id,
            task.attempt,
            self.settings.lease_seconds,
        )
        return Claim(task=task, token=token)

    def heartbeat(self, task_id: str, token: str) -> bool:
        ok = self.store.heartbeat(
            task_id, token, self._wall_now() + self.settings.lease_seconds
        )
        if not ok:
            logger.info(
                "heartbeat rejected: task=%s is no longer held by this token "
                "(lease lost / task terminal)",
                task_id,
            )
        return ok

    def complete(self, task_id: str, token: str, result: Any) -> Task:
        status = self.store.complete(task_id, token, self._wall_now(), result)
        return self._resolve_terminal_status(task_id, status, action="complete")

    def fail(self, task_id: str, token: str, error: Exception) -> Task:
        now = self._wall_now()
        task = self.store.get_task(task_id)
        if task is None:
            raise TaskNotFound(task_id=task_id)
        from .worker import PermanentTaskError, TransientTaskError

        error_type = type(error).__name__
        message = str(error)[:2000]

        from .worker import PermanentTaskError, TransientTaskError

        # Retry policy:
        #   * PermanentTaskError  -> stop immediately, never retried.
        #   * TransientTaskError  -> retry until max_attempts, then stop.
        #   * any other exception -> treated as transient until attempts are
        #     exhausted, then recorded as permanent failure.
        attempts_exhausted = task.attempt >= task.max_attempts
        if isinstance(error, PermanentTaskError):
            permanent = True
        elif isinstance(error, TransientTaskError):
            permanent = attempts_exhausted
        else:
            permanent = attempts_exhausted

        if permanent:
            if not isinstance(error, PermanentTaskError) and attempts_exhausted:
                error_type = f"{error_type}(max_attempts_exhausted)"
            logger.info(
                "task %s attempt %s permanent failure: %s: %s",
                task_id,
                task.attempt,
                error_type,
                message,
            )
            status = self.store.permanently_fail(
                task_id, token, now, error_type, message
            )
        else:
            delay = self.backoff_delay(task.attempt)
            next_run_at = now + delay
            logger.info(
                "task %s attempt %s transient failure (%s): retry scheduled "
                "in %.2fs (attempt %s/%s)",
                task_id,
                task.attempt,
                error_type,
                delay,
                task.attempt + 1,
                task.max_attempts,
            )
            status = self.store.schedule_retry(
                task_id, token, now, next_run_at, error_type, message
            )
        return self._resolve_terminal_status(task_id, status, action="fail")

    def _resolve_terminal_status(
        self, task_id: str, status: str, *, action: str
    ) -> Task:
        if status == "ok":
            task = self.store.get_task(task_id)
            logger.info(
                "%s accepted: task=%s state=%s",
                action,
                task_id,
                task.state.value if task else "?",
            )
            return task  # type: ignore[return-value]
        if status == "not_found":
            raise TaskNotFound(task_id=task_id)
        if status == "cancelled":
            task = self.store.get_task(task_id)
            logger.info(
                "%s rejected: task=%s state=CANCELLED (cancel wins race)",
                action,
                task_id,
            )
            return task  # type: ignore[return-value]
        if status == "already_completed":
            raise TaskAlreadyCompleted(
                f"task '{task_id}' already SUCCEEDED", task_id=task_id
            )
        if status in ("invalid_token", "permanently_failed", "terminal"):
            logger.info("%s rejected: task=%s reason=%s", action, task_id, status)
            raise NotLeaseHolder(
                f"{action} rejected for task '{task_id}': {status}", task_id=task_id
            )
        raise InvalidStateTransition(status, task_id=task_id)

    # ------------------------------------------------------------------ #
    # Maintenance
    # ------------------------------------------------------------------ #
    def maintenance(self) -> None:
        now = self._wall_now()
        self.store.make_retry_due(now)
        cancelled = self.store.cancel_due_tasks(now)
        if cancelled:
            logger.info("maintenance cancelled %s task(s): %s", len(cancelled), cancelled)

    def stats(self) -> dict[str, Any]:
        data = self.store.stats()
        logger.info("stats requested -> %s", data)
        return data

    def close(self) -> None:
        self.store.close()


# late import to avoid a cycle in type-only position
def build_service(
    settings: Settings | None = None,
    store: TaskStore | None = None,
    *,
    clock: Callable[[], float] = time.time,
) -> SchedulerService:
    from .config import load_settings

    settings = settings or load_settings()
    store = store or TaskStore(settings.db_path)
    return SchedulerService(settings, store, clock=clock)
