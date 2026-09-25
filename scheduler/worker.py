"""Background worker pool that claims, executes, heartbeats and reports tasks."""
from __future__ import annotations

import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from .models import TaskState
from .service import SchedulerService

logger = logging.getLogger("scheduler.worker")

# A handler maps task_type -> callable(payload, context) -> result.
TaskHandler = Callable[[dict[str, Any], "ExecutionContext"], Any]


class TransientTaskError(Exception):
    """Raised by handlers for failures that should be retried with backoff."""


class PermanentTaskError(Exception):
    """Raised by handlers for failures that must not be retried."""


class CancellationRequested(Exception):
    """Raised internally when a cancel request is observed during execution."""


class UnknownTaskType(PermanentTaskError):
    """No handler is registered for the task type."""


class ExecutionContext:
    def __init__(
        self,
        task_id: str,
        attempt: int,
        is_cancelled: Callable[[], bool],
        sleep: Callable[[float], None],
    ) -> None:
        self.task_id = task_id
        self.attempt = attempt
        self._is_cancelled = is_cancelled
        self._sleep = sleep

    def cancelled(self) -> bool:
        return self._is_cancelled()

    def raise_if_cancelled(self) -> None:
        if self._is_cancelled():
            raise CancellationRequested("cancel requested during execution")

    def sleep(self, seconds: float) -> None:
        """Cancellable sleep: raises CancellationRequested if requested."""
        self._sleep(seconds)
        self.raise_if_cancelled()


class WorkerPool:
    def __init__(
        self,
        service: SchedulerService,
        handlers: dict[str, TaskHandler],
        concurrency: int | None = None,
        worker_id: str | None = None,
    ) -> None:
        self.service = service
        self.handlers = dict(handlers)
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.concurrency = (
            concurrency if concurrency is not None else service.settings.worker_concurrency
        )
        self._stop = threading.Event()
        self._executor: ThreadPoolExecutor | None = None
        self._threads: list[threading.Thread] = []

    # ------------------------------------------------------------------ #
    def is_cancelled(self, task_id: str) -> bool:
        task = self.service.get_task(task_id)
        return task.cancel_requested or task.state == TaskState.CANCELLED

    def _execute(self, claim_token: str, task_id: str, attempt: int) -> None:
        settings = self.service.settings
        stop_heartbeat = threading.Event()

        def _heartbeat_loop() -> None:
            while not stop_heartbeat.wait(settings.heartbeat_interval_seconds):
                if self._stop.is_set():
                    return
                try:
                    ok = self.service.heartbeat(task_id, claim_token)
                except Exception:
                    logger.exception(
                        "task %s: heartbeat error", task_id
                    )
                    return
                if not ok:
                    logger.info(
                        "task %s: heartbeat lost lease; stopping heartbeat", task_id
                    )
                    return

        hb = threading.Thread(
            target=_heartbeat_loop,
            name=f"hb-{task_id[:8]}",
            daemon=True,
        )
        hb.start()
        try:
            task = self.service.get_task(task_id)
            handler = self.handlers.get(task.task_type)
            if handler is None:
                raise UnknownTaskType(
                    f"no handler registered for task_type={task.task_type!r}"
                )

            def _sleep(seconds: float) -> None:
                # Interruptible by pool shutdown; cancellation is checked after.
                self._stop.wait(seconds)

            ctx = ExecutionContext(
                task_id=task_id,
                attempt=attempt,
                is_cancelled=lambda: self.is_cancelled(task_id),
                sleep=_sleep,
            )
            logger.info(
                "task %s: executing handler %r attempt %s/%s payload=%s",
                task_id,
                task.task_type,
                attempt,
                task.max_attempts,
                task.payload,
            )
            result = handler(task.payload, ctx)
            final = self.service.complete(task_id, claim_token, result)
            logger.info(
                "task %s: reported completion, final state=%s",
                task_id,
                final.state.value,
            )
        except CancellationRequested:
            logger.info("task %s: handler observed cancellation", task_id)
            # Let maintenance/lease expiry move it to CANCELLED; the completion
            # path also honours cancel_requested, so just release quietly.
            try:
                self.service.complete(task_id, claim_token, None)
            except Exception:
                pass
        except Exception as exc:
            if self._stop.is_set():
                logger.info(
                    "task %s: worker shutting down; leaving lease to expire for "
                    "takeover (%s)",
                    task_id,
                    type(exc).__name__,
                )
                return
            logger.info(
                "task %s: handler raised %s: %s",
                task_id,
                type(exc).__name__,
                exc,
            )
            try:
                final = self.service.fail(task_id, claim_token, exc)
                logger.info(
                    "task %s: reported failure, final state=%s",
                    task_id,
                    final.state.value,
                )
            except Exception:
                logger.exception(
                    "task %s: error while reporting failure", task_id
                )
        finally:
            stop_heartbeat.set()

    # ------------------------------------------------------------------ #
    def _poll_loop(self) -> None:
        interval = self.service.settings.poll_interval_seconds
        while not self._stop.is_set():
            try:
                claim = self.service.claim(self.worker_id)
            except Exception:
                logger.exception("claim failed")
                self._stop.wait(interval)
                continue
            if claim is None:
                self._stop.wait(interval)
                continue
            if self._executor is not None:
                self._executor.submit(
                    self._execute, claim.token, claim.task.id, claim.task.attempt
                )

    def _maintenance_loop(self) -> None:
        """Make due retries claimable and finalize due cancel requests.

        This also covers processes where tasks are RUNNING with a dead holder
        and a cancel was requested: once the lease expires the task becomes
        CANCELLED without waiting for another claim.
        """
        interval = max(0.05, self.service.settings.poll_interval_seconds)
        while not self._stop.wait(interval):
            try:
                self.service.maintenance()
            except Exception:
                logger.exception("maintenance iteration failed")

    def start(self) -> None:
        if self._threads:
            return
        self._stop.clear()
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency,
            thread_name_prefix=f"{self.worker_id}-exec",
        )
        for i in range(self.concurrency):
            t = threading.Thread(
                target=self._poll_loop,
                name=f"{self.worker_id}-poll-{i}",
                daemon=True,
            )
            t.start()
            self._threads.append(t)
        m = threading.Thread(
            target=self._maintenance_loop,
            name=f"{self.worker_id}-maintenance",
            daemon=True,
        )
        m.start()
        self._threads.append(m)
        logger.info(
            "worker pool %s started with concurrency=%s poll=%ss lease=%ss",
            self.worker_id,
            self.concurrency,
            self.service.settings.poll_interval_seconds,
            self.service.settings.lease_seconds,
        )

    def stop(self, timeout: float | None = None) -> None:
        logger.info("worker pool %s stopping", self.worker_id)
        self._stop.set()
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads.clear()
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
            self._executor = None
        logger.info("worker pool %s stopped", self.worker_id)

    def run_forever(self) -> None:
        self.start()
        try:
            while not self._stop.is_set():
                self._stop.wait(0.5)
        except KeyboardInterrupt:
            pass
        finally:
            self.stop()
