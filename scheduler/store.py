"""SQLite-backed persistent storage with transactional state transitions.

Concurrency model:
  * A single connection guarded by an in-process lock serializes writers.
  * Every transition runs in one ``BEGIN IMMEDIATE`` transaction and uses a
    conditional ``UPDATE ... WHERE <expected state/token>`` so that, even under
    thread/process contention, a transition applies at most once.
  * SQLite file locking plus the same conditional statements also protects
    multiple processes sharing the database file.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from typing import Any

from .models import Task, TaskEvent, TaskState

logger = logging.getLogger("scheduler.store")

_RUNNING = TaskState.RUNNING.value
_PENDING = TaskState.PENDING.value
_RETRY_WAIT = TaskState.RETRY_WAIT.value
_SUCCEEDED = TaskState.SUCCEEDED.value
_FAILED = TaskState.PERMANENTLY_FAILED.value
_CANCELLED = TaskState.CANCELLED.value

_TASK_COLUMNS = (
    "id, idempotency_key, owner, task_type, payload, state, attempt, "
    "max_attempts, lease_owner, lease_token, lease_expires_at, cancel_requested, "
    "created_at, updated_at, next_run_at, result, last_error_type, last_error_message"
)


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            db_path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._configure()
        self.init_schema()

    def _configure(self) -> None:
        cur = self._conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.execute("PRAGMA busy_timeout=10000")
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    @contextmanager
    def _tx(self):
        """Serialize writers and open a single BEGIN IMMEDIATE transaction."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def init_schema(self) -> None:
        # executescript commits any pending transaction; run it under the
        # in-process lock and rely on busy_timeout for cross-process DDL.
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE,
                    owner TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempt INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    lease_owner TEXT,
                    lease_token TEXT,
                    lease_expires_at REAL,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    next_run_at REAL NOT NULL DEFAULT 0,
                    result TEXT,
                    last_error_type TEXT,
                    last_error_message TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_due
                    ON tasks(state, next_run_at);
                CREATE TABLE IF NOT EXISTS task_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    at REAL NOT NULL,
                    attempt INTEGER,
                    lease_owner TEXT,
                    detail TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_events_task
                    ON task_events(task_id, id);
                """
            )
        logger.info("store schema ready at %s", self.db_path)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ #
    # Mapping
    # ------------------------------------------------------------------ #
    @staticmethod
    def _row_to_task(row: sqlite3.Row) -> Task:
        return Task(
            id=row["id"],
            idempotency_key=row["idempotency_key"],
            owner=row["owner"],
            task_type=row["task_type"],
            payload=json.loads(row["payload"]),
            state=TaskState(row["state"]),
            attempt=row["attempt"],
            max_attempts=row["max_attempts"],
            lease_owner=row["lease_owner"],
            lease_token=row["lease_token"],
            lease_expires_at=row["lease_expires_at"],
            cancel_requested=bool(row["cancel_requested"]),
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            next_run_at=row["next_run_at"],
            result=(json.loads(row["result"]) if row["result"] is not None else None),
            last_error_type=row["last_error_type"],
            last_error_message=row["last_error_message"],
        )

    def _append_event(
        self,
        task_id: str,
        event_type: str,
        now: float,
        attempt: int | None = None,
        lease_owner: str | None = None,
        detail: dict[str, Any] | None = None,
    ) -> None:
        self._conn.execute(
            "INSERT INTO task_events (task_id, event_type, at, attempt, "
            "lease_owner, detail) VALUES (?, ?, ?, ?, ?, ?)",
            (
                task_id,
                event_type,
                now,
                attempt,
                lease_owner,
                json.dumps(detail or {}, ensure_ascii=False),
            ),
        )

    # ------------------------------------------------------------------ #
    # Reads
    # ------------------------------------------------------------------ #
    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {_TASK_COLUMNS} FROM tasks WHERE id = ?", (task_id,)
            ).fetchone()
        return self._row_to_task(row) if row is not None else None

    def get_task_by_idempotency_key(self, key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                f"SELECT {_TASK_COLUMNS} FROM tasks WHERE idempotency_key = ?",
                (key,),
            ).fetchone()
        return self._row_to_task(row) if row is not None else None

    def insert_task(self, task: Task) -> bool:
        """Insert a new task. Returns False on idempotency-key collision."""
        with self._tx():
            try:
                self._conn.execute(
                    "INSERT INTO tasks (id, idempotency_key, owner, task_type, "
                    "payload, state, attempt, max_attempts, lease_owner, lease_token, "
                    "lease_expires_at, cancel_requested, created_at, updated_at, "
                    "next_run_at, result, last_error_type, last_error_message) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        task.id,
                        task.idempotency_key,
                        task.owner,
                        task.task_type,
                        json.dumps(task.payload, ensure_ascii=False),
                        task.state.value,
                        task.attempt,
                        task.max_attempts,
                        task.lease_owner,
                        task.lease_token,
                        task.lease_expires_at,
                        1 if task.cancel_requested else 0,
                        task.created_at,
                        task.updated_at,
                        task.next_run_at,
                        json.dumps(task.result, ensure_ascii=False)
                        if task.result is not None
                        else None,
                        task.last_error_type,
                        task.last_error_message,
                    ),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def append_event(self, event: TaskEvent) -> int:
        with self._tx():
            cur = self._conn.execute(
                "INSERT INTO task_events (task_id, event_type, at, attempt, "
                "lease_owner, detail) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    event.task_id,
                    event.event_type
                    if isinstance(event.event_type, str)
                    else event.event_type.value,
                    event.at,
                    event.attempt,
                    event.lease_owner,
                    json.dumps(event.detail, ensure_ascii=False),
                ),
            )
            return int(cur.lastrowid)

    def list_events(self, task_id: str) -> list[TaskEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, task_id, event_type, at, attempt, lease_owner, detail "
                "FROM task_events WHERE task_id = ? ORDER BY id",
                (task_id,),
            ).fetchall()
        return [
            TaskEvent(
                id=r["id"],
                task_id=r["task_id"],
                event_type=r["event_type"],
                at=r["at"],
                attempt=r["attempt"],
                lease_owner=r["lease_owner"],
                detail=json.loads(r["detail"]),
            )
            for r in rows
        ]

    # ------------------------------------------------------------------ #
    # Claiming / takeover
    # ------------------------------------------------------------------ #
    def claim_next(
        self, worker_id: str, now: float, token: str, expires_at: float
    ) -> Task | None:
        """Atomically claim a pending task or take over an expired lease."""
        with self._tx():
            # 1. Retry waits that are due become pending again.
            rows = self._conn.execute(
                "SELECT id, attempt FROM tasks WHERE state = ? AND next_run_at <= ?",
                (_RETRY_WAIT, now),
            ).fetchall()
            due_ids = [r["id"] for r in rows]
            if due_ids:
                self._conn.execute(
                    "UPDATE tasks SET state = ?, updated_at = ? "
                    "WHERE state = ? AND next_run_at <= ?",
                    (_PENDING, now, _RETRY_WAIT, now),
                )
                for r in rows:
                    self._append_event(
                        r["id"], "RETRY_AVAILABLE", now, attempt=r["attempt"]
                    )

            # 2. Cancel requests on non-running tasks take effect immediately.
            rows = self._conn.execute(
                "SELECT id, attempt FROM tasks WHERE cancel_requested = 1 "
                "AND state IN (?, ?)",
                (_PENDING, _RETRY_WAIT),
            ).fetchall()
            if rows:
                self._conn.execute(
                    "UPDATE tasks SET state = ?, lease_owner = NULL, "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = ? "
                    "WHERE cancel_requested = 1 AND state IN (?, ?)",
                    (_CANCELLED, now, _PENDING, _RETRY_WAIT),
                )
                for r in rows:
                    self._append_event(
                        r["id"],
                        "CANCELLED",
                        now,
                        attempt=r["attempt"],
                        detail={"reason": "cancel_before_start"},
                    )

            # 3. Pick a candidate: pending, or running with an expired lease.
            while True:
                row = self._conn.execute(
                    f"SELECT {_TASK_COLUMNS} FROM tasks "
                    "WHERE state = ? OR (state = ? AND lease_expires_at IS NOT NULL "
                    "AND lease_expires_at <= ?) "
                    "ORDER BY created_at LIMIT 1",
                    (_PENDING, _RUNNING, now),
                ).fetchone()
                if row is None:
                    return None
                task = self._row_to_task(row)

                if task.state == TaskState.RUNNING:
                    # Expired lease.
                    if task.cancel_requested:
                        self._conn.execute(
                            "UPDATE tasks SET state = ?, lease_owner = NULL, "
                            "lease_token = NULL, lease_expires_at = NULL, "
                            "updated_at = ? WHERE id = ? AND state = ?",
                            (_CANCELLED, now, task.id, _RUNNING),
                        )
                        self._append_event(
                            task.id,
                            "CANCELLED",
                            now,
                            attempt=task.attempt,
                            lease_owner=task.lease_owner,
                            detail={"reason": "lease_timeout_with_cancel_request"},
                        )
                        logger.info(
                            "task %s: RUNNING lease expired with cancel request "
                            "-> CANCELLED",
                            task.id,
                        )
                        continue
                    cur = self._conn.execute(
                        "UPDATE tasks SET state = ?, attempt = attempt + 1, "
                        "lease_owner = ?, lease_token = ?, lease_expires_at = ?, "
                        "updated_at = ? WHERE id = ? AND state = ? "
                        "AND lease_expires_at <= ?",
                        (
                            _RUNNING,
                            worker_id,
                            token,
                            expires_at,
                            now,
                            task.id,
                            _RUNNING,
                            now,
                        ),
                    )
                    if cur.rowcount != 1:
                        continue
                    self._append_event(
                        task.id,
                        "TAKEN_OVER",
                        now,
                        attempt=task.attempt + 1,
                        lease_owner=worker_id,
                        detail={
                            "previous_owner": task.lease_owner,
                            "previous_attempt": task.attempt,
                            "lease_expired_at": task.lease_expires_at,
                        },
                    )
                    logger.info(
                        "task %s: lease of %s expired, taken over by %s "
                        "(attempt %s)",
                        task.id,
                        task.lease_owner,
                        worker_id,
                        task.attempt + 1,
                    )
                    return self.get_task(task.id)

                # PENDING (cancel requests already filtered in step 2).
                cur = self._conn.execute(
                    "UPDATE tasks SET state = ?, attempt = attempt + 1, "
                    "lease_owner = ?, lease_token = ?, lease_expires_at = ?, "
                    "updated_at = ? WHERE id = ? AND state = ? AND cancel_requested = 0",
                    (
                        _RUNNING,
                        worker_id,
                        token,
                        expires_at,
                        now,
                        task.id,
                        _PENDING,
                    ),
                )
                if cur.rowcount != 1:
                    continue
                self._append_event(
                    task.id,
                    "CLAIMED",
                    now,
                    attempt=task.attempt + 1,
                    lease_owner=worker_id,
                )
                logger.info(
                    "task %s: PENDING -> RUNNING claimed by %s (attempt %s)",
                    task.id,
                    worker_id,
                    task.attempt + 1,
                )
                return self.get_task(task.id)

    def heartbeat(self, task_id: str, token: str, expires_at: float) -> bool:
        # Routine heartbeats only extend lease_expires_at; they are not appended
        # to history to avoid unbounded event growth. Lease liveness is visible
        # via lease_expires_at, and loss is explained by TAKEN_OVER/CANCELLED.
        with self._tx():
            cur = self._conn.execute(
                "UPDATE tasks SET lease_expires_at = ? WHERE id = ? AND state = ? "
                "AND lease_token = ?",
                (expires_at, task_id, _RUNNING, token),
            )
            if cur.rowcount != 1:
                return False
        return True

    # ------------------------------------------------------------------ #
    # Terminal / retry transitions. Return a status string for the service.
    # ------------------------------------------------------------------ #
    def _load_locked(self, task_id: str) -> sqlite3.Row | None:
        return self._conn.execute(
            f"SELECT {_TASK_COLUMNS} FROM tasks WHERE id = ?", (task_id,)
        ).fetchone()

    def complete(
        self, task_id: str, token: str, now: float, result: Any
    ) -> str:
        with self._tx():
            row = self._load_locked(task_id)
            if row is None:
                return "not_found"
            if row["state"] == _CANCELLED:
                return "cancelled"
            if row["state"] == _SUCCEEDED:
                return "already_completed"
            if row["state"] == _FAILED:
                return "permanently_failed"
            if row["state"] != _RUNNING or row["lease_token"] != token:
                return "invalid_token"
            if row["cancel_requested"]:
                # Cancel-vs-complete race: cancellation always wins.
                self._conn.execute(
                    "UPDATE tasks SET state = ?, lease_owner = NULL, "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = ? "
                    "WHERE id = ?",
                    (_CANCELLED, now, task_id),
                )
                self._append_event(
                    task_id,
                    "COMPLETION_REJECTED",
                    now,
                    attempt=row["attempt"],
                    lease_owner=row["lease_owner"],
                    detail={"reason": "cancel_requested"},
                )
                self._append_event(
                    task_id,
                    "CANCELLED",
                    now,
                    attempt=row["attempt"],
                    detail={"reason": "cancel_wins_over_completion"},
                )
                logger.info(
                    "task %s: completion rejected because cancel was requested",
                    task_id,
                )
                return "cancelled"
            cur = self._conn.execute(
                "UPDATE tasks SET state = ?, result = ?, lease_owner = NULL, "
                "lease_token = NULL, lease_expires_at = NULL, updated_at = ? "
                "WHERE id = ? AND state = ? AND lease_token = ?",
                (
                    _SUCCEEDED,
                    json.dumps(result, ensure_ascii=False),
                    now,
                    task_id,
                    _RUNNING,
                    token,
                ),
            )
            if cur.rowcount != 1:
                return "invalid_token"
            self._append_event(
                task_id,
                "COMPLETED",
                now,
                attempt=row["attempt"],
                lease_owner=row["lease_owner"],
            )
            logger.info("task %s: RUNNING -> SUCCEEDED", task_id)
            return "ok"

    def schedule_retry(
        self,
        task_id: str,
        token: str,
        now: float,
        next_run_at: float,
        error_type: str,
        error_message: str,
    ) -> str:
        with self._tx():
            row = self._load_locked(task_id)
            if row is None:
                return "not_found"
            if row["state"] == _CANCELLED:
                return "cancelled"
            if row["state"] in (_SUCCEEDED, _FAILED):
                return "terminal"
            if row["state"] != _RUNNING or row["lease_token"] != token:
                return "invalid_token"
            if row["cancel_requested"]:
                self._conn.execute(
                    "UPDATE tasks SET state = ?, lease_owner = NULL, "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = ? "
                    "WHERE id = ?",
                    (_CANCELLED, now, task_id),
                )
                self._append_event(
                    task_id,
                    "COMPLETION_REJECTED",
                    now,
                    attempt=row["attempt"],
                    lease_owner=row["lease_owner"],
                    detail={"reason": "cancel_requested", "stage": "retry"},
                )
                self._append_event(
                    task_id,
                    "CANCELLED",
                    now,
                    attempt=row["attempt"],
                    detail={"reason": "cancel_wins_over_retry"},
                )
                logger.info(
                    "task %s: retry rejected because cancel was requested", task_id
                )
                return "cancelled"
            cur = self._conn.execute(
                "UPDATE tasks SET state = ?, lease_owner = NULL, lease_token = NULL, "
                "lease_expires_at = NULL, next_run_at = ?, last_error_type = ?, "
                "last_error_message = ?, updated_at = ? "
                "WHERE id = ? AND state = ? AND lease_token = ?",
                (
                    _RETRY_WAIT,
                    next_run_at,
                    error_type,
                    error_message,
                    now,
                    task_id,
                    _RUNNING,
                    token,
                ),
            )
            if cur.rowcount != 1:
                return "invalid_token"
            self._append_event(
                task_id,
                "RETRY_SCHEDULED",
                now,
                attempt=row["attempt"],
                lease_owner=row["lease_owner"],
                detail={
                    "next_run_at": next_run_at,
                    "backoff_seconds": max(0.0, next_run_at - now),
                    "error_type": error_type,
                    "error_message": error_message,
                },
            )
            logger.info(
                "task %s: RUNNING -> RETRY_WAIT until %.3f (%s)",
                task_id,
                next_run_at,
                error_type,
            )
            return "ok"

    def permanently_fail(
        self,
        task_id: str,
        token: str,
        now: float,
        error_type: str,
        error_message: str,
    ) -> str:
        with self._tx():
            row = self._load_locked(task_id)
            if row is None:
                return "not_found"
            if row["state"] == _CANCELLED:
                return "cancelled"
            if row["state"] in (_SUCCEEDED, _FAILED):
                return "terminal"
            if row["state"] != _RUNNING or row["lease_token"] != token:
                return "invalid_token"
            if row["cancel_requested"]:
                self._conn.execute(
                    "UPDATE tasks SET state = ?, lease_owner = NULL, "
                    "lease_token = NULL, lease_expires_at = NULL, updated_at = ? "
                    "WHERE id = ?",
                    (_CANCELLED, now, task_id),
                )
                self._append_event(
                    task_id,
                    "COMPLETION_REJECTED",
                    now,
                    attempt=row["attempt"],
                    lease_owner=row["lease_owner"],
                    detail={"reason": "cancel_requested", "stage": "fail"},
                )
                self._append_event(
                    task_id,
                    "CANCELLED",
                    now,
                    attempt=row["attempt"],
                    detail={"reason": "cancel_wins_over_failure"},
                )
                return "cancelled"
            cur = self._conn.execute(
                "UPDATE tasks SET state = ?, last_error_type = ?, "
                "last_error_message = ?, lease_owner = NULL, lease_token = NULL, "
                "lease_expires_at = NULL, updated_at = ? "
                "WHERE id = ? AND state = ? AND lease_token = ?",
                (
                    _FAILED,
                    error_type,
                    error_message,
                    now,
                    task_id,
                    _RUNNING,
                    token,
                ),
            )
            if cur.rowcount != 1:
                return "invalid_token"
            self._append_event(
                task_id,
                "PERMANENT_FAILURE",
                now,
                attempt=row["attempt"],
                lease_owner=row["lease_owner"],
                detail={"error_type": error_type, "error_message": error_message},
            )
            logger.info(
                "task %s: RUNNING -> PERMANENTLY_FAILED (%s)",
                task_id,
                error_type,
            )
            return "ok"

    # ------------------------------------------------------------------ #
    # Cancellation
    # ------------------------------------------------------------------ #
    def request_cancel(self, task_id: str, now: float) -> tuple[str, bool]:
        """Returns (status, effective). ``effective`` means task is CANCELLED."""
        with self._tx():
            row = self._load_locked(task_id)
            if row is None:
                return ("not_found", False)
            if row["state"] == _CANCELLED:
                return ("already_cancelled", False)
            if row["state"] in (_SUCCEEDED, _FAILED):
                return ("terminal:" + row["state"], False)
            if row["state"] in (_PENDING, _RETRY_WAIT):
                if row["cancel_requested"]:
                    return ("cancel_in_progress", False)
                self._conn.execute(
                    "UPDATE tasks SET state = ?, cancel_requested = 1, "
                    "lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL, "
                    "updated_at = ? WHERE id = ?",
                    (_CANCELLED, now, task_id),
                )
                self._append_event(
                    task_id,
                    "CANCEL_REQUESTED",
                    now,
                    attempt=row["attempt"],
                )
                self._append_event(
                    task_id,
                    "CANCELLED",
                    now,
                    attempt=row["attempt"],
                    detail={"reason": "cancel_before_start"},
                )
                logger.info("task %s: %s -> CANCELLED", task_id, row["state"])
                return ("ok", True)
            # RUNNING
            if row["cancel_requested"]:
                return ("cancel_in_progress", False)
            self._conn.execute(
                "UPDATE tasks SET cancel_requested = 1, updated_at = ? WHERE id = ?",
                (now, task_id),
            )
            self._append_event(
                task_id,
                "CANCEL_REQUESTED",
                now,
                attempt=row["attempt"],
                lease_owner=row["lease_owner"],
                detail={"lease_expires_at": row["lease_expires_at"]},
            )
            logger.info(
                "task %s: cancel requested while RUNNING (lease holder %s)",
                task_id,
                row["lease_owner"],
            )
            return ("ok", False)

    def cancel_due_tasks(self, now: float) -> list[str]:
        """Cancel requested tasks that are due: expired leases or not started."""
        cancelled: list[str] = []
        with self._tx():
            rows = self._conn.execute(
                "SELECT id, attempt, lease_owner FROM tasks WHERE cancel_requested = 1 "
                "AND (state IN (?, ?) OR (state = ? AND lease_expires_at <= ?))",
                (_PENDING, _RETRY_WAIT, _RUNNING, now),
            ).fetchall()
            for r in rows:
                self._conn.execute(
                    "UPDATE tasks SET state = ?, lease_owner = NULL, lease_token = NULL, "
                    "lease_expires_at = NULL, updated_at = ? "
                    "WHERE id = ? AND cancel_requested = 1",
                    (_CANCELLED, now, r["id"]),
                )
                self._append_event(
                    r["id"],
                    "CANCELLED",
                    now,
                    attempt=r["attempt"],
                    lease_owner=r["lease_owner"],
                    detail={"reason": "cancel_request_due"},
                )
                cancelled.append(r["id"])
                logger.info("task %s: -> CANCELLED by maintenance", r["id"])
        return cancelled

    # ------------------------------------------------------------------ #
    # Maintenance / introspection
    # ------------------------------------------------------------------ #
    def make_retry_due(self, now: float) -> int:
        with self._tx():
            rows = self._conn.execute(
                "SELECT id, attempt FROM tasks WHERE state = ? AND next_run_at <= ?",
                (_RETRY_WAIT, now),
            ).fetchall()
            if not rows:
                return 0
            self._conn.execute(
                "UPDATE tasks SET state = ?, updated_at = ? "
                "WHERE state = ? AND next_run_at <= ?",
                (_PENDING, now, _RETRY_WAIT, now),
            )
            for r in rows:
                self._append_event(
                    r["id"], "RETRY_AVAILABLE", now, attempt=r["attempt"]
                )
            logger.info("%s retry-wait task(s) became due", len(rows))
            return len(rows)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT state, COUNT(*) AS c FROM tasks GROUP BY state"
            ).fetchall()
        counts = {r["state"]: r["c"] for r in rows}
        return {
            "total": sum(counts.values()),
            "by_state": counts,
            "backlog": counts.get(_PENDING, 0),
            "in_flight": counts.get(_RUNNING, 0),
            "retry_wait": counts.get(_RETRY_WAIT, 0),
        }
