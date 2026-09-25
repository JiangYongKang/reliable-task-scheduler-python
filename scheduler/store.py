"""SQLite 持久化层。

- 单连接 + 可重入锁，保证多线程下的串行写入；
- 变更操作使用 BEGIN IMMEDIATE 事务，保证 claim/complete/cancel 的原子性；
- 任务表以 idempotency_key 唯一约束保证重复提交不产生重复任务。
"""
from __future__ import annotations

import json
import sqlite3
import threading

from scheduler.errors import (
    InvalidStateTransitionError,
    LeaseConflictError,
    TaskNotFoundError,
)
from scheduler.models import Task, TaskEvent, TaskState

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    owner TEXT,
    lease_expires_at REAL,
    not_before REAL NOT NULL,
    result TEXT,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tasks_claimable ON tasks(state, not_before);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor TEXT NOT NULL,
    at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id, id);
"""


def _row_to_task(row: sqlite3.Row) -> Task:
    return Task(
        task_id=row["task_id"],
        idempotency_key=row["idempotency_key"],
        payload=json.loads(row["payload"]),
        state=TaskState(row["state"]),
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        owner=row["owner"],
        lease_expires_at=row["lease_expires_at"],
        not_before=row["not_before"],
        result=json.loads(row["result"]) if row["result"] is not None else None,
        error=row["error"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class TaskStore:
    def __init__(self, db_path: str) -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False,
                                     isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- 内部工具 ---------------------------------------------------------

    def _rollback_safe(self) -> None:
        try:
            if self._conn.in_transaction:
                self._conn.execute("ROLLBACK")
        except sqlite3.OperationalError:
            pass

    def _add_event_locked(self, event: TaskEvent) -> None:
        self._conn.execute(
            "INSERT INTO events(task_id, from_state, to_state, reason, actor, at)"
            " VALUES (?,?,?,?,?,?)",
            (event.task_id, event.from_state, event.to_state,
             event.reason, event.actor, event.at),
        )

    def _get_locked(self, task_id: str) -> Task | None:
        row = self._conn.execute(
            "SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        return _row_to_task(row) if row else None

    # -- 写入 -------------------------------------------------------------

    def insert_task(self, task: Task, event: TaskEvent) -> bool:
        """按幂等键插入；已存在返回 False（不产生重复任务）。"""
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                cur = self._conn.execute(
                    "INSERT OR IGNORE INTO tasks(task_id, idempotency_key, payload,"
                    " state, attempt, max_attempts, owner, lease_expires_at,"
                    " not_before, result, error, created_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (task.task_id, task.idempotency_key, json.dumps(task.payload),
                     task.state.value, task.attempt, task.max_attempts, task.owner,
                     task.lease_expires_at, task.not_before,
                     json.dumps(task.result) if task.result is not None else None,
                     task.error, task.created_at, task.updated_at),
                )
                if cur.rowcount == 1:
                    self._add_event_locked(event)
                self._conn.execute("COMMIT")
                return cur.rowcount == 1
            except Exception:
                self._rollback_safe()
                raise

    def claim_next(self, owner: str, now: float,
                   lease_seconds: float) -> tuple[Task, TaskEvent] | None:
        """原子领取一个可运行任务；含租约超时接管。"""
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                row = self._conn.execute(
                    "SELECT * FROM tasks WHERE"
                    " (state='PENDING' AND not_before<=?)"
                    " OR (state='RUNNING' AND lease_expires_at<=?)"
                    " ORDER BY created_at LIMIT 1",
                    (now, now),
                ).fetchone()
                if row is None:
                    self._conn.execute("COMMIT")
                    return None
                prev = _row_to_task(row)
                takeover = prev.state == TaskState.RUNNING
                lease_expires = now + lease_seconds
                cur = self._conn.execute(
                    "UPDATE tasks SET state='RUNNING', owner=?, lease_expires_at=?,"
                    " attempt=attempt+1, updated_at=?"
                    " WHERE task_id=? AND state=? AND owner IS ?",
                    (owner, lease_expires, now, prev.task_id,
                     prev.state.value, prev.owner),
                )
                if cur.rowcount != 1:
                    raise LeaseConflictError(f"task {prev.task_id} claim lost")
                reason = "lease_expired_takeover" if takeover else "claimed"
                event = TaskEvent(task_id=prev.task_id,
                                  from_state=prev.state.value,
                                  to_state=TaskState.RUNNING.value,
                                  reason=reason, actor=owner, at=now)
                self._add_event_locked(event)
                self._conn.execute("COMMIT")
                claimed = self.get_task(prev.task_id)
                assert claimed is not None
                return claimed, event
            except Exception:
                self._rollback_safe()
                raise

    def complete(self, task_id: str, owner: str, result: object,
                 now: float) -> tuple[Task, TaskEvent]:
        with self._lock:
            return self._finish_locked(task_id, owner, now,
                                       to_state=TaskState.SUCCEEDED,
                                       result=result, error=None,
                                       reason="completed")

    def fail(self, task_id: str, owner: str, error: str, permanent: bool,
             not_before: float, now: float) -> tuple[Task, TaskEvent]:
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                task = self._get_locked(task_id)
                if task is None:
                    raise TaskNotFoundError(task_id)
                if task.state != TaskState.RUNNING:
                    raise InvalidStateTransitionError(
                        task_id, task.state.value, "fail")
                if task.owner != owner:
                    raise LeaseConflictError(
                        f"task {task_id} owned by {task.owner!r}, not {owner!r}")
                if permanent or task.attempt >= task.max_attempts:
                    to_state, reason = TaskState.FAILED, "permanent_failure"
                    nb, lease, new_owner = task.not_before, None, None
                else:
                    to_state, reason = TaskState.PENDING, "transient_failure_retry"
                    nb, lease, new_owner = not_before, None, None
                self._conn.execute(
                    "UPDATE tasks SET state=?, owner=?, lease_expires_at=?,"
                    " not_before=?, error=?, updated_at=? WHERE task_id=?",
                    (to_state.value, new_owner, lease, nb, error, now, task_id),
                )
                event = TaskEvent(task_id=task_id,
                                  from_state=TaskState.RUNNING.value,
                                  to_state=to_state.value,
                                  reason=reason, actor=owner, at=now)
                self._add_event_locked(event)
                self._conn.execute("COMMIT")
                updated = self.get_task(task_id)
                assert updated is not None
                return updated, event
            except Exception:
                self._rollback_safe()
                raise

    def _finish_locked(self, task_id: str, owner: str, now: float,
                       to_state: TaskState, result: object,
                       error: str | None, reason: str) -> tuple[Task, TaskEvent]:
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            task = self._get_locked(task_id)
            if task is None:
                raise TaskNotFoundError(task_id)
            if task.state != TaskState.RUNNING:
                raise InvalidStateTransitionError(
                    task_id, task.state.value, "complete")
            if task.owner != owner:
                raise LeaseConflictError(
                    f"task {task_id} owned by {task.owner!r}, not {owner!r}")
            self._conn.execute(
                "UPDATE tasks SET state=?, owner=NULL, lease_expires_at=NULL,"
                " result=?, error=?, updated_at=? WHERE task_id=?",
                (to_state.value,
                 json.dumps(result) if result is not None else None,
                 error, now, task_id),
            )
            event = TaskEvent(task_id=task_id, from_state=TaskState.RUNNING.value,
                              to_state=to_state.value, reason=reason,
                              actor=owner, at=now)
            self._add_event_locked(event)
            self._conn.execute("COMMIT")
            updated = self.get_task(task_id)
            assert updated is not None
            return updated, event
        except Exception:
            self._rollback_safe()
            raise

    def cancel(self, task_id: str, now: float) -> tuple[Task, TaskEvent]:
        """取消 PENDING/RUNNING 任务；终态或未知任务被拒绝。"""
        with self._lock:
            try:
                self._conn.execute("BEGIN IMMEDIATE")
                task = self._get_locked(task_id)
                if task is None:
                    raise TaskNotFoundError(task_id)
                if task.state.terminal:
                    raise InvalidStateTransitionError(
                        task_id, task.state.value, "cancel")
                self._conn.execute(
                    "UPDATE tasks SET state='CANCELLED', owner=NULL,"
                    " lease_expires_at=NULL, updated_at=? WHERE task_id=?",
                    (now, task_id),
                )
                event = TaskEvent(task_id=task_id, from_state=task.state.value,
                                  to_state=TaskState.CANCELLED.value,
                                  reason="cancelled", actor="api", at=now)
                self._add_event_locked(event)
                self._conn.execute("COMMIT")
                updated = self.get_task(task_id)
                assert updated is not None
                return updated, event
            except Exception:
                self._rollback_safe()
                raise

    # -- 查询 -------------------------------------------------------------

    def get_task(self, task_id: str) -> Task | None:
        with self._lock:
            return self._get_locked(task_id)

    def get_task_by_key(self, idempotency_key: str) -> Task | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE idempotency_key=?",
                (idempotency_key,)).fetchone()
            return _row_to_task(row) if row else None

    def list_events(self, task_id: str) -> list[TaskEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE task_id=? ORDER BY id",
                (task_id,)).fetchall()
            return [TaskEvent(task_id=r["task_id"], from_state=r["from_state"],
                              to_state=r["to_state"], reason=r["reason"],
                              actor=r["actor"], at=r["at"]) for r in rows]

    def stats(self, now: float) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT state, COUNT(*) c FROM tasks GROUP BY state").fetchall()
            out = {s.value: 0 for s in TaskState}
            for r in rows:
                out[r["state"]] = r["c"]
            out["claimable"] = self._conn.execute(
                "SELECT COUNT(*) c FROM tasks WHERE"
                " (state='PENDING' AND not_before<=?)"
                " OR (state='RUNNING' AND lease_expires_at<=?)",
                (now, now)).fetchone()["c"]
            retrying = self._conn.execute(
                "SELECT COUNT(*) c FROM tasks WHERE state='PENDING'"
                " AND attempt>0").fetchone()["c"]
            out["retrying"] = retrying
            return out
