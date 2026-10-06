"""调度业务逻辑：幂等提交、领取、完成、失败重试、取消、查询。"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Callable

from scheduler.config import SchedulerConfig
from scheduler.errors import TaskNotFoundError
from scheduler.models import Task, TaskEvent, TaskState
from scheduler.store import TaskStore

# 取消通知回调：任务被成功取消（PENDING/RUNNING -> CANCELLED）时调用
CancelListener = Callable[[str], None]


class SchedulerService:
    def __init__(self, config: SchedulerConfig,
                 now_fn: Callable[[], float] = time.time) -> None:
        self._config = config
        self._now = now_fn
        self._store = TaskStore(config.db_path)
        self._cancel_listeners: list[CancelListener] = []
        self._listeners_lock = threading.Lock()

    def close(self) -> None:
        self._store.close()

    def submit(self, idempotency_key: str, payload: dict[str, Any],
               max_attempts: int | None = None) -> tuple[Task, bool]:
        """提交任务；相同幂等键返回既有任务，不重复入队。返回 (任务, 是否新建)。"""
        if not idempotency_key:
            raise ValueError("idempotency_key must be non-empty")
        now = self._now()
        task = Task(
            task_id=uuid.uuid4().hex,
            idempotency_key=idempotency_key,
            payload=payload,
            state=TaskState.PENDING,
            attempt=0,
            max_attempts=max_attempts or self._config.max_attempts,
            owner=None,
            lease_expires_at=None,
            not_before=now,
            result=None,
            error=None,
            created_at=now,
            updated_at=now,
        )
        event = TaskEvent(task_id=task.task_id, from_state=None,
                          to_state=TaskState.PENDING.value,
                          reason="submitted", actor="api", at=now)
        created = self._store.insert_task(task, event)
        if not created:
            existing = self._store.get_task_by_key(idempotency_key)
            assert existing is not None
            return existing, False
        return task, True

    def claim(self, worker_id: str) -> Task | None:
        """领取一个可运行任务；租约超时的 RUNNING 任务可被接管。"""
        out = self._store.claim_next(worker_id, self._now(),
                                     self._config.lease_seconds)
        return out[0] if out else None

    def complete(self, task_id: str, owner: str, result: Any) -> Task:
        task, _ = self._store.complete(task_id, owner, result, self._now())
        return task

    def fail(self, task_id: str, owner: str, error: str,
             permanent: bool = False) -> Task:
        """临时失败按指数退避回到 PENDING；永久失败或超次数进入 FAILED。"""
        now = self._now()
        backoff = min(
            self._config.backoff_base_seconds * (2 ** 0),
            self._config.backoff_max_seconds,
        )
        task = self._store.get_task(task_id)
        if task is not None:
            backoff = min(
                self._config.backoff_base_seconds * (2 ** max(task.attempt - 1, 0)),
                self._config.backoff_max_seconds,
            )
        updated, _ = self._store.fail(task_id, owner, error, permanent,
                                      now + backoff, now)
        return updated

    def renew_lease(self, task_id: str, owner: str) -> bool:
        """执行期间续约；任务已不再属于该执行者（被取消/接管/终态）时返回 False。"""
        return self._store.renew_lease(
            task_id, owner, self._now(), self._config.lease_seconds)

    def cancel(self, task_id: str) -> Task:
        task, _ = self._store.cancel(task_id, self._now())
        # 通知本进程内的执行引擎打断执行体
        with self._listeners_lock:
            listeners = list(self._cancel_listeners)
        for listener in listeners:
            listener(task_id)
        return task

    def add_cancel_listener(self, listener: CancelListener) -> None:
        with self._listeners_lock:
            self._cancel_listeners.append(listener)

    def remove_cancel_listener(self, listener: CancelListener) -> None:
        with self._listeners_lock:
            try:
                self._cancel_listeners.remove(listener)
            except ValueError:
                pass

    def get(self, task_id: str, requester: str | None = None) -> Task:
        """查询任务；非当前持有者的调用方看不到 payload/result。"""
        task = self._store.get_task(task_id)
        if task is None:
            raise TaskNotFoundError(task_id)
        if task.owner is not None and requester != task.owner:
            task = Task(**{**task.__dict__, "payload": {}, "result": None})
        return task

    def history(self, task_id: str) -> list[TaskEvent]:
        if self._store.get_task(task_id) is None:
            raise TaskNotFoundError(task_id)
        return self._store.list_events(task_id)

    def stats(self) -> dict[str, int]:
        return self._store.stats(self._now())
