"""服务自驱执行器。

- 调度线程按可配置并发度自动领取任务并分派给工作者线程执行；
- 每个执行中的任务由心跳线程按固定间隔续约租约，长任务不会被误接管；
- 续约失败（任务被取消或已易主）会置位取消标记，执行体经
  ``ExecutionContext`` 协作式中断；取消与完成的竞争由存储层仲裁，
  最终只有一种结局落库。
"""
from __future__ import annotations

import itertools
import logging
import threading
from collections.abc import Callable
from typing import Any

from scheduler.errors import SchedulerError
from scheduler.models import Task
from scheduler.service import SchedulerService

logger = logging.getLogger("scheduler.executor")


class TaskCancelledError(Exception):
    """执行体通过 ctx.check_cancelled() 感知取消后的中断信号。"""


class PermanentTaskError(Exception):
    """执行体抛出该异常表示永久失败，不再重试。"""


class ExecutionContext:
    """传给执行体的协作式取消上下文。"""

    def __init__(self) -> None:
        self._cancel_event = threading.Event()

    @property
    def cancelled(self) -> bool:
        return self._cancel_event.is_set()

    def check_cancelled(self) -> None:
        """任务已被取消/易主时抛出 TaskCancelledError，打断执行体。"""
        if self.cancelled:
            raise TaskCancelledError("task cancelled or lease lost")

    def _request_cancel(self) -> None:
        self._cancel_event.set()


# 执行体签名：接收任务与取消上下文，返回值作为任务结果落库。
TaskHandler = Callable[[Task, ExecutionContext], Any]


class TaskExecutor:
    """后台执行器：自动领取 -> 续约 -> 执行 -> 完成/失败/取消。"""

    def __init__(self, service: SchedulerService, handler: TaskHandler,
                 concurrency: int, poll_interval: float,
                 renew_interval: float, worker_prefix: str = "executor") -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self._service = service
        self._handler = handler
        self._concurrency = concurrency
        self._poll_interval = poll_interval
        self._renew_interval = renew_interval
        self._worker_prefix = worker_prefix
        self._stop = threading.Event()
        self._dispatcher: threading.Thread | None = None
        self._workers: set[threading.Thread] = set()
        self._lock = threading.Lock()
        self._seq = itertools.count(1)

    # -- 生命周期 ---------------------------------------------------------

    def start(self) -> None:
        if self._dispatcher is not None:
            return
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop, name="executor-dispatcher", daemon=True)
        self._dispatcher.start()
        logger.info("executor started concurrency=%d poll_interval=%.3fs "
                    "renew_interval=%.3fs", self._concurrency,
                    self._poll_interval, self._renew_interval)

    def stop(self, wait: bool = True) -> None:
        """停止调度；wait=True 时等待在途任务执行收尾。"""
        self._stop.set()
        if self._dispatcher is not None:
            self._dispatcher.join(timeout=5)
            self._dispatcher = None
        if wait:
            self._join_workers()
        logger.info("executor stopped")

    def active_count(self) -> int:
        with self._lock:
            return len(self._workers)

    def _join_workers(self) -> None:
        while True:
            with self._lock:
                self._workers = {w for w in self._workers if w.is_alive()}
                workers = list(self._workers)
            if not workers:
                return
            for w in workers:
                w.join(timeout=5)

    # -- 调度循环 ----------------------------------------------------------

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            self._prune_workers()
            if self.active_count() < self._concurrency:
                task = self._service.claim(self._next_worker_id())
                if task is not None:
                    self._spawn_worker(task)
                    continue  # 还有空位就继续领取，不睡眠
            self._stop.wait(self._poll_interval)

    def _next_worker_id(self) -> str:
        return f"{self._worker_prefix}-{next(self._seq)}"

    def _prune_workers(self) -> None:
        with self._lock:
            self._workers = {w for w in self._workers if w.is_alive()}

    def _spawn_worker(self, task: Task) -> None:
        owner = task.owner
        assert owner is not None
        thread = threading.Thread(target=self._run_task, args=(task, owner),
                                  name=f"executor-worker-{task.task_id[:8]}",
                                  daemon=True)
        with self._lock:
            self._workers.add(thread)
        thread.start()
        logger.info("dispatch task=%s owner=%s attempt=%d active=%d",
                    task.task_id, owner, task.attempt, self.active_count())

    # -- 单个任务的执行 ----------------------------------------------------

    def _run_task(self, task: Task, owner: str) -> None:
        ctx = ExecutionContext()
        heartbeat_stop = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(task.task_id, owner, ctx, heartbeat_stop),
            name=f"executor-heartbeat-{task.task_id[:8]}", daemon=True)
        heartbeat.start()
        logger.info("execute begin task=%s owner=%s", task.task_id, owner)
        try:
            result = self._handler(task, ctx)
        except TaskCancelledError:
            # 任务已被取消（或租约丢失）：不再写入任何结果
            logger.info("execute interrupted task=%s reason=cancelled", task.task_id)
        except PermanentTaskError as err:
            self._safe_fail(task.task_id, owner, str(err) or "permanent_failure",
                            permanent=True)
        except Exception as err:  # noqa: BLE001 临时失败一律退避重试
            logger.info("execute transient failure task=%s error=%r",
                        task.task_id, err)
            self._safe_fail(task.task_id, owner, f"{type(err).__name__}: {err}",
                            permanent=False)
        else:
            if ctx.cancelled:
                # 取消与完成几乎同时发生：以存储层状态为准，不写入结果
                logger.info("execute finished but cancelled task=%s "
                            "reason=cancel_wins", task.task_id)
            else:
                self._safe_complete(task.task_id, owner, result)
        finally:
            heartbeat_stop.set()
            heartbeat.join(timeout=5)

    def _heartbeat_loop(self, task_id: str, owner: str, ctx: ExecutionContext,
                        stop: threading.Event) -> None:
        while not stop.wait(self._renew_interval):
            try:
                self._service.renew(task_id, owner)
                logger.debug("renew ok task=%s owner=%s", task_id, owner)
            except SchedulerError as err:
                # 任务被取消或已被接管：通知执行体中断
                logger.info("renew rejected task=%s owner=%s code=%s "
                            "-> request cancel", task_id, owner, err.code)
                ctx._request_cancel()
                return

    # -- 结果落库（竞争由存储层仲裁） --------------------------------------

    def _safe_complete(self, task_id: str, owner: str, result: Any) -> None:
        try:
            done = self._service.complete(task_id, owner, result)
            logger.info("execute done task=%s state=%s", task_id, done.state.value)
        except SchedulerError as err:
            logger.info("complete rejected task=%s code=%s "
                        "reason=state_changed_during_execution",
                        task_id, err.code)

    def _safe_fail(self, task_id: str, owner: str, error: str,
                   permanent: bool) -> None:
        try:
            updated = self._service.fail(task_id, owner, error, permanent)
            logger.info("execute failed task=%s state=%s attempt=%d",
                        task_id, updated.state.value, updated.attempt)
        except SchedulerError as err:
            logger.info("fail rejected task=%s code=%s "
                        "reason=state_changed_during_execution",
                        task_id, err.code)
