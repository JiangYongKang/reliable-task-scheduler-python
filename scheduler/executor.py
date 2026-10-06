"""服务自驱执行引擎：自动领取、续约、执行并回报任务。"""
from __future__ import annotations

import json
import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any

from scheduler.errors import SchedulerError
from scheduler.models import Task, TaskState
from scheduler.service import SchedulerService

logger = logging.getLogger("scheduler.executor")

# 执行体签名：接收 payload 与执行上下文，返回结果（可序列化的 JSON 对象）
TaskHandler = Callable[[dict[str, Any], "ExecutionContext"], Any]

# payload 中用于选择执行体的字段名；缺省归入 "default"
TYPE_KEY = "type"
DEFAULT_TYPE = "default"


class RetryTaskError(Exception):
    """执行体主动抛出：临时失败，按退避策略重试。"""


class PermanentTaskError(Exception):
    """执行体主动抛出：永久失败，不重试。"""


class ExecutionCancelledError(Exception):
    """任务在执行期间被取消，执行体应尽快退出。"""


class ExecutionContext:
    """传给执行体的运行上下文：取消信号、可打断的等待。"""

    def __init__(self, task_id: str, attempt: int) -> None:
        self.task_id = task_id
        self.attempt = attempt
        self._cancel = threading.Event()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def signal_cancel(self) -> None:
        self._cancel.set()

    def raise_if_cancelled(self) -> None:
        if self._cancel.is_set():
            raise ExecutionCancelledError(self.task_id)

    def sleep_interruptible(self, seconds: float) -> bool:
        """可被取消打断的等待；返回是否在等待期间被取消。"""
        return self._cancel.wait(timeout=seconds)


class ExecutionEngine:
    """自驱执行引擎。

    - 固定 ``worker_concurrency`` 个工作线程循环领取任务，超出并发度的任务
      自然留在队列（PENDING）中排队等待；
    - 独立续约线程在执行期间周期性延长租约；执行者进程崩溃后续约停止，
      租约到期后任务才可被接管；
    - 取消通过协作信号传递，执行体可在小段可观察时间内停下来。
    """

    def __init__(self, service: SchedulerService,
                 worker_id: str | None = None) -> None:
        self._service = service
        self._config = service._config
        self._owner = worker_id or f"engine-{uuid.uuid4().hex[:8]}"
        self._handlers: dict[str, TaskHandler] = {}
        self._stop = threading.Event()
        self._workers: list[threading.Thread] = []
        self._renewer: threading.Thread | None = None
        # task_id -> 当前在本进程内执行的上下文
        self._active: dict[str, ExecutionContext] = {}
        self._active_lock = threading.Lock()
        self._started = False

    # -- 注册与生命周期 ---------------------------------------------------

    def register_handler(self, task_type: str, handler: TaskHandler) -> None:
        """按 payload["type"] 注册执行体；未命中时使用 "default"。"""
        self._handlers[task_type] = handler

    def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stop.clear()
        self._service.add_cancel_listener(self._on_cancel)
        concurrency = max(1, int(self._config.worker_concurrency))
        logger.info("engine starting owner=%s concurrency=%s lease=%ss"
                    " renew_interval=%ss poll_interval=%ss",
                    self._owner, concurrency, self._config.lease_seconds,
                    self._config.lease_renew_interval_seconds,
                    self._config.poll_interval_seconds)
        for i in range(concurrency):
            t = threading.Thread(target=self._worker_loop, name=f"worker-{i}",
                                 daemon=True)
            t.start()
            self._workers.append(t)
        self._renewer = threading.Thread(target=self._renew_loop,
                                         name="lease-renewer", daemon=True)
        self._renewer.start()

    def stop(self, timeout: float | None = None) -> None:
        """停止领取新任务并等待在执行任务自然结束；不打断、不回滚它们。"""
        if not self._started:
            return
        logger.info("engine stopping owner=%s timeout=%s", self._owner, timeout)
        self._stop.set()
        self._service.remove_cancel_listener(self._on_cancel)
        for t in self._workers:
            t.join(timeout=timeout)
        if self._renewer is not None:
            self._renewer.join(timeout=timeout)
        self._workers.clear()
        self._renewer = None
        self._started = False
        logger.info("engine stopped owner=%s", self._owner)

    def wait_until_drained(self, terminal_count: int,
                           timeout: float = 10.0) -> bool:
        """测试辅助：等待终态任务数达到期望值。"""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            stats = self._service.stats()
            done = (stats[TaskState.SUCCEEDED.value]
                    + stats[TaskState.FAILED.value]
                    + stats[TaskState.CANCELLED.value])
            if done >= terminal_count:
                return True
            time.sleep(0.02)
        return False

    # -- 取消信号 ---------------------------------------------------------

    def _on_cancel(self, task_id: str) -> None:
        with self._active_lock:
            ctx = self._active.get(task_id)
        if ctx is not None:
            logger.info("cancel interrupt signalled task=%s", task_id)
            ctx.signal_cancel()

    # -- 续约 -------------------------------------------------------------

    def _renew_loop(self) -> None:
        configured = max(0.01,
                         float(self._config.lease_renew_interval_seconds))
        # 续约周期不得超过租约的一半：即使某次续约恰好失败，下一次仍有充足
        # 时间在租约到期前补上；正常执行者绝不会因调度延迟被误判失联
        interval = min(configured, self._config.lease_seconds / 2.0)
        # 周期续约兜底；任务在领取时已由工作线程立即续约一次，不存在启动空窗
        while not self._stop.wait(timeout=interval):
            with self._active_lock:
                active = dict(self._active)
            for task_id, ctx in active.items():
                if ctx.cancelled:
                    continue  # 已取消，续约无意义
                try:
                    ok = self._service.renew_lease(task_id, self._owner)
                except SchedulerError as err:
                    ok = False
                    logger.warning("renew error task=%s code=%s err=%s",
                                   task_id, err.code, err)
                if ok:
                    logger.debug("renewed lease task=%s owner=%s",
                                 task_id, self._owner)
                else:
                    # 任务已被取消或被判定失联接管：通知执行体立即停下，
                    # 避免失联执行者继续写入结果。
                    logger.warning("renew lost task=%s owner=%s:"
                                   " interrupting execution",
                                   task_id, self._owner)
                    ctx.signal_cancel()

    # -- 工作线程 ---------------------------------------------------------

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            task = self._service.claim(self._owner)
            if task is None:
                self._stop.wait(timeout=self._config.poll_interval_seconds)
                continue
            self._run_one(task)

    def _run_one(self, task: Task) -> None:
        ctx = ExecutionContext(task.task_id, task.attempt)
        with self._active_lock:
            self._active[task.task_id] = ctx
        logger.info("exec start task=%s key=%s attempt=%s type=%s payload=%s",
                    task.task_id, task.idempotency_key, task.attempt,
                    task.payload.get(TYPE_KEY, DEFAULT_TYPE),
                    {k: v for k, v in task.payload.items() if k != TYPE_KEY})
        try:
            # 登记后再查一次：取消可能恰好发生在领取与登记之间
            fresh = self._service.get(task.task_id, requester=self._owner)
            if fresh.state is not TaskState.RUNNING:
                logger.info("exec skip task=%s reason=state_%s",
                            task.task_id, fresh.state.value)
                return
            # 领取后立即续约一次，消除「领取 -> 首个续约周期」之间的租约空窗；
            # 续约失败说明任务已不再属于本执行者（被取消/接管），直接中止
            if not self._service.renew_lease(task.task_id, self._owner):
                logger.warning("initial renew lost task=%s owner=%s:"
                               " aborting before handler", task.task_id,
                               self._owner)
                return
            handler = self._handlers.get(
                str(task.payload.get(TYPE_KEY, DEFAULT_TYPE)),
                self._handlers.get(DEFAULT_TYPE))
            if handler is None:
                raise PermanentTaskError(
                    f"no handler registered for task type "
                    f"{task.payload.get(TYPE_KEY, DEFAULT_TYPE)!r}")
            result = handler(dict(task.payload), ctx)
            ctx.raise_if_cancelled()
            try:
                json.dumps(result)  # 结果必须可持久化为 JSON
            except (TypeError, ValueError) as err:
                raise PermanentTaskError(
                    f"handler returned non-JSON-serializable result: {err}"
                ) from err
            self._report_success(task.task_id, result)
        except ExecutionCancelledError:
            logger.info("exec interrupted task=%s attempt=%s"
                        " reason=cancelled", task.task_id, task.attempt)
        except PermanentTaskError as err:
            self._report_failure(task.task_id, str(err), permanent=True)
        except RetryTaskError as err:
            self._report_failure(task.task_id, str(err), permanent=False)
        except Exception as err:  # noqa: BLE001 - 执行体异常必须被收敛为失败
            self._report_failure(task.task_id,
                                 f"{type(err).__name__}: {err}", permanent=False)
        finally:
            with self._active_lock:
                self._active.pop(task.task_id, None)

    def _report_success(self, task_id: str, result: Any) -> None:
        try:
            done = self._service.complete(task_id, self._owner, result)
            logger.info("exec success task=%s -> %s result=%s",
                        task_id, done.state.value, result)
        except SchedulerError as err:
            # 取消与完成竞争、或租约已失：结果不得落库
            logger.warning("exec complete rejected task=%s code=%s err=%s",
                           task_id, err.code, err)

    def _report_failure(self, task_id: str, error: str, *,
                        permanent: bool) -> None:
        try:
            failed = self._service.fail(task_id, self._owner, error,
                                        permanent=permanent)
            logger.info("exec failure task=%s -> %s permanent=%s error=%s",
                        task_id, failed.state.value, permanent, error)
        except SchedulerError as err:
            logger.warning("exec fail rejected task=%s code=%s err=%s",
                           task_id, err.code, err)
