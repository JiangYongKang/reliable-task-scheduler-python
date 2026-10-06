import os
import time

import uvicorn
from fastapi import FastAPI

from scheduler.api import create_router
from scheduler.config import SchedulerConfig
from scheduler.executor import (
    ExecutionContext,
    PermanentTaskError,
    TaskExecutor,
)
from scheduler.models import Task
from scheduler.service import SchedulerService

app = FastAPI()


# 保持既有公开接口行为
@app.get("/")
def read_root():
    return {"Hello": "World"}


def demo_handler(task: Task, ctx: ExecutionContext):
    """内置演示执行体，按 payload 模拟工作，便于本地验证自驱执行。

    payload 约定：
    - sleep_seconds: 模拟耗时（分段睡眠，可响应取消）
    - fail: "transient" 临时失败（触发退避重试）/ "permanent" 永久失败
    - result: 成功后写入的结果，缺省回显 payload
    """
    payload = task.payload
    deadline = time.monotonic() + float(payload.get("sleep_seconds", 0))
    while time.monotonic() < deadline:
        ctx.check_cancelled()
        time.sleep(min(0.05, deadline - time.monotonic()))
    mode = payload.get("fail")
    if mode == "permanent":
        raise PermanentTaskError("payload requested permanent failure")
    if mode == "transient":
        raise RuntimeError("payload requested transient failure")
    return payload.get("result", {"echo": payload})


def _build_config() -> SchedulerConfig:
    config = SchedulerConfig(
        db_path=os.environ.get("SCHEDULER_DB", "scheduler.db"),
        lease_seconds=float(os.environ.get("SCHEDULER_LEASE_SECONDS", "30")),
        max_attempts=int(os.environ.get("SCHEDULER_MAX_ATTEMPTS", "3")),
        backoff_base_seconds=float(
            os.environ.get("SCHEDULER_BACKOFF_BASE_SECONDS", "1")),
        backoff_max_seconds=float(
            os.environ.get("SCHEDULER_BACKOFF_MAX_SECONDS", "60")),
        executor_enabled=os.environ.get("SCHEDULER_EXECUTOR_ENABLED", "1") == "1",
        executor_concurrency=int(os.environ.get("SCHEDULER_EXECUTOR_CONCURRENCY", "4")),
        executor_poll_interval_seconds=float(
            os.environ.get("SCHEDULER_EXECUTOR_POLL_INTERVAL_SECONDS", "0.5")),
        renew_interval_seconds=float(
            os.environ.get("SCHEDULER_RENEW_INTERVAL_SECONDS", "10")),
    )
    return config


config = _build_config()
service = SchedulerService(config)
app.include_router(create_router(service))

executor = TaskExecutor(
    service,
    demo_handler,
    concurrency=config.executor_concurrency,
    poll_interval=config.executor_poll_interval_seconds,
    renew_interval=config.renew_interval_seconds,
)


@app.on_event("startup")
def _start_executor() -> None:
    if config.executor_enabled:
        executor.start()


@app.on_event("shutdown")
def _stop_executor() -> None:
    executor.stop()


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
