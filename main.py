import contextlib
import logging
import os

import uvicorn
from fastapi import FastAPI

from scheduler.api import create_router
from scheduler.config import SchedulerConfig
from scheduler.executor import ExecutionEngine
from scheduler.service import SchedulerService

logging.basicConfig(
    level=os.environ.get("SCHEDULER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


def _build_service() -> SchedulerService:
    config = SchedulerConfig(
        db_path=os.environ.get("SCHEDULER_DB", "scheduler.db"),
        lease_seconds=float(os.environ.get("SCHEDULER_LEASE_SECONDS", "30")),
        max_attempts=int(os.environ.get("SCHEDULER_MAX_ATTEMPTS", "3")),
        backoff_base_seconds=float(
            os.environ.get("SCHEDULER_BACKOFF_BASE_SECONDS", "1")),
        backoff_max_seconds=float(
            os.environ.get("SCHEDULER_BACKOFF_MAX_SECONDS", "60")),
        worker_concurrency=int(os.environ.get("SCHEDULER_WORKER_CONCURRENCY", "4")),
        lease_renew_interval_seconds=float(
            os.environ.get("SCHEDULER_LEASE_RENEW_INTERVAL_SECONDS", "10")),
        poll_interval_seconds=float(
            os.environ.get("SCHEDULER_POLL_INTERVAL_SECONDS", "0.1")),
    )
    return SchedulerService(config)


service = _build_service()
# SCHEDULER_ENABLE_EXECUTOR=false 可退回纯 API 模式（既有手工 claim/complete 行为）
engine: ExecutionEngine | None = None
if os.environ.get("SCHEDULER_ENABLE_EXECUTOR", "true").lower() in ("1", "true", "yes"):
    engine = ExecutionEngine(service)

    def _default_handler(payload, ctx):
        """内置执行体：原样回显 payload，便于冒烟与无注册类型的默认处理。"""
        return {"echo": payload}

    engine.register_handler("default", _default_handler)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    if engine is not None:
        engine.start()
    try:
        yield
    finally:
        if engine is not None:
            engine.stop(timeout=5.0)


app = FastAPI(lifespan=lifespan)

# 保持既有公开接口行为
@app.get("/")
def read_root():
    return {"Hello": "World"}


app.include_router(create_router(service))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
