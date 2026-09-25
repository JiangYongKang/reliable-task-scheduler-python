import os

import uvicorn
from fastapi import FastAPI

from scheduler.api import create_router
from scheduler.config import SchedulerConfig
from scheduler.service import SchedulerService

app = FastAPI()

# 保持既有公开接口行为
@app.get("/")
def read_root():
    return {"Hello": "World"}


def _build_service() -> SchedulerService:
    config = SchedulerConfig(
        db_path=os.environ.get("SCHEDULER_DB", "scheduler.db"),
        lease_seconds=float(os.environ.get("SCHEDULER_LEASE_SECONDS", "30")),
        max_attempts=int(os.environ.get("SCHEDULER_MAX_ATTEMPTS", "3")),
        backoff_base_seconds=float(
            os.environ.get("SCHEDULER_BACKOFF_BASE_SECONDS", "1")),
        backoff_max_seconds=float(
            os.environ.get("SCHEDULER_BACKOFF_MAX_SECONDS", "60")),
    )
    return SchedulerService(config)


service = _build_service()
app.include_router(create_router(service))


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
