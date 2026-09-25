"""HTTP service entrypoint for the local reliable task scheduler.

The pre-existing ``GET /`` contract (``{"Hello": "World"}``) is preserved.
"""
from __future__ import annotations

import logging
import os

import uvicorn

logging.basicConfig(
    level=os.environ.get("SCHEDULER_LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

from scheduler.api import create_app
from scheduler.config import load_settings
from scheduler.service import build_service
from scheduler.worker import (
    PermanentTaskError,
    TransientTaskError,
    WorkerPool,
)

settings = load_settings()
service = build_service(settings)

# Built-in demo handlers. Real deployments register handlers here; unknown
# task types fail permanently (they are programming/configuration errors).
def _echo(payload, ctx):
    # Cooperative cancellation check; long work should call this periodically.
    ctx.raise_if_cancelled()
    return {"echo": payload}


def _flaky(payload, ctx):
    ctx.raise_if_cancelled()
    import random

    if random.random() < float(payload.get("fail_probability", 0.0)):
        raise TransientTaskError("simulated transient failure")
    if payload.get("permanent"):
        raise PermanentTaskError("simulated permanent failure")
    return {"ok": True, "payload": payload}


worker: WorkerPool | None = None
if settings.worker_enabled and os.environ.get("SCHEDULER_SKIP_WORKER") != "1":
    worker = WorkerPool(
        service,
        handlers={"echo": _echo, "flaky": _flaky},
        concurrency=settings.worker_concurrency,
    )
    worker.start()

app = create_app(service)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="127.0.0.1", port=port)
