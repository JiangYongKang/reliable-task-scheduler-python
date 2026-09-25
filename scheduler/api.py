"""FastAPI routes exposing the scheduler over HTTP."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field

from .errors import SchedulerError, Unauthorized
from .models import SubmitRequest, TaskState
from .service import SchedulerService

logger = logging.getLogger("scheduler.api")


class SubmitBody(BaseModel):
    task_type: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None
    owner: str = "default"
    max_attempts: int | None = Field(default=None, ge=1, le=100)


def create_app(service: SchedulerService) -> FastAPI:
    app = FastAPI(
        title="Local Reliable Task Scheduler",
        version="0.1.0",
    )

    def _raise(err: SchedulerError) -> None:
        logger.info(
            "request rejected: code=%s http=%s task=%s message=%s",
            err.code,
            err.http_status,
            err.task_id,
            err.message,
        )
        raise HTTPException(
            status_code=err.http_status,
            detail={"code": err.code, "message": err.message, "task_id": err.task_id},
        )

    def require_management(
        x_management_token: str | None = Header(default=None),
    ) -> None:
        if x_management_token != service.settings.management_token:
            _raise(Unauthorized("missing or invalid X-Management-Token"))

    # ------------------------------------------------------------------ #
    # Preserve the pre-existing public endpoint exactly.
    # ------------------------------------------------------------------ #
    @app.get("/")
    def read_root():
        return {"Hello": "World"}

    @app.post("/tasks", status_code=201)
    def submit_task(body: SubmitBody):
        logger.info(
            "HTTP POST /tasks input: owner=%s type=%s key=%s payload=%s",
            body.owner,
            body.task_type,
            body.idempotency_key,
            body.payload,
        )
        try:
            result = service.submit(
                SubmitRequest(
                    task_type=body.task_type,
                    payload=body.payload,
                    idempotency_key=body.idempotency_key,
                    owner=body.owner,
                    max_attempts=body.max_attempts,
                )
            )
        except SchedulerError as err:
            _raise(err)
        return {
            "task_id": result.task_id,
            "created": result.created,
            "deduplicated": not result.created,
        }

    @app.get("/tasks/{task_id}")
    def get_task(
        task_id: str,
        owner: str | None = Query(default=None),
        x_owner: str | None = Header(default=None, alias="X-Owner"),
    ):
        owner = owner or x_owner
        logger.info("HTTP GET /tasks/%s input: owner=%s", task_id, owner)
        try:
            task = service.get_task(task_id, owner=owner)
        except SchedulerError as err:
            _raise(err)
        return task.to_public_dict()

    @app.get("/tasks/{task_id}/history")
    def get_history(
        task_id: str,
        owner: str | None = Query(default=None),
        x_owner: str | None = Header(default=None, alias="X-Owner"),
    ):
        owner = owner or x_owner
        logger.info("HTTP GET /tasks/%s/history input: owner=%s", task_id, owner)
        try:
            events = service.get_history(task_id, owner=owner)
        except SchedulerError as err:
            _raise(err)
        return {"task_id": task_id, "events": events}

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(
        task_id: str,
        owner: str | None = Query(default=None),
        x_owner: str | None = Header(default=None, alias="X-Owner"),
    ):
        owner = owner or x_owner
        logger.info("HTTP POST /tasks/%s/cancel input: owner=%s", task_id, owner)
        try:
            task = service.cancel(task_id, owner=owner)
        except SchedulerError as err:
            _raise(err)
        return {
            "task_id": task.id,
            "state": task.state.value,
            "cancel_requested": task.cancel_requested,
        }

    # ------------------------------------------------------------------ #
    # Management/introspection endpoints (token-protected).
    # ------------------------------------------------------------------ #
    @app.get("/internal/stats", dependencies=[Depends(require_management)])
    def stats():
        data = service.stats()
        return data

    @app.post(
        "/internal/maintenance",
        dependencies=[Depends(require_management)],
        status_code=200,
    )
    def maintenance():
        service.maintenance()
        return {"ok": True}

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app
