"""HTTP 接口层。

日志覆盖：请求输入、状态迁移、拒绝依据与错误分类。
"""
from __future__ import annotations

import logging
from dataclasses import asdict
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from scheduler.errors import (
    InvalidStateTransitionError,
    LeaseConflictError,
    SchedulerError,
    TaskNotFoundError,
)
from scheduler.models import Task, TaskState
from scheduler.service import SchedulerService

logger = logging.getLogger("scheduler.api")


class SubmitRequest(BaseModel):
    idempotency_key: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = Field(default=None, ge=1, le=100)


class CompleteRequest(BaseModel):
    owner: str = Field(min_length=1)
    result: Any = None


class FailRequest(BaseModel):
    owner: str = Field(min_length=1)
    error: str = Field(min_length=1)
    permanent: bool = False


class CancelRequest(BaseModel):
    requester: str | None = None


def task_dict(task: Task) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "payload": task.payload,
        "state": task.state.value,
        "terminal": task.state.terminal,
        "attempt": task.attempt,
        "max_attempts": task.max_attempts,
        "owner": task.owner,
        "lease_expires_at": task.lease_expires_at,
        "not_before": task.not_before,
        "result": task.result,
        "error": task.error,
        "created_at": task.created_at,
        "updated_at": task.updated_at,
    }


def _http_error(err: SchedulerError, action: str) -> HTTPException:
    """错误分类 -> HTTP 状态码与拒绝依据。"""
    if isinstance(err, TaskNotFoundError):
        status, reason = 404, "unknown_task"
    elif isinstance(err, InvalidStateTransitionError):
        status, reason = 409, "invalid_state_transition"
    elif isinstance(err, LeaseConflictError):
        status, reason = 409, "lease_conflict"
    else:
        status, reason = 500, err.code
    logger.warning("reject action=%s code=%s reason=%s error=%s",
                   action, err.code, reason, err)
    raise HTTPException(status_code=status,
                        detail={"code": err.code, "reason": reason,
                                "message": str(err)})


def create_router(service: SchedulerService) -> APIRouter:
    router = APIRouter(prefix="/tasks")

    @router.post("")
    def submit_task(body: SubmitRequest, request: Request):
        logger.info("request POST /tasks idempotency_key=%s payload=%s",
                    body.idempotency_key, body.payload)
        try:
            task, created = service.submit(body.idempotency_key, body.payload,
                                           body.max_attempts)
        except ValueError as err:
            logger.warning("reject action=submit code=bad_input error=%s", err)
            raise HTTPException(status_code=400,
                                detail={"code": "bad_input", "message": str(err)})
        logger.info("transition task=%s -> %s created=%s", task.task_id,
                    task.state.value, created)
        return {"created": created, "task": task_dict(task)}

    @router.post("/claim")
    def claim_task(request: Request, worker_id: str = "worker"):
        logger.info("request POST /tasks/claim worker_id=%s", worker_id)
        task = service.claim(worker_id)
        if task is None:
            logger.info("claim result=None reason=no_claimable_task")
            return {"task": None}
        logger.info("transition task=%s -> %s owner=%s attempt=%s",
                    task.task_id, task.state.value, task.owner, task.attempt)
        return {"task": task_dict(task)}

    @router.post("/{task_id}/complete")
    def complete_task(task_id: str, body: CompleteRequest):
        logger.info("request POST /tasks/%s/complete owner=%s",
                    task_id, body.owner)
        try:
            task = service.complete(task_id, body.owner, body.result)
        except SchedulerError as err:
            _http_error(err, "complete")
        logger.info("transition task=%s -> %s", task_id, task.state.value)
        return {"task": task_dict(task)}

    @router.post("/{task_id}/fail")
    def fail_task(task_id: str, body: FailRequest):
        logger.info("request POST /tasks/%s/fail owner=%s permanent=%s",
                    task_id, body.owner, body.permanent)
        try:
            task = service.fail(task_id, body.owner, body.error, body.permanent)
        except SchedulerError as err:
            _http_error(err, "fail")
        logger.info("transition task=%s -> %s attempt=%s not_before=%s",
                    task_id, task.state.value, task.attempt, task.not_before)
        return {"task": task_dict(task)}

    @router.post("/{task_id}/cancel")
    def cancel_task(task_id: str, body: CancelRequest | None = None):
        logger.info("request POST /tasks/%s/cancel", task_id)
        try:
            task = service.cancel(task_id)
        except SchedulerError as err:
            _http_error(err, "cancel")
        logger.info("transition task=%s -> %s", task_id, task.state.value)
        return {"task": task_dict(task)}

    @router.get("/{task_id}")
    def get_task(task_id: str, requester: str | None = None):
        logger.info("request GET /tasks/%s requester=%s", task_id, requester)
        try:
            task = service.get(task_id, requester)
        except SchedulerError as err:
            _http_error(err, "get")
        return {"task": task_dict(task)}

    @router.get("/{task_id}/history")
    def get_history(task_id: str):
        logger.info("request GET /tasks/%s/history", task_id)
        try:
            events = service.history(task_id)
        except SchedulerError as err:
            _http_error(err, "history")
        return {"events": [asdict(e) for e in events]}

    @router.get("")
    def stats():
        return {"stats": service.stats()}

    return router
