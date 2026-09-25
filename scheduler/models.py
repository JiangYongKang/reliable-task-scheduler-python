"""任务状态与数据模型定义。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any


class TaskState(str, enum.Enum):
    """任务状态机。

    PENDING   -> RUNNING   (claim 领取)
    PENDING   -> CANCELLED (cancel 取消)
    RUNNING   -> SUCCEEDED (complete 完成)
    RUNNING   -> FAILED    (永久失败)
    RUNNING   -> PENDING   (临时失败，退避后重试 / 租约超时接管)
    RUNNING   -> CANCELLED (取消与执行竞争，取消获胜)
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def terminal(self) -> bool:
        return self in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED)


@dataclass
class Task:
    task_id: str
    idempotency_key: str
    payload: dict[str, Any]
    state: TaskState
    attempt: int
    max_attempts: int
    owner: str | None
    lease_expires_at: float | None
    not_before: float
    result: Any
    error: str | None
    created_at: float
    updated_at: float


@dataclass
class TaskEvent:
    """状态迁移历史记录。"""

    task_id: str
    from_state: str | None
    to_state: str
    reason: str
    actor: str
    at: float = field(default=0.0)
