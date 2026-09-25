"""错误分类：所有拒绝原因可按类型区分。"""
from __future__ import annotations


class SchedulerError(Exception):
    """调度器错误基类。"""

    code = "scheduler_error"


class TaskNotFoundError(SchedulerError):
    """未知任务。"""

    code = "task_not_found"


class InvalidStateTransitionError(SchedulerError):
    """非法状态迁移。"""

    code = "invalid_state_transition"

    def __init__(self, task_id: str, current: str, action: str) -> None:
        super().__init__(f"task {task_id}: cannot {action} from state {current}")
        self.task_id = task_id
        self.current = current
        self.action = action


class LeaseConflictError(SchedulerError):
    """租约冲突：任务已被其他执行者持有或租约已失效。"""

    code = "lease_conflict"


class NotClaimableError(SchedulerError):
    """任务当前不可领取。"""

    code = "not_claimable"
