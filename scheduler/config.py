"""可配置项。"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class SchedulerConfig:
    db_path: str = "scheduler.db"
    lease_seconds: float = 30.0
    max_attempts: int = 3
    backoff_base_seconds: float = 1.0
    backoff_max_seconds: float = 60.0
    # ---- 自驱执行器 ----
    executor_enabled: bool = True
    executor_concurrency: int = 4
    executor_poll_interval_seconds: float = 0.5
    # 执行期间租约续约间隔；应明显小于 lease_seconds
    renew_interval_seconds: float = 10.0
