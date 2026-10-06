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
    # 自驱执行：工作线程并发度
    worker_concurrency: int = 4
    # 自驱执行：租约续约间隔，应明显小于 lease_seconds（默认约为 1/3）
    lease_renew_interval_seconds: float = 10.0
    # 自驱执行：无任务可领取时的轮询间隔
    poll_interval_seconds: float = 0.1
