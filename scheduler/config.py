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
