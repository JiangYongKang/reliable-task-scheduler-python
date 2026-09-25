"""Scheduler configuration (environment variables, prefix SCHEDULER_)."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw not in (None, "") else default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    db_path: str = "scheduler.db"
    lease_seconds: float = 30.0
    heartbeat_interval_seconds: float = 10.0
    heartbeat_max_missed: int = 3
    max_attempts: int = 5
    base_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 60.0
    poll_interval_seconds: float = 1.0
    worker_enabled: bool = True
    worker_concurrency: int = 4
    management_token: str = "local-dev-token"


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name)
    return raw if raw not in (None, "") else default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def load_settings() -> Settings:
    """Build settings from SCHEDULER_* environment variables."""
    return Settings(
        db_path=_env_str("SCHEDULER_DB_PATH", "scheduler.db"),
        lease_seconds=_env_float("SCHEDULER_LEASE_SECONDS", 30.0),
        heartbeat_interval_seconds=_env_float(
            "SCHEDULER_HEARTBEAT_INTERVAL_SECONDS", 10.0
        ),
        heartbeat_max_missed=_env_int("SCHEDULER_HEARTBEAT_MAX_MISSED", 3),
        max_attempts=_env_int("SCHEDULER_MAX_ATTEMPTS", 5),
        base_backoff_seconds=_env_float("SCHEDULER_BASE_BACKOFF_SECONDS", 1.0),
        max_backoff_seconds=_env_float("SCHEDULER_MAX_BACKOFF_SECONDS", 60.0),
        poll_interval_seconds=_env_float("SCHEDULER_POLL_INTERVAL_SECONDS", 1.0),
        worker_enabled=_env_bool("SCHEDULER_WORKER_ENABLED", True),
        worker_concurrency=_env_int("SCHEDULER_WORKER_CONCURRENCY", 4),
        management_token=_env_str("SCHEDULER_MANAGEMENT_TOKEN", "local-dev-token"),
    )
