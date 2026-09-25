"""Shared fixtures and logging setup.

Logs (printed by the service layer) include:
  * request input:        submit/cancel/claim inputs
  * state transitions:    PENDING -> RUNNING -> SUCCEEDED/RETRY_WAIT/...
  * rejection reasons:    explicit error code for every denied operation
"""
from __future__ import annotations

import logging
import os
import tempfile

import pytest

from scheduler.config import Settings
from scheduler.service import SchedulerService
from scheduler.store import TaskStore


@pytest.fixture(autouse=True)
def _show_scheduler_logs(caplog):
    caplog.set_level(logging.INFO, logger="scheduler")


class FakeClock:
    """Deterministic controllable clock shared by service and tests."""

    def __init__(self, start: float = 1000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        db_path=str(tmp_path / "scheduler.db"),
        lease_seconds=30.0,
        heartbeat_interval_seconds=5.0,
        max_attempts=3,
        base_backoff_seconds=1.0,
        max_backoff_seconds=8.0,
        poll_interval_seconds=0.02,
        worker_enabled=False,
        worker_concurrency=2,
    )


@pytest.fixture()
def store(settings):
    s = TaskStore(settings.db_path)
    yield s
    s.close()


@pytest.fixture()
def service(settings, store, clock):
    return SchedulerService(settings, store, clock=clock)
