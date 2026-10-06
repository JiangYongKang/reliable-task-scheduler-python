"""测试公共夹具。

开启 scheduler.api 日志：请求输入、状态迁移、拒绝依据均打印到 stdout。
"""
from __future__ import annotations

import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    force=True,
)
logging.getLogger("scheduler.api").setLevel(logging.DEBUG)
logging.getLogger("scheduler.executor").setLevel(logging.DEBUG)


class FakeClock:
    """可手动推进的时钟，便于模拟租约超时与退避。"""

    def __init__(self, start: float = 1_000.0) -> None:
        self.t = start

    def now(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def service(tmp_path, clock):
    from scheduler.config import SchedulerConfig
    from scheduler.service import SchedulerService

    svc = SchedulerService(
        SchedulerConfig(
            db_path=str(tmp_path / "scheduler.db"),
            lease_seconds=10.0,
            max_attempts=3,
            backoff_base_seconds=1.0,
            backoff_max_seconds=60.0,
        ),
        now_fn=clock.now,
    )
    yield svc
    svc.close()
