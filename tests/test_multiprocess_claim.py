"""Two OS processes sharing one SQLite file must not claim the same task."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap

from scheduler.models import SubmitRequest


WORKER_SOURCE = textwrap.dedent("""
    import json, sys, time
    from scheduler.config import Settings
    from scheduler.store import TaskStore
    from scheduler.service import SchedulerService

    settings = Settings(
        db_path=sys.argv[1],
        lease_seconds=30.0, max_attempts=3, base_backoff_seconds=1.0,
        max_backoff_seconds=8.0, poll_interval_seconds=0.01,
        worker_enabled=False, worker_concurrency=1,
    )
    svc = SchedulerService(settings, TaskStore(settings.db_path), clock=time.time)
    claimed = []
    empty_polls = 0
    total = int(sys.argv[3])
    while empty_polls < 200 and len(claimed) < total:
        c = svc.claim(sys.argv[2])
        if c is None:
            empty_polls += 1
            time.sleep(0.01)
            continue
        empty_polls = 0
        claimed.append(c.task.id)
        svc.complete(c.task.id, c.token, {"by": sys.argv[2]})
    print(json.dumps(claimed))
""")


def test_two_processes_claim_disjoint_sets(settings, service):
    n = 30
    ids = [
        service.submit(SubmitRequest(task_type="echo", payload={"i": i}, owner="o")).task_id
        for i in range(n)
    ]

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = dict(os.environ, PYTHONPATH=root)
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", WORKER_SOURCE, settings.db_path, f"proc-{i}", str(n)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            text=True,
        )
        for i in range(2)
    ]
    outputs = []
    for p in procs:
        out, err = p.communicate(timeout=30)
        assert p.returncode == 0, err
        outputs.append(json.loads(out.strip()))

    a, b = outputs
    assert set(a).isdisjoint(set(b)), f"overlap: {set(a) & set(b)}"
    assert sorted(a + b) == sorted(ids)
    assert service.stats()["by_state"].get("SUCCEEDED", 0) == n
