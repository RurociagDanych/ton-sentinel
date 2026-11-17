"""V1b: the local PostgreSQL must not run as PID 1 next to a shell healthcheck.

As PID 1 the postmaster reaps every orphan in the container. A CMD-SHELL healthcheck that
exceeds its timeout loses its ``sh``; the orphaned ``pg_isready`` then exits 2 ("no response"),
the postmaster logs "untracked child process ... exited with exit code 2" and crash-restarts
all backends ("the database system is in recovery mode" for every concurrent test).
"""
from pathlib import Path

import yaml

COMPOSE = Path(__file__).resolve().parents[2] / "deploy" / "compose.yaml"


def test_postgres_runs_under_an_init_that_reaps_healthcheck_orphans():
    postgres = yaml.safe_load(COMPOSE.read_text())["services"]["postgres"]
    assert postgres.get("init") is True
    assert postgres["healthcheck"]["test"][0] == "CMD-SHELL"   # the orphan source stays covered
