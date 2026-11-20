"""V1b: the local PostgreSQL must not run as PID 1 next to a shell healthcheck."""

from pathlib import Path

import yaml

COMPOSE = Path(__file__).resolve().parents[2] / "deploy" / "compose.yaml"


def test_postgres_runs_under_an_init_that_reaps_healthcheck_orphans():
    postgres = yaml.safe_load(COMPOSE.read_text())["services"]["postgres"]
    assert postgres.get("init") is True
    assert postgres["healthcheck"]["test"][0] == "CMD-SHELL"
