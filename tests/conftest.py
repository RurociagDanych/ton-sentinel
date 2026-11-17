"""Real service fixtures; never skip absent required integration services."""
import json
import os
import uuid
from pathlib import Path
import pytest

# Synthetic-v1 fixtures are test-only. The production path refuses them unless this
# exact switch is set; the test suite sets it for itself and its subprocesses. Production-path
# tests remove it with monkeypatch (tests/unit/test_live_ingest.py, test_live_ingest_pg.py).
os.environ['TON_SENTINEL_SYNTHETIC_FIXTURES'] = 'test-only'

def pytest_runtest_setup(item):
    """Refuse service bodies and fixtures during explicit offline qualification."""
    if os.environ.get('TON_SENTINEL_STATIC_ONLY') == '1' and any(
        item.get_closest_marker(name) is not None for name in ('integration', 'k3s')
    ):
        pytest.fail('BLOCKED: offline static qualification cannot run service/K3s tests; '
                    'separate authorized qualification required')


def _drop_probe_resources(settings):
    """Drop one feasibility probe's isolated schema and Kafka topic (``probe_<32 hex>`` only)."""
    import re

    import psycopg
    from kafka.admin import KafkaAdminClient
    from kafka.errors import UnknownTopicOrPartitionError
    from psycopg import sql

    from ton_sentinel.feasibility import local_dsn

    for key in ("schema", "topic"):
        if not re.fullmatch(r"probe_[0-9a-f]{32}", settings[key]):
            raise ValueError("refusing to drop a non-probe identifier")
    with psycopg.connect(local_dsn(), autocommit=True) as connection:
        connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
            sql.Identifier(settings["schema"])))
    admin = KafkaAdminClient(bootstrap_servers="kafka:9092", request_timeout_ms=15000)
    try:
        admin.delete_topics([settings["topic"]])
    except UnknownTopicOrPartitionError:
        pass                                    # the probe failed before creating it
    finally:
        admin.close()


@pytest.fixture
def probe_cleanup():
    """Register probe settings; their schema and topic are dropped after the test."""
    registered = []
    yield registered.append
    for settings in registered:
        _drop_probe_resources(settings)


@pytest.fixture
def probe_settings(probe_cleanup):
    if os.environ.get("TON_SENTINEL_LOCAL_FIXTURE") != "1":
        pytest.fail("BLOCKED: use scripts/local-check with isolated real services")
    key = "probe_" + uuid.uuid4().hex
    root = Path("/runtime") / key
    root.mkdir()
    settings = {"topic": key, "schema": key, "checkpoint": str(root / "checkpoint"),
                "evidence": str(Path("/evidence") / key),
                "fixture": json.loads(Path("tests/fixtures/feasibility.json").read_text())}
    probe_cleanup(settings)
    return settings
