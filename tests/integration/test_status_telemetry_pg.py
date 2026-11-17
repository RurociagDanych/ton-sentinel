"""/status storage and last-recorded-activity telemetry on real PostgreSQL,
over HTTP, through sentinel_reader."""
import json
import threading
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel.api import create_app
from ton_sentinel.db.serving import READER_TABLES
from tests.integration.test_baseline_grid_pg import qualify
from tests.integration.test_detection import W, ready_world  # noqa: F401
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401

ISO = "to_char({} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')"


@pytest.fixture
def served(db, reader_dsn):  # noqa: F811
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _telemetry(base):
    with urlopen(base + "/status", timeout=8) as response:
        return json.loads(response.read().decode("utf-8"))


def test_storage_matches_postgres_and_is_limited_to_reader_tables(served):
    db, base = served
    ready_world(db)
    db.commit()
    before = db.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    db.commit()
    payload = _telemetry(base)
    db.commit()
    after = db.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    db.commit()
    storage = payload["telemetry"]["storage"]
    size = storage["database_size"]
    assert size["status"] == "observed" and before <= size["bytes"] <= after + 8 * 1024 * 1024
    tables = storage["largest_tables"]
    assert tables["status"] == "observed" and 1 <= len(tables["value"]) <= 5
    names = [row["table"] for row in tables["value"]]
    assert set(names) <= READER_TABLES
    sizes = [row["total_bytes"] for row in tables["value"]]
    assert sizes == sorted(sizes, reverse=True) and all(s > 0 for s in sizes)
    truth = db.execute("SELECT pg_total_relation_size(to_regclass(%s))", (names[0],)).fetchone()[0]
    db.commit()
    assert sizes[0] == truth
    assert payload["operational_domains"]["database_storage"] == "observed"
    assert payload["operational_domains"]["disk"] == "unsupported"


def test_last_recorded_activity_equals_database_maxima(served):
    db, base = served
    world, lease = ready_world(db)
    assert qualify(db, lease, world, 30 * W) is True
    db.execute("UPDATE outbox SET publication_state='acknowledged', published_at=clock_timestamp(), "
               "acknowledged_at=clock_timestamp() WHERE event_id = (SELECT min(event_id) FROM outbox)")
    db.commit()
    truth = db.execute(
        f"SELECT {ISO.format('max(created_at)')}, {ISO.format('max(published_at)')} FROM outbox"
    ).fetchone()
    stream = db.execute(f"SELECT {ISO.format('max(committed_at)')} FROM stream_batches").fetchone()[0]
    db.commit()
    assert truth[0] is not None and truth[1] is not None
    act = _telemetry(base)["telemetry"]["last_recorded_activity"]
    assert act["outbox_created"]["at"] == truth[0]
    assert act["outbox_published"]["at"] == truth[1]
    assert act["stream_commit"]["at"] == stream
    assert act["canonical_actions"] == dict(act["canonical_actions"], status="unavailable",
                                            reason="table_not_in_reader_allowlist", at=None)
    for entry in act.values():
        assert entry["meaning"] == "last_recorded_activity_not_ingestion_freshness"


def test_empty_database_is_none_recorded_and_unmeasured_stays_unmeasured(served):
    db, base = served
    telemetry = _telemetry(base)["telemetry"]
    act = telemetry["last_recorded_activity"]
    for name in ("outbox_created", "outbox_published", "stream_commit"):
        assert act[name]["status"] == "none_recorded" and act[name]["at"] is None
    # Without the probes Kafka lag and disk are not measured (not unsupported)
    assert telemetry["kafka_lag"]["status"] == "not_measured"
    assert telemetry["spark_state"]["status"] == "not_measured"
    assert telemetry["disk"]["status"] == "not_measured"
    assert telemetry["disk"]["reason"] == "database_volume_not_mounted"
    assert telemetry["spark_progress"]["status"] == "none_recorded"
    assert telemetry["maintenance"]["status"] == "none_recorded"
    assert "value" not in telemetry["storage"]["database_size"] or \
        telemetry["storage"]["database_size"].get("value") is None


def test_revoked_table_is_unavailable_not_zero(served):
    db, base = served
    db.execute("REVOKE SELECT ON outbox FROM sentinel_reader")
    db.commit()
    act = _telemetry(base)["telemetry"]["last_recorded_activity"]
    for name in ("outbox_created", "outbox_published"):
        assert act[name]["status"] == "unavailable" and act[name]["reason"] == "query_failed"
        assert act[name]["at"] is None
