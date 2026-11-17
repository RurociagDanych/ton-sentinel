"""/status pending outbox and repair equal PostgreSQL, over real HTTP."""
import json
import os
import threading
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest

from ton_sentinel.api import create_app
from tests.integration.test_baseline_grid_pg import qualify
from tests.integration.test_detection import W, ready_world  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401


def _dsn(db_connection):
    return psycopg.conninfo.make_conninfo(
        os.environ["TON_SENTINEL_TEST_DSN"], options=f"-c search_path={db_connection.schema_name}")


@pytest.fixture
def served(db):
    server = make_server("127.0.0.1", 0, create_app(dsn=_dsn(db)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _status(base):
    with urlopen(base + "/status", timeout=8) as response:
        return json.loads(response.read().decode("utf-8"))


def _truth(db):
    db.commit()
    outbox = db.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
    repair = db.execute("SELECT count(*) FROM dirty_windows WHERE status='pending'").fetchone()[0]
    db.commit()
    return outbox, repair


def test_empty_database_reports_observed_zero(served):
    db, base = served
    payload = _status(base)
    assert payload["pending_work"]["outbox"] == {
        "status": "observed", "pending": 0, "oldest_pending_age_seconds": None}
    assert payload["pending_work"]["repair"] == {"status": "observed", "pending_windows": 0}


def test_pending_counts_equal_postgres_after_production_transitions(served):
    db, base = served
    world, lease = ready_world(db)
    # qualifying one more window dirties its dependents; nothing promotes them afterwards
    assert qualify(db, lease, world, 30 * W) is True
    outbox, repair = _truth(db)
    assert outbox > 0 and repair > 0, (outbox, repair)  # the world really left work pending
    payload = _status(base)
    assert payload["pending_work"]["outbox"]["pending"] == outbox
    assert payload["pending_work"]["outbox"]["oldest_pending_age_seconds"] >= 0
    assert payload["pending_work"]["repair"]["pending_windows"] == repair
    assert payload["operational_domains"]["outbox"] == "observed"
    assert payload["operational_domains"]["repair"] == "observed"
    # The Spark position is read from stream_batches; watermark stays unmeasured
    assert payload["operational_domains"]["spark_checkpoint"] == "observed"
    assert payload["operational_domains"]["spark_watermark"] == "unsupported"
    # acknowledge everything exactly as outbox.py does: the count follows the database to zero
    db.execute("UPDATE outbox SET publication_state='acknowledged', published_at=clock_timestamp(), "
               "acknowledged_at=clock_timestamp() WHERE publication_state<>'acknowledged'")
    db.commit()
    after = _status(base)["pending_work"]["outbox"]
    assert after == {"status": "observed", "pending": 0, "oldest_pending_age_seconds": None}


def test_missing_table_is_unavailable_not_zero(served):
    db, base = served
    db.execute("DROP TABLE dirty_windows CASCADE")  # scoped to this test's private schema
    db.commit()
    payload = _status(base)
    assert payload["pending_work"]["repair"] == {"status": "unavailable"}
    assert payload["operational_domains"]["repair"] == "unavailable"
    assert payload["pending_work"]["outbox"]["status"] == "observed"
