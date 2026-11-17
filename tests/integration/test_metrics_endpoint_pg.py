"""GET /metrics over HTTP on real PostgreSQL through the reader role, incl. unreadable sources."""
import threading
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel.api import create_app
from ton_sentinel.db.serving import READER_TABLES
from tests.integration.test_detection import ready_world  # noqa: F401
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401
from tests.unit.test_metrics_endpoint import family_of, parse, values


def _serve(dsn):
    server = make_server("127.0.0.1", 0, create_app(dsn=dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


@pytest.fixture
def served(db, reader_dsn):  # noqa: F811
    assert "user=rdr_" in reader_dsn  # a member of sentinel_reader, never the admin role
    server, thread, base = _serve(reader_dsn)
    try:
        yield db, base
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def _scrape(base):
    with urlopen(base + "/metrics", timeout=10) as response:
        assert response.headers["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"
        return parse(response.read().decode("utf-8"))


def test_metrics_match_the_database_through_the_reader_role(served):
    db, base = served
    ready_world(db)
    db.commit()
    types, _, samples = _scrape(base)
    db.commit()
    expected_rows = db.execute("SELECT count(*) FROM served_features").fetchone()[0]
    expected_windows = db.execute(
        "SELECT count(DISTINCT (network, window_start, window_end)) FROM served_features"
    ).fetchone()[0]
    pending = db.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()[0]
    repair = db.execute(
        "SELECT count(*) FROM dirty_windows WHERE status = 'pending'").fetchone()[0]
    db.commit()
    assert expected_rows > 0
    for source in ("database", "status_query", "telemetry", "pipeline"):
        assert values(samples, "ton_sentinel_source_up", source=source) == [1.0], source
    assert values(samples, "ton_sentinel_serving_feature_rows") == [float(expected_rows)]
    assert values(samples, "ton_sentinel_serving_windows") == [float(expected_windows)]
    assert values(samples, "ton_sentinel_outbox_pending") == [float(pending)]
    assert values(samples, "ton_sentinel_repair_pending") == [float(repair)]
    assert sum(values(samples, "ton_sentinel_served_rows")) == float(expected_rows)
    assert values(samples, "ton_sentinel_database_size_bytes")[0] > 0
    tables = {ls["table"] for n, ls, _ in samples if n == "ton_sentinel_table_size_bytes"}
    assert tables and tables <= READER_TABLES
    assert {ls["network"] for n, ls, _ in samples if n == "ton_sentinel_pipeline_backlog"} == {
        "mainnet", "testnet"}
    assert "ton_sentinel_coverage_ratio" not in types
    assert len(samples) <= 200
    for name, _, _ in samples:
        assert family_of(name, types) in types


def test_unreadable_outbox_is_omitted_and_flagged_never_zero(served):
    db, base = served
    ready_world(db)
    db.execute("REVOKE SELECT ON outbox FROM sentinel_reader")
    db.commit()
    types, _, samples = _scrape(base)
    names = {n for n, _, _ in samples}
    assert "ton_sentinel_outbox_pending" not in names
    assert "ton_sentinel_outbox_oldest_pending_age_seconds" not in names
    assert values(samples, "ton_sentinel_source_up", source="status_query") == [0.0]
    assert values(samples, "ton_sentinel_source_up", source="telemetry") == [0.0]
    assert values(samples, "ton_sentinel_source_up", source="database") == [1.0]
    assert values(samples, "ton_sentinel_serving_feature_rows")[0] > 0
    assert values(samples, "ton_sentinel_repair_pending") != []
    assert not any(ls.get("activity", "").startswith("outbox")
                   for n, ls, _ in samples if n == "ton_sentinel_last_activity_timestamp_seconds")


def test_unreadable_database_exports_no_database_gauge():
    server, thread, base = _serve("postgresql://nobody@127.0.0.1:1/none")
    try:
        types, _, samples = _scrape(base)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
    for source in ("database", "status_query", "telemetry", "pipeline"):
        assert values(samples, "ton_sentinel_source_up", source=source) == [0.0]
    assert not any(n.startswith("ton_sentinel_serving_") or n.endswith("_pending")
                   or "database_size" in n for n in types)
