"""State rules and shape of the /status telemetry block (fake connection)."""

import json
from datetime import datetime, timezone
from unittest.mock import patch

from ton_sentinel.api import create_app
from ton_sentinel.db import serving


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Connection:
    """Routes a statement to canned rows by a substring; a value of Exception fails it."""
    def __init__(self, routes):
        self.routes = routes
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        self.statements.append(statement)
        for needle, rows in self.routes.items():
            if needle in statement:
                if rows is Exception:
                    raise RuntimeError("denied")
                return _Result(rows)
        return _Result([])


FULL = {
    "pg_database_size": [(123456789,)],
    "pg_total_relation_size": [("outbox", 4096), ("served_features", 1024)],
    "max(created_at)": [("2026-11-15T10:00:00Z",)],
    "max(published_at)": [("2026-11-15T10:01:00Z",)],
    "max(committed_at)": [("2026-11-15T10:02:00Z",)],
}


def _walk(value):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def test_storage_observed_with_provenance_and_scope():
    t = serving.read_telemetry(_Connection(dict(FULL)))
    size = t["storage"]["database_size"]
    assert size["status"] == "observed" and size["bytes"] == 123456789
    assert size["scope"] == "whole_database" and "pg_database_size" in size["provenance"]
    tables = t["storage"]["largest_tables"]
    assert tables["status"] == "observed"
    assert tables["value"] == [{"table": "outbox", "total_bytes": 4096},
                               {"table": "served_features", "total_bytes": 1024}]
    assert "pg_total_relation_size" in tables["provenance"]


def test_unreadable_storage_is_unavailable_with_reason_never_zero():
    routes = dict(FULL, pg_database_size=Exception, pg_total_relation_size=Exception)
    t = serving.read_telemetry(_Connection(routes))
    for part in (t["storage"]["database_size"], t["storage"]["largest_tables"]):
        assert part["status"] == "unavailable" and part["reason"] == "query_failed"
        assert part.get("bytes") is None and part.get("value") is None


def test_activity_is_labelled_last_recorded_not_ingestion_freshness():
    t = serving.read_telemetry(_Connection(dict(FULL)))
    act = t["last_recorded_activity"]
    assert act["outbox_created"]["at"] == "2026-11-15T10:00:00Z"
    assert act["outbox_published"]["at"] == "2026-11-15T10:01:00Z"
    assert act["stream_commit"]["at"] == "2026-11-15T10:02:00Z"
    for entry in act.values():
        assert entry["meaning"] == "last_recorded_activity_not_ingestion_freshness"
        assert entry["provenance"]
    assert "not ingestion freshness" in t["label"]


def test_canonical_actions_is_unavailable_because_reader_cannot_read_it():
    assert "canonical_actions" not in serving.READER_TABLES
    conn = _Connection(dict(FULL))
    t = serving.read_telemetry(conn)
    canonical = t["last_recorded_activity"]["canonical_actions"]
    assert canonical["status"] == "unavailable"
    assert canonical["reason"] == "table_not_in_reader_allowlist"
    assert not any("canonical_actions" in s for s in conn.statements)


def test_empty_tables_are_none_recorded_not_a_timestamp():
    routes = dict(FULL, **{"max(created_at)": [(None,)], "max(published_at)": [(None,)],
                           "max(committed_at)": []})
    act = serving.read_telemetry(_Connection(routes))["last_recorded_activity"]
    assert act["outbox_created"] == dict(act["outbox_created"], status="none_recorded", at=None)
    assert act["outbox_published"]["status"] == "none_recorded"
    assert act["stream_commit"]["status"] == "none_recorded"


def test_failed_activity_query_is_unavailable():
    routes = dict(FULL, **{"max(published_at)": Exception})
    act = serving.read_telemetry(_Connection(routes))["last_recorded_activity"]
    assert act["outbox_published"] == dict(act["outbox_published"], status="unavailable",
                                           reason="query_failed", at=None)
    assert act["outbox_created"]["status"] == "observed"


def test_kafka_spark_and_disk_stay_unmeasured_with_precise_reasons():
    # Without the probes, Kafka lag and disk are not measured (configuration,
    # not credentials, is the reason); Spark state store stays not measured. Maintenance and
    # rates are now read from PostgreSQL (see test_status_telemetry_completion.py).
    t = serving.read_telemetry(_Connection(dict(FULL)))
    assert t["kafka_lag"] == {
        "status": "not_measured", "value": None, "reason": "broker_probe_not_configured",
        "provenance": "not queried"}
    assert t["spark_state"] == {
        "status": "not_measured", "value": None,
        "reason": "no_persistent_checkpoint_location_in_stack", "provenance": "not queried"}
    assert t["disk"] == {
        "status": "not_measured", "value": None, "reason": "database_volume_not_mounted",
        "provenance": "not queried"}
    assert t["maintenance"]["status"] == "none_recorded"
    assert t["rates"]["status"] == "unavailable"


def test_no_zero_values_for_unmeasured_and_no_forbidden_content():
    t = serving.read_telemetry(_Connection({}))
    for node in _walk(t):
        if node.get("status") in ("not_measured", "unsupported", "unavailable", "none_recorded"):
            assert node.get("value") in (None, ) or "value" not in node
            assert node.get("bytes") is None
            assert node.get("at") is None
    text = json.dumps(t)
    assert "password" not in text.lower() and "postgresql://" not in text


def _status(routes_by_stmt):
    app = create_app(dsn="explicit-test-dsn")
    result = {}

    def start_response(status, headers, *exc):
        result["status"] = status

    with patch("ton_sentinel.api.psycopg.connect",
               return_value=_Connection(routes_by_stmt)):
        body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/status",
                             "QUERY_STRING": ""}, start_response))
    return result["status"], json.loads(body)


def test_status_exposes_telemetry_and_updates_storage_domain_only():
    routes = dict(FULL, **{"count(*), count(DISTINCT": [
        (7, 3, 1710000300, datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc))]})
    status, body = _status(routes)
    assert status == "200 OK"
    assert body["telemetry"]["storage"]["database_size"]["bytes"] == 123456789
    domains = body["operational_domains"]
    assert domains["database_storage"] == "observed"
    assert domains["disk"] == "unsupported" and domains["spark_state"] == "unsupported"
    assert body["serving_freshness"]["lag"] == {"status": "unknown", "meaning": "not_measured"}


def test_status_storage_domain_is_unavailable_when_unreadable():
    routes = dict(FULL, pg_database_size=Exception, **{"count(*), count(DISTINCT": [
        (7, 3, 1710000300, datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc))]})
    _, body = _status(routes)
    assert body["operational_domains"]["database_storage"] == "unavailable"
