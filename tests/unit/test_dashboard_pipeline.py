"""Dashboard API unit tests: /v1/pipeline validation, response shape, unavailable/not_measured, safety."""

import json
import re
from unittest.mock import patch

import pytest

from ton_sentinel import api, dashboard_payload
from ton_sentinel.db.serving import READER_TABLES, read_pipeline

PROV = dashboard_payload.build_provenance(qualification=[], scored=0)
SECTIONS = {"outbox", "dirty_windows_pending_by_role", "scoring_backlog", "stream_progress",
            "ingestion_freshness", "kafka_lag"}


class Conn:
    """Fake connection: statements naming a table in ``fail`` raise; others return ``rows``."""
    def __init__(self, fail=(), rows=None):
        self.fail, self.rows, self.sql = set(fail), {"stream_batches": [], **(rows or {})}, []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        self.sql.append(statement)
        for table in self.fail:
            if re.search(rf"\b{table}\b", statement):
                raise RuntimeError("permission denied")
        self.current = next((v for k, v in self.rows.items() if re.search(rf"\b{k}\b", statement)),
                            [] if "GROUP BY" in statement else [(0, None)])
        return self

    def fetchall(self):
        return self.current

    def fetchone(self):
        return self.current[0] if self.current else None


def call(query, method="GET"):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    body = b"".join(api.create_app(dsn="x")({"REQUEST_METHOD": method, "PATH_INFO": "/v1/pipeline",
                                             "QUERY_STRING": query}, start_response))
    return out["status"], json.loads(body)


def run(query, pipeline):
    with patch("ton_sentinel.api.psycopg.connect", return_value=Conn()), \
         patch("ton_sentinel.api.read_pipeline", return_value=pipeline) as mock, \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = call(query)
    return status, body, mock


def pipeline_value(**over):
    value = {name: {"status": "observed", "value": 0} for name in SECTIONS}
    value.update(over)
    return value


@pytest.mark.parametrize("query", ["", "network=devnet", "network=testnet&from=1",
                                   "network=testnet&network=mainnet", "network=testnet&x=1"])
def test_pipeline_network_required_and_params_strict(query):
    status, body, mock = run(query, pipeline_value())
    assert status == "400 Bad Request" and body == {"error": "invalid_query"}
    mock.assert_not_called()


def test_pipeline_only_get_is_served():
    assert call("network=testnet", method="POST")[0].startswith("405")


def test_response_shape_carries_provenance_banner_and_label():
    status, body, mock = run("network=testnet", pipeline_value())
    assert status == "200 OK"
    assert set(body) == {"network", "pipeline", "label", "honesty_banner", "provenance"}
    assert mock.call_args.kwargs == {"network": "testnet"}
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert body["provenance"]["certification_state"] == "not_certified"
    assert "not" in body["label"].lower() and "unavailable" in body["label"].lower()


def test_every_section_has_status_and_unavailable_has_reason_never_zero():
    result = read_pipeline(Conn(), network="testnet")
    assert set(result) == SECTIONS
    for section in result.values():
        assert section["status"] in ("observed", "unavailable", "not_measured", "none_recorded")
    broken = read_pipeline(Conn(fail=["outbox", "dirty_windows", "detector_current",
                                      "served_features", "stream_batches"]), network="testnet")
    for name in ("outbox", "dirty_windows_pending_by_role", "stream_progress"):
        assert broken[name]["status"] == "unavailable" and broken[name]["value"] is None
        assert broken[name]["reason"]
    backlog = broken["scoring_backlog"]["value"]
    assert backlog["unscored_served_windows"]["status"] == "unavailable"
    assert backlog["stale_results"]["status"] == "unavailable"
    assert backlog["stale_results"]["value"] is None


def test_scoring_backlog_counts_are_independent():
    result = read_pipeline(Conn(fail=["served_features"], rows={"detector_current": [(4, 90.0)]}),
                           network="testnet")
    backlog = result["scoring_backlog"]["value"]
    assert backlog["unscored_served_windows"]["status"] == "unavailable"
    assert backlog["unscored_served_windows"]["value"] is None
    assert backlog["stale_results"] == {"status": "observed", "value": 4,
                                        "oldest_age_seconds": 90.0}


def test_freshness_and_lag_are_not_measured_with_reason():
    result = read_pipeline(Conn(), network="mainnet")
    for name in ("ingestion_freshness", "kafka_lag"):
        assert result[name]["status"] == "not_measured"
        assert result[name]["value"] is None and result[name]["reason"]


def test_stream_progress_exposes_only_timestamp_and_count():
    stamp = "2026-01-02T03:04:05Z"
    result = read_pipeline(Conn(rows={"stream_batches": [(stamp, 7)]}), network="testnet")
    assert result["stream_progress"]["value"] == {
        "committed_at": "2026-01-02T03:04:05Z", "record_count": 7}
    selected = [s for s in Conn().sql]
    conn = Conn()
    read_pipeline(conn, network="testnet")
    stream_sql = next(s for s in conn.sql if "stream_batches" in s)
    for forbidden in ("offset_metadata", "environment_id", "query_id", "topic", "record_digest"):
        assert forbidden not in stream_sql
    assert selected == []


def test_reads_only_reader_tables():
    conn = Conn()
    read_pipeline(conn, network="testnet")
    tables = set()
    for statement in conn.sql:
        tables |= set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+)\b(?!\()", statement))
    assert tables and tables <= READER_TABLES, tables - READER_TABLES


def test_read_pipeline_validates_network_before_querying():
    conn = Conn()
    with pytest.raises(ValueError):
        read_pipeline(conn, network="devnet")
    assert conn.sql == []


def test_safety_filter_strips_forbidden_keys_and_refuses_forbidden_values():
    dirty = pipeline_value(stream_progress={"status": "observed", "lease_owner": "w1",
                                            "offset_metadata": {"a": 1}, "value": 1})
    status, body, _ = run("network=testnet", dirty)
    assert status == "200 OK"
    text = json.dumps(body)
    assert "lease_owner" not in text and "offset_metadata" not in text
    hostile = pipeline_value(outbox={"status": "unavailable", "value": None,
                                     "reason": "postgresql://u:p@h/db"})
    status, body, _ = run("network=testnet", hostile)
    assert status == "503 Service Unavailable" and body == {"error": "unavailable"}
