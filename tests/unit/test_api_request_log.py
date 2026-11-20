"""One structured, redacted log record per API request (no database)."""

import json
import logging
from unittest.mock import patch

import pytest

from ton_sentinel.api import create_app

PASSWORD = "log-dsn-pw-73"
FIELDS = {"event", "method", "route", "status", "duration_ms"}


def _call(app, path, query="", method="GET"):
    def start_response(status, headers):
        pass

    return b"".join(app({"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query},
                        start_response))


@pytest.fixture
def app():
    return create_app(dsn=f"postgresql://log-db-user-x9:{PASSWORD}@127.0.0.1:1/none")


def _records(caplog):
    return [json.loads(r.getMessage()) for r in caplog.records if r.name == "ton_sentinel.api"]


def test_each_request_emits_one_json_record_with_route_status_and_duration(app, caplog):
    caplog.set_level(logging.INFO, logger="ton_sentinel.api")
    with patch("psycopg.connect", side_effect=AssertionError("must not connect")):
        _call(app, "/v1/alerts", "network=devnet")
    [record] = _records(caplog)
    assert set(record) == FIELDS
    assert record["event"] == "http_request" and record["method"] == "GET"
    assert record["route"] == "/v1/alerts" and record["status"] == 400
    assert type(record["duration_ms"]) is float and record["duration_ms"] >= 0


def test_database_failure_logs_503_without_dsn_query_or_driver_text(app, caplog):
    caplog.set_level(logging.INFO, logger="ton_sentinel.api")
    _call(app, "/v1/alerts", "network=testnet&token=jetton%3Aprivate-token-55")
    _call(app, "/status")
    text = caplog.text + json.dumps(_records(caplog))
    assert [(r["route"], r["status"]) for r in _records(caplog)] == [
        ("/v1/alerts", 503), ("/status", 503)]
    for leaked in (PASSWORD, "127.0.0.1", "log-db-user-x9", "private-token-55", "network=",
                   "connection", "Error"):
        assert leaked not in text, leaked


def test_unmatched_paths_and_methods_are_not_echoed_into_logs(app, caplog):
    caplog.set_level(logging.INFO, logger="ton_sentinel.api")
    _call(app, "/admin/secret-path-91", "x=1")
    _call(app, "/v1/alerts", "network=testnet", method="POST")
    _call(app, "/v1/alerts", "network=testnet", method="X-SECRET-VERB")
    records = _records(caplog)
    assert [(r["route"], r["method"], r["status"]) for r in records] == [
        ("unmatched", "GET", 404), ("/v1/alerts", "POST", 405), ("/v1/alerts", "other", 405)]
    assert "secret-path-91" not in caplog.text and "X-SECRET-VERB" not in caplog.text


def test_log_records_are_single_line(app, caplog):
    caplog.set_level(logging.INFO, logger="ton_sentinel.api")
    _call(app, "/v1/alerts\n{\"forged\": 1}")
    [record] = caplog.records
    assert "\n" not in record.getMessage() and json.loads(record.getMessage())["route"] == "unmatched"
