"""Bounded keyset cursors for the detector/alert views (no database)."""

import base64
import json
from unittest.mock import MagicMock, patch

import pytest

from ton_sentinel.api import create_app

NET = "testnet"


def _call(app, query, path):
    result = {}

    def start_response(status, headers):
        result["status"] = status

    body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": path, "QUERY_STRING": query},
                        start_response))
    return result["status"], json.loads(body)


@pytest.fixture
def app():
    return create_app(dsn="postgresql://u:cursor-pw-41@127.0.0.1:1/none")


def _doc(token, window_start, detector):
    return {"network": NET, "token": token, "direction": "both", "window_start": window_start,
            "detector": detector}


class Recorder:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    def __call__(self, connection, **kwargs):
        self.calls.append(kwargs)
        return list(self.rows)


def _with(reader_name, recorder, app, query, path):
    with patch("psycopg.connect", MagicMock()), patch(f"ton_sentinel.api.{reader_name}", recorder):
        return _call(app, query, path)


@pytest.mark.parametrize("path,reader,make", [
    ("/v1/alerts", "read_alerts", lambda t, w, d: _doc(t, w, d)),
    ("/v1/detections", "read_detector_results", lambda t, w, d: _doc(t, w, d)),
])
def test_truncated_current_view_returns_a_cursor_that_continues_after_the_last_key(
        app, path, reader, make):
    rows = [make("jetton:A", 600, "a.v1"), make("jetton:A", 600, "b.v1"), make("jetton:B", 0, "a.v1")]
    first = Recorder(rows)
    status, body = _with(reader, first, app, "network=testnet&limit=2", path)
    name = path.rsplit("/", 1)[1]
    assert status.startswith("200") and body[name] == rows[:2] and body["truncated"] is True
    assert first.calls[0]["after"] is None and first.calls[0]["limit"] == 2
    cursor = body["next_cursor"]
    second = Recorder(rows[2:])
    status, body = _with(reader, second, app, f"network=testnet&limit=2&cursor={cursor}", path)
    assert status.startswith("200") and body[name] == rows[2:] and body["truncated"] is False
    assert "next_cursor" not in body
    assert second.calls[0]["after"] == ("jetton:A", 600, "b.v1", "both")


def test_history_cursor_continues_after_the_last_revision(app):
    query = "network=testnet&token=jetton%3AA&window_start=600&detector=a.v1&limit=1"
    first = Recorder([{"alert_revision": 1}, {"alert_revision": 2}])
    status, body = _with("read_alert_history", first, app, query, "/v1/alerts/history")
    assert status.startswith("200") and body["history"] == [{"alert_revision": 1}]
    second = Recorder([{"alert_revision": 2}])
    status, body = _with("read_alert_history", second, app,
                         query + "&cursor=" + body["next_cursor"], "/v1/alerts/history")
    assert status.startswith("200") and body["history"] == [{"alert_revision": 2}]
    assert second.calls[0]["after"] == 1 and "next_cursor" not in body


def _issue(app, path, query, reader, rows):
    return _with(reader, Recorder(rows), app, query, path)[1]["next_cursor"]


def _forge(payload):
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def test_cursor_is_bound_to_its_view_and_filters_and_rejected_before_connecting(app):
    rows = [_doc("jetton:A", 600, "a.v1"), _doc("jetton:A", 600, "b.v1")]
    cursor = _issue(app, "/v1/alerts", "network=testnet&limit=1", "read_alerts", rows)
    bad = [
        ("/v1/detections", f"network=testnet&limit=1&cursor={cursor}"),
        ("/v1/alerts", f"network=mainnet&limit=1&cursor={cursor}"),
        ("/v1/alerts", f"network=testnet&token=jetton%3AA&limit=1&cursor={cursor}"),
        ("/v1/alerts", "network=testnet&cursor=" + cursor[:-2]),
        ("/v1/alerts", "network=testnet&cursor=!!"),
        ("/v1/alerts", "network=testnet&cursor="),
        ("/v1/alerts", "network=testnet&cursor=" + "A" * 5000),
        ("/v1/alerts", "network=testnet&cursor=" + _forge(
            {"v": 1, "e": "alerts", "f": {"network": "testnet", "token": None,
                                          "window_start": None},
             "k": ["jetton:A", 601, "a.v1", "both"]})),
        ("/v1/alerts", "network=testnet&cursor=" + _forge(
            {"v": 1, "e": "alerts", "f": {"network": "testnet", "token": None,
                                          "window_start": None},
             "k": ["jetton:A", 600, "'; DROP TABLE x;--", "both"]})),
        ("/v1/alerts", "network=testnet&cursor=" + _forge(
            {"v": 2, "e": "alerts", "f": {"network": "testnet", "token": None,
                                          "window_start": None},
             "k": ["jetton:A", 600, "a.v1", "both"]})),
    ]
    for path, query in bad:
        with patch("psycopg.connect", side_effect=AssertionError("must not connect")):
            status, body = _call(app, query, path)
        assert status.startswith("400") and body == {"error": "invalid_query"}, (path, query)


def test_history_cursor_rejects_a_non_positive_or_foreign_revision(app):
    base = "network=testnet&token=jetton%3AA&window_start=600&detector=a.v1"
    filters = {"network": "testnet", "token": "jetton:A", "window_start": 600, "detector": "a.v1"}
    for key in (0, -1, "1", True, 2 ** 63):
        cursor = _forge({"v": 1, "e": "history", "f": filters, "k": key})
        with patch("psycopg.connect", side_effect=AssertionError("must not connect")):
            status, _ = _call(app, f"{base}&cursor={cursor}", "/v1/alerts/history")
        assert status.startswith("400"), key
    other = _forge({"v": 1, "e": "history", "f": {**filters, "detector": "b.v1"}, "k": 1})
    with patch("psycopg.connect", side_effect=AssertionError("must not connect")):
        status, _ = _call(app, f"{base}&cursor={other}", "/v1/alerts/history")
    assert status.startswith("400")
