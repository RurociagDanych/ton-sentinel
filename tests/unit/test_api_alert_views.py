"""Request boundary of the detector/alert read views, no database needed."""

import json
from unittest.mock import patch

import pytest

from ton_sentinel.api import create_app

VIEWS = ("/v1/detections", "/v1/alerts", "/v1/alerts/history")
HISTORY = "network=testnet&token=jetton%3AA&window_start=7200&detector=swap_count_spike.v1"


def _call(app, query="", path="/v1/alerts", method="GET"):
    result = {}

    def start_response(status, headers):
        result["status"] = status
        result["headers"] = dict(headers)

    body = b"".join(app({"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query},
                        start_response))
    return result["status"], result["headers"], json.loads(body)


@pytest.fixture
def app():
    return create_app(dsn="postgresql://u:pw-not-rendered@127.0.0.1:1/none")


@pytest.mark.parametrize("path", VIEWS)
@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
def test_every_new_view_rejects_non_get_before_touching_the_database(app, path, method):
    with patch("psycopg.connect", side_effect=AssertionError("must not connect")):
        status, headers, body = _call(app, HISTORY, path, method)
    assert status.startswith("405") and headers["Allow"] == "GET" and body == {"error": "read_only"}


@pytest.mark.parametrize("path", VIEWS[:2])
@pytest.mark.parametrize("query", [
    "", "network=devnet", "network=testnet&limit=0", "network=testnet&limit=1001",
    "network=testnet&limit=-1", "network=testnet&limit=abc", "network=testnet&bogus=1",
    "network=testnet&token=", "network=testnet&window_start=301", "network=testnet&network=mainnet",
    "network=testnet&token=%ZZ", "network=testnet&" + "x=1&" * 20,
    "network=testnet&token=" + "a" * 5000,
])
def test_current_views_validate_bounds_before_connecting(app, path, query):
    with patch("psycopg.connect", side_effect=AssertionError("must not connect")):
        status, _, body = _call(app, query, path)
    assert status.startswith("400") and set(body) == {"error"}


@pytest.mark.parametrize("query", [
    "", "network=testnet", HISTORY + "&limit=0", HISTORY + "&limit=1001",
    HISTORY.replace("swap_count_spike.v1", "'; DROP TABLE x;--"),
    HISTORY.replace("&detector=swap_count_spike.v1", ""), HISTORY.replace("&window_start=7200", ""),
    HISTORY + "&state=active", HISTORY.replace("7200", "7201"),
])
def test_history_requires_the_full_key_and_validates_before_connecting(app, query):
    with patch("psycopg.connect", side_effect=AssertionError("must not connect")):
        status, _, body = _call(app, query, "/v1/alerts/history")
    assert status.startswith("400") and set(body) == {"error"}


@pytest.mark.parametrize("path,query", [("/v1/detections", "network=testnet"),
                                        ("/v1/alerts", "network=testnet"),
                                        ("/v1/alerts/history", HISTORY)])
def test_database_failure_is_a_sanitized_503_without_dsn_or_driver_text(app, path, query):
    status, _, body = _call(app, query, path)
    text = json.dumps(body)
    assert status.startswith("503") and body == {"error": "unavailable"}
    assert "pw-not-rendered" not in text and "127.0.0.1" not in text
