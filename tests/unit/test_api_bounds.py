"""Focused request-boundary regressions for the feature read API."""

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from ton_sentinel.api import create_app
from ton_sentinel.db.serving import read_features, read_windows


def _call(app, query="", path="/v1/features", method="GET"):
    result = {}

    def start_response(status, headers):
        result["status"] = status
        result["headers"] = dict(headers)

    body = b"".join(app({"REQUEST_METHOD": method, "PATH_INFO": path,
                        "QUERY_STRING": query}, start_response))
    return result["status"], json.loads(body)


def test_status_reports_database_availability_without_claiming_readiness():
    class Result:
        def fetchone(self):
            return (7, 3, 1710000300, datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc))

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def transaction(self):
            return self

        def execute(self, statement, parameters=None):
            return Result()

    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", return_value=Connection()):
        status, body = _call(app, path="/status")

    assert status == "200 OK"
    assert body["status"] == "ok"
    assert body["status_meaning"] == "database_query_succeeded"
    assert body["status_query"] == "available"
    assert body["feature_rows"] == 7
    assert body["windows"] == 3
    assert body["serving_freshness"] == {
        "status": "observed",
        "scope": "global_all_scopes",
        "latest_window_end": 1710000300,
        "latest_serving_write_at": "2024-03-09T16:05:00Z",
        "lag": {"status": "unknown", "meaning": "not_measured"},
        "limitations": (
            "Serving-row observations only; not pipeline/source freshness or per-scope freshness. "
            "The two maxima are independent and may describe different rows."
        ),
    }
    assert body["availability"] == {"process": "available", "database": "available"}
    assert body["readiness"] == "unknown"
    assert body["source"] == {"status": "unknown"}
    assert body["coverage"] == {"status": "unknown"}
    assert body["backup"] == "not_configured"
    # Outbox and repair are read from PostgreSQL; this fake answers neither, so they are
    # unavailable (never zero). spark_checkpoint, maintenance and rates are read
    # from PostgreSQL too and the fake answers none of them; disk needs the volume probe.
    assert body["operational_domains"] == {
        "outbox": "unavailable", "repair": "unavailable",
        "spark_checkpoint": "unavailable", "spark_watermark": "unsupported",
        "spark_state": "unsupported", "database_storage": "unavailable",
        "disk": "unsupported", "maintenance": "unavailable", "rates": "unavailable"}
    assert body["health_endpoint_meaning"] == "process_and_database_connectivity_only"


def test_status_database_failure_is_sanitized_and_not_reported_as_zero():
    secret_detail = "postgres://private-user:private-password@private-host/private-db"
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", side_effect=RuntimeError(secret_detail)):
        status, body = _call(app, path="/status")

    assert status == "503 Service Unavailable"
    assert body["status"] == "unavailable"
    assert body["status_meaning"] == "database_query_unavailable"
    assert body["status_query"] == "unavailable"
    assert body["availability"] == {"process": "available", "database": "unavailable"}
    assert "feature_rows" not in body
    assert "windows" not in body
    assert body["serving_freshness"]["status"] == "unavailable"
    assert body["serving_freshness"]["latest_window_end"] is None
    assert body["serving_freshness"]["latest_serving_write_at"] is None
    assert body["readiness"] == "unknown"
    assert body["source"] == {"status": "unknown"}
    assert body["coverage"] == {"status": "unknown"}
    assert body["backup"] == "not_configured"
    assert secret_detail not in json.dumps(body)
    assert "explicit-test-dsn" not in json.dumps(body)


@pytest.mark.parametrize(
    ("aggregate", "expected_status", "expected_end", "expected_write"),
    [
        ((0, 0, None, None), "empty", None, None),
        ((2, 1, 1710000300, datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc)),
         "observed", 1710000300, "2024-03-09T16:05:00Z"),
    ],
)
def test_status_observations_reach_wsgi_json_and_demo(aggregate, expected_status,
                                                       expected_end, expected_write):
    class Result:
        def fetchone(self):
            return aggregate

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def transaction(self):
            return self

        def execute(self, *_):
            return Result()

    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", return_value=Connection()):
        status, body = _call(app, path="/status")
        assert status == "200 OK"
        assert body["serving_freshness"]["status"] == expected_status
        assert body["serving_freshness"]["latest_window_end"] == expected_end
        assert body["serving_freshness"]["latest_serving_write_at"] == expected_write
        assert body["serving_freshness"]["scope"] == "global_all_scopes"
        assert body["serving_freshness"]["lag"] == {
            "status": "unknown", "meaning": "not_measured"
        }

        result = {}

        def start_response(response_status, headers):
            result["status"] = response_status

        document = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/"}, start_response))
    rendered = document.decode("utf-8")
    assert result["status"] == "200 OK"
    assert expected_status in rendered
    assert "global_all_scopes" in rendered
    assert str(expected_end) in rendered if expected_end is not None else "unavailable" in rendered
    assert expected_write in rendered if expected_write is not None else "unavailable" in rendered
    assert "unknown" in rendered and "not measured" in rendered
    assert "not pipeline/source freshness or per-scope freshness" in rendered
    assert "independent and may describe different rows" in rendered


def test_status_query_failure_renders_freshness_unavailable():
    app = create_app(dsn="explicit-test-dsn")

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def transaction(self):
            return self

        def execute(self, *_):
            raise RuntimeError("private query details")

    with patch("ton_sentinel.api.psycopg.connect", return_value=Connection()):
        status, body = _call(app, path="/status")
        assert status == "503 Service Unavailable"
        assert body["serving_freshness"]["status"] == "unavailable"
        result = {}

        def start_response(response_status, headers):
            result["status"] = response_status

        document = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/"}, start_response))
    rendered = document.decode("utf-8")
    assert result["status"] == "503 Service Unavailable"
    assert "unavailable" in rendered
    assert "private query details" not in rendered


def test_health_response_keeps_its_existing_limited_shape():
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def transaction(self):
            return self

        def execute(self, *_):
            return None

    with patch("ton_sentinel.api.psycopg.connect", return_value=Connection()):
        status, body = _call(create_app(dsn="explicit-test-dsn"), path="/health")
    assert (status, body) == ("200 OK", {"status": "ok"})


@pytest.mark.parametrize("path", ["/health", "/status", "/", "/v1/features", "/v1/windows"])
def test_every_read_path_sets_transaction_read_only_before_other_sql(path):
    class Result:
        def fetchone(self):
            return (7, 3, 1710000300, datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc))

        def fetchall(self):
            return []

    class Connection:
        def __init__(self):
            self.statements = []

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def transaction(self):
            return self

        def execute(self, statement, parameters=None):
            self.statements.append(statement)
            return Result()

    connection = Connection()
    result = {}

    def start_response(status, _headers):
        result["status"] = status

    with patch("ton_sentinel.api.psycopg.connect", return_value=connection):
        body = b"".join(create_app(dsn="explicit-test-dsn")(
            {"REQUEST_METHOD": "GET", "PATH_INFO": path, "QUERY_STRING": ""},
            start_response,
        ))

    assert result["status"] == "200 OK"
    assert connection.statements[:2] == [
        "SET TRANSACTION READ ONLY",
        "SELECT set_config('statement_timeout', %s, true)",
    ]
    assert len(connection.statements) >= 3
    assert body


@pytest.mark.parametrize("query", [
    "network=", "token=", "limit=0", "limit=-1", "limit=1001", "limit=99999999999999999999",
    "window_start=9223372036854775808", "network=testnet&network=mainnet",
    "%6Eetwork=testnet&network=mainnet", "unknown=x", "=x", "network=testnet&",
    "&network=testnet", "network=testnet&x=1&y=2&z=3&u=4&v=5",
    "x" * 4097,
    "token=%", "token=%2", "token=%GG", "%GG=x", "token=%FF",
    "token=%C3%28", "token=%C0%AF", "token=%ED%A0%80",
])
def test_invalid_queries_are_rejected_before_database_connect(query):
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect") as connect:
        status, body = _call(app, query)
    assert status == "400 Bad Request"
    assert body == {"error": "invalid_query"} or body == {"error": "invalid_network"}
    connect.assert_not_called()


def test_read_features_adds_exact_token_filter_and_bound_limit():
    class Result:
        def fetchall(self):
            return []

    class Connection:
        def execute(self, statement, parameters):
            self.statement = statement
            self.parameters = parameters
            return Result()

    connection = Connection()
    assert read_features(connection, token="JETTON:A+B", limit=17) == []
    assert "token=%s" in connection.statement
    assert connection.statement.endswith("ORDER BY network, window_start, window_end, token LIMIT %s")
    assert connection.parameters == ["JETTON:A+B", 17]


def test_read_features_uses_parameterized_stable_keyset_and_fetches_one_extra():
    class Result:
        def fetchall(self):
            return []

    class Connection:
        def execute(self, statement, parameters):
            self.statement = statement
            self.parameters = parameters
            return Result()

    connection = Connection()
    key = ("testnet", 300, 600, "JETTON:%ą")
    assert read_features(connection, network="testnet", limit=2, after=key,
                         fetch_extra=True) == []
    assert "(network, window_start, window_end, token) > (%s, %s, %s, %s)" in connection.statement
    assert connection.statement.endswith("ORDER BY network, window_start, window_end, token LIMIT %s")
    assert "JETTON:%ą" not in connection.statement
    assert connection.parameters == ["testnet", *key, 3]


@pytest.mark.parametrize("cursor", ["!", "A", "_", "x" * 4097])
def test_invalid_or_cross_filter_cursor_is_rejected_before_database_connect(cursor):
    from urllib.parse import urlencode

    app = create_app(dsn="explicit-test-dsn")
    for params in ({"cursor": cursor}, {"network": "testnet", "cursor": cursor}):
        with patch("ton_sentinel.api.psycopg.connect") as connect:
            status, body = _call(app, urlencode(params))
        assert status == "400 Bad Request"
        assert body == {"error": "invalid_query"}
        connect.assert_not_called()


@pytest.mark.parametrize("filters,key", [
    ({"network": "testnet"}, ("mainnet", 0, 300, "T")),
    ({"window_start": 300}, ("testnet", 0, 300, "T")),
    ({"window_end": 600}, ("testnet", 0, 300, "T")),
    ({"token": "T"}, ("testnet", 0, 300, "U")),
    ({}, ("testnet", 9223372036854775808, 9223372036854776108, "T")),
])
def test_cursor_key_bounds_and_filter_consistency_fail_before_connect(filters, key):
    from urllib.parse import urlencode
    from ton_sentinel.api import _encode_cursor

    complete = dict.fromkeys(("network", "window_start", "window_end", "token"))
    complete.update(filters)
    cursor = _encode_cursor(complete, key)
    with patch("ton_sentinel.api.psycopg.connect") as connect:
        status, body = _call(create_app(dsn="explicit-test-dsn"),
                             urlencode({**filters, "cursor": cursor}))
    assert (status, body) == ("400 Bad Request", {"error": "invalid_query"})
    connect.assert_not_called()


@pytest.mark.parametrize("limit", [0, -1, 1001, True, 1.5])
def test_database_reader_rejects_unbounded_limits(limit):
    class Connection:
        def execute(self, *_):
            pytest.fail("invalid limit reached SQL")

    with pytest.raises(ValueError):
        read_features(Connection(), limit=limit)


def test_read_windows_uses_parameterized_filters_and_stable_window_keyset():
    class Result:
        def fetchall(self):
            return []

    class Connection:
        def execute(self, statement, parameters):
            self.statement = statement
            self.parameters = parameters
            return Result()

    connection = Connection()
    key = ("testnet", 300, 600)
    assert read_windows(connection, network="testnet", token="JETTON:A+B", limit=2,
                        after=key, fetch_extra=True) == []
    assert "token=%s" in connection.statement
    assert "(network, window_start, window_end) > (%s, %s, %s)" in connection.statement
    assert "GROUP BY network, window_start, window_end" in connection.statement
    assert connection.statement.endswith("ORDER BY network, window_start, window_end LIMIT %s")
    assert "JETTON:A+B" not in connection.statement
    assert connection.parameters == ["testnet", "JETTON:A+B", *key, 3]


@pytest.mark.parametrize("limit", [0, -1, 1001, True, 1.5])
def test_window_database_reader_rejects_unbounded_limits(limit):
    class Connection:
        def execute(self, *_):
            pytest.fail("invalid limit reached SQL")

    with pytest.raises(ValueError):
        read_windows(Connection(), limit=limit)


@pytest.mark.parametrize("query", [
    "network=", "limit=0", "limit=1001", "window_start=9223372036854775808",
    "network=testnet&network=mainnet", "unknown=x", "token=%FF", "x" * 4097,
])
def test_invalid_window_queries_are_rejected_before_database_connect(query):
    with patch("ton_sentinel.api.psycopg.connect") as connect:
        status, body = _call(create_app(dsn="explicit-test-dsn"), query, path="/v1/windows")
    assert status == "400 Bad Request"
    assert body in ({"error": "invalid_query"}, {"error": "invalid_network"})
    connect.assert_not_called()


def test_window_cursor_is_endpoint_and_filter_bound():
    from urllib.parse import urlencode
    from ton_sentinel.api import _encode_window_cursor

    filters = {"network": None, "window_start": None, "window_end": None, "token": None}
    cursor = _encode_window_cursor(filters, ("testnet", 0, 300))
    for path, query in (("/v1/features", {"cursor": cursor}),
                        ("/v1/windows", {"network": "mainnet", "cursor": cursor})):
        with patch("ton_sentinel.api.psycopg.connect") as connect:
            status, body = _call(create_app(dsn="explicit-test-dsn"), urlencode(query), path=path)
        assert status == "400 Bad Request"
        assert body == {"error": "invalid_query"}
        connect.assert_not_called()


def test_window_continuation_uses_url_encoded_filter_bytes():
    from urllib.parse import parse_qs, urlencode
    from ton_sentinel.api import _decode_window_cursor

    token = "JETTON:A+B/%ą"
    filters = {"network": None, "window_start": None, "window_end": None, "token": token}
    rows = [
        {"network": "testnet", "window_start": 0, "window_end": 300},
        {"network": "testnet", "window_start": 300, "window_end": 600},
    ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def transaction(self):
            return self

        def execute(self, *_):
            return None

    with patch("ton_sentinel.api.read_windows", return_value=rows), \
            patch("ton_sentinel.api.psycopg.connect", return_value=Connection()):
        status, body = _call(create_app(dsn="explicit-test-dsn"), urlencode({"token": token, "limit": 1}),
                             path="/v1/windows")

    assert status == "200 OK"
    cursor = body["next_cursor"]
    assert _decode_window_cursor(cursor, filters) == ("testnet", 0, 300)
    continuation = urlencode({"token": token, "limit": 1})
    full_query = f"{continuation}&cursor={cursor}"
    assert "%2B" in continuation and "%2F" in continuation and "%C4%85" in continuation
    assert len(continuation.encode("ascii")) > len("token=" + token + "&limit=1")
    assert len(full_query.encode("ascii")) <= 4096 + 4096 + len("&cursor=")
    assert parse_qs(full_query)["token"] == [token]
