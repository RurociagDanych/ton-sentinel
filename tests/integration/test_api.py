import re
import json
import os
import threading
import uuid
from html.parser import HTMLParser
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest

from ton_sentinel.api import _status_payload, create_app, read_features as actual_read_features
from ton_sentinel.db.migrate import migrate


def _visible_text(html):
    """Text as a reader sees it: inline tags removed without adding spaces, whitespace collapsed."""
    return " ".join(re.sub(r"<[^>]+>", "", html).split())


@pytest.fixture
def database():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "api_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    scoped_dsn = psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={schema}")
    try:
        with psycopg.connect(scoped_dsn) as connection:
            migrate(connection)
            connection.cursor().executemany(
                "INSERT INTO served_features "
                "(network, window_start, window_end, token, generation, swaps, input_raw, output_raw, "
                "known_traders, unknown_count, coverage_status, status, provisional, write_fence) "
                "VALUES (%s,%s,%s,%s,'fixture',1,NULL,NULL,0,0,'bounded_observed','provisional',true,1)",
                [
                    ("testnet", 0, 300, "JETTON:A",),
                    ("testnet", 0, 300, "JETTON:A+B",),
                    ("testnet", 0, 300, "JETTON:Z",),
                    ("testnet", 300, 600, "JETTON:A+B",),
                    ("mainnet", 0, 300, "JETTON:A+B",),
                ],
            )
        yield scoped_dsn
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _request(base, path, *, method="GET"):
    try:
        with urlopen(Request(base + path, method=method), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


class _TextParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self.tags = []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)

    def handle_data(self, data):
        self.text.append(data)


def _html_request(base, path="/"):
    try:
        with urlopen(base + path, timeout=8) as response:
            return response.status, response.headers, response.read().decode("utf-8")
    except HTTPError as error:
        return error.code, error.headers, error.read().decode("utf-8")


def test_bounded_deterministic_feature_readback_over_loopback(database):
    server = make_server("127.0.0.1", 0, create_app(dsn=database))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        assert _request(base, "/health") == (200, {"status": "ok"})
        status, payload = _request(base, "/v1/features?network=testnet")
        assert status == 200
        assert [(row["window_start"], row["token"]) for row in payload["features"]] == [
            (0, "JETTON:A"), (0, "JETTON:A+B"), (0, "JETTON:Z"), (300, "JETTON:A+B")
        ]
        assert set(payload) == {"features"}
        status, limited = _request(base, "/v1/features?network=testnet&limit=2")
        assert status == 200
        assert [(row["window_start"], row["token"]) for row in limited["features"]] == [
            (0, "JETTON:A"), (0, "JETTON:A+B")
        ]

        exact_token = urlencode({"token": "JETTON:A+B"})
        status, matching = _request(base, "/v1/features?" + exact_token)
        assert status == 200
        assert [(row["network"], row["window_start"], row["token"])
                for row in matching["features"]] == [
            ("mainnet", 0, "JETTON:A+B"), ("testnet", 0, "JETTON:A+B"),
            ("testnet", 300, "JETTON:A+B"),
        ]
        status, exact_window_token = _request(
            base, "/v1/features?" + urlencode({"network": "testnet", "window_start": "0",
                                                "window_end": "300", "token": "JETTON:A+B"})
        )
        assert status == 200
        assert [row["token"] for row in exact_window_token["features"]] == ["JETTON:A+B"]

        status, operational = _request(base, "/status")
        assert status == 200
        assert operational["feature_rows"] == 5
        assert operational["windows"] == 3
        freshness = operational["serving_freshness"]
        assert freshness["status"] == "observed"
        assert freshness["scope"] == "global_all_scopes"
        assert freshness["latest_window_end"] == 600
        assert freshness["latest_serving_write_at"].endswith("Z")
        assert freshness["lag"] == {"status": "unknown", "meaning": "not_measured"}
        assert "not pipeline/source freshness" in freshness["limitations"]
        assert "independent" in freshness["limitations"]
        assert operational["status_meaning"] == "database_query_succeeded"
        assert operational["availability"] == {"process": "available", "database": "available"}
        assert operational["readiness"] == "unknown"
        assert operational["source"] == {"status": "unknown"}
        assert operational["coverage"] == {"status": "unknown"}
        assert operational["backup"] == "not_configured"
        assert operational["operational_domains"] == {
            "outbox": "observed", "repair": "observed",
            "spark_checkpoint": "observed", "spark_watermark": "unsupported",
            "spark_state": "unsupported", "database_storage": "observed",
            "disk": "unsupported", "maintenance": "observed", "rates": "observed"}
        assert operational["health_endpoint_meaning"] == "process_and_database_connectivity_only"

        html_status, headers, document = _html_request(base)
        assert html_status == 200
        assert headers.get_content_type() == "text/html"
        parsed = _TextParser()
        parsed.feed(document)
        rendered = " ".join(" ".join(parsed.text).split())
        assert "TON Sentinel operational demo" in rendered
        assert "database_query_succeeded" in rendered
        assert "available / available" in rendered
        assert "Readiness unknown" in rendered
        assert "Source unknown" in rendered and "not live-certified" in rendered
        assert "Coverage unknown" in rendered and "not certified" in rendered
        assert "Backup not_configured" in rendered
        assert "Observed served-feature rows 5" in rendered
        assert "Observed distinct windows 3" in rendered
        # The parser joins text nodes with spaces, so "observed (" may read "observed ( ".
        text = re.sub(r"\(\s+", "(", re.sub(r"\s+\)", ")", rendered))
        assert "Observed serving freshness observed (global_all_scopes)" in text
        assert "Latest observed served window end (UTC epoch seconds) 600" in rendered
        assert freshness["latest_serving_write_at"] in rendered
        assert "Source or pipeline lag unknown — not measured" in rendered
        assert "not pipeline/source freshness or per-scope freshness" in rendered
        assert document.count("<code>unsupported</code>") == 3
        assert "not a readiness or coverage check" in rendered

        assert _request(base, "/v1/features?limit=1001")[0] == 400
        assert _request(base, "/v1/features?network=testnet&network=mainnet")[0] == 400
        for malformed in ("%", "%2", "%GG", "%FF", "%C3%28", "%C0%AF", "%ED%A0%80"):
            assert _request(base, "/v1/features?token=" + malformed) == (
                400, {"error": "invalid_query"}
            )
        assert _request(base, "/v1/features", method="POST")[0] == 405

        # A real blocked SQL read must expire, rather than monopolize the HTTP server.
        with psycopg.connect(database) as blocker:
            blocker.execute("LOCK TABLE served_features IN ACCESS EXCLUSIVE MODE")
            assert _request(base, "/v1/features") == (503, {"error": "unavailable"})
            status, unavailable = _request(base, "/status")
            assert status == 503
            assert unavailable["availability"] == {
                "process": "available", "database": "available"
            }
            assert unavailable["status_meaning"] == "database_query_unavailable"
            assert unavailable["status_query"] == "unavailable"
            assert unavailable["readiness"] == "unknown"
            assert "feature_rows" not in unavailable
            assert "windows" not in unavailable
            html_status, headers, document = _html_request(base)
            assert html_status == 503
            assert headers.get_content_type() == "text/html"
            parsed = _TextParser()
            parsed.feed(document)
            rendered = " ".join(" ".join(parsed.text).split())
            assert "database_query_unavailable" in rendered
            assert "Observed served-feature rows unavailable" in rendered
            assert "Observed distinct windows unavailable" in rendered
            assert _request(base, "/health") == (200, {"status": "ok"})
        assert _request(base, "/v1/features?limit=1")[0] == 200

        with psycopg.connect(database) as connection:
            connection.execute(
                "INSERT INTO served_features "
                "(network, window_start, window_end, token, generation, swaps, input_raw, output_raw, "
                "known_traders, unknown_count, coverage_status, status, provisional, write_fence) "
                "SELECT 'testnet',0,300,'JETTON:B' || lpad(i::text,3,'0'),'fixture',1,NULL,NULL,"
                "0,0,'bounded_observed','provisional',true,1 FROM generate_series(1,105) AS i"
            )
        status, default_page = _request(base, "/v1/features")
        assert status == 200
        assert len(default_page["features"]) == 100
        status, full_page = _request(base, "/v1/features?limit=1000")
        assert status == 200
        assert len(full_page["features"]) == 110
        assert default_page["features"] == full_page["features"][:100]
        assert _request(base, "/v1/features")[1] == default_page
        with psycopg.connect(database) as connection:
            for original, literal in (("JETTON:B001", "JETTON:%GG"), ("JETTON:B002", "JETTON:ą")):
                connection.execute(
                    "UPDATE served_features SET token=%s WHERE token=%s", (literal, original)
                )
        for literal in ("JETTON:%GG", "JETTON:ą"):
            status, matching = _request(base, "/v1/features?" + urlencode({"token": literal}))
            assert status == 200
            assert [row["token"] for row in matching["features"]] == [literal]

        status, expected = _request(base, "/v1/features?" + urlencode(
            {"network": "testnet", "limit": "1000"}))
        assert status == 200
        expected_keys = [(row["network"], row["window_start"], row["window_end"], row["token"])
                         for row in expected["features"]]
        paged, cursor, page_count = [], None, 0
        while True:
            parameters = {"network": "testnet", "limit": "7"}
            if cursor is not None:
                parameters["cursor"] = cursor
            status, page = _request(base, "/v1/features?" + urlencode(parameters))
            assert status == 200
            paged.extend(page["features"])
            page_count += 1
            cursor = page.get("next_cursor")
            if cursor is None:
                assert "next_cursor" not in page
                break
            if page_count == 1:
                assert _request(base, "/v1/features?" + urlencode(
                    {"network": "mainnet", "limit": "7", "cursor": cursor}))[0] == 400
        paged_keys = [(row["network"], row["window_start"], row["window_end"], row["token"])
                      for row in paged]
        assert paged_keys == expected_keys
        assert len(paged_keys) == len(set(paged_keys))
        assert page_count > 1
        assert "JETTON:%GG" in [key[3] for key in paged_keys]
        assert "JETTON:ą" in [key[3] for key in paged_keys]
        long_token = "JETTON:" + "L" * 1400
        with psycopg.connect(database) as connection:
            connection.execute("UPDATE served_features SET token=%s WHERE token='JETTON:A+B'",
                               (long_token,))
        parameters = {"token": long_token, "limit": "1"}
        status, page = _request(base, "/v1/features?" + urlencode(parameters))
        assert status == 200
        assert "next_cursor" in page
        next_query = urlencode({**parameters, "cursor": page["next_cursor"]})
        assert len(next_query) > 4096
        status, next_page = _request(base, "/v1/features?" + next_query)
        assert status == 200
        assert next_page["features"][0]["token"] == long_token
        assert next_page["features"] != page["features"]

        # An unrepresentable key fails explicitly instead of emitting a broken cursor.
        huge_token = "JETTON:" + "H" * 4000
        with psycopg.connect(database) as connection:
            connection.execute("UPDATE served_features SET token=%s WHERE token=%s",
                               (huge_token, long_token))
        assert _request(base, "/v1/features?" + urlencode(
            {"token": huge_token, "limit": "1"})) == (
                422, {"error": "pagination_key_too_large"})
        for hostile in ("!", "A", "_", "x" * 4097):
            assert _request(base, "/v1/features?" + urlencode(
                {"cursor": hostile, "limit": "2"}))[0] == 400
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_demo_database_connection_failure_is_safe_html():
    server = make_server("127.0.0.1", 0,
                         create_app(dsn="host=/no-such-postgres-socket user=private_user dbname=private_db"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, headers, document = _html_request(f"http://127.0.0.1:{server.server_port}")
        assert status == 503
        assert headers.get_content_type() == "text/html"
        parsed = _TextParser()
        parsed.feed(document)
        rendered = " ".join(" ".join(parsed.text).split())
        assert "available / unavailable" in rendered
        assert "database_query_unavailable" in rendered
        assert "Observed served-feature rows unavailable" in rendered
        assert "no-such-postgres-socket" not in document
        assert "private_user" not in document and "private_db" not in document
        assert "Traceback" not in document
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_read_only_transaction_blocks_write_from_product_read_function(database):
    server = make_server("127.0.0.1", 0, create_app(dsn=database))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with psycopg.connect(database) as connection:
            before = connection.execute("SELECT count(*) FROM served_features").fetchone()[0]

        blocked = []

        def intercepted_read(connection, **kwargs):
            rows = actual_read_features(connection, **kwargs)
            try:
                connection.execute("UPDATE served_features SET swaps=swaps WHERE token='JETTON:A'")
            except psycopg.errors.ReadOnlySqlTransaction as error:
                blocked.append(error.sqlstate)
                raise
            return rows

        with patch("ton_sentinel.api.read_features", side_effect=intercepted_read) as reader:
            status, payload = _request(base, "/v1/features?network=testnet")

        assert status == 503
        assert payload == {"error": "unavailable"}
        reader.assert_called_once()
        assert blocked == ["25006"]
        with psycopg.connect(database) as connection:
            after = connection.execute("SELECT count(*) FROM served_features").fetchone()[0]
            value = connection.execute(
                "SELECT swaps FROM served_features WHERE token='JETTON:A'"
            ).fetchone()[0]
        assert after == before == 5
        assert value == 1
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_demo_escapes_dynamic_status_text():
    payload = _status_payload(database="available", feature_rows=5, windows=3)
    payload["source"]["status"] = '<img src=x onerror="bad">'
    payload["coverage"]["status"] = "<script>bad</script>"
    server = make_server("127.0.0.1", 0, create_app(dsn="unused-by-patched-status-read"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with patch("ton_sentinel.api._read_status", return_value=("200 OK", payload)):
            status, headers, document = _html_request(f"http://127.0.0.1:{server.server_port}")
        assert status == 200
        assert headers.get_content_type() == "text/html"
        parsed = _TextParser()
        parsed.feed(document)
        assert "img" not in parsed.tags and "script" not in parsed.tags
        assert '<img src=x onerror="bad">' in " ".join(parsed.text)
        assert "<script>bad</script>" in " ".join(parsed.text)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_bounded_window_summaries_paginate_without_duplicates(database):
    # Mixed per-token generations stay a count, not a fabricated window generation.
    with psycopg.connect(database) as connection:
        connection.execute(
            "UPDATE served_features SET generation='other', provisional=false "
            "WHERE network='testnet' AND window_start=0 AND token='JETTON:A'")
    server = make_server("127.0.0.1", 0, create_app(dsn=database))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        collected = []
        cursor = None
        first_cursor = None
        while True:
            params = {"limit": "1"}
            if cursor:
                params["cursor"] = cursor
            status, page = _request(base, "/v1/windows?" + urlencode(params))
            assert status == 200
            collected.extend(page["windows"])
            cursor = page.get("next_cursor")
            if first_cursor is None:
                first_cursor = cursor
            if cursor is None:
                assert "next_cursor" not in page
                break
        keys = [(row["network"], row["window_start"], row["window_end"]) for row in collected]
        assert keys == [("mainnet", 0, 300), ("testnet", 0, 300), ("testnet", 300, 600)]
        assert len(keys) == len(set(keys)) == 3
        assert collected[1]["feature_rows"] == 3
        assert collected[1]["provisional_rows"] == 2
        assert collected[1]["generation_count"] == 2
        assert collected[1]["statuses"] == ["provisional"]
        assert not ({"generation", "token", "input_raw", "output_raw", "payload"} & set(collected[1]))
        assert _request(base, "/v1/windows?token=missing") == (200, {"windows": []})
        token_filter = urlencode({"token": "JETTON:A+B"})
        status, filtered = _request(base, "/v1/windows?" + token_filter)
        assert status == 200
        assert [(row["network"], row["window_start"], row["feature_rows"])
                for row in filtered["windows"]] == [
            ("mainnet", 0, 1), ("testnet", 0, 1), ("testnet", 300, 1),
        ]
        assert _request(base, "/v1/windows?network=testnet&window_start=0&window_end=300")[0] == 200
        assert _request(base, "/v1/windows?network=nope")[0] == 400
        assert _request(base, "/v1/windows?cursor=bad")[0] == 400
        assert first_cursor is not None
        assert _request(base, "/v1/windows?network=mainnet&limit=1&cursor=" + first_cursor)[0] == 400
        assert _request(base, "/v1/windows", method="POST") == (405, {"error": "read_only"})
        with psycopg.connect(database) as connection:
            connection.execute("DELETE FROM served_features")
        assert _request(base, "/v1/windows") == (200, {"windows": []})
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_window_database_failure_is_sanitized():
    server = make_server("127.0.0.1", 0,
                         create_app(dsn="host=/no-such-postgres-socket user=private_user dbname=private_db"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        status, payload = _request(f"http://127.0.0.1:{server.server_port}", "/v1/windows")
        assert status == 503
        assert payload == {"error": "unavailable"}
        assert "private_user" not in json.dumps(payload)
        assert "private_db" not in json.dumps(payload)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
