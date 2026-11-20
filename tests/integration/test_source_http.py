"""Bounded REST transport behavior against real loopback HTTP servers."""

import json
import base64
import os
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
import psycopg

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import migrate
from ton_sentinel.source.client import RESTPageClient
from ton_sentinel.source.scan import ScanPolicy, scan_range


ACTION = {"schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
          "network": "testnet", "trace_id": "trace-exact", "action_id": "action-exact",
          "action_end": 12}


class Server:
    def __init__(self, behavior):
        self.behavior = behavior
        self.requests = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append(self.path)
                status, headers, body = owner.behavior(self)
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.origin = f"http://127.0.0.1:{self.httpd.server_port}"

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)


def response(status=200, body=b"[]", headers=None):
    return status, headers or {"Content-Type": "application/json"}, body


def client(server, **limits):
    return RESTPageClient(server.origin, "/v1/actions", **limits)


def epoch():
    return SourceEpoch("testnet", "offline:source", "offline:filter",
                       "offline:build", "offline:policy")


def with_server(behavior, callback):
    server = Server(behavior)
    try:
        return callback(server)
    finally:
        server.close()


def test_fetch_page_requests_only_fixed_route_and_maps_half_open_end_inclusive():
    def run(server):
        page = client(server).fetch_page(epoch(), 10, 20, 4, 7)
        assert page == [ACTION]
        parsed = urlsplit(server.requests[0])
        assert parsed.path == "/v1/actions"
        assert parse_qs(parsed.query) == {"start": ["10"], "end": ["19"],
                                          "offset": ["4"], "limit": ["7"]}
    with_server(lambda _request: response(body=json.dumps([ACTION]).encode()), run)


def test_redirect_auth_and_invalid_json_fail_closed_without_following_redirect():
    def redirect(request):
        return response(302, b"", {"Location": "http://127.0.0.1:1/elsewhere"})
    with_server(redirect, lambda server: _assert_raises(ValueError, lambda:
        client(server).fetch_page(epoch(), 10, 20, 0, 2)))

    with_server(lambda _request: response(401, b"unauthorized"),
                lambda server: _assert_raises(ValueError, lambda:
                    client(server).fetch_page(epoch(), 10, 20, 0, 2)))
    for invalid in (b'{"items":[]}', b"not-json", b'[{"x":1,"x":2}]'):
        with_server(lambda _request: response(body=invalid),
                    lambda server: _assert_raises(ValueError, lambda:
                        client(server).fetch_page(epoch(), 10, 20, 0, 2)))


def test_oversized_or_compressed_response_fails_closed():
    body = json.dumps([ACTION]).encode()
    with_server(lambda _request: response(body=body), lambda server: _assert_raises(ValueError, lambda:
        client(server, max_body_bytes=8).fetch_page(epoch(), 10, 20, 0, 2)))
    with_server(lambda _request: response(headers={"Content-Type": "application/json",
                                                   "Content-Encoding": "gzip"}, body=body),
                lambda server: _assert_raises(ValueError, lambda:
                    client(server).fetch_page(epoch(), 10, 20, 0, 2)))


def test_slow_body_cannot_extend_total_request_deadline():
    class SlowHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", "6")
            self.end_headers()
            for byte in b"[1,2] ":
                try:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.4)
                except (BrokenPipeError, ConnectionResetError):
                    break

        def log_message(self, format, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), SlowHandler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    server = type("Loopback", (), {"origin": f"http://127.0.0.1:{httpd.server_port}"})()
    started = time.monotonic()
    try:
        _assert_raises(ValueError, lambda: client(server, max_seconds=1, request_timeout=5)
                       .fetch_page(epoch(), 10, 20, 0, 2))
        assert time.monotonic() - started < 1.8
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def test_expired_accounting_callback_prevents_network_request():
    def run(server):
        with pytest.raises(ValueError, match="time budget exhausted"):
            client(server).fetch_page(
                epoch(), 10, 20, 0, 2, remaining_seconds=0.05,
                before_request=lambda: time.sleep(0.1))
        assert server.requests == []
    with_server(lambda _request: response(), run)


def test_decode_finishing_after_deadline_cannot_return_success(monkeypatch):
    from ton_sentinel.source import client as client_module

    original_loads = client_module.json.loads

    def delayed_loads(*args, **kwargs):
        payload = original_loads(*args, **kwargs)
        time.sleep(0.4)
        return payload

    monkeypatch.setattr(client_module.json, "loads", delayed_loads)

    def run(server):
        with pytest.raises(ValueError, match="time budget exhausted"):
            client(server).fetch_page(epoch(), 10, 20, 0, 2, remaining_seconds=0.3)
        assert len(server.requests) == 1
    with_server(lambda _request: response(), run)


def test_retry_after_zero_retries_transient_status_and_succeeds_within_cap():
    attempts = []
    def behavior(_request):
        attempts.append(1)
        if len(attempts) == 1:
            return response(429, b"busy", {"Retry-After": "0"})
        return response(body=json.dumps([ACTION]).encode())
    def run(server):
        assert client(server, max_retries=1, max_requests=2).fetch_page(epoch(), 10, 20, 0, 2) == [ACTION]
        assert len(server.requests) == 2
    with_server(behavior, run)


def test_scan_persists_transport_retry_request_budget_across_resume():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    calls = []
    action = {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet", "trace_id": base64.b64encode(bytes([21]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([22]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1", "action_end": 11,
        "status": "success", "eligible": True, "route_proven": True,
        "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }
    def behavior(_request):
        calls.append(1)
        if len(calls) == 1:
            return response(503, b"busy")
        return response(body=json.dumps([action]).encode())

    schema = "http_budget_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with_server(behavior, lambda server: _assert_scan_budget(server, dsn, schema, calls))
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _assert_scan_budget(server, dsn, schema, calls):
    from ton_sentinel.source.scan import resume_scan

    bounded = RESTPageClient(server.origin, "/v1/actions", max_requests=2,
                             max_retries=1)
    policy = ScanPolicy(start=10, end=20, page_size=1, max_pages=3,
                        max_actions=2, max_requests=2)
    with psycopg.connect(dsn) as writer:
        writer.execute(f'SET search_path TO "{schema}"')
        migrate(writer)
        range_id = scan_range(writer, bounded, epoch(), policy)
        observed = writer.execute(
            "SELECT status,request_count,page_count,action_count FROM source_ranges WHERE range_id=%s",
            (range_id,),
        ).fetchone()
        assert observed == ("incomplete", 2, 2, 1)
        resume_scan(writer, bounded, range_id, epoch(), policy)
    assert len(calls) == 2


def test_transient_retry_exhaustion_and_wrong_route_fail_closed():
    def run(server):
        _assert_raises(ValueError, lambda: client(server, max_retries=1, max_requests=2)
                       .fetch_page(epoch(), 10, 20, 0, 2))
        assert len(server.requests) == 2
    with_server(lambda _request: response(503, b"unavailable"), run)
    with_server(lambda request: response(404) if urlsplit(request.path).path != "/v1/actions"
                else response(), lambda server: _assert_raises(ValueError, lambda:
        RESTPageClient(server.origin, "/v1/other").fetch_page(epoch(), 10, 20, 0, 2)))
    with_server(lambda _request: response(429, b"busy", {"Retry-After": "3"}),
                lambda server: _assert_raises(ValueError, lambda:
                    client(server, max_retries=1, max_requests=2, max_retry_after=1)
                    .fetch_page(epoch(), 10, 20, 0, 2)))


def test_real_http_page_flows_through_scan_range_and_independent_pg_readback():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.skip("TON_SENTINEL_TEST_DSN not set; PostgreSQL scan/readback unavailable")
    action = {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet", "trace_id": base64.b64encode(bytes([21]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([22]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1", "action_end": 11,
        "status": "success", "eligible": True, "route_proven": True,
        "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }
    def behavior(request):
        query = parse_qs(urlsplit(request.path).query)
        if query["start"] == ["30"]:
            return response(401, b"unauthorized")
        offset = int(query["offset"][0])
        return response(body=json.dumps([action] if offset == 0 else []).encode())

    schema = "http_scan_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        server = Server(behavior)
        try:
            with psycopg.connect(dsn) as writer:
                writer.execute(f'SET search_path TO "{schema}"')
                migrate(writer)
                range_id = scan_range(
                    writer, client(server), epoch(),
                    ScanPolicy(start=10, end=20, page_size=1, max_pages=3,
                               max_actions=2),
                )
                failed_range = scan_range(
                    writer, client(server), epoch(),
                    ScanPolicy(start=30, end=40, page_size=1, max_pages=3,
                               max_actions=2),
                )
            with psycopg.connect(dsn) as reader:
                reader.execute(f'SET search_path TO "{schema}"')
                persisted = reader.execute(
                    "SELECT status,page_count,action_count FROM source_ranges WHERE range_id=%s",
                    (range_id,),
                ).fetchone()
                observations = reader.execute(
                    "SELECT payload->>'trace_id',payload->>'action_id' "
                    "FROM action_observations WHERE range_id=%s ORDER BY ordinal",
                    (range_id,),
                ).fetchall()
                failed = reader.execute(
                    "SELECT status,action_count FROM source_ranges WHERE range_id=%s",
                    (failed_range,),
                ).fetchone()
                failed_observations = reader.execute(
                    "SELECT count(*) FROM action_observations WHERE range_id=%s",
                    (failed_range,),
                ).fetchone()[0]
            assert persisted == ("complete", 2, 1)
            assert observations == [(action["trace_id"], action["action_id"])]
            assert failed == ("incomplete", 0)
            assert failed_observations == 0
            assert len(server.requests) == 3
        finally:
            server.close()
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _assert_raises(exception, function):
    with pytest.raises(exception):
        function()
