"""Configuration and input validation for the bounded REST page client."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from urllib.parse import parse_qs, urlsplit

import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.source.client import RESTPageClient


def epoch():
    return SourceEpoch("testnet", "offline:source", "offline:filter",
                       "offline:build", "offline:policy")


def test_client_requires_one_explicit_origin_and_fixed_route():
    with pytest.raises(ValueError):
        RESTPageClient("https://example.test", "/v1/actions?url=https://other.test")
    with pytest.raises(ValueError):
        RESTPageClient("https://user:pass@example.test", "/v1/actions")
    with pytest.raises(ValueError):
        RESTPageClient("https://example.test", "//other.test/actions")


def test_fetch_page_rejects_invalid_epoch_bounds_before_network():
    client = RESTPageClient("http://127.0.0.1:1", "/v1/actions")
    for args in (
        (object(), 10, 20, 0, 10),
        (epoch(), True, 20, 0, 10),
        (epoch(), 10, 10, 0, 10),
        (epoch(), 10, 20, -1, 10),
        (epoch(), 10, 20, 0, 0),
        (epoch(), 10, 20, 0, 1001),
    ):
        with pytest.raises(ValueError):
            client.fetch_page(*args)


@pytest.mark.parametrize("remaining_seconds", [
    0, -1, True, False, float("nan"), float("inf"), -float("inf"), "1", object(),
])
def test_fetch_page_rejects_invalid_remaining_seconds_before_side_effects(remaining_seconds, monkeypatch):
    client = RESTPageClient("http://127.0.0.1:1", "/v1/actions")
    effects = []

    class NoNetwork:
        def open(self, *_args, **_kwargs):
            effects.append("network")
            raise AssertionError("network must not be reached")

    monkeypatch.setattr(client, "_opener", NoNetwork())
    with pytest.raises(ValueError, match="remaining_seconds"):
        client.fetch_page(
            epoch(), 10, 20, 0, 2,
            remaining_seconds=remaining_seconds,
            before_request=lambda: effects.append("before_request"),
            reserve_bytes=lambda _size: effects.append("reserve_bytes"),
        )
    assert effects == []


@pytest.mark.parametrize("remaining_bytes", [
    0, -1, True, False, 1.0, "2", object(),
])
def test_fetch_page_rejects_invalid_remaining_bytes_before_side_effects(remaining_bytes, monkeypatch):
    client = RESTPageClient("http://127.0.0.1:1", "/v1/actions")
    effects = []

    class NoNetwork:
        def open(self, *_args, **_kwargs):
            effects.append("network")
            raise AssertionError("network must not be reached")

    monkeypatch.setattr(client, "_opener", NoNetwork())
    with pytest.raises(ValueError, match="remaining_bytes"):
        client.fetch_page(
            epoch(), 10, 20, 0, 2,
            remaining_seconds=1,
            remaining_bytes=remaining_bytes,
            before_request=lambda: effects.append("before_request"),
            reserve_bytes=lambda _size: effects.append("reserve_bytes"),
        )
    assert effects == []


def test_fetch_page_accepts_fractional_seconds_and_exact_byte_limit_over_loopback():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            body = b"[]"
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    reserved = []
    try:
        client = RESTPageClient(f"http://127.0.0.1:{server.server_port}", "/v1/actions")
        assert client.fetch_page(
            epoch(), 10, 20, 0, 2,
            remaining_seconds=0.5,
            remaining_bytes=2,
            reserve_bytes=reserved.append,
        ) == []
        assert client.fetch_page(
            epoch(), 10, 20, 0, 2,
            remaining_seconds=10 ** 1000,
            remaining_bytes=2,
            reserve_bytes=reserved.append,
        ) == []
        assert reserved == [2, 2]
        assert len(requests) == 2
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_fetch_trace_uses_trace_id_lookup_and_preserves_incomplete_result():
    trace = {"trace_id": "trace-exact", "is_incomplete": True,
             "trace_info": {"classification_state": "pending"}, "actions": []}
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append(self.path)
            body = json.dumps({"traces": [trace], "address_book": {}, "metadata": {}}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = RESTPageClient(f"http://127.0.0.1:{server.server_port}", "/api/v3/traces")
        result = client.fetch_trace(epoch(), "trace-exact")
        assert result["traces"] == [trace]
        query = parse_qs(urlsplit(requests[0]).query)
        assert urlsplit(requests[0]).path == "/api/v3/traces"
        assert query == {"trace_id": ["trace-exact"], "include_actions": ["true"],
                         "limit": ["1"], "offset": ["0"]}
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.parametrize("route,body", [
    ("/v1/actions", b'[{"nested": [1e999]}]'),
    ("/v1/actions", b'[{"nested": {"value": -1e999}}]'),
    ("/api/v3/traces", b'{"traces": [{"trace_id":"trace-exact","nested":[1e999]}]}'),
    ("/api/v3/traces", b'{"traces": [{"trace_id":"trace-exact","nested":{"value":-1e999}}]}'),
])
def test_source_endpoints_reject_overflowed_exponent_numbers_over_loopback(route, body):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        client = RESTPageClient(f"http://127.0.0.1:{server.server_port}", route)
        with pytest.raises(ValueError, match="invalid source JSON"):
            if route == "/v1/actions":
                client.fetch_page(epoch(), 10, 20, 0, 2)
            else:
                client.fetch_trace(epoch(), "trace-exact")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_source_endpoints_preserve_finite_float_and_huge_integer_over_loopback():
    page_body = b'[{"float":1e308,"integer":999999999999999999999999999999999999}]'
    trace_body = b'{"traces":[{"trace_id":"trace-exact","float":-1e308,"integer":999999999999999999999999999999999999}]}'
    bodies = {"/v1/actions": page_body, "/api/v3/traces": trace_body}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = bodies[urlsplit(self.path).path]
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        origin = f"http://127.0.0.1:{server.server_port}"
        page = RESTPageClient(origin, "/v1/actions").fetch_page(epoch(), 10, 20, 0, 2)[0]
        trace = RESTPageClient(origin, "/api/v3/traces").fetch_trace(epoch(), "trace-exact")["traces"][0]
        assert page["float"] == 1e308 and type(page["float"]) is float
        assert trace["float"] == -1e308 and type(trace["float"]) is float
        assert page["integer"] == trace["integer"]
        assert type(page["integer"]) is int
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
