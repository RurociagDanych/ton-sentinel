"""Read-only trace lookup normalization against real loopback HTTP."""

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
from urllib.parse import parse_qs, urlsplit

import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.source.client import RESTPageClient
from ton_sentinel.source.trace import lookup_trace


def epoch():
    return SourceEpoch("testnet", "offline:source", "offline:filter",
                       "offline:build", "offline:policy")


class Server:
    def __init__(self, status, payload, headers=None):
        self.requests = []
        self.status = status
        self.payload = payload
        self.headers = headers or {"Content-Type": "application/json"}
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append(self.path)
                body = json.dumps(owner.payload).encode()
                self.send_response(owner.status)
                for name, value in owner.headers.items():
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


def run(payload, *, status=200, headers=None, **limits):
    server = Server(status, payload, headers)
    try:
        client = RESTPageClient(server.origin, "/api/v3/traces", **limits)
        result = lookup_trace(client, epoch(), "trace-exact")
        return result, server.requests
    finally:
        server.close()


def trace(**changes):
    value = {"trace_id": "trace-exact", "is_incomplete": False,
             "trace_info": {"trace_state": "complete",
                            "classification_state": "classified"},
             "actions": [{"action_id": "action-one"}, {"action_id": "action-two"}]}
    value.update(changes)
    return value


def test_returns_action_keys_only_for_complete_classified_trace():
    result, requests = run({"traces": [trace()]})
    assert result.trace_id == "trace-exact"
    assert result.state == "classified_complete"
    assert result.action_keys == ("action-one", "action-two")
    query = parse_qs(urlsplit(requests[0]).query)
    assert query == {"trace_id": ["trace-exact"], "include_actions": ["true"],
                     "limit": ["1"], "offset": ["0"]}
    assert result.global_coverage is False


def test_unclassified_and_incomplete_traces_never_prove_empty_keys():
    unclassified = trace(trace_info={"trace_state": "complete",
                                     "classification_state": "unclassified"}, actions=[])
    result, _ = run({"traces": [unclassified]})
    assert result.state == "unclassified"
    assert result.action_keys is None
    incomplete = trace(is_incomplete=True, trace_info={"trace_state": "pending",
                                                       "classification_state": "classified"},
                       actions=[])
    result, _ = run({"traces": [incomplete]})
    assert result.state == "incomplete"
    assert result.action_keys is None


@pytest.mark.parametrize("payload", [
    {"traces": []},
    {"traces": [{"trace_id": "trace-exact", "actions": []}]},
    {"traces": [trace(trace_id="other-trace")]},
    {"traces": [trace(actions=[{"id": "no-verified-action-key"}])]},
])
def test_absence_or_malformed_fields_do_not_become_empty_key_proofs(payload):
    with pytest.raises(ValueError):
        run(payload)


def test_unsupported_classification_and_trace_states_are_rejected():
    item = trace(trace_info={"trace_state": "complete", "classification_state": "pending"})
    with pytest.raises(ValueError, match="unsupported classification_state"):
        run({"traces": [item]})
    item = trace(trace_info={"trace_state": "unknown", "classification_state": "classified"})
    with pytest.raises(ValueError):
        run({"traces": [item]})


def test_http_error_and_transport_budgets_fail_closed():
    with pytest.raises(ValueError, match="HTTP 404"):
        run({"traces": []}, status=404)
    with pytest.raises(ValueError, match="byte limit"):
        run({"traces": [trace()]}, max_body_bytes=16)
    with pytest.raises(ValueError, match="request budget"):
        run({"traces": [trace()]}, max_requests=1, max_retries=1)


def test_redirect_is_not_followed():
    server = Server(302, {}, {"Location": "http://127.0.0.1:1/elsewhere"})
    try:
        with pytest.raises(ValueError):
            lookup_trace(RESTPageClient(server.origin, "/api/v3/traces"),
                         epoch(), "trace-exact")
        assert len(server.requests) == 1
    finally:
        server.close()
