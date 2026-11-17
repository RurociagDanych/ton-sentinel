"""Dashboard API on real PostgreSQL over HTTP (sentinel_reader login): the endpoints the quality view uses."""
import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from tests.integration.test_detection import NET, SCORED, TOK, ready_world
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401

DASH = Path(__file__).resolve().parents[2] / "src" / "ton_sentinel" / "dashboard"
RANGE = {"from": SCORED - 3600, "to": SCORED + 3600}
COUNTERS = {"served_rows_by_coverage_status", "served_windows", "dirty_windows_pending_by_role",
            "revision_quarantine_open", "delivery_quarantine", "qualifications_by_class", "scan_quarantine",
            "coverage_ranges_pending", "coverage_ranges_bounded_observed"}
STATES = {"observed", "zero", "partial", "provisional", "missing", "pre_universe"}


@pytest.fixture
def served(db, reader_dsn):  # noqa: F811
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get(base, path, query=None):
    url = base + path + ("?" + urlencode(query) if query else "")
    try:
        with urlopen(Request(url), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def test_quality_assets_are_served_with_csp_and_match_the_files(served):
    _, base = served
    for name in ("quality.js", "quality-model.js"):
        with urlopen(base + f"/dashboard/assets/{name}", timeout=8) as response:
            assert response.status == 200 and response.read() == (DASH / name).read_bytes()
            assert response.headers["Content-Type"].startswith("text/javascript")
            assert "script-src 'self'" in response.headers["Content-Security-Policy"]


def test_counters_are_observed_or_unavailable_with_reason_never_zero_for_unreadable(served):
    db, base = served
    ready_world(db)
    status, body = get(base, "/v1/quality", {"network": NET, **RANGE})
    assert status == 200 and set(body["quality"]) == COUNTERS
    for key, counter in body["quality"].items():
        assert counter["status"] in ("observed", "unavailable"), key
        if counter["status"] == "unavailable":
            assert isinstance(counter["reason"], str) and counter["reason"] and counter.get("value") is None, key
    for key in ("scan_quarantine", "coverage_ranges_pending", "coverage_ranges_bounded_observed"):
        assert body["quality"][key] == {"status": "unavailable", "value": None, "reason": "not_network_scoped"}
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER and body["provenance"]["source_kind"]


def test_tokens_then_coverage_chain_lists_windows_with_known_states_and_provenance(served):
    db, base = served
    ready_world(db)
    status, tokens = get(base, "/v1/tokens", {"network": NET, **RANGE, "limit": 100})
    assert status == 200 and any(t["token"] == TOK for t in tokens["tokens"])
    status, cov = get(base, "/v1/coverage", {"network": NET, "token": TOK, **RANGE, "limit": 1000})
    assert status == 200 and cov["coverage"]
    starts = [w["window_start"] for w in cov["coverage"]]
    assert starts == sorted(set(starts)) and all(s % 300 == 0 and RANGE["from"] <= s < RANGE["to"] for s in starts)
    for window in cov["coverage"]:
        assert window["state"] in STATES and isinstance(window["reasons"], list)
    for response in (tokens, cov):
        for key in ("source_kind", "certification_state", "coverage_state", "scoring_state", "honesty_banner"):
            assert isinstance(response["provenance"][key], str) and response["provenance"][key], key
    assert any(w["state"] in ("missing", "pre_universe") for w in cov["coverage"]), "gaps stay listed, not filled"
