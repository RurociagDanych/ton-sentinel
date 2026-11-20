"""Dashboard API over HTTP on real PostgreSQL: the endpoints the overview reads, and the client vocabulary."""

import json
import re
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel.api import create_app
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401

DASH = Path(__file__).resolve().parents[2] / "src" / "ton_sentinel" / "dashboard"
PROVENANCE_KEYS = {"source_kind", "certification_state", "coverage_state", "scoring_state",
                   "honesty_banner", "generated_at"}


@pytest.fixture
def base(db, reader_dsn):  # noqa: F811
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get(base, path, query=None, raw=False):
    url = base + path + ("?" + urlencode(query) if query else "")
    try:
        with urlopen(Request(url), timeout=8) as response:
            body = response.read()
            return response.status, (body if raw else json.loads(body)), dict(response.headers)
    except HTTPError as error:
        return error.code, json.loads(error.read()), dict(error.headers)


def legend_states():
    return set(re.findall(r'^  "([a-z_]+)": \{ label:', (DASH / "states.js").read_text(), re.M))


def collect_statuses(value, out):
    if isinstance(value, dict):
        if isinstance(value.get("status"), str):
            out.add(value["status"])
        for child in value.values():
            collect_statuses(child, out)
    elif isinstance(value, list):
        for child in value:
            collect_statuses(child, out)
    return out


def test_assets_the_overview_needs_are_served_with_csp(base):
    for name in ("overview.js", "model.js", "states.js"):
        status, body, headers = get(base, f"/dashboard/assets/{name}", raw=True)
        assert status == 200 and body == (DASH / name).read_bytes()
        assert headers["Content-Type"].startswith("text/javascript")
        assert "script-src 'self'" in headers["Content-Security-Policy"]


def test_status_unchanged_shape_has_every_field_the_overview_reads(base):
    status, body, _ = get(base, "/status")
    assert status == 200
    for key in ("availability", "status_query", "readiness", "source", "coverage", "backup",
                "gates", "pending_work", "serving_freshness", "feature_rows", "windows"):
        assert key in body, key
    assert body["readiness"] == "unknown" and body["backup"] == "not_configured"
    assert body["gates"]["stateful_streaming"]["status"] == "not_qualified"
    assert body["gates"]["live_source"]["status"] == "not_qualified"
    assert "provenance" not in body
    assert collect_statuses(body, set()) <= legend_states() | {"unknown"}


def test_pipeline_and_quality_carry_provenance_and_only_legend_states(base):
    for path, query in (("/v1/pipeline", {"network": "mainnet"}),
                        ("/v1/quality", {"network": "mainnet", "from": 0, "to": 3600})):
        status, body, _ = get(base, path, query)
        assert status == 200, body
        assert PROVENANCE_KEYS <= set(body["provenance"])
        assert all(isinstance(body["provenance"][k], str) and body["provenance"][k]
                   for k in PROVENANCE_KEYS)
        assert isinstance(body["label"], str) and body["label"]
        unknown = collect_statuses(body, set()) - legend_states()
        assert unknown == set(), unknown
        provenance_states = {body["provenance"][k] for k in (
            "source_kind", "certification_state", "coverage_state", "scoring_state")}
        assert provenance_states <= legend_states()


def test_pipeline_unavailable_and_not_measured_are_explicit_never_zero(base):
    _, body, _ = get(base, "/v1/pipeline", {"network": "testnet"})
    pipeline = body["pipeline"]
    assert pipeline["ingestion_freshness"]["status"] == "not_measured"
    assert pipeline["ingestion_freshness"]["value"] is None
    assert pipeline["kafka_lag"]["status"] == "not_measured"
    backlog = pipeline["scoring_backlog"]["value"]
    for section in (backlog["unscored_served_windows"], backlog["stale_results"]):
        assert section["status"] in ("observed", "unavailable")
        if section["status"] == "unavailable":
            assert "value" not in section or section["value"] is None


def test_quality_range_is_what_the_view_sends(base):
    to = 1_700_000_100
    to -= to % 300
    status, body, _ = get(base, "/v1/quality", {"network": "mainnet", "from": to - 7 * 86400, "to": to})
    assert status == 200 and (body["from"], body["to"]) == (to - 7 * 86400, to)
    assert get(base, "/v1/quality", {"network": "mainnet", "from": to - 7 * 86400 - 300, "to": to})[0] == 400
    assert get(base, "/v1/quality", {"network": "mainnet", "from": 1, "to": 301})[0] == 400


def test_policy_still_serves_banner_for_the_shell(base):
    status, body, _ = get(base, "/v1/policy")
    assert status in (200, 404)
    if status == 200:
        assert PROVENANCE_KEYS <= set(body["provenance"])
