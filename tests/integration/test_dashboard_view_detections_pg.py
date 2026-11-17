"""Dashboard API on real PostgreSQL over HTTP (sentinel_reader login): the endpoints the detections view uses."""
import json
import threading
from fractions import Fraction
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from ton_sentinel.detectors import _median_exact, _published
from tests.integration.test_detection import COUNT, NET, SCORED, TOK, WALLETS, ready_world
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401
from ton_sentinel.db.work import promote_scores, score_window

DASH = Path(__file__).resolve().parents[2] / "src" / "ton_sentinel" / "dashboard"
RANGE = {"from": SCORED - 3600, "to": SCORED + 3600}


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


def promote_first(db):
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()


def view_flow(base):
    """The exact request chain of the view: range -> feed -> baseline by digest -> policy by id."""
    status, listed = get(base, "/v1/detections/range", {"network": NET, "token": TOK, **RANGE, "limit": 1000})
    assert status == 200
    status, feed = get(base, "/v1/alerts/feed", {"network": NET, **RANGE, "limit": 1000})
    assert status == 200
    return listed, feed


def test_view_assets_are_served_with_csp_and_match_the_files(served):
    _, base = served
    for name in ("detections.js", "detections-model.js"):
        with urlopen(base + f"/dashboard/assets/{name}", timeout=8) as response:
            assert response.status == 200 and response.read() == (DASH / name).read_bytes()
            assert response.headers["Content-Type"].startswith("text/javascript")
            assert "script-src 'self'" in response.headers["Content-Security-Policy"]


def test_view_endpoints_carry_provenance_and_the_banner_for_every_response(served):
    db, base = served
    promote_first(db)
    listed, feed = view_flow(base)
    digest = listed["detections"][0]["input_digest"]
    policy_id = listed["detections"][0]["policy_id"]
    _, baseline = get(base, "/v1/baseline", {"network": NET, "input_digest": digest})
    _, policy = get(base, "/v1/policy", {"policy_id": policy_id})
    _, tokens = get(base, "/v1/tokens", {"network": NET, **RANGE, "limit": 100})
    for body in (listed, feed, baseline, policy, tokens):
        for key in ("source_kind", "certification_state", "coverage_state", "scoring_state",
                    "honesty_banner", "generated_at"):
            assert isinstance(body["provenance"][key], str) and body["provenance"][key], key
        assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER


def test_pin_and_policy_read_by_the_view_reproduce_each_stored_result(served):
    db, base = served
    promote_first(db)
    listed, _ = view_flow(base)
    assert {d["detector"] for d in listed["detections"]} == {COUNT, WALLETS}
    for doc in listed["detections"]:
        status, baseline = get(base, "/v1/baseline", {"network": NET, "input_digest": doc["input_digest"]})
        assert status == 200
        (pin,) = baseline["baselines"]
        status, policy_body = get(base, "/v1/policy", {"policy_id": doc["policy_id"]})
        assert status == 200
        policy = policy_body["policy"]
        assert policy["policy_id"] == pin["policy"]["policy_id"] == doc["policy_id"]
        field = "swaps" if doc["detector"] == COUNT else "known_traders"
        eligible = [c for c in pin["cells"] if c["state"] in ("observed", "zero")]
        if field == "known_traders":
            eligible = [c for c in eligible if c["swaps"] == 0 or Fraction(
                c["swaps"] - c["unknown_count"], c["swaps"]) >= Fraction(policy["attribution_floor"])]
        values = [c[field] for c in eligible]
        median = _median_exact(values)
        mad = _median_exact([abs(Fraction(v) - median) for v in values])
        scale = max(Fraction(policy["scale_floor"]), Fraction(policy["mad_multiplier"]) * mad)
        score = (Fraction(pin["current_cell"][field]) - median) / scale
        assert (_published(median), _published(mad), _published(scale), _published(score),
                len(eligible)) == (doc["median"], doc["mad"], doc["scale"], doc["score"], doc["sample_count"])
        assert doc["threshold"] == policy["score_threshold"]


def test_alert_feed_status_for_a_listed_result_is_one_of_the_four_states_or_absent(served):
    db, base = served
    promote_first(db)
    listed, feed = view_flow(base)
    keyed = {}
    for alert in feed["alerts"]:
        keyed.setdefault((alert["token"], alert["window_start"], alert["detector"]), []).append(alert)
    assert feed["alerts"], "the promoted triggered window must have produced an alert revision"
    for alerts in keyed.values():
        assert {a["state"] for a in alerts} <= {"active", "pending", "retracted", "inactive"}
    triggered = [d for d in listed["detections"] if d["triggered"]]
    for doc in triggered:
        assert (TOK, doc["window_start"], doc["detector"]) in keyed
    for doc in listed["detections"]:
        if not doc["triggered"]:
            assert doc["status"] in ("scored", "cold_start", "insufficient_coverage", "missing")


def test_missing_pin_and_missing_policy_are_explicit_404s_the_view_can_show(served):
    db, base = served
    promote_first(db)
    assert get(base, "/v1/baseline", {"network": NET, "input_digest": "sha256:" + "0" * 64})[0] == 404
    assert get(base, "/v1/policy", {"policy_id": "sha256:" + "0" * 64})[0] == 404
