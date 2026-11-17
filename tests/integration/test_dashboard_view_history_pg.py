"""Dashboard API on real PostgreSQL over HTTP (sentinel_reader login): the endpoints the alert history view uses."""
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
from ton_sentinel.db.work import promote_scores, score_window

DASH = Path(__file__).resolve().parents[2] / "src" / "ton_sentinel" / "dashboard"
RANGE = {"from": SCORED - 3600, "to": SCORED + 3600}
LABELS = {"current", "stale", "superseded", "pending"}
STATES = {"active", "pending", "retracted", "inactive"}


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


def test_history_assets_are_served_with_csp_and_match_the_files(served):
    _, base = served
    for name in ("history.js", "history-model.js"):
        with urlopen(base + f"/dashboard/assets/{name}", timeout=8) as response:
            assert response.status == 200 and response.read() == (DASH / name).read_bytes()
            assert response.headers["Content-Type"].startswith("text/javascript")
            assert "script-src 'self'" in response.headers["Content-Security-Policy"]


def test_feed_then_drilldown_chain_carries_provenance_and_known_labels(served):
    db, base = served
    promote_first(db)
    status, feed = get(base, "/v1/alerts/feed", {"network": NET, **RANGE, "limit": 1000})
    assert status == 200 and feed["alerts"], "the promoted triggered window must have produced an alert revision"
    seen = 0
    for item in feed["alerts"]:
        assert item["state"] in STATES and item["label"] in LABELS
        status, body = get(base, "/v1/detections/revisions", {
            "network": NET, "token": item["token"], "window_start": item["window_start"],
            "detector": item["detector"], "limit": 1000})
        assert status == 200
        for response in (feed, body):
            for key in ("source_kind", "certification_state", "coverage_state", "scoring_state",
                        "honesty_banner", "generated_at"):
                assert isinstance(response["provenance"][key], str) and response["provenance"][key], key
            assert response["honesty_banner"] == dashboard_payload.HONESTY_BANNER
        revisions = body["revisions"]
        assert [r["result_revision"] for r in revisions] == sorted(r["result_revision"] for r in revisions)
        for rev in revisions:
            assert rev["label"] in LABELS
            assert rev["alert_state"] is None or rev["alert_state"] in STATES
        # the result revision a feed item points at is one of the listed revisions of its key
        assert item["result_revision"] in {r["result_revision"] for r in revisions}
        seen += 1
    assert seen == len(feed["alerts"])


def test_drilldown_of_an_unknown_key_is_an_empty_list_not_an_invented_revision(served):
    db, base = served
    promote_first(db)
    status, body = get(base, "/v1/detections/revisions", {
        "network": NET, "token": TOK + ":none", "window_start": SCORED,
        "detector": "swap_count_spike.v1", "limit": 1000})
    assert status == 200 and body["revisions"] == [] and "provenance" in body
