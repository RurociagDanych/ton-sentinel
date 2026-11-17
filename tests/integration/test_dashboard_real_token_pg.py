"""Dashboard API on real PostgreSQL over HTTP (reader login): routes that carry a REAL recorded
jetton token identifier (tests/fixtures/live/g3_window.json) answer 200 with provenance.

Only the token identifiers and the window grid come from the recording; the served rows are
written by the series helper (behaviour check, not a quality claim)."""
import json
from pathlib import Path

import pytest

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.work import score_pending

from tests.integration.test_revisions import db  # noqa: F401
from tests.integration.test_dashboard_series_pg import (  # noqa: F401
    _insert, base, get, reader_dsn)

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "live" / "g3_window.json"
RECORDED = json.loads(FIXTURE.read_text())
W0, W1 = RECORDED["window"]
TOKENS = sorted({row["token"] for row in RECORDED["truth"]["window_aggregates_all_dex"]
                 if row["token"].startswith("jetton:")})[:2]
NET = "mainnet"
RANGE = {"from": W0, "to": W1}


@pytest.fixture
def seeded(db):  # noqa: F811
    for start in range(W0, W1, 300):
        for token in TOKENS:
            _insert(db, NET, start, token, swaps=3)
    db.commit()
    return db


def test_recorded_tokens_are_real_raw_jetton_identifiers():
    assert len(TOKENS) == 2
    assert all(t.split(":")[1] in ("0", "-1") and len(t.split(":")[2]) == 64 for t in TOKENS)


def test_token_index_serves_recorded_jetton_identifiers(base):
    status, body = get(base, "/v1/tokens", {"network": NET, **RANGE})
    assert status == 200 and "provenance" in body
    assert [t["token"] for t in body["tokens"]] == sorted(TOKENS, key=str.encode)


@pytest.mark.parametrize("path", ["/v1/series", "/v1/coverage", "/v1/detections/range"])
def test_token_routes_serve_a_recorded_jetton_identifier(base, path):
    status, body = get(base, path, {"network": NET, "token": TOKENS[0], **RANGE})
    assert status == 200 and "provenance" in body and body["token"] == TOKENS[0]


def test_detection_routes_serve_results_that_name_a_recorded_token(base, seeded):
    lease = acquire_transition(seeded, "q1-real-token-score", 300)
    seeded.commit()
    report = score_pending(seeded, lease, network=NET, limit=1000)
    seeded.commit()
    assert report["first_scored"] == len(TOKENS) * len(range(W0, W1, 300))
    status, body = get(base, "/v1/detections/range", {"network": NET, "token": TOKENS[0], **RANGE})
    assert status == 200 and "provenance" in body
    assert body["detections"] == []  # these rows only yield pending results, hidden by design
    status, body = get(base, "/v1/detections/revisions", {
        "network": NET, "token": TOKENS[0], "window_start": W0, "detector": "swap_count_spike.v1"})
    assert status == 200 and "provenance" in body and len(body["revisions"]) == 1
    assert TOKENS[0] in json.dumps(body["revisions"][0])  # explanation names the real token
