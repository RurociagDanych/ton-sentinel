"""Dashboard API on real PostgreSQL over HTTP, through the sentinel_reader login: GET /v1/baseline."""
import json
import threading
from fractions import Fraction
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from ton_sentinel.config import DETECTOR_POLICY_V1
from ton_sentinel.detectors import _median_exact, _published
from tests.integration.test_baseline_grid_pg import promote_rows, row
from tests.integration.test_detection import (
    COUNT, NET, SCORED, TOK, WALLETS, current_results, ready_world,
)
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401
from ton_sentinel.db.work import promote_scores, rescore_stale, score_window

POLICY = DETECTOR_POLICY_V1


@pytest.fixture
def served(db, reader_dsn):  # noqa: F811
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}", reader_dsn
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get(base, query, path="/v1/baseline"):
    try:
        with urlopen(Request(base + path + "?" + urlencode(query)), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def _strings(value):
    """Every string key and value anywhere in a JSON document."""
    if isinstance(value, dict):
        return [s for k, v in value.items() for s in [k, *_strings(v)]]
    if isinstance(value, list):
        return [s for v in value for s in _strings(v)]
    return [value] if isinstance(value, str) else []


def promote_first(db):
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()
    return lease


def published(db, detector):
    doc, _stale = current_results(db)[detector]
    return doc


def test_pinned_input_reproduces_published_median_mad_scale_and_score(served):
    db, base, _ = served
    promote_first(db)
    for detector, field in ((COUNT, "swaps"), (WALLETS, "known_traders")):
        doc = published(db, detector)
        status, body = get(base, {"network": NET, "input_digest": doc["input_digest"]})
        assert status == 200, body
        (pin,) = body["baselines"]
        assert pin["input_digest"] == doc["input_digest"] and pin["policy"]["policy_id"] == doc[
            "policy_id"]
        eligible = [c for c in pin["cells"] if c["state"] in ("observed", "zero")]
        if field == "known_traders":
            eligible = [c for c in eligible
                        if c["swaps"] == 0
                        or Fraction(c["swaps"] - c["unknown_count"], c["swaps"])
                        >= Fraction(POLICY.attribution_floor)]
        values = [c[field] for c in eligible]
        median = _median_exact(values)
        mad = _median_exact([abs(Fraction(v) - median) for v in values])
        scale = max(Fraction(POLICY.scale_floor), Fraction(POLICY.mad_multiplier) * mad)
        score = (Fraction(pin["current_cell"][field]) - median) / scale
        assert (_published(median), _published(mad), _published(scale), _published(score)) == (
            doc["median"], doc["mad"], doc["scale"], doc["score"])
        assert len(eligible) == doc["sample_count"]


def test_response_has_digest_cells_states_counts_policy_and_provenance(served):
    db, base, _ = served
    promote_first(db)
    digest = published(db, COUNT)["input_digest"]
    status, body = get(base, {"network": NET, "input_digest": digest})
    assert status == 200 and body["truncated"] is False
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert body["provenance"]["certification_state"] == "not_certified"
    assert body["provenance"]["qualification_class"] != "unavailable"
    (pin,) = body["baselines"]
    assert pin["policy"] == {"policy_id": POLICY.identity,
                             "lookback_seconds": POLICY.lookback_seconds,
                             "min_prior_windows": POLICY.min_prior_windows}
    assert pin["token"] == TOK and pin["window_start"] == SCORED
    expected = POLICY.lookback_seconds // 300
    starts = [c["window_start"] for c in pin["cells"]]
    assert starts == [max(0, SCORED - POLICY.lookback_seconds) + 300 * i
                      for i in range(len(starts))] and len(starts) <= expected
    states = {c["state"] for c in pin["cells"]}
    assert states <= {"observed", "zero", "partial", "provisional", "missing", "pre_universe"}
    assert {"observed", "zero"} <= states
    counts = pin["coverage_counts"]
    assert counts["cells_total"] == len(pin["cells"]) and counts["expected"] == expected
    assert sum(counts["by_state"].values()) == counts["cells_total"]
    assert counts["eligible"] == pin["eligible_count"]
    assert pin["current_cell"]["window_start"] == SCORED
    assert pin["status"] in ("ready", "cold_start", "pending", "missing")
    # selecting by the window key finds the same pin
    status, by_key = get(base, {"network": NET, "token": TOK, "window_start": SCORED})
    assert status == 200 and digest in [p["input_digest"] for p in by_key["baselines"]]


def test_no_lease_owner_lineage_or_address_content_in_any_baseline_response(served):
    db, base, _ = served
    promote_first(db)
    owner = db.execute("SELECT lease_owner FROM baseline_inputs LIMIT 1").fetchone()[0]
    db.commit()
    assert owner
    digest = published(db, COUNT)["input_digest"]
    for query in ({"network": NET, "input_digest": digest},
                  {"network": NET, "token": TOK, "window_start": SCORED}):
        status, body = get(base, query)
        text = json.dumps(body)
        assert status == 200
        assert dashboard_payload.find_violations(body) == []
        for needle in ("lease_owner", "lease_fence", "active_epoch", "lineage", "revision\"",
                       "wallet"):
            assert needle not in text, needle
        assert owner not in _strings(body)
        for pin in body["baselines"]:
            for cell in [*pin["cells"], pin["current_cell"]]:
                assert set(cell) == {"window_start", "state", "swaps", "known_traders",
                                     "unknown_count", "coverage", "synthetic"}


def test_absent_pin_is_explicit_404_and_bad_selectors_are_400(served):
    db, base, _ = served
    promote_first(db)
    absent = "sha256:" + "0" * 64
    assert get(base, {"network": NET, "input_digest": absent}) == (
        404, {"error": "baseline_not_found"})
    assert get(base, {"network": "mainnet", "input_digest": published(db, COUNT)[
        "input_digest"]})[0] == 404  # digest exists, but not on this network
    assert get(base, {"network": NET, "token": "jetton:none", "window_start": SCORED})[0] == 404
    assert get(base, {"network": NET, "token": TOK, "window_start": SCORED + 300})[0] == 404
    for bad in ({"network": NET}, {"network": NET, "input_digest": "sha256:xyz"},
                {"network": NET, "token": TOK, "window_start": SCORED + 1},
                {"network": NET, "token": TOK, "window_start": SCORED, "limit": 11},
                {"network": NET, "token": TOK, "window_start": SCORED, "limit": 0},
                {"network": NET, "input_digest": absent, "token": TOK}):
        assert get(base, bad)[0] == 400, bad


def test_repin_after_correction_lists_each_pin_newest_first_within_limit(served):
    db, base, _ = served
    lease = promote_first(db)
    first = published(db, COUNT)["input_digest"]
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])  # correction moves the input
    db.commit()
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    db.commit()
    second = published(db, COUNT)["input_digest"]
    assert first != second
    query = {"network": NET, "token": TOK, "window_start": SCORED}
    status, body = get(base, query)
    digests = [p["input_digest"] for p in body["baselines"]]
    assert status == 200 and digests[0] == second and first in digests
    stamps = [p["pinned_at"] for p in body["baselines"]]
    assert stamps == sorted(stamps, reverse=True)
    status, body = get(base, {**query, "limit": 1})
    assert [p["input_digest"] for p in body["baselines"]] == [second]
    assert body["truncated"] is True and body["limit"] == 1
    # the older pin stays readable by its digest
    assert get(base, {"network": NET, "input_digest": first})[0] == 200


def test_reader_role_is_read_only_on_baseline_inputs_and_endpoint_never_writes(served):
    db, base, dsn = served
    promote_first(db)
    digest = published(db, COUNT)["input_digest"]
    with psycopg.connect(dsn) as reader:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("UPDATE baseline_inputs SET status = status")
    db.commit()
    before = db.execute("SELECT count(*) FROM baseline_inputs").fetchone()[0]
    db.commit()
    assert get(base, {"network": NET, "input_digest": digest})[0] == 200
    after = db.execute("SELECT count(*) FROM baseline_inputs").fetchone()[0]
    db.commit()
    assert before == after
