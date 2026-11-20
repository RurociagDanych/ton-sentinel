"""Both v1 detectors over the recorded real live window with injected anomalies, on real PostgreSQL."""

import json
import os
from collections import defaultdict
from fractions import Fraction
from pathlib import Path

import psycopg
import pytest

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.work import promote_scores, rescore_stale, rescore_window, score_window
from ton_sentinel.source.live import live_swap_batch
from tests.integration.test_baseline_grid_pg import ev, promote_rows, qualify, row
from tests.integration.test_detection import (
    COUNT, NET, SCORED, W, WALLETS, alerts, current_results, history, oracle_for,
    stored_record,
)
from tests.integration.test_revisions import World, db, val  # noqa: F401

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "live" / "g3_window.json"
SYNTHETIC = "SYNTHETIC"
TOK_COUNT = "jetton:0:e3a65933a97b5e4"
TOK_WALLETS = "jetton:0:0d716e395b44e9f"
TOK_CONTROL = "jetton:0:b113a994b5024a1"
WINDOW0 = 1794559800


def real_windows():
    """{token: [(swaps, known_traders, unknown) for the 3 real windows]} from the recording,
    computed in plain Python (independent of Spark and of the product feature path)."""
    fx = json.loads(FIXTURE.read_text())
    os.environ.setdefault("TON_SENTINEL_SYNTHETIC_FIXTURES", "test-only")
    batch = live_swap_batch(fx["action_responses"], fx["trace_responses"], network=fx["network"],
                            native_aliases=frozenset(fx["native_aliases"]))
    swaps, traders, unknown = defaultdict(int), defaultdict(set), defaultdict(int)
    for payload in batch.payloads:
        if not payload["eligible"]:
            continue
        index = (payload["action_end"] - WINDOW0) // W
        for side in ("input", "output"):
            key = (payload[side]["asset"], index)
            swaps[key] += 1
            if payload.get("trader") and payload.get("trader_unambiguous"):
                traders[key].add(payload["trader"])
            else:
                unknown[key] += 1
    tokens = {key[0] for key in swaps}
    return {t: [(swaps[(t, i)], len(traders[(t, i)]), unknown[(t, i)]) for i in range(3)]
            for t in tokens}


def full(prefix, real):
    (token,) = [t for t in real if t.startswith(prefix)]
    return token


def median_mad_score(values, observed):
    ordered = sorted(values)
    median = Fraction(ordered[len(ordered) // 2 - 1] + ordered[len(ordered) // 2], 2)
    deviations = sorted(abs(v - median) for v in values)
    mad = (deviations[len(deviations) // 2 - 1] + deviations[len(deviations) // 2]) / 2
    scale = max(Fraction("1.4826") * mad, Fraction(1))
    return (observed - median) / scale


def published(value):
    scaled = round(value * 10 ** 9)
    whole, part = divmod(scaled, 10 ** 9)
    return str(whole) + ("." + str(part).rjust(9, "0").rstrip("0") if part else "")


def build(db, real, injected):
    """SYNTHETIC grid: windows 0..23 repeat the 3 real windows, window 24 (SCORED) is real
    window 0 unless ``injected`` overrides a token's row (swaps, known, unknown)."""
    tokens = [full(p, real) for p in (TOK_COUNT, TOK_WALLETS, TOK_CONTROL)]
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "detect", 300)
    for i in range(25):
        assert qualify(db, lease, world, i * W) is True
    for i in range(24):
        promote_rows(db, lease, i * W, [row(i * W, t, real[t][i % 3][0], real[t][i % 3][1],
                                            real[t][i % 3][2]) for t in tokens])
    promote_rows(db, lease, SCORED, [row(SCORED, t, *injected.get(t, real[t][0]))
                                     for t in tokens])
    return lease, tokens


def test_synthetic_injected_anomalies_alert_with_pinned_explanations_control_stays_quiet(db):
    real = real_windows()
    t_count, t_wallets, t_control = (full(p, real) for p in (TOK_COUNT, TOK_WALLETS, TOK_CONTROL))
    spike = {t_count: (60, real[t_count][0][1], real[t_count][0][2]),
             t_wallets: (18, 18, real[t_wallets][0][2])}
    lease, _ = build(db, real, spike)
    for token in (t_count, t_wallets, t_control):
        rescore_window(db, lease, NET, token, SCORED)

    got = {(a["token"], a["detector"]) for a in alerts(db)}
    assert got == {(t_count, COUNT), (t_wallets, WALLETS)}
    assert history(db, COUNT, t_control) == [] and history(db, WALLETS, t_control) == []
    control = current_results(db, t_control)
    assert [control[COUNT][0]["triggered"], control[WALLETS][0]["triggered"]] == [False, False]

    for token, detector, index in ((t_count, COUNT, 0), (t_wallets, WALLETS, 1)):
        doc = current_results(db, token)[detector][0]
        both = oracle_for(db, doc["input_digest"])
        assert doc in both and doc["triggered"] is True and doc["status"] == "scored"
        pinned = stored_record(db, doc["input_digest"])
        assert pinned["token"] == token and pinned["window_start"] == SCORED
        base = [real[token][i % 3][index] for i in range(24)]
        assert doc["score"] == published(median_mad_score(base, spike[token][index]))
        assert doc["sample_count"] == 24
    # the count spike does not fake a wallets alert and vice versa (hand-set to be orthogonal)
    assert current_results(db, t_count)[WALLETS][0]["triggered"] is False
    assert current_results(db, t_wallets)[COUNT][0]["triggered"] is False


def test_synthetic_alert_is_revision_fenced_and_removing_the_injection_retracts_it(db):
    real = real_windows()
    t_count, t_wallets, t_control = (full(p, real) for p in (TOK_COUNT, TOK_WALLETS, TOK_CONTROL))
    spike = {t_count: (60, real[t_count][0][1], real[t_count][0][2]),
             t_wallets: (18, 18, real[t_wallets][0][2])}
    lease, _ = build(db, real, spike)
    old, old_results = score_window(db, NET, t_count, SCORED)
    db.commit()
    assert promote_scores(db, lease, old, old_results) is True
    assert history(db, COUNT, t_count) == [(1, "active")]
    rescore_window(db, lease, NET, t_wallets, SCORED)
    assert history(db, WALLETS, t_wallets) == [(1, "active")]

    promote_rows(db, lease, SCORED, [row(SCORED, t, *real[t][0])
                                     for t in (t_count, t_wallets, t_control)])
    assert history(db, COUNT, t_count)[-1][1] == "pending" and alerts(db) == []
    assert sorted(rescore_stale(db, lease, network=NET)) == sorted(
        [(NET, t_count, SCORED), (NET, t_wallets, SCORED)])
    assert history(db, COUNT, t_count) == [(1, "active"), (2, "pending"), (3, "retracted")]
    assert history(db, WALLETS, t_wallets) == [(1, "active"), (2, "pending"), (3, "retracted")]
    assert alerts(db) == []
    retracted = current_results(db, t_count)[COUNT][0]
    assert retracted["triggered"] is False
    assert history(db, COUNT, t_control) == []

    db.commit()
    assert promote_scores(db, lease, old, old_results) is False
    assert history(db, COUNT, t_count)[-1] == (3, "retracted")
    for args, refusal in (((4, "active", 1, old.digest, old.dependency_epoch),
                           "current, fresh detector"),
                          ((4, "active", 2, retracted["input_digest"], old.dependency_epoch),
                           "does not match its result")):
        with pytest.raises(psycopg.Error, match=refusal):
            alert_insert_for(db, t_count, *args)
        db.rollback()
    assert alerts(db) == []


def alert_insert_for(db, token, revision, state, result_revision, digest, epoch):
    db.execute("INSERT INTO alert_revisions(network,token,direction,window_start,detector_id,"
               "alert_revision,state,result_revision,input_digest,dependency_epoch,reason,"
               "lease_owner,lease_fence) VALUES (%s,%s,'both',%s,%s,%s,%s,%s,%s,%s,'raw','x',1)",
               (NET, token, SCORED, COUNT, revision, state, result_revision, digest, epoch))
