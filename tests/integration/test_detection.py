"""Explainable detectors, fenced score
promotion and revision-fenced alert state.

Expected detector results come from the independent oracle ``tests.oracle.
expected_detector_outcome``, fed only with the stored pinned record (``baseline_inputs``) and
the stored policy body (``detector_policies``), plus literal hand-computed values. Product
helpers are used only to build fixture state and to run the product path under test.
"""
import dataclasses
import json

import psycopg
import pytest

from ton_sentinel.config import DETECTOR_POLICY_V1
from ton_sentinel.contracts import BaselineInput
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.ledger import accept_corroborated_range
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.db.serving import WindowKey, read_alert_history, read_alerts, \
    read_detector_results
from ton_sentinel.db.work import (
    activate_detector_policy, close_baseline_dependents, promote_scores,
    register_detector_policy, rescore_stale, rescore_window, score_window,
)
from ton_sentinel.detectors import score_baseline
from ton_sentinel.source.scan import corroborate_range
from tests.integration.test_baseline_grid_pg import ev, promote_rows, qualify, row
from tests.integration.test_repair_fencing import world_with_correction
from tests.integration.test_revisions import Pages, World, b64, db, payload, val  # noqa: F401
from tests.integration.test_revisions import policy as scan_policy
from tests.oracle import expected_detector_outcome

W = 300
NET = "testnet"
TOK = "jetton:A"
SCORED = 24 * W
COUNT, WALLETS = "swap_count_spike.v1", "active_wallets_spike.v1"
RECORD = ("input_digest", "network", "token", "direction", "window_start", "policy_id",
          "lookback_seconds", "min_prior_windows", "universe_version", "admitted_window_start",
          "dependency_epoch", "active_epoch", "status", "eligible_count", "certification",
          "current_cell", "cells")


# ------------------------------------------------------------------------------- helpers
def ready_world(db, *, current=(12, 6, 0), eligible=24, qualify_current=True):
    """jetton:A: window 0 observed (1 swap), windows 1..eligible-1 qualified zeros, scored
    window 24 promoted with ``current`` (swaps, known, unknown)."""
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "detect", 300)
    for i in range(eligible):
        assert qualify(db, lease, world, i * W) is True
    if qualify_current:
        assert qualify(db, lease, world, SCORED) is True
    promote_rows(db, lease, 0, [row(0, TOK, 1)])
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, *current)])
    return world, lease


def stored_record(db, digest):
    db.commit()
    values = db.execute("SELECT " + ",".join(RECORD) + " FROM baseline_inputs "
                        "WHERE input_digest=%s", (digest,)).fetchone()
    db.commit()
    return dict(zip(RECORD, values))


def policy_body(db, policy_id):
    body = db.execute("SELECT body FROM detector_policies WHERE policy_id=%s",
                      (policy_id,)).fetchone()[0]
    db.commit()
    return body


def oracle_for(db, digest):
    record = stored_record(db, digest)
    return expected_detector_outcome(record, policy_body(db, record["policy_id"]))


def current_results(db, token=TOK, window=SCORED):
    """Stored current result documents (stale or not), keyed by detector."""
    db.commit()
    rows = db.execute(
        "SELECT r.detector_id, r.result, c.stale FROM detector_current c JOIN detector_results r "
        "USING (network,token,direction,window_start,detector_id,result_revision) "
        "WHERE c.network=%s AND c.token=%s AND c.window_start=%s",
        (NET, token, window)).fetchall()
    db.commit()
    return {d: (doc, stale) for d, doc, stale in rows}


def history(db, detector, token=TOK, window=SCORED):
    db.commit()
    out = [(h["alert_revision"], h["state"]) for h in
           read_alert_history(db, network=NET, token=token, window_start=window,
                              detector=detector)]
    db.commit()
    return out


def counts(db):
    db.commit()
    out = tuple(db.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in (
        "baseline_inputs", "detector_results", "detector_current", "alert_revisions"))
    db.commit()
    return out


def alerts(db, **kw):
    db.commit()
    out = read_alerts(db, network=NET, **kw)
    db.commit()
    return out


def served(db, **kw):
    db.commit()
    out = read_detector_results(db, network=NET, **kw)
    db.commit()
    return out


# ----------------------------------------------------------------- explainable persistence
def test_score_promotion_pins_input_and_persists_both_explainable_results(db):
    world, lease = ready_world(db)
    assert counts(db) == (0, 0, 0, 0)
    results = rescore_window(db, lease, NET, TOK, SCORED)
    db.commit()
    (digest,), = db.execute("SELECT input_digest FROM baseline_inputs").fetchall()
    expected = oracle_for(db, digest)
    assert list(results) == expected
    stored = current_results(db)
    assert [stored[COUNT][0], stored[WALLETS][0]] == expected
    assert not stored[COUNT][1] and not stored[WALLETS][1]
    # hand-computed: baseline [1, 0 x 23] -> median 0, MAD 0, scale 1
    count, wallets = expected
    assert (count["score"], count["triggered"], count["sample_count"]) == ("12", True, 24)
    assert (wallets["score"], wallets["triggered"], wallets["attribution"]) == ("6", True, "1")
    # typed columns agree with the document; the explanation reproduces from the pin alone
    typed = db.execute("SELECT detector_id, status, triggered, score::text, input_digest, "
                       "policy_id, result_revision FROM detector_results ORDER BY detector_id"
                       ).fetchall()
    assert typed == [(WALLETS, "scored", True, "6", digest, DETECTOR_POLICY_V1.identity, 1),
                     (COUNT, "scored", True, "12", digest, DETECTOR_POLICY_V1.identity, 1)]
    pinned = BaselineInput.from_record(stored_record(db, digest))
    assert list(score_baseline(pinned, DETECTOR_POLICY_V1)) == expected
    assert count["explanation"] == (
        f"swap_count_spike.v1 testnet/jetton:A [7200,7500): observed 12 swaps vs baseline "
        f"median 0, MAD 0, scale 1 from 24 eligible prior windows (lookback 604800s); score "
        f"12, threshold 6, floor 10: ACTIVE; input {digest}")
    # both alerts are active at revision 1 and served as current
    assert history(db, COUNT) == [(1, "active")] and history(db, WALLETS) == [(1, "active")]
    active = alerts(db)
    assert [(a["token"], a["window_start"], a["detector"], a["state"], a["alert_revision"],
             a["score"], a["input_digest"]) for a in active] == [
        (TOK, SCORED, WALLETS, "active", 1, "6", digest),
        (TOK, SCORED, COUNT, "active", 1, "12", digest)]
    assert [r["detector"] for r in served(db, token=TOK)] == [
        WALLETS, COUNT]
    # unchanged inputs: re-scoring is a no-op (no new pin, result or alert revision)
    assert list(rescore_window(db, lease, NET, TOK, SCORED)) == expected
    assert counts(db) == (1, 2, 2, 2)


def test_moved_input_or_forged_result_or_lost_lease_refuses_score_promotion(db):
    world, lease = ready_world(db, eligible=23)          # window 23 is still missing
    baseline, results = score_window(db, NET, TOK, SCORED)
    assert results[0]["status"] == "cold_start"
    assert qualify(db, lease, world, 23 * W) is True      # the input moves after the build
    db.commit()
    assert promote_scores(db, lease, baseline, results) is False
    assert counts(db) == (0, 0, 0, 0)                     # nothing pinned, scored or alerted
    fresh, fresh_results = score_window(db, NET, TOK, SCORED)
    assert fresh.digest != baseline.digest and fresh_results[0]["status"] == "scored"
    forged = (dict(fresh_results[0], triggered=False), fresh_results[1])
    db.commit()
    with pytest.raises(ValueError):
        promote_scores(db, lease, fresh, forged)
    with pytest.raises(ValueError):
        promote_scores(db, lease, fresh, results)          # results of another input
    acquire_transition(db, "newer-writer")                 # supersedes ``lease``
    assert promote_scores(db, lease, fresh, fresh_results) is False
    assert counts(db) == (0, 0, 0, 0)


# -------------------------------------------------------------- coverage recomputes own score
def test_recorded_coverage_invalidates_and_recomputes_its_own_window_score(db):
    world, lease = ready_world(db, qualify_current=False)  # scored window row is unqualified
    first = rescore_window(db, lease, NET, TOK, SCORED)
    assert [(r["status"], r["reasons"], r["score"]) for r in first] == [
        ("missing", ["current_partial"], None)] * 2
    assert [r["status"] for r in served(db)] == ["missing"] * 2
    assert alerts(db) == []
    assert qualify(db, lease, world, SCORED) is True       # coverage of the scored window
    # the same transaction made the window's own results non-current
    assert {d: s for d, (_, s) in current_results(db).items()} == {COUNT: True, WALLETS: True}
    assert served(db) == []
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    after = current_results(db)
    digest = after[COUNT][0]["input_digest"]
    assert [after[COUNT][0], after[WALLETS][0]] == oracle_for(db, digest)
    assert (after[COUNT][0]["current_state"], after[COUNT][0]["score"],
            after[COUNT][0]["triggered"]) == ("observed", "12", True)
    assert not after[COUNT][1]
    revisions = db.execute("SELECT detector_id, result_revision, status FROM detector_results "
                           "WHERE detector_id=%s ORDER BY result_revision", (COUNT,)).fetchall()
    assert revisions == [(COUNT, 1, "missing"), (COUNT, 2, "scored")]
    assert history(db, COUNT) == [(1, "active")]
    assert rescore_stale(db, lease, network=NET) == []     # nothing stale is left


# ------------------------------------------------------------------ dependents close on re-pin
def dependents(db, token, start):
    db.commit()
    out = db.execute("SELECT status, dirty_generation FROM dirty_windows WHERE network=%s "
                     "AND token=%s AND window_start=%s AND role='baseline_dependents'",
                     (NET, token, start)).fetchone()
    db.commit()
    return tuple(out)


def test_baseline_dependents_close_only_after_the_score_is_repinned(db):
    world, lease = ready_world(db, eligible=23)
    cold = rescore_window(db, lease, NET, TOK, SCORED)
    assert cold[0]["status"] == "cold_start" and cold[0]["sample_count"] == 23
    assert qualify(db, lease, world, 23 * W) is True       # dirties dependents of window 23
    status, generation = dependents(db, TOK, 23 * W)
    assert status == "pending"
    assert current_results(db)[COUNT][1] is True           # the dependent score is stale
    db.commit()
    assert close_baseline_dependents(db, lease, NET, TOK, 23 * W, generation) is False
    with pytest.raises(psycopg.Error, match="before every stale dependent score"):
        db.execute("UPDATE dirty_windows SET status='done' WHERE role='baseline_dependents'")
    db.rollback()
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    assert dependents(db, TOK, 23 * W) == ("done", generation)
    after = current_results(db)[COUNT][0]
    assert (after["status"], after["sample_count"], after["triggered"]) == ("scored", 24, True)
    assert after == oracle_for(db, after["input_digest"])[0]
    # the closing re-pin is a non-pending pin at or after the dependents' epoch
    pins = db.execute("SELECT status, dependency_epoch FROM baseline_inputs "
                      "ORDER BY dependency_epoch").fetchall()
    assert [p[0] for p in pins] == ["cold_start", "ready"] and pins[1][1] > pins[0][1]


def test_revision_correction_invalidates_at_once_and_closes_after_repair_and_rescoring(db):
    world, evidence, auth = world_with_correction(db)     # X=A at window 0, moves to 600
    lease = acquire_transition(db, "repair", 300)
    for start in (0, 300, 600, 900):
        assert qualify(db, lease, world, start) is True
    promote_rows(db, lease, 0, [row(0, TOK, 1), row(0, "TON", 1)])
    promote_rows(db, lease, 900, [row(900, TOK, 1)])
    first = rescore_window(db, lease, NET, TOK, 900)
    assert (first[0]["status"], first[0]["sample_count"]) == ("cold_start", 3)
    decide_revision(db, lease, evidence, auth)            # dirties windows 0 and 600
    db.commit()
    assert current_results(db, window=900)[COUNT][1] is True
    assert served(db) == []
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, 900)]
    pending = current_results(db, window=900)[COUNT][0]
    assert (pending["status"], pending["reasons"]) == ("pending", ["baseline_provisional"])
    assert served(db) == []    # pending is never served current
    assert dependents(db, TOK, 0)[0] == "pending"
    promote_rows(db, lease, 0, [])                         # repair both moved windows
    promote_rows(db, lease, 600, [row(600, TOK, 1), row(600, "TON", 1)])
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, 900)]
    final = current_results(db, window=900)
    assert [final[COUNT][0], final[WALLETS][0]] == oracle_for(
        db, final[COUNT][0]["input_digest"])
    record = stored_record(db, final[COUNT][0]["input_digest"])
    assert [(c["start"], c["end"], c["state"]) for c in record["cells"]] == [
        (0, 300, "zero"), (300, 600, "zero"), (600, 900, "observed")]
    assert final[COUNT][0]["status"] == "cold_start"
    db.commit()
    left = db.execute("SELECT token, window_start, status FROM dirty_windows "
                      "WHERE role='baseline_dependents' ORDER BY token COLLATE \"C\", window_start").fetchall()
    assert left == [("TON", 0, "done"), ("TON", 600, "done"), (TOK, 0, "done"),
                    (TOK, 600, "done")]


# ---------------------------------------------------------------- revision-fenced alert state
def alert_insert(db, revision, state, result_revision, digest, epoch):
    db.execute("INSERT INTO alert_revisions(network,token,direction,window_start,detector_id,"
               "alert_revision,state,result_revision,input_digest,dependency_epoch,reason,"
               "lease_owner,lease_fence) VALUES (%s,%s,'both',%s,%s,%s,%s,%s,%s,%s,'raw','x',1)",
               (NET, TOK, SCORED, COUNT, revision, state, result_revision, digest, epoch))


def test_alert_state_is_revision_fenced_and_a_stale_revision_cannot_reopen_it(db):
    world, lease = ready_world(db)
    old, old_results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, old, old_results) is True
    assert history(db, COUNT) == [(1, "active")]
    # a correction of the scored window's features makes the alert pending at once ...
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    assert history(db, COUNT) == [(1, "active"), (2, "pending")]
    assert alerts(db) == []
    # ... and the recomputed score retracts it explicitly
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    assert history(db, COUNT) == [(1, "active"), (2, "pending"), (3, "retracted")]
    assert history(db, WALLETS) == [(1, "active"), (2, "pending"), (3, "retracted")]
    retracted = current_results(db)[COUNT][0]
    assert (retracted["score"], retracted["reasons"]) == (
        "2", ["below_swap_floor", "below_score_threshold"])
    # the old worker's triggered result cannot re-open the alert through the product path
    db.commit()
    assert promote_scores(db, lease, old, old_results) is False
    assert history(db, COUNT)[-1] == (3, "retracted")
    # nor through raw SQL: stale result, reused superseded input, gaps, rewrites
    db.commit()
    now = retracted["input_digest"]
    for args, refusal in (
            ((4, "active", 1, old.digest, old.dependency_epoch), "current, fresh detector"),
            ((3, "active", 2, now, old.dependency_epoch), "sequential"),
            ((5, "retracted", 2, now, old.dependency_epoch), "sequential"),
            ((4, "pending", 2, now, old.dependency_epoch), "retracted -> pending"),
            ((4, "active", 2, now, old.dependency_epoch), "does not match its result"),
            ((4, "active", 2, now, old.dependency_epoch - 1), "stale alert revision")):
        with pytest.raises(psycopg.Error, match=refusal):
            alert_insert(db, *args)
        db.rollback()
    for statement, refusal in (
            ("UPDATE alert_revisions SET state='active'", "immutable"),
            ("DELETE FROM alert_revisions", "immutable"),
            ("UPDATE detector_current SET result_revision=1", "newer, fresh revision"),
            ("UPDATE detector_results SET triggered=true", "immutable")):
        with pytest.raises(psycopg.Error, match=refusal):
            db.execute(statement)
        db.rollback()
    # even the old triggered result forced current by hand cannot re-open the alert
    db.execute("INSERT INTO detector_results(network,token,direction,window_start,detector_id,"
               "result_revision,input_digest,policy_id,dependency_epoch,status,triggered,score,"
               "result,lease_owner,lease_fence) SELECT network,token,direction,window_start,"
               "detector_id,3,input_digest,policy_id,dependency_epoch,status,triggered,score,"
               "result,'raw',1 FROM detector_results WHERE detector_id=%s AND result_revision=1",
               (COUNT,))
    db.execute("UPDATE detector_current SET result_revision=3 WHERE detector_id=%s", (COUNT,))
    with pytest.raises(psycopg.Error, match="superseded input cannot re-open"):
        alert_insert(db, 4, "active", 3, old.digest, old.dependency_epoch)
    db.rollback()
    # only a newer input re-opens it, as a new revision
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 12, 6, 0)])
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    assert history(db, COUNT) == [(1, "active"), (2, "pending"), (3, "retracted"),
                                  (4, "active")]
    reopened = alerts(db)
    assert {a["detector"] for a in reopened} == {COUNT, WALLETS}
    assert all(a["input_digest"] not in (old.digest, retracted["input_digest"])
               for a in reopened)
    full = read_alert_history(db, network=NET, token=TOK, window_start=SCORED, detector=COUNT)
    assert [h["input_digest"] for h in full][:3] == [old.digest, old.digest,
                                                     retracted["input_digest"]]
    assert [h["result_revision"] for h in full] == [1, 1, 2, 3]


def test_policy_change_invalidates_results_and_refuses_scores_of_the_old_policy(db):
    world, lease = ready_world(db)
    rescore_window(db, lease, NET, TOK, SCORED)
    old, old_results = score_window(db, NET, TOK, SCORED)
    v2 = dataclasses.replace(DETECTOR_POLICY_V1, version="detector-policy.v2-test",
                             score_threshold="13")
    db.commit()
    assert register_detector_policy(db, lease, v2) is True
    assert register_detector_policy(db, lease, v2) is False          # idempotent
    assert activate_detector_policy(db, lease, v2) is True
    assert {d: s for d, (_, s) in current_results(db).items()} == {COUNT: True, WALLETS: True}
    assert history(db, COUNT) == [(1, "active"), (2, "pending")]
    db.commit()
    with pytest.raises(psycopg.Error, match="only by recomputation"):  # no un-staling by hand
        db.execute("UPDATE detector_current SET stale=false, stale_reason=NULL")
    db.rollback()
    assert promote_scores(db, lease, old, old_results) is False      # computed under v1
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    now = current_results(db)
    assert now[COUNT][0]["policy_id"] == v2.identity
    assert [now[COUNT][0], now[WALLETS][0]] == oracle_for(db, now[COUNT][0]["input_digest"])
    assert (now[COUNT][0]["score"], now[COUNT][0]["threshold"], now[COUNT][0]["triggered"]) \
        == ("12", "13", False)
    assert history(db, COUNT) == [(1, "active"), (2, "pending"), (3, "retracted")]
    assert activate_detector_policy(db, lease, v2) is False          # already active


def test_active_alert_goes_inactive_when_inputs_become_missing_or_zero_or_unscorable(db):
    """Body: 'an actual active alert becoming inactive and absent -> zero -> missing changes'."""
    world, lease = ready_world(db)
    rescore_window(db, lease, NET, TOK, SCORED)
    assert history(db, COUNT) == [(1, "active")]
    # absent -> zero: the window's rows are replaced by none; qualified coverage makes it a
    # covered zero, which is scored (not missing) and retracts the alert explicitly
    promote_rows(db, lease, SCORED, [])
    assert history(db, COUNT)[-1] == (2, "pending") and alerts(db) == []
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    zero = current_results(db)[COUNT][0]
    assert (zero["current_state"], zero["status"], zero["score"]) == ("zero", "scored", "0")
    assert history(db, COUNT) == [(1, "active"), (2, "pending"), (3, "retracted")]
    # re-open with real swaps, then lose the coverage's source epoch: current cell is no longer
    # qualified -> missing, never a fake zero -> the alert becomes inactive, not retracted
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 12, 6, 0)])
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    assert history(db, COUNT)[-1] == (4, "active")
    db.commit()
    db.execute("UPDATE active_source_epoch SET epoch='{\"universe\":\"changed\"}'::jsonb")
    db.commit()
    assert history(db, COUNT)[-1] == (5, "pending") and alerts(db) == []
    assert {s for _, s in current_results(db).values()} == {True}
    db.commit()
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    gone = current_results(db)
    assert {gone[d][0]["status"] for d in (COUNT, WALLETS)} == {"missing"}
    assert [gone[d][0] for d in (COUNT, WALLETS)] == oracle_for(db, gone[COUNT][0]["input_digest"])
    assert history(db, COUNT)[-1] == (6, "inactive")
    assert history(db, WALLETS)[-1][1] == "inactive"
    assert alerts(db) == []


# ----------------------------------------------------------------------- real Spark repair
@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("detection-test")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def traded(k, end, trader, i_asset="TON", o_asset=TOK):
    body = payload(ev(k), val(end=end, i_asset=i_asset, o_asset=o_asset))
    body.update(trader=trader, trader_method="explicit", trader_unambiguous=True)
    return body


def test_spark_repair_then_score_promotion_end_to_end(db, spark):
    from ton_sentinel.db.work import repair_windows
    world = World(db)
    actions = [traded(0, 7, b64(150))]                    # wallets: canonical source IDs
    actions += [traded(k, SCORED + k, b64(150 + k)) for k in range(1, 13)]
    group = corroborate_range(db, Pages(actions), world.epoch, scan_policy())
    assert accept_corroborated_range(db, acquire_transition(db, "seed"), group)
    lease = acquire_transition(db, "spark", 300)
    for i in range(25):
        assert qualify(db, lease, world, i * W) is True
    db.commit()
    repair_windows(db, lease, spark, [WindowKey(NET, i * W, i * W + W) for i in range(25)])
    db.commit()
    for token in (TOK, "TON"):
        results = rescore_window(db, lease, NET, token, SCORED)
        assert list(results) == oracle_for(db, results[0]["input_digest"])
        # independent hand expectation from the seeded actions: one swap by one wallet in
        # window 0, qualified empty windows 1..23, twelve swaps by twelve wallets at 24
        assert [(r["observed"], r["median"], r["score"], r["triggered"]) for r in results] \
            == [(12, "0", "12", True), (12, "0", "12", True)]
        record = stored_record(db, results[0]["input_digest"])
        assert [(c["start"], c["end"], c["state"], c["swaps"]) for c in record["cells"]] == [
            (0, 300, "observed", 1), (300, 7200, "zero", 0)]
    assert {(a["token"], a["detector"]) for a in alerts(db)} == {
        (TOK, COUNT), (TOK, WALLETS), ("TON", COUNT), ("TON", WALLETS)}
