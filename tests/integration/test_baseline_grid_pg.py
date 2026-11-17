"""Qualified baseline grid and input contract.

Expected states come from the hand-authored oracle in ``tests/fixtures/truth/expected.json``
(``baseline_grid``) or are written out literally below. Product helpers are used only to build
fixture state (corroborated walks, promotions, qualifications), never to decide expectations.
"""
import json
from pathlib import Path

import psycopg
import pytest

from ton_sentinel.config import DETECTOR_POLICY_V1
from ton_sentinel.contracts import BaselineInput
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.db.serving import WindowKey, replace_features
from ton_sentinel.db.work import (
    build_baseline_input, close_baseline_dependents, freeze_work, pin_baseline_input, promote,
    record_qualification,
)
from tests.integration.test_repair_fencing import world_with_correction
from tests.integration.test_revisions import World, db, val  # noqa: F401  (db fixture)

ORACLE = json.loads((Path(__file__).parents[1] / "fixtures" / "truth" / "expected.json")
                    .read_text())["baseline_grid"]
W = 300
NET = "testnet"


# ------------------------------------------------------------------------------- helpers
def row(start, token, swaps, known=None, unknown=None, generation="g"):
    known = swaps if known is None else known
    unknown = 0 if unknown is None else unknown
    return {"network": NET, "window_start": start, "window_end": start + W, "token": token,
            "generation": generation, "swaps": swaps, "input_raw": swaps, "output_raw": None,
            "known_traders": known, "unknown_count": unknown,
            "coverage_status": "unknown", "status": "provisional", "provisional": True}


def promote_rows(db, lease, start, rows):
    db.commit()
    frozen = freeze_work(db, lease, [WindowKey(NET, start, start + W)])
    db.commit()
    assert promote(db, lease, frozen, rows) is True


def qualify(db, lease, world, start, delta=60):
    db.commit()
    return record_qualification(
        db, lease, network=NET, window_start=start, mode="bounded_observed",
        mapping_version="synthetic-map.v1", delta_seconds=delta,
        trace_ranges=[[start, start + W + delta]], source_epoch=json.loads(world.epoch.to_json()),
        synthetic=True)


def build(db, token, start):
    db.commit()
    return build_baseline_input(db, NET, token, start)


def states(baseline):
    return [c.state for c in baseline.cells]


def epoch_now(db):
    db.commit()
    return db.execute("SELECT epoch FROM dependency_epoch").fetchone()[0]


def insert_record(db, record, owner="x", fence=1):
    columns = sorted(record)
    values = [json.dumps(record[c]) if c in ("cells", "current_cell") else record[c]
              for c in columns]
    db.execute("INSERT INTO baseline_inputs(" + ",".join(columns) + ",lease_owner,lease_fence)"
               " VALUES (" + ",".join(["%s"] * len(columns)) + ",%s,%s)", (*values, owner, fence))


def oracle_index():
    out = {}
    for entry in ORACLE["entries"]:
        for i in range(entry["first"], entry["last"] + 1):
            out[i] = entry
    return out


def ev(i):
    return (100 + i, 200 + i)


# ------------------------------------------------------------------- policy identity / schema
def test_v1_detector_policy_is_registered_with_its_exact_identity(db):
    got = db.execute("SELECT policy_id, version, body FROM detector_policies").fetchall()
    assert got == [(DETECTOR_POLICY_V1.identity, "detector-policy.v1",
                    json.loads(DETECTOR_POLICY_V1.to_json()))]
    db.commit()
    with pytest.raises(psycopg.Error):
        db.execute("UPDATE detector_policies SET version='x'")
    db.rollback()
    with pytest.raises(psycopg.Error):
        db.execute("INSERT INTO detector_policies(policy_id,version,body) VALUES "
                   "('not-a-digest','v2','{}'::jsonb)")
    db.rollback()


def test_qualification_schema_rejects_fixture_certification_and_bad_shapes(db):
    base = ("INSERT INTO action_window_qualification(network,window_start,window_end,mode,"
            "mapping_version,delta_seconds,trace_ranges,source_epoch,synthetic,certification,"
            "dependency_epoch,lease_owner,lease_fence) VALUES "
            "('testnet',%s,%s,%s,'m',%s,%s::jsonb,'{}'::jsonb,%s,%s::jsonb,1,'o',1)")
    bad = [
        (0, 300, "certified", 0, "[[0,300]]", True, '{"c":1}'),     # synthetic certified
        (0, 300, "certified", 0, "[[0,300]]", False, None),         # certified w/o evidence
        (0, 300, "bounded_observed", 0, "[]", True, None),          # no trace ranges
        (0, 600, "bounded_observed", 0, "[[0,600]]", True, None),   # not one window
        (1, 301, "bounded_observed", 0, "[[1,301]]", True, None),   # unaligned
        (0, 300, "bounded_observed", -1, "[[0,300]]", True, None),  # negative delta
        (0, 300, "gap", 0, "[[0,300]]", True, None),
    ]
    for args in bad:
        with pytest.raises(psycopg.Error):
            db.execute(base, args)
        db.rollback()


# ---------------------------------------------------------------------- the oracle scenario
def oracle_world(db):
    """Build the ``baseline_grid`` scenario from its hand-authored setup facts."""
    index = oracle_index()
    world = World(db)
    actions = {ev(i): val(end=i * W + 7, i_asset=e["setup"]["canonical_action"][0],
                          o_asset=e["setup"]["canonical_action"][1])
               for i, e in index.items() if "canonical_action" in e["setup"]}
    world.seed(actions)
    lease = acquire_transition(db, "grid")
    for i in sorted(index):
        if index[i]["setup"].get("qualified"):
            assert qualify(db, lease, world, i * W) is True
    for i in sorted(index):                      # ascending: admission is the first promotion
        setup = index[i]["setup"]
        if "promoted_row" in setup:
            r = setup["promoted_row"]
            promote_rows(db, lease, i * W, [row(i * W, ORACLE["token"], r["swaps"],
                                                r["known_traders"], r["unknown_count"])])
        elif "promoted_other_token" in setup:
            promote_rows(db, lease, i * W, [row(i * W, setup["promoted_other_token"], 1)])
    for i in sorted(index):
        if index[i]["setup"].get("pending_target"):
            db.execute("INSERT INTO dirty_windows(network,token,window_start,role,window_end,"
                       "covers_start,covers_end,dependency_epoch,reason,cause_event_id) VALUES "
                       "(%s,%s,%s,'target',%s,%s,%s,1,'fixture','fixture')",
                       (NET, ORACLE["token"], i * W, i * W + W, i * W, i * W + W))
            db.commit()
    newer = acquire_transition(db, "direct-writer")     # a non-canonical write after promotion
    for i in sorted(index):
        direct = index[i]["setup"].get("direct_write_after_promotion")
        if direct:
            replace_features(db, newer, [row(i * W, ORACLE["token"], direct["swaps"],
                                             direct["known_traders"], direct["unknown_count"])],
                             windows=[WindowKey(NET, i * W, i * W + W)])
            db.commit()
    return world, newer


def test_grid_states_match_the_hand_authored_oracle(db):
    world, lease = oracle_world(db)
    index = oracle_index()
    scored = ORACLE["scored_index"] * W
    got = build(db, ORACLE["token"], scored)
    assert [c.window_start for c in got.cells] == [i * W for i in range(ORACLE["scored_index"])]
    assert states(got) == [index[i]["expected"] for i in range(ORACLE["scored_index"])]
    assert got.current.state == index[ORACLE["scored_index"]]["expected"]
    want = ORACLE["expected_input"]
    assert (got.status, got.eligible_count, len(got.cells), got.certification) == (
        want["status"], want["eligible_count"], want["cells"], want["certification"])
    # exact pinned values: an observed cell carries the promoted row, a zero carries zeros
    by_start = {c.window_start: c for c in got.cells}
    assert (by_start[3000].swaps, by_start[3000].known_traders, by_start[3000].unknown_count) \
        == (2, 2, 0)
    assert (by_start[3300].swaps, by_start[3300].known_traders) == (0, 0)
    assert by_start[9900].swaps == 4 and by_start[9900].coverage is None       # partial
    assert by_start[9600].swaps is None                                        # missing
    assert (got.current.swaps, got.current.known_traders, got.current.unknown_count) == (12, 10, 2)
    universe = db.execute("SELECT token, admitted_window_start FROM token_universe "
                          "ORDER BY token COLLATE \"C\"").fetchall()
    assert universe == [("jetton:A", 3000), ("jetton:Z", 9300)]
    # reproducible: an unchanged database rebuilds the identical input
    assert build(db, ORACLE["token"], scored) == got


def test_pin_reads_back_exactly_and_is_idempotent(db):
    world, lease = oracle_world(db)
    scored = ORACLE["scored_index"] * W
    got = build(db, ORACLE["token"], scored)
    db.commit()
    assert pin_baseline_input(db, lease, got) is True
    assert pin_baseline_input(db, lease, got) is True        # same digest: no second row
    db.commit()
    rows = db.execute("SELECT input_digest, status, eligible_count, certification, cells,"
                      " current_cell, policy_id FROM baseline_inputs").fetchall()
    assert len(rows) == 1
    digest, status, eligible, certification, cells, current, policy_id = rows[0]
    assert (digest, status, eligible, certification, policy_id) == (
        got.digest, "pending", 23, "synthetic_bounded_observed", DETECTOR_POLICY_V1.identity)
    record = got.to_record()
    assert cells == record["cells"] and current == record["current_cell"]
    assert BaselineInput.from_record({**record, "cells": cells, "current_cell": current}) == got
    db.commit()
    with pytest.raises(psycopg.Error):
        db.execute("UPDATE baseline_inputs SET status='ready'")
    db.rollback()
    with pytest.raises(psycopg.Error):
        db.execute("DELETE FROM baseline_inputs")
    db.rollback()


def test_pin_of_a_stale_input_writes_nothing(db):
    world, lease = oracle_world(db)
    got = build(db, ORACLE["token"], ORACLE["scored_index"] * W)
    assert qualify(db, lease, world, 38 * W) is True        # coverage change after the build
    db.commit()
    assert pin_baseline_input(db, lease, got) is False
    db.commit()
    assert db.execute("SELECT count(*) FROM baseline_inputs").fetchone()[0] == 0


# ------------------------------------------------------------ schema rejects bad baseline inputs
def mutated(record, fn):
    copy = json.loads(json.dumps(record))
    fn(copy)
    return copy


def test_schema_rejects_incomplete_cross_scope_or_forged_inputs(db):
    world, lease = oracle_world(db)
    good = build(db, ORACLE["token"], ORACLE["scored_index"] * W).to_record()
    db.commit()

    def gap(r):
        r["cells"][0]["end"] -= W

    def short(r):
        r["cells"].pop()

    def cross_token(r):
        r["cells"][3]["token"] = "jetton:B"

    def cross_network(r):
        r["cells"][3]["network"] = "mainnet"

    def into_current(r):
        r["cells"][-1]["end"] += W

    def wrong_count(r):
        r["eligible_count"] = 24

    def wrong_status(r):
        r["status"] = "cold_start"

    def certified_fixture(r):
        r["certification"] = "certified"

    def bad_state(r):
        r["cells"][0]["state"] = "assumed_zero"

    def zero_with_value(r):
        zero = next(c for c in r["cells"] if c["state"] == "zero")
        zero["swaps"] = 1

    def current_other_token(r):
        r["current_cell"]["token"] = "jetton:B"

    def unknown_policy(r):
        r["policy_id"] = "sha256:" + "0" * 64

    def policy_mismatch(r):
        r["min_prior_windows"] = 23

    for fn in (gap, short, cross_token, cross_network, into_current, wrong_count, wrong_status,
               certified_fixture, bad_state, zero_with_value, current_other_token,
               unknown_policy, policy_mismatch):
        with pytest.raises(psycopg.Error):
            insert_record(db, mutated(good, fn))
        db.rollback()
    insert_record(db, good)                               # the unmodified record is accepted
    db.commit()
    assert db.execute("SELECT count(*) FROM baseline_inputs").fetchone()[0] == 1


def test_full_seven_day_span_is_exact_and_bounded_by_runs(db):
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "grid")
    scored = 2100 * W                                     # lookback is not clipped here
    admitted = scored - 100 * W
    for k in (99, 98, 97):
        assert qualify(db, lease, world, scored - k * W) is True
    assert qualify(db, lease, world, admitted) is True
    promote_rows(db, lease, admitted, [row(admitted, "jetton:A", 3)])
    got = build(db, "jetton:A", scored)
    assert len(got.cells) == 2016
    assert got.cells[0].window_start == scored - 7 * 86400
    assert got.cells[-1].window_start == scored - W
    record = got.to_record()
    assert [(r["start"], r["end"], r["state"]) for r in record["cells"]] == [
        (scored - 7 * 86400, admitted, "pre_universe"), (admitted, admitted + W, "observed"),
        (scored - 99 * W, scored - 96 * W, "zero"), (scored - 96 * W, scored, "missing")]
    assert (got.status, got.eligible_count, got.current.state) == ("missing", 4, "missing")
    assert pin_baseline_input(db, lease, got) is True


# --------------------------------------------------------------------- universe / cold start
def test_universe_admission_is_data_derived_versioned_and_immutable(db):
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "grid")
    assert db.execute("SELECT count(*) FROM token_universe").fetchone()[0] == 0
    promote_rows(db, lease, 1200, [row(1200, "jetton:N", 2), row(1200, "TON", 1)])
    promote_rows(db, lease, 300, [row(300, "jetton:N", 1)])         # earlier, later promoted
    promote_rows(db, lease, 1500, [row(1500, "jetton:M", 0, 0, 0)])  # zero row admits nothing
    got = db.execute("SELECT token, admitted_window_start, universe_version FROM token_universe "
                     "ORDER BY universe_version").fetchall()
    assert got == [("TON", 1200, 1), ("jetton:N", 1200, 2)]
    assert db.execute("SELECT version FROM token_universe_version").fetchone()[0] == 2
    db.commit()
    with pytest.raises(psycopg.Error):
        db.execute("UPDATE token_universe SET admitted_window_start=300")
    db.rollback()
    # windows before admission are not a zero history even when qualified ...
    for start in range(0, 1500, W):
        qualify(db, lease, world, start)
    early = build(db, "jetton:N", 1200)
    assert states(early) == ["pre_universe", "observed", "pre_universe", "pre_universe"]
    assert (early.status, early.eligible_count) == ("cold_start", 1)
    # ... and an unobserved token has no grid at all
    unseen = build(db, "jetton:Q", 1200)
    assert set(states(unseen)) == {"pre_universe"} and unseen.status == "missing"


def test_cold_start_boundary_23_then_24_after_a_coverage_qualification(db):
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "grid")
    for i in list(range(0, 23)) + [30]:           # recorded before any token is admitted
        qualify(db, lease, world, i * W)
    promote_rows(db, lease, 0, [row(0, "jetton:C", 2)])
    promote_rows(db, lease, 30 * W, [row(30 * W, "jetton:C", 11)])
    before = build(db, "jetton:C", 30 * W)
    assert (before.status, before.eligible_count) == ("cold_start", 23)
    epoch = epoch_now(db)
    assert qualify(db, lease, world, 23 * W) is True
    after = build(db, "jetton:C", 30 * W)
    assert (after.status, after.eligible_count) == ("ready", 24)
    assert after.cells[23].state == "zero"
    # the coverage change advanced the dependency epoch and dirtied the dependents of every
    # universe token admitted at or before that window (only jetton:C was ever promoted)
    assert epoch_now(db) == epoch + 1
    dirty = db.execute("SELECT token,window_start,role,status,covers_start,covers_end "
                       "FROM dirty_windows ORDER BY token").fetchall()
    assert dirty == [("jetton:C", 23 * W, "baseline_dependents", "pending", 24 * W,
                      23 * W + 7 * 86400 + W)]
    # repeating the same qualification is a no-op; a conflicting one is refused
    assert qualify(db, lease, world, 23 * W) is False
    assert epoch_now(db) == epoch + 1
    with pytest.raises(ValueError):
        qualify(db, lease, world, 23 * W, delta=120)
    db.rollback()


def test_qualification_must_match_the_active_source_epoch(db):
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "grid")
    other = json.loads(world.epoch.to_json())
    other["source_fingerprint"] = "synthetic:other"
    db.commit()
    with pytest.raises(ValueError):
        record_qualification(
            db, lease, network=NET, window_start=0, mode="bounded_observed",
            mapping_version="m", delta_seconds=0, trace_ranges=[[0, 300]],
            source_epoch=other, synthetic=True)
    db.rollback()
    assert db.execute("SELECT count(*) FROM action_window_qualification").fetchone()[0] == 0


def test_qualification_makes_in_flight_repair_stale(db):
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "grid")
    db.commit()
    frozen = freeze_work(db, lease, [WindowKey(NET, 0, W)])
    qualify(db, lease, world, 0)
    db.commit()
    assert promote(db, lease, frozen, [row(0, "TON", 1)]) is False


# --------------------------------------------------------------- baseline_dependents closure
def close(db, *args):
    db.commit()
    return close_baseline_dependents(db, *args)


def dependents_state(db, token, start):
    db.commit()
    return db.execute("SELECT status, dirty_generation, dependency_epoch FROM dirty_windows "
                      "WHERE token=%s AND window_start=%s AND role='baseline_dependents'",
                      (token, start)).fetchone()


def test_dependents_close_only_after_target_repair_and_every_stale_score_is_repinned(db):
    world, evidence, auth = world_with_correction(db)     # X=A at window 0; candidate moves it
    lease = acquire_transition(db, "repair")
    tok = "jetton:A"
    assert qualify(db, lease, world, 900) is True        # scored window is covered ...
    promote_rows(db, lease, 900, [row(900, tok, 1)])      # ... and admits jetton:A at 900
    stale = build(db, tok, 900)                           # pinned before the correction
    assert (stale.status, stale.current.state) == ("cold_start", "observed")
    assert pin_baseline_input(db, lease, stale) is True
    decide_revision(db, lease, evidence, auth)            # dirties windows 0 and 600
    db.commit()
    status, generation, dep_epoch = dependents_state(db, tok, 0)
    assert (status, generation) == ("pending", 1) and dep_epoch > stale.dependency_epoch
    # 1. the target of window 0 is still pending
    assert close(db, lease, NET, tok, 0, generation) is False
    with pytest.raises(psycopg.Error):
        db.execute("UPDATE dirty_windows SET status='done' WHERE token=%s AND window_start=0 "
                   "AND role='baseline_dependents'", (tok,))
    db.rollback()
    # 2. target repaired, but scored window 900 still has only the stale pre-change pin
    promote_rows(db, lease, 0, [])
    assert close(db, lease, NET, tok, 0, generation) is False
    with pytest.raises(psycopg.Error):
        db.execute("UPDATE dirty_windows SET status='done' WHERE token=%s AND window_start=0 "
                   "AND role='baseline_dependents'", (tok,))
    db.rollback()
    # 3. a re-pin whose baseline is still provisional (window 600 pending) does not count
    repin = build(db, tok, 900)
    assert repin.status == "pending" and repin.cells[2].state == "provisional"
    assert pin_baseline_input(db, lease, repin) is True
    assert close(db, lease, NET, tok, 0, generation) is False
    # 4. after window 600 is repaired the re-pin is final; wrong generation still refused
    promote_rows(db, lease, 600, [row(600, tok, 1), row(600, "TON", 1)])
    final = build(db, tok, 900)
    assert final.status != "pending"
    assert pin_baseline_input(db, lease, final) is True
    assert close(db, lease, NET, tok, 0, generation + 1) is False
    assert close(db, lease, NET, tok, 0, generation) is True
    assert dependents_state(db, tok, 0)[0] == "done"
    # closing is one-shot: nothing pending is left for that generation
    assert close(db, lease, NET, tok, 0, generation) is False


# ----------------------------------------------------------------------- real Spark repair
@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    session = (SparkSession.builder.master("local[2]").appName("baseline-grid-test")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_spark_repair_feeds_the_grid_and_admits_the_universe(db, spark):
    from ton_sentinel.db.work import repair_windows
    world = World(db)
    world.seed({ev(1): val(end=11), ev(2): val(end=905), ev(3): val(end=907, o_asset="jetton:B")})
    lease = acquire_transition(db, "spark")
    for start in (0, 300, 600, 900, 1200):
        qualify(db, lease, world, start)
    db.commit()
    repair_windows(db, lease, spark, [WindowKey(NET, 0, W), WindowKey(NET, 900, 1200)])
    db.commit()
    universe = db.execute("SELECT token, admitted_window_start FROM token_universe "
                          "ORDER BY token COLLATE \"C\"").fetchall()
    assert universe == [("TON", 0), ("jetton:A", 0), ("jetton:B", 900)]
    got = build(db, "jetton:A", 1200)
    # independent expectation from the seeded values: jetton:A swaps 1 in window 0 and 1 in
    # window 900; windows 300/600 and the scored window 1200 are qualified and never seen
    assert [(c.window_start, c.state, c.swaps) for c in got.cells] == [
        (0, "observed", 1), (300, "zero", 0), (600, "zero", 0), (900, "observed", 1)]
    assert (got.current.state, got.current.swaps) == ("zero", 0)
    assert (got.status, got.eligible_count) == ("cold_start", 4)
    ton = build(db, "TON", 1200)
    assert [(c.state, c.swaps) for c in ton.cells] == [
        ("observed", 1), ("zero", 0), ("zero", 0), ("observed", 2)]
