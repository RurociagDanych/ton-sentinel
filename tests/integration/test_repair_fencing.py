"""Frozen repair inputs, atomic fenced promotion, stale rejection.

Fixture setup reuses the revision World (corroborated walks, grants). The assertions read the
database directly and never call product helpers to decide expected values.
"""
import json
import os
import threading
import time
import uuid

import psycopg
import pytest

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.db.serving import WindowKey, replace_features
from ton_sentinel.db.work import FrozenWork, freeze_work, promote
from tests.integration.test_revisions import (  # noqa: F401  (fixture + helpers)
    A, B, X, World, db, eid, mono, revise,
)

W0 = WindowKey("testnet", 0, 300)       # X = A lands here
W1 = WindowKey("testnet", 600, 900)     # X = B lands here
BASE = WindowKey("testnet", 300, 600)    # trailing baseline of W1


def freeze(db, *args, **kw):
    db.commit()
    return freeze_work(db, *args, **kw)


def promote_idle(db, *args, **kw):
    db.commit()
    return promote(db, *args, **kw)


def feature(window, token, swaps, generation="g"):
    return {"network": window.network, "window_start": window.window_start,
            "window_end": window.window_end, "token": token, "generation": generation,
            "swaps": swaps, "input_raw": 5, "output_raw": None, "known_traders": 0,
            "unknown_count": swaps, "coverage_status": "bounded_observed",
            "status": "provisional", "provisional": True}


def served(db):
    db.commit()
    return db.execute("SELECT window_start,token,generation,swaps,write_fence FROM served_features "
                      "ORDER BY window_start,token COLLATE \"C\"").fetchall()


def promotions(db):
    db.commit()
    return db.execute("SELECT count(*) FROM repair_promotions").fetchone()[0]


def dirty(db):
    db.commit()
    return db.execute("SELECT token,window_start,role,status,dirty_generation FROM dirty_windows "
                      "ORDER BY token COLLATE \"C\",window_start,role").fetchall()


def connect(db):
    conn = psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"])
    conn.execute(f'SET search_path TO "{db.schema_name}"')
    conn.commit()
    return conn


def world_with_correction(db):
    """Seeded X=A plus an authorized candidate revising X to B (not yet applied)."""
    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: B}, {1: {2}})
    auth = world.authorization(mono(revise(X, B, src=2, prev=1)))
    return world, evidence, auth


def seeded_baseline(db, lease):
    replace_features(db, lease, [feature(BASE, "TON", 9, "base")], windows=[BASE])
    db.commit()


# ------------------------------------------------------------------------------- freezing
def test_freeze_pins_current_inputs_epoch_fence_and_dirty_state(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    assert isinstance(frozen, FrozenWork)
    assert (frozen.owner, frozen.fence, frozen.epoch) == ("repair", lease.fence, 0)
    assert [(i[0], i[1]) for i in frozen.inputs] == [(eid(X), 1)]
    assert json.loads(frozen.inputs[0][3])["action_end"] == 11
    assert frozen.targets == (W0,)


def test_frozen_inputs_cannot_change_under_a_repair(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    digest, inputs = frozen.input_digest, frozen.inputs
    # a correction moves X out of window 0 while the repair "computes"
    decide_revision(db, lease, evidence, auth)
    db.commit()
    assert db.execute("SELECT accepted_generation FROM canonical_current").fetchone()[0] == 2
    assert frozen.inputs is inputs and frozen.input_digest == digest
    assert [(i[0], i[1]) for i in frozen.inputs] == [(eid(X), 1)]
    fresh = freeze(db, lease, [W0])
    assert fresh.inputs == () and fresh.epoch == 1 and fresh.input_digest != digest
    assert freeze(db, lease, [W1]).inputs[0][1] == 2      # new value lives in window 600
    with pytest.raises(Exception):
        frozen.inputs = ()


def test_freeze_with_stale_lease_is_rejected_without_writes(db):
    from ton_sentinel.db.fencing import LeaseRejected
    world, _, _ = world_with_correction(db)
    old = acquire_transition(db, "old")
    acquire_transition(db, "new")
    with pytest.raises(LeaseRejected):
        freeze(db, old, [W0])


# ------------------------------------------------------------------------------ promotion
def test_promote_replaces_windows_atomically_and_closes_the_pinned_dirty_rows(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    decide_revision(db, lease, evidence, auth)           # dirties windows 0 and 600
    db.commit()
    frozen = freeze(db, lease, [W0, W1])
    assert {r[3] for r in dirty(db)} == {"pending"}
    ok = promote_idle(db, lease, frozen, [feature(W1, "TON", 1), feature(W1, "jetton:B", 1)])
    assert ok is True
    assert served(db) == [(600, "TON", "g", 1, lease.fence), (600, "jetton:B", "g", 1, lease.fence)]
    states = {(r[1], r[2]): r[3] for r in dirty(db)}
    assert states[(0, "target")] == states[(600, "target")] == "done"
    assert states[(0, "baseline_dependents")] == "pending"
    assert promotions(db) == 1


def test_promote_failure_after_every_write_leaves_no_partial_promotion(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    decide_revision(db, lease, evidence, auth)
    db.commit()
    seeded_baseline(db, lease)
    frozen = freeze(db, lease, [W0, W1])
    before = (served(db), dirty(db), promotions(db))

    def boom(connection):
        assert connection.execute("SELECT count(*) FROM repair_promotions").fetchone()[0] == 1
        raise RuntimeError("postgres failed at commit")

    with pytest.raises(RuntimeError):
        promote_idle(db, lease, frozen, [feature(W1, "TON", 1)], commit_check=boom)
    assert (served(db), dirty(db), promotions(db)) == before
    assert promote_idle(db, lease, frozen, [feature(W1, "TON", 1)]) is True       # retry works


def test_duplicate_retry_of_a_promoted_work_writes_nothing(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    decide_revision(db, lease, evidence, auth)
    db.commit()
    frozen = freeze(db, lease, [W1])
    assert promote_idle(db, lease, frozen, [feature(W1, "TON", 1)]) is True
    after = (served(db), dirty(db), promotions(db))
    assert promote_idle(db, lease, frozen, [feature(W1, "TON", 99)]) is False
    assert (served(db), dirty(db), promotions(db)) == after


def test_empty_or_zero_result_promotion_clears_stale_rows_for_the_window(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    replace_features(db, lease, [feature(W0, "TON", 3)], windows=[W0])
    db.commit()
    frozen = freeze(db, lease, [W0])
    assert promote_idle(db, lease, frozen, []) is True
    assert served(db) == []


# ------------------------------------------------------------------------ stale rejection
def test_stale_baseline_rejected(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    seeded_baseline(db, lease)
    # control: unchanged baseline promotes
    control = freeze(db, lease, [W1])
    assert promote_idle(db, lease, control, [feature(W1, "TON", 1)]) is True
    replace_features(db, lease, [], windows=[W1])
    db.commit()
    frozen = freeze(db, lease, [W1])
    assert frozen.baseline and frozen.epoch == 0
    # a baseline window is replaced under the same lease: epoch and inputs are unchanged
    replace_features(db, lease, [feature(BASE, "TON", 10, "other")], windows=[BASE])
    db.commit()
    before = (served(db), dirty(db), promotions(db))
    assert promote_idle(db, lease, frozen, [feature(W1, "TON", 1)]) is False
    assert (served(db), dirty(db), promotions(db)) == before
    assert not any(r[0] == 600 for r in served(db))


def test_correction_during_paused_computation_makes_promotion_stale(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    seeded_baseline(db, lease)
    frozen = freeze(db, lease, [W0])
    decide_revision(db, lease, evidence, auth)          # same lease: only the epoch tells
    db.commit()
    before = served(db)
    assert promote_idle(db, lease, frozen, [feature(W0, "TON", 1)]) is False
    assert served(db) == before and promotions(db) == 0


def test_coverage_or_universe_change_during_paused_computation_makes_promotion_stale(db):
    world, _, _ = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    db.execute(
        "INSERT INTO action_window_coverage(coverage_id,network,epoch,window_start,window_end,"
        "status,eligible_zero,reason,lineage) VALUES (%s,'testnet',%s,0,300,'missing',false,"
        "'no_action_time_scan_evidence','[]')",
        (str(uuid.uuid4()), json.dumps(json.loads(world.epoch.to_json()))))
    db.commit()
    assert promote_idle(db, lease, frozen, [feature(W0, "TON", 1)]) is False
    assert served(db) == []
    frozen2 = freeze(db, lease, [W0])
    db.execute("UPDATE active_source_epoch SET epoch='{\"universe\":\"changed\"}'::jsonb")
    db.commit()
    assert promote_idle(db, lease, frozen2, [feature(W0, "TON", 1)]) is False
    assert served(db) == [] and promotions(db) == 0


# --------------------------------------------------------------------------------- fencing
def test_repair_fencing_superseded_or_expired_lease_cannot_promote(db):
    world, _, _ = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    acquire_transition(db, "usurper")
    assert promote_idle(db, lease, frozen, [feature(W0, "TON", 1)]) is False
    short = acquire_transition(db, "short", ttl_seconds=1)
    frozen = freeze(db, short, [W0])
    time.sleep(1.2)
    assert promote_idle(db, short, frozen, [feature(W0, "TON", 1)]) is False
    assert served(db) == [] and promotions(db) == 0


def test_repair_fencing_racing_writer_waits_for_promotion_then_redirties(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    inside, release, order = threading.Event(), threading.Event(), []

    def hold(connection):
        inside.set()
        assert release.wait(20)

    def writer():
        with connect(db) as conn:
            inside.wait(20)
            decide_revision(conn, lease, evidence, auth)        # same lease token
            order.append("writer-committed")

    thread = threading.Thread(target=writer)
    thread.start()
    result = []
    promoter = threading.Thread(target=lambda: result.append(
        promote_idle(db, lease, frozen, [feature(W0, "TON", 1)], commit_check=hold)))
    promoter.start()
    assert inside.wait(20)
    time.sleep(1.0)
    assert order == []                                           # writer is fenced out
    order.append("promotion-releasing")
    release.set()
    promoter.join(20)
    thread.join(20)
    assert result == [True]
    assert order == ["promotion-releasing", "writer-committed"]
    assert db.execute("SELECT epoch FROM dependency_epoch").fetchone()[0] == 1
    db.commit()
    states = {(r[0], r[1], r[2]): r[3] for r in dirty(db)}
    assert states[("TON", 0, "target")] == "pending"            # the late correction re-dirties it


def test_two_racing_promotions_of_the_same_work_promote_once(db):
    world, _, _ = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    barrier, outcomes = threading.Barrier(2), []

    def racer(swaps):
        with connect(db) as conn:
            barrier.wait()
            outcomes.append(promote(conn, lease, frozen, [feature(W0, "TON", swaps)]))

    threads = [threading.Thread(target=racer, args=(n,)) for n in (1, 2)]
    [t.start() for t in threads]
    [t.join(30) for t in threads]
    assert sorted(outcomes) == [False, True]
    assert promotions(db) == 1 and len(served(db)) == 1


def test_promotion_table_is_immutable(db):
    world, _, _ = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    assert promote_idle(db, lease, frozen, []) is True
    for sql in ("UPDATE repair_promotions SET epoch=99", "DELETE FROM repair_promotions",
                "TRUNCATE repair_promotions"):
        with pytest.raises(psycopg.Error):
            db.execute(sql)
        db.rollback()
