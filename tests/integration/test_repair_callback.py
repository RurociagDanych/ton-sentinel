"""The canonical microbatch callback hands over to the fenced
freeze_work -> compute_replacement -> promote path (real PostgreSQL + real Spark).

Expected values are read from the database or derived from the fixture action values;
they are never produced by the product functions under test.
"""
import json

import pytest

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.spark import streaming
from ton_sentinel.spark.streaming import process_canonical_microbatch
from tests.integration.test_canonical_stream import _audited_batch, _batch, spark  # noqa: F401
from tests.integration.test_repair_fencing import (  # noqa: F401  (fixture + helpers)
    X, connect, db, dirty, eid, promotions, served, world_with_correction,
)


def trigger_of(db, event_id):
    """The persisted (first-acceptance) outbox envelope of ``event_id`` as a broker record."""
    envelope, topic, incarnation = db.execute(
        "SELECT delivery_envelope,delivery_topic,delivery_topic_incarnation FROM outbox "
        "WHERE event_id=%s", (event_id,)).fetchone()
    db.commit()
    return (event_id.encode(), json.dumps(envelope, separators=(",", ":")).encode()), \
        topic, incarnation


def run(db, lease, spark, record, incarnation, **audit):
    db.commit()
    if audit:
        topic = audit.pop("topic")
        frame = _audited_batch(spark, [(topic, 0, 0, record[0], record[1])])
        return process_canonical_microbatch(
            db, lease, spark, frame, topic_incarnation=incarnation, replay_epoch=0,
            topic=topic, **audit)
    return process_canonical_microbatch(db, lease, spark, _batch(spark, [record]),
                                        topic_incarnation=incarnation, replay_epoch=0)


def promoted_windows(db):
    db.commit()
    rows = [r[0] for r in db.execute("SELECT windows FROM repair_promotions "
                                     "ORDER BY promoted_at").fetchall()]
    db.commit()
    return rows


def test_callback_serves_corrected_current_not_first_acceptance_through_promotion(db, spark):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "callback")
    record, _, incarnation = trigger_of(db, eid(X))
    # Control: before any correction X = A (action_end 11) is served in window [0, 300).
    assert run(db, lease, spark, record, incarnation) == 2
    assert [(r[0], r[1], r[3]) for r in served(db)] == [(0, "TON", 1), (0, "jetton:A", 1)]
    assert promoted_windows(db) == [[["testnet", 0, 300]]]
    # An authorized correction moves X to action_end 605; the replayed trigger still
    # carries the first-acceptance envelope (window 0) but must recompute from current.
    decide_revision(db, lease, evidence, auth)
    db.commit()
    assert run(db, lease, spark, record, incarnation) == 0
    assert served(db) == []                                  # stale X=A withdrawn from window 0
    assert promoted_windows(db) == [[["testnet", 0, 300]], [["testnet", 0, 300]]]
    states = {(r[0], r[1], r[2]): r[3] for r in dirty(db)}
    assert states[("TON", 0, "target")] == states[("jetton:A", 0, "target")] == "done"
    assert states[("TON", 600, "target")] == "pending"       # other side awaits its own repair
    assert states[("TON", 0, "baseline_dependents")] == "pending"


def test_callback_requeues_fresh_inputs_when_a_correction_lands_during_computation(
        db, spark, monkeypatch):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "callback")
    record, _, incarnation = trigger_of(db, eid(X))
    original, calls = streaming.compute_replacement, []

    def paused(session, frozen):
        calls.append(len(frozen.inputs))
        if len(calls) == 1:                    # a prior correction commits while we compute
            with connect(db) as other:
                decide_revision(other, lease, evidence, auth)
        return original(session, frozen)

    monkeypatch.setattr(streaming, "compute_replacement", paused)
    assert run(db, lease, spark, record, incarnation) == 0
    assert calls == [1, 0]                     # stale frozen inputs, then fresh inputs
    assert served(db) == [] and promotions(db) == 1


def test_callback_with_lease_lost_during_computation_raises_and_writes_nothing(
        db, spark, monkeypatch):
    world, _, _ = world_with_correction(db)
    lease = acquire_transition(db, "callback")
    record, topic, incarnation = trigger_of(db, eid(X))
    original = streaming.compute_replacement

    def usurped(session, frozen):
        with connect(db) as other:
            acquire_transition(other, "usurper")
        return original(session, frozen)

    monkeypatch.setattr(streaming, "compute_replacement", usurped)
    with pytest.raises(RuntimeError, match="lease expired or fencing token"):
        run(db, lease, spark, record, incarnation, topic=topic, batch_id=0,
            environment_id="env", query_id="q", pipeline_epoch="pe")
    db.rollback()
    assert served(db) == [] and promotions(db) == 0
    assert db.execute("SELECT count(*) FROM stream_batches").fetchone()[0] == 0


def test_callback_audit_and_serving_commit_in_the_promotion_transaction(db, spark, monkeypatch):
    world, _, _ = world_with_correction(db)
    lease = acquire_transition(db, "callback")
    record, topic, incarnation = trigger_of(db, eid(X))
    audit = dict(topic=topic, batch_id=0, environment_id="env", query_id="q", pipeline_epoch="pe")
    original = streaming.replace_features

    def fail_after_write(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("injected failure after serving replacement")

    monkeypatch.setattr(streaming, "replace_features", fail_after_write)
    with pytest.raises(RuntimeError, match="injected failure"):
        run(db, lease, spark, record, incarnation, **dict(audit))
    db.rollback()
    assert served(db) == [] and promotions(db) == 0
    assert db.execute("SELECT count(*) FROM stream_batches").fetchone()[0] == 0
    db.commit()
    monkeypatch.setattr(streaming, "replace_features", original)
    assert run(db, lease, spark, record, incarnation, **dict(audit)) == 2
    assert promotions(db) == 1
    assert db.execute("SELECT count(*) FROM stream_batches").fetchone()[0] == 1
