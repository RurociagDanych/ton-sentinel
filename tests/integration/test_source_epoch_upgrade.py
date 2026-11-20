"""Source upgrades start an audited epoch, never cross-provider revision comparisons (real PostgreSQL)."""

import json

import psycopg
import pytest

from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.revisions import decide_revision, open_source_epoch
from ton_sentinel.db.work import freeze_work, promote
from tests.integration.test_repair_fencing import (  # noqa: F401  (fixture + helpers)
    A, B, W0, X, World, db, dirty, eid, feature, mono, promotions, revise, served,
)
from tests.integration.test_revisions import epoch

V2 = epoch("synthetic:source-v2")
AUDIT = {"authority_id": "owner-upgrade-audit", "reason": "provider build upgrade",
         "audit_start": 0, "audit_end": 3600}


def snapshot(db):
    db.commit()
    rows = (
        db.execute("SELECT event_id,accepted_generation,semantic_digest,eligibility_suppressed "
                   "FROM canonical_current ORDER BY event_id").fetchall(),
        db.execute("SELECT count(*) FROM canonical_revisions").fetchone()[0],
        db.execute("SELECT subject,reason,status FROM revision_quarantine "
                   "ORDER BY subject").fetchall(),
        db.execute("SELECT epoch FROM dependency_epoch").fetchone()[0],
        db.execute("SELECT epoch FROM active_source_epoch").fetchone()[0],
        dirty(db),
    )
    db.commit()
    return rows


def transitions(db):
    db.commit()
    rows = db.execute("SELECT previous_epoch,new_epoch,authority_id,reason,audit_start,audit_end,"
                      "dependency_epoch FROM source_epoch_transitions ORDER BY transition_id"
                      ).fetchall()
    db.commit()
    return rows


def test_new_provider_walk_without_audited_transition_is_refused_without_writes(db):
    world = World(db)
    world.seed({X: A})
    world.epoch = V2
    evidence = world.candidate({X: B}, {1: {2}})
    auth = world.authorization(mono(revise(X, B, src=2, prev=1)))
    lease = acquire_transition(db, "decider")
    before = snapshot(db)
    with pytest.raises(ValueError, match="explicit audited source epoch transition required"):
        decide_revision(db, lease, evidence, auth)
    db.rollback()
    assert snapshot(db) == before


def test_open_source_epoch_records_immutable_audit_and_bumps_the_epoch_once(db):
    world = World(db)
    world.seed({X: A})
    lease = acquire_transition(db, "upgrade")
    v1 = json.loads(world.epoch.to_json())
    db.commit()
    assert open_source_epoch(db, lease, V2, AUDIT) is True
    after = snapshot(db)
    assert after[3] == 1 and after[4] == json.loads(V2.to_json())
    assert transitions(db) == [(v1, json.loads(V2.to_json()), "owner-upgrade-audit",
                                "provider build upgrade", 0, 3600, 1)]
    db.commit()
    assert open_source_epoch(db, lease, V2, AUDIT) is False
    assert snapshot(db) == after and len(transitions(db)) == 1
    for sql in ("UPDATE source_epoch_transitions SET authority_id='x'",
                "DELETE FROM source_epoch_transitions", "TRUNCATE source_epoch_transitions"):
        with pytest.raises(psycopg.Error):
            db.execute(sql)
        db.rollback()


@pytest.mark.parametrize("audit", [
    None, {}, dict(AUDIT, authority_id=" "), dict(AUDIT, reason=""),
    dict(AUDIT, audit_end=0), dict(AUDIT, audit_start=1), dict(AUDIT, audit_end=True),
    dict(AUDIT, extra="x"),
])
def test_open_source_epoch_requires_an_approved_bounded_audit(db, audit):
    world = World(db)
    world.seed({X: A})
    lease = acquire_transition(db, "upgrade")
    before = snapshot(db)
    with pytest.raises(ValueError):
        open_source_epoch(db, lease, V2, audit)
    db.rollback()
    assert snapshot(db) == before and transitions(db) == []


def test_open_source_epoch_is_fenced_and_never_switches_network(db):
    world = World(db)
    world.seed({X: A})
    stale = acquire_transition(db, "old")
    acquire_transition(db, "new")
    before = snapshot(db)
    with pytest.raises(LeaseRejected):
        open_source_epoch(db, stale, V2, AUDIT)
    db.rollback()
    lease = acquire_transition(db, "upgrade")
    from ton_sentinel.contracts import SourceEpoch
    mainnet = SourceEpoch("mainnet", "synthetic:source-v2", "synthetic:filter",
                          "synthetic:build", "synthetic:policy")
    with pytest.raises(ValueError, match="network"):
        open_source_epoch(db, lease, mainnet, AUDIT)
    db.rollback()
    assert snapshot(db) == before and transitions(db) == []


def test_frozen_work_from_before_the_upgrade_cannot_promote(db):
    world = World(db)
    world.seed({X: A})
    lease = acquire_transition(db, "repair")
    db.commit()
    frozen = freeze_work(db, lease, [W0])
    db.commit()
    assert open_source_epoch(db, lease, V2, AUDIT) is True
    db.commit()
    assert promote(db, lease, frozen, [feature(W0, "TON", 1)]) is False
    assert served(db) == [] and promotions(db) == 0


def test_after_upgrade_old_epoch_values_are_not_revised_by_new_provider_ordering(db):
    world = World(db)
    world.seed({X: A})
    lease = acquire_transition(db, "upgrade")
    db.commit()
    assert open_source_epoch(db, lease, V2, AUDIT) is True
    world.epoch = V2
    evidence = world.candidate({X: B}, {1: {2}})
    # a monotonic grant from the new provider must not be compared to the old provider's
    # revision numbers: the old-epoch value stays, the change is quarantined, eligibility off
    auth = world.authorization(mono(revise(X, B, src=2, prev=1)))
    changes = decide_revision(db, lease, evidence, auth)
    db.commit()
    assert [(c.kind, c.reason) for c in changes] == [("quarantine", "unauthorized_change")]
    current = db.execute("SELECT accepted_generation,action->>'action_end',eligibility_suppressed "
                         "FROM canonical_current WHERE event_id=%s", (eid(X),)).fetchone()
    assert current == (1, "11", True)
    assert db.execute("SELECT count(*) FROM canonical_revisions").fetchone()[0] == 0
