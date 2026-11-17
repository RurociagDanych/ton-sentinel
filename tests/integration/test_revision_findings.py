"""Explicit revision-order, disappearance and
changed-action-ID acceptance tests on real PostgreSQL.

Each scenario runs through the independent ``Oracle`` of test_revisions (via
``run_scenario``) and then asserts the persisted state directly. The old-Kafka-delivery
after payload/guard expiry is covered by the retention tests, not here.
"""
from tests.integration.test_revisions import (  # noqa: F401  (fixture + helpers)
    A, B, C, X, Y, b64, db, eid, mono, owner, retract, revise, add, run_scenario,
)


def current(db, ev):
    db.commit()
    row = db.execute(
        "SELECT disposition,eligibility_suppressed,semantic_digest,"
        "action->'input'->>'raw_amount',(action->>'action_end')::bigint,source_revision "
        "FROM canonical_current WHERE event_id=%s", (eid(ev),)).fetchone()
    db.commit()
    return row


def revisions(db):
    db.commit()
    rows = db.execute("SELECT event_id,change_kind,source_revision FROM canonical_revisions "
                      "ORDER BY accepted_generation").fetchall()
    db.commit()
    return rows


def quarantine(db):
    db.commit()
    rows = db.execute("SELECT subject,reason FROM revision_quarantine WHERE status='open' "
                      "ORDER BY subject").fetchall()
    db.commit()
    return rows


def keyset(db, trace):
    db.commit()
    row = db.execute("SELECT action_ids FROM trace_keysets WHERE trace_id=%s",
                     (b64(trace),)).fetchone()
    db.commit()
    return None if row is None else row[0]


# ------------------------------------------------------------ (1) A -> B -> stale A
def test_finding5_a_then_b_then_stale_a_is_refused_and_current_stays_b(db):
    _, _, (first, second, third) = run_scenario(db, {X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        # stale A: fully corroborated, a newer source revision number, still refused
        {"obs": {X: A}, "lookups": {1: {2}}, "auth": mono(revise(X, A, src=3))},
        # same stale A replayed even with an older revision number
        {"obs": {X: A}, "lookups": {1: {2}}, "auth": mono(revise(X, A, src=1))}])
    assert [(c.kind, c.reason) for c in first] == [("revision", "source_monotonic_revision")]
    assert [(c.kind, c.reason) for c in second] == [("quarantine", "superseded_digest")]
    assert third == ()          # identical quarantine already open: a no-op, nothing written
    disposition, suppressed, _, amount, end, src = current(db, X)
    assert (disposition, amount, end, src) == ("active", "100", 605, 2)   # B stays current
    assert suppressed is True
    assert [(k, s) for _, k, s in revisions(db)] == [("revision", 2)]       # no A revision
    assert [r for _, r in quarantine(db)] == ["superseded_digest"]


# ------------------------------------------- (2) disappearance, then reappearance
def test_finding5_disappearance_is_quarantined_not_retracted_then_reappearance_clears(db):
    _, _, (gone, back) = run_scenario(db, {X: A}, [
        {"obs": {}, "lookups": {1: set()}},
        {"obs": {X: A}, "lookups": {1: {2}}}])
    assert [(c.kind, c.reason) for c in gone] == [
        ("quarantine", "unauthorized_disappearance")]
    assert [(c.kind, c.reason) for c in back] == [("cleared", "resolved")]
    assert current(db, X)[:2] == ("active", False)
    assert revisions(db) == [] and quarantine(db) == []


def test_finding5_disappearance_without_retraction_keeps_value_suppressed(db):
    run_scenario(db, {X: A}, [
        {"obs": {}, "lookups": {1: set()}},
        {"obs": {}, "lookups": {1: set()}, "auth": mono(retract(X), trace_sets={1: set()})}])
    # a monotonic contract is never removal authority: the value stays active, suppressed
    assert current(db, X)[:2] == ("active", True)
    assert revisions(db) == []
    assert [r for _, r in quarantine(db)] == ["unauthorized_disappearance"]


def test_finding5_retracted_identity_reappears_only_under_authority(db):
    _, _, (retracted, refused, readmitted) = run_scenario(db, {X: A}, [
        {"obs": {}, "lookups": {1: set()}, "auth": owner(retract(X), trace_sets={1: set()})},
        {"obs": {X: A}, "lookups": {1: {2}}},                        # reappears, no authority
        {"obs": {X: A}, "lookups": {1: {2}},
         "auth": owner(revise(X, A), trace_sets={1: {2}})}])          # owner re-admission
    assert [c.kind for c in retracted] == ["retraction"]
    assert [c.kind for c in refused] == ["quarantine"]
    assert [c.kind for c in readmitted][0] == "revision"
    assert [k for _, k, _ in revisions(db)] == ["retraction", "revision"]
    assert current(db, X)[:2] == ("active", False)


def test_finding5_reappearance_without_authority_stays_retracted(db):
    run_scenario(db, {X: A}, [
        {"obs": {}, "lookups": {1: set()}, "auth": owner(retract(X), trace_sets={1: set()})},
        {"obs": {X: A}, "lookups": {1: {2}}}])
    assert current(db, X)[0] == "retracted"
    assert [k for _, k, _ in revisions(db)] == ["retraction"]
    assert len(quarantine(db)) == 1


# --------------------------------- (3) changed action IDs inside one trace
def test_finding5_changed_action_ids_unauthorized_leave_trace_untouched(db):
    # trace 1 was {2}; the source now lists {3}: one member leaves, another arrives
    _, _, (changes,) = run_scenario(db, {X: A}, [
        {"obs": {Y: B}, "lookups": {1: {3}}}])
    assert [(c.kind, c.reason) for c in changes] == [
        ("quarantine", "unauthorized_trace_replacement")]
    assert current(db, X)[:2] == ("active", True)          # old entry kept, ineligible
    assert current(db, Y) is None                          # new ID not admitted
    assert revisions(db) == []
    assert keyset(db, 1) == [b64(2)]


def test_finding5_changed_action_ids_partial_grant_replaces_nothing(db):
    # the new ID is granted but the old ID's retraction is not: whole trace or nothing
    _, _, (changes,) = run_scenario(db, {X: A}, [
        {"obs": {Y: B}, "lookups": {1: {3}},
         "auth": owner(add(Y, B), trace_sets={1: {3}})}])
    assert [c.kind for c in changes] == ["quarantine"]
    assert current(db, X)[:2] == ("active", True)
    assert current(db, Y) is None
    assert revisions(db) == []
    assert keyset(db, 1) == [b64(2)]


def test_finding5_changed_action_ids_with_one_stale_member_replaces_nothing(db):
    _, _, (changes,) = run_scenario(db, {X: A}, [
        {"obs": {Y: B}, "lookups": {1: {3}},
         "auth": owner(add(Y, B), retract(X, gen="stale"), trace_sets={1: {3}})}])
    assert [(c.kind, c.reason) for c in changes] == [("quarantine", "stale_authorization")]
    assert current(db, X)[:2] == ("active", True)
    assert current(db, Y) is None
    assert revisions(db) == []
    assert keyset(db, 1) == [b64(2)]


def test_finding5_changed_action_ids_fully_authorized_replace_whole_trace_at_once(db):
    _, _, (changes,) = run_scenario(db, {X: A}, [
        {"obs": {Y: B}, "lookups": {1: {3}},
         "auth": owner(add(Y, B), retract(X), trace_sets={1: {3}})}])
    assert sorted(c.kind for c in changes) == ["retraction", "revision"]
    assert current(db, X)[0] == "retracted"
    assert current(db, Y)[:2] == ("active", False)
    generations = [g for (g,) in db.execute(
        "SELECT accepted_generation FROM canonical_revisions ORDER BY 1").fetchall()]
    db.commit()
    assert len(generations) == 2 and generations[1] == generations[0] + 1
    assert quarantine(db) == []
