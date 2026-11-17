"""Admit whole windows only when retained context and resource
bounds suffice; refuse otherwise with a durable missing/unrepairable record, never partially.

Clock and admission contract: spec section 9.  Expected values are written by hand or read
directly from the tables; product helpers only build fixtures.
Fixture world: event X = A ends at action time 11 (window 0); the authorized
correction moves it to window 600.
"""
import psycopg
import pytest

from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.db.serving import WindowKey, replace_features
from ton_sentinel.db.work import (
    admit_windows, admit_work, freeze_work, record_refusal, repair_windows,
)
from tests.integration.test_repair_fencing import (  # noqa: F401  (fixture + helpers)
    db, feature, world_with_correction,
)
from tests.integration.test_revisions import A, X, Z, World, eid

DAY = 86400
NOW = 7 * DAY                         # C_event = 0 with seven-day retention
POLICY = {"event_retention_seconds": 7 * DAY, "repair_margin_seconds": 0,
          "baseline_lookback_seconds": 600, "derived_retention_seconds": 30 * DAY,
          "max_windows": 4, "max_actions": 10, "max_input_bytes": 100_000,
          "max_baseline_rows": 10}
W0 = WindowKey("testnet", 0, 300)
W3 = WindowKey("testnet", 300, 600)
W6 = WindowKey("testnet", 600, 900)

_STATE_TABLES = (
    "served_features", "dirty_windows", "repair_promotions", "dependency_epoch",
    "canonical_current", "canonical_actions", "action_window_coverage", "baseline_inputs",
    "detector_current", "detector_results", "window_sweep_closures", "token_universe",
    "retained_context_horizons", "active_source_epoch")


def win(start):
    return WindowKey("testnet", start, start + 300)


def work_state(db):
    """Exact content digest of every table work or retention may change (refusals excluded)."""
    db.commit()
    state = {}
    for table in _STATE_TABLES:
        state[table] = db.execute(
            f"SELECT count(*), md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) "
            f"FROM {table} t").fetchone()
    db.commit()
    return state


def refusals(db):
    db.commit()
    rows = db.execute(
        "SELECT network, window_start, window_end, status, reasons, work_windows, decided_at,"
        " event_cutoff, repair_cutoff, dependency_epoch, lease_owner, lease_fence"
        " FROM window_admission_refusals ORDER BY window_start").fetchall()
    db.commit()
    return rows


def set_horizon(db, data_class, retained_from):
    """What a retention run leaves behind: the oldest retained window of a data class."""
    db.execute(
        "INSERT INTO retained_context_horizons(network,data_class,retained_from) "
        "VALUES ('testnet',%s,%s) ON CONFLICT (network,data_class) DO UPDATE "
        "SET retained_from=EXCLUDED.retained_from", (data_class, retained_from))
    db.commit()


def x_bytes(db):
    db.commit()
    value = db.execute("SELECT octet_length(action::text) FROM canonical_actions "
                       "WHERE event_id=%s", (eid(X),)).fetchone()[0]
    db.commit()
    return value


# ----------------------------------------------------------------------- whole admission
def test_whole_window_margin(db):
    """Whole closed windows at/after C_repair are admitted with pins; nothing is written."""
    world_with_correction(db)
    before = work_state(db)
    policy = dict(POLICY, repair_margin_seconds=600)       # C_repair = ceil(0 + 600) = 600
    inside = admit_work(db, (W3,), NOW, policy)
    assert (inside.admitted, inside.status) == (False, "unrepairable")
    assert inside.reasons == ((W3, "inside_repair_margin"),)
    at = admit_work(db, (W6,), NOW, policy)
    assert (at.admitted, at.status, at.reasons) == (True, "admitted", ())
    assert (at.event_cutoff, at.repair_cutoff, at.derived_cutoff) == (0, 600, NOW - 30 * DAY)
    # detector v1 lookback (7 days) outranks the 600 s repair lookback (span as requested;
    # the context check clips it at time 0, where no earlier window exists)
    assert at.lookback_seconds == 7 * DAY
    assert at.baseline_spans == ((W6, 600 - 7 * DAY, 600),)
    admission = admit_work(db, (W0,), NOW, POLICY)
    assert admission.admitted is True
    assert dict(admission.usage) == {"windows": 1, "actions": 1, "input_bytes": x_bytes(db),
                                     "baseline_rows": 0}
    open_ = admit_work(db, (win(NOW - 300),), NOW - 1, POLICY)
    assert (open_.admitted, open_.status) == (False, "deferred")
    assert work_state(db) == before and refusals(db) == []


# -------------------------------------------------------------- retained context refusals
def test_refuses_when_target_payload_is_not_retained_and_records_it_durably(db):
    world_with_correction(db)
    lease = acquire_transition(db, "repair")
    db.commit()
    set_horizon(db, "canonical_payload", 300)              # window 0 payload deleted
    before = work_state(db)
    admission = admit_windows(db, lease, (W0,), NOW, POLICY)
    assert (admission.admitted, admission.status) == (False, "unrepairable")
    assert admission.reasons == ((W0, "payload_not_retained"),)
    assert work_state(db) == before                         # only the refusal row is new
    assert refusals(db) == [("testnet", 0, 300, "unrepairable", ["payload_not_retained"],
                             [["testnet", 0, 300]], NOW, 0, 0, 0, "repair", lease.fence)]
    with pytest.raises(ValueError, match="not admitted"):
        freeze_work(db, lease, admission)
    # a window whose payload is still retained passes the same horizon
    assert admit_work(db, (W3,), NOW, POLICY).admitted is True


def test_refuses_when_leading_baseline_context_is_not_retained(db):
    world_with_correction(db)
    lease = acquire_transition(db, "repair")
    db.commit()
    # detector baseline of window 600 needs every prior window from 0; features kept from 300
    set_horizon(db, "derived_features", 300)
    before = work_state(db)
    admission = admit_windows(db, lease, (W6,), NOW, POLICY)
    assert (admission.admitted, admission.status) == (False, "missing")
    assert admission.reasons == ((W6, "baseline_context_not_retained"),)
    assert work_state(db) == before
    assert [(r[1], r[3], r[4]) for r in refusals(db)] == [
        (600, "missing", ["baseline_context_not_retained"])]


def test_refuses_leading_context_beyond_derived_retention(db):
    world_with_correction(db)
    lease = acquire_transition(db, "repair")
    db.commit()
    # T = 30 days + 600, derived retention 30 days: context must start at or after 600
    at = 30 * DAY + 600
    policy = dict(POLICY, event_retention_seconds=31 * DAY)
    old = admit_windows(db, lease, (W6,), at, policy)       # span starts at 0
    assert (old.status, old.reasons) == ("missing", ((W6, "baseline_context_expired"),))
    young = admit_work(db, (win(7 * DAY + 600),), at, policy)
    assert young.admitted is True                           # span starts at 600
    assert [(r[1], r[3]) for r in refusals(db)] == [(600, "missing")]


# --------------------------------------------------------------------- resource bounds
def test_refuses_oversize_work_at_the_exact_resource_bound(db):
    World(db).seed({X: A, Z: A})                            # two actions in window 0
    lease = acquire_transition(db, "repair")
    replace_features(db, lease, [feature(W3, "TON", 9, "base"),
                                 feature(W3, "jetton:A", 9, "base")], windows=[W3])
    db.commit()
    size = db.execute("SELECT sum(octet_length(action::text)) FROM canonical_actions "
                      "WHERE event_id IN (%s,%s)", (eid(X), eid(Z))).fetchone()[0]
    db.commit()
    exact = dict(POLICY, max_windows=2, max_actions=2, max_input_bytes=size,
                 max_baseline_rows=2)
    fits = admit_work(db, (W0, W6), NOW, exact)
    assert fits.admitted is True
    assert dict(fits.usage) == {"windows": 2, "actions": 2, "input_bytes": size,
                                "baseline_rows": 2}
    before = work_state(db)
    expected = []
    for name, dimension in (("max_windows", "windows"), ("max_actions", "actions"),
                            ("max_input_bytes", "input_bytes"),
                            ("max_baseline_rows", "baseline_rows")):
        refused = admit_windows(db, lease, (W0, W6), NOW, dict(exact, **{name: exact[name] - 1}))
        assert (refused.admitted, refused.status) == (False, "unrepairable")
        assert refused.reasons == ((W0, "over_budget:" + dimension),
                                   (W6, "over_budget:" + dimension))
        expected += [(0, "unrepairable", ["over_budget:" + dimension]),
                     (600, "unrepairable", ["over_budget:" + dimension])]
    assert work_state(db) == before                         # refusals change no work state
    assert sorted((r[1], r[3], r[4]) for r in refusals(db)) == sorted(expected)


def test_inputs_that_grow_after_admission_are_refused_at_freeze_before_any_write(db):
    world_with_correction(db)
    lease = acquire_transition(db, "repair")
    db.commit()
    admission = admit_work(db, (W6,), NOW, dict(POLICY, max_baseline_rows=1))
    assert admission.admitted is True and dict(admission.usage)["baseline_rows"] == 0
    replace_features(db, lease, [feature(W3, "TON", 9, "a"), feature(W3, "jetton:A", 2, "a")],
                     windows=[W3])                          # leading context grows to 2 rows
    db.commit()
    before = work_state(db)
    with pytest.raises(ValueError, match="exceeds the admitted resource budget"):
        freeze_work(db, lease, admission)
    with pytest.raises(ValueError, match="exceeds the admitted resource budget"):
        repair_windows(db, lease, None, admission,
                       compute=lambda *_: pytest.fail("computed oversize work"))
    assert work_state(db) == before


# --------------------------------------------------------------- no partial admission
def test_move_across_cutoff_is_not_partial(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    decide_revision(db, lease, evidence, auth)              # X moves from window 0 to 600
    db.commit()
    set_horizon(db, "canonical_payload", 300)              # old side's payload is gone
    before = work_state(db)
    pending = db.execute("SELECT count(*) FROM dirty_windows WHERE status='pending'").fetchone()[0]
    db.commit()
    move = admit_windows(db, lease, (W6, W0), NOW, POLICY)
    assert (move.admitted, move.status) == (False, "unrepairable")
    assert move.reasons == ((W0, "payload_not_retained"),)  # new side alone would pass
    with pytest.raises(ValueError, match="not admitted"):
        repair_windows(db, lease, None, move, compute=lambda *_: pytest.fail("computed"))
    assert work_state(db) == before                         # no served row, promotion, epoch
    assert db.execute("SELECT count(*) FROM dirty_windows WHERE status='pending'"
                      ).fetchone()[0] == pending > 0
    db.commit()
    rows = refusals(db)
    assert [(r[1], r[3], r[4], r[5]) for r in rows] == [
        (0, "unrepairable", ["payload_not_retained"], [["testnet", 0, 300], ["testnet", 600, 900]]),
        (600, "unrepairable", ["work_unit_refused"], [["testnet", 0, 300], ["testnet", 600, 900]]),
    ]
    assert {r[9] for r in rows} == {1}                      # dependency epoch at the decision


# ------------------------------------------------------ durable, fenced, idempotent record
def test_refusal_record_is_fenced_idempotent_and_immutable(db):
    world_with_correction(db)
    old = acquire_transition(db, "old")
    lease = acquire_transition(db, "repair")
    db.commit()
    set_horizon(db, "canonical_payload", 300)
    refused = admit_work(db, (W0,), NOW, POLICY)
    db.commit()
    with pytest.raises(LeaseRejected):
        record_refusal(db, old, refused)
    assert refusals(db) == []
    assert record_refusal(db, lease, refused) is True
    assert record_refusal(db, lease, refused) is False      # same decision: no second row
    assert len(refusals(db)) == 1
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE window_admission_refusals SET status='missing'")
    db.rollback()
    admitted = admit_work(db, (W3,), NOW, POLICY)
    db.commit()
    with pytest.raises(ValueError):
        record_refusal(db, lease, admitted)
    assert len(refusals(db)) == 1


def test_retained_horizon_only_moves_forward(db):
    set_horizon(db, "canonical_payload", 600)
    with pytest.raises(psycopg.errors.RaiseException):
        set_horizon(db, "canonical_payload", 300)
    db.rollback()
    with pytest.raises(psycopg.errors.CheckViolation):
        set_horizon(db, "derived_features", 301)
    db.rollback()
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM retained_context_horizons")
    db.rollback()
    assert db.execute("SELECT data_class, retained_from FROM retained_context_horizons"
                      ).fetchall() == [("canonical_payload", 600)]
    db.commit()


# =============================================================================================
# Bounded retention deletes only proven-covered data, advances the
# retained horizon in the same transaction, never deletes what admitted or pending work needs,
# and retires identity guards only after proven coverage. #  Fixture world: X ends at 11 (window 0), Z at 311
# (window 300), Q at 611 (window 600). At T_PAYLOAD whole windows 0 and 300 are past the
# seven-day cutoff; window 600 is not.
import os  # noqa: E402
import threading  # noqa: E402
import time  # noqa: E402

from ton_sentinel.db.fencing import validate_transition  # noqa: E402
from ton_sentinel.maintenance import (  # noqa: E402
    apply_retention, hold_admission, plan_retention, release_hold,
)
from ton_sentinel.spark.stream import fingerprint  # noqa: E402
from tests.integration.test_revisions import val  # noqa: E402

Q = (6, 7)
R = (8, 9)
RPOLICY = {"event_retention_seconds": 7 * DAY, "guard_retention_seconds": 30 * DAY,
           "max_windows": 10}
T_PAYLOAD = 7 * DAY + 600
T_GUARD = 30 * DAY + 600
_RETENTION_TABLES = (
    "canonical_actions", "canonical_current", "canonical_revisions", "outbox", "scan_membership",
    "event_identity_guards", "retention_runs", "retention_holds", "retained_context_horizons",
    "identity_guard_horizons", "served_features", "dirty_windows", "window_sweep_closures")


def retention_state(db):
    db.commit()
    state = {}
    for table in _RETENTION_TABLES:
        state[table] = db.execute(
            f"SELECT count(*), md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) "
            f"FROM {table} t").fetchone()
    db.commit()
    return state


def read(db, sql, args=()):
    db.commit()
    rows = db.execute(sql, args).fetchall()
    db.commit()
    return rows


def events(db):
    return sorted(r[0] for r in read(db, "SELECT event_id FROM canonical_actions"))


def horizon(db, data_class="canonical_payload"):
    rows = read(db, "SELECT retained_from FROM retained_context_horizons "
                    "WHERE network='testnet' AND data_class=%s", (data_class,))
    return rows[0][0] if rows else None


def acknowledge(db):
    """What a confirmed publication leaves: every outbox row acknowledged."""
    db.execute("UPDATE outbox SET publication_state='acknowledged', "
               "published_at=clock_timestamp(), acknowledged_at=clock_timestamp()")
    db.commit()


def close(db, lease, key):
    """What the sweeper leaves: a canonical repair of ``key`` plus a closure on its inputs."""
    pinned = {}

    def compute(_spark, frozen):
        pinned["fingerprint"], pinned["work_id"] = fingerprint(frozen.inputs), frozen.work_id
        return [feature(key, "TON", len(frozen.inputs), "closed")]

    def stage(connection):
        connection.execute(
            "INSERT INTO window_sweep_closures(network,window_start,reason,fingerprint,work_id) "
            "VALUES (%s,%s,'idle',%s,%s) ON CONFLICT (network,window_start) DO UPDATE SET "
            "fingerprint=EXCLUDED.fingerprint, work_id=EXCLUDED.work_id",
            (key.network, key.window_start, pinned["fingerprint"], pinned["work_id"]))

    db.commit()
    repair_windows(db, lease, None, (key,), compute=compute, stage=stage)


def covered_world(db, close_windows=(W0, W3)):
    World(db).seed({X: A, Z: val(end=311), Q: val(end=611)})
    acknowledge(db)
    lease = acquire_transition(db, "maintenance", ttl_seconds=300)
    for key in close_windows:
        close(db, lease, key)
    return lease


def payload(plan):
    (section,) = plan["payload"]
    return section


def guards(plan):
    (section,) = plan["guards"]
    return section


def pending_dirty(db, window_start, status="pending"):
    db.execute(
        "INSERT INTO dirty_windows(network,token,window_start,role,window_end,covers_start,"
        "covers_end,dependency_epoch,reason,cause_event_id,status) VALUES "
        "('testnet','TON',%s,'target',%s,%s,%s,1,'test',%s,%s)",
        (window_start, window_start + 300, window_start, window_start + 300, eid(X), status))
    db.commit()


def test_retention_refuses_expired_windows_whose_coverage_is_not_proven(db):
    lease = covered_world(db, close_windows=(W3,))           # window 0 never closed
    before = retention_state(db)
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    section = payload(plan)
    assert (section["network"], section["from"], section["to"], section["windows"]) == (
        "testnet", 0, 0, [])
    assert section["blocked"] == [0, ["coverage_not_proven"]]
    assert section["cutoff"] == 600 and plan["now"] == T_PAYLOAD
    result = apply_retention(db, lease, plan)
    assert result["status"] == "nothing_to_delete"
    assert retention_state(db) == before                    # nothing deleted, no horizon, no run
    # a closure on inputs that are no longer current is not proof either
    close(db, lease, W0)
    db.execute("UPDATE window_sweep_closures SET fingerprint=%s WHERE window_start=0",
               ("sha256:" + "0" * 64,))
    db.commit()
    before = retention_state(db)
    stale = payload(plan_retention(db, T_PAYLOAD, RPOLICY))
    assert (stale["to"], stale["blocked"]) == (0, [0, ["coverage_not_current"]])
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "nothing_to_delete"
    assert retention_state(db) == before


def test_covered_windows_are_deleted_with_their_horizon_in_one_audited_transaction(db):
    lease = covered_world(db)
    expected_guards = sorted(read(db, (
        "SELECT event_id, accepted_generation, semantic_digest FROM canonical_actions "
        "WHERE event_id IN (%s,%s)"), (eid(X), eid(Z))))
    served_before = read(db, "SELECT window_start, token, generation FROM served_features "
                             "ORDER BY window_start")
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    section = payload(plan)
    assert (section["from"], section["to"], section["windows"], section["blocked"],
            section["bounded"]) == (0, 600, [0, 300], None, False)
    assert section["events"] == sorted([eid(X), eid(Z)])
    result = apply_retention(db, lease, plan)
    assert result["status"] == "applied"
    assert events(db) == [eid(Q)]                            # window 600 payload retained
    assert read(db, "SELECT event_id FROM outbox") == [(eid(Q),)]
    assert read(db, "SELECT event_id FROM scan_membership") == [(eid(Q),)]
    assert horizon(db) == 600
    # payload expiry leaves compact identity guards with generation and digest
    assert sorted(read(db, "SELECT event_id, accepted_generation, semantic_digest "
                           "FROM event_identity_guards")) == expected_guards
    assert read(db, "SELECT event_id, window_start, last_window, disposition, eligible "
                    "FROM event_identity_guards ORDER BY window_start") == [
        (eid(X), 0, 0, "active", True), (eid(Z), 300, 300, "active", True)]
    # audit: exact cutoff, bounds and before/after counts, written in the deleting transaction
    (run,) = read(db, "SELECT kind, network, now_at, cutoff, from_window, to_window, windows,"
                      " counts_before->>'canonical_actions', counts_after->>'canonical_actions',"
                      " counts_after->>'event_identity_guards', lease_owner, lease_fence"
                      " FROM retention_runs")
    assert run == ("payload", "testnet", T_PAYLOAD, 600, 0, 600, [0, 300], "3", "1", "2",
                   "maintenance", lease.fence)
    (same,) = read(db, (
        "SELECT r.txid %% 4294967296 = h.xmin::text::bigint, "
        "       r.txid %% 4294967296 = g.xmin::text::bigint "
        "FROM retention_runs r, retained_context_horizons h, event_identity_guards g "
        "WHERE h.data_class='canonical_payload' AND g.event_id=%s"), (eid(X),))
    assert same == (True, True)                              # horizon, guards, run: one tx
    # retained derived history stays queryable; admission now reads the real horizon
    assert read(db, "SELECT window_start, token, generation FROM served_features "
                    "ORDER BY window_start") == served_before
    refused = admit_work(db, (W0,), NOW, POLICY)
    assert refused.reasons == ((W0, "payload_not_retained"),)
    # a retry of the same plan is idempotent: no second run, nothing else deleted
    after = retention_state(db)
    again = apply_retention(db, lease, plan)
    assert again["status"] == "already_applied" and again["runs"] == result["runs"]
    assert retention_state(db) == after
    # the guard keeps the expired identity out of the additive path; so does the horizon.
    # A source re-delivery is quarantined (never new, never a raw database
    # exception); the database gate itself still refuses any raw insert of either identity.
    with pytest.raises(psycopg.errors.RaiseException, match="identity guard"):
        raw_insert(db, X, A)
    db.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="no longer retained"):
        raw_insert(db, R, val(end=12))
    db.rollback()
    World(db).seed({X: A})
    World(db).seed({R: val(end=12)})
    assert retention_state(db) == after                      # nothing new, nothing re-added
    assert [(e, w, r) for e, _, w, r in quarantined(db)] == sorted([
        (eid(X), 0, ["identity_guarded", "payload_expired"]),
        (eid(R), 0, ["payload_expired"])])


def test_a_failed_deletion_rolls_back_everything(db):
    lease = covered_world(db)
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    before = retention_state(db)
    # fail at the first guard write, in the middle of the deletion and at the horizon write
    for table, when in (("event_identity_guards", "BEFORE INSERT"),
                        ("canonical_actions", "BEFORE DELETE"),
                        ("retained_context_horizons", "BEFORE INSERT OR UPDATE")):
        db.execute("CREATE FUNCTION injected_failure() RETURNS trigger LANGUAGE plpgsql AS "
                   "$$ BEGIN RAISE EXCEPTION 'injected failure'; END $$")
        db.execute(f"CREATE TRIGGER injected_failure {when} ON {table} "
                   "FOR EACH ROW EXECUTE FUNCTION injected_failure()")
        db.commit()
        with pytest.raises(psycopg.errors.RaiseException, match="injected failure"):
            apply_retention(db, lease, plan)
        db.rollback()
        db.execute(f"DROP TRIGGER injected_failure ON {table}")
        db.execute("DROP FUNCTION injected_failure()")
        db.commit()
        assert retention_state(db) == before                 # no row, guard, run or horizon
    assert apply_retention(db, lease, plan)["status"] == "applied"   # the retry succeeds
    assert events(db) == [eid(Q)] and horizon(db) == 600


def test_pending_repair_blocks_deletion_of_its_window(db):
    lease = covered_world(db)
    pending_dirty(db, 300)
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    assert (payload(plan)["to"], payload(plan)["windows"], payload(plan)["blocked"]) == (
        300, [0], [300, ["pending_repair"]])
    assert apply_retention(db, lease, plan)["status"] == "applied"
    assert events(db) == sorted([eid(Z), eid(Q)]) and horizon(db) == 300
    db.execute("UPDATE dirty_windows SET status='done' WHERE window_start=300")
    db.commit()
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    assert events(db) == [eid(Q)] and horizon(db) == 600


def test_quarantine_and_unacknowledged_delivery_block_deletion(db):
    world, evidence, _ = world_with_correction(db)           # X = A in window 0
    lease = acquire_transition(db, "maintenance", ttl_seconds=300)
    close(db, lease, W0)
    unacked = payload(plan_retention(db, T_PAYLOAD, RPOLICY))
    assert (unacked["to"], unacked["blocked"]) == (0, [0, ["unacknowledged_delivery"]])
    acknowledge(db)
    decide_revision(db, lease, evidence)                     # unauthorized change: quarantine
    db.commit()
    before = retention_state(db)
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    assert payload(plan)["to"] == 0
    assert "open_quarantine" in payload(plan)["blocked"][1]
    assert apply_retention(db, lease, plan)["status"] == "nothing_to_delete"
    assert retention_state(db) == before


def test_admitted_work_holds_its_context_until_released(db):
    lease = covered_world(db)
    admission = admit_work(db, (W3,), NOW, POLICY)           # leading context clipped at 0
    assert admission.admitted
    hold = hold_admission(db, lease, admission)
    assert hold_admission(db, lease, admission) == hold     # idempotent
    assert read(db, "SELECT network, hold_from, hold_until, released_at IS NULL "
                    "FROM retention_holds") == [("testnet", 0, 600, True)]
    before = retention_state(db)
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    assert (payload(plan)["to"], payload(plan)["blocked"]) == (
        0, [0, ["admitted_work_hold"]])
    assert apply_retention(db, lease, plan)["status"] == "nothing_to_delete"
    assert retention_state(db) == before
    assert release_hold(db, lease, admission) is True
    assert release_hold(db, lease, admission) is False      # already released
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    assert horizon(db) == 600
    # work admitted before the deletion can no longer take a hold on deleted context
    with pytest.raises(ValueError, match="no longer retained"):
        hold_admission(db, lease, admission)
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM retention_holds")
    db.rollback()


def test_concurrent_fenced_work_is_serialized_and_makes_the_plan_stale(db):
    lease = covered_world(db)
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    before = retention_state(db)
    locked, results = threading.Event(), {}

    def worker():
        with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as other:
            other.execute(f'SET search_path TO "{db.schema_name}"')
            other.commit()
            with other.transaction():
                validate_transition(other, lease)            # holds the shared slot row
                other.execute(
                    "INSERT INTO dirty_windows(network,token,window_start,role,window_end,"
                    "covers_start,covers_end,dependency_epoch,reason,cause_event_id) VALUES "
                    "('testnet','TON',0,'target',300,0,300,1,'concurrent',%s)", (eid(X),))
                locked.set()
                time.sleep(1.5)
                results["releasing_at"] = time.monotonic()

    thread = threading.Thread(target=worker)
    thread.start()
    assert locked.wait(10)
    result = apply_retention(db, lease, plan)                # waits for the slot, then re-checks
    finished = time.monotonic()
    thread.join(10)
    assert finished >= results["releasing_at"]              # retention waited for the slot
    assert result["status"] == "stale"
    assert events(db) == sorted([eid(X), eid(Z), eid(Q)]) and horizon(db) is None
    changed = retention_state(db)
    assert changed["dirty_windows"] != before["dirty_windows"]
    assert {k: v for k, v in changed.items() if k != "dirty_windows"} == {
        k: v for k, v in before.items() if k != "dirty_windows"}


def test_stale_lease_cannot_delete(db):
    lease = covered_world(db)
    plan = plan_retention(db, T_PAYLOAD, RPOLICY)
    acquire_transition(db, "newer-owner")
    db.commit()
    before = retention_state(db)
    with pytest.raises(LeaseRejected):
        apply_retention(db, lease, plan)
    assert retention_state(db) == before


def test_guards_retire_only_after_proven_coverage_and_stay_fenced_after(db):
    lease = covered_world(db)
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    # guards outlive the payload: nothing retires before the 30-day guard cutoff
    young = guards(plan_retention(db, T_PAYLOAD + DAY, RPOLICY))
    assert (young["from"], young["to"], young["windows"]) == (0, 0, [])
    # proven coverage is the closure on exactly the guarded identities; a stale one blocks
    (fp0,) = read(db, "SELECT fingerprint FROM window_sweep_closures WHERE window_start=0")[0]
    db.execute("UPDATE window_sweep_closures SET fingerprint=%s WHERE window_start=0",
               ("sha256:" + "1" * 64,))
    db.commit()
    before = retention_state(db)
    plan = plan_retention(db, T_GUARD, RPOLICY)
    assert (guards(plan)["to"], guards(plan)["blocked"]) == (0, [0, ["coverage_not_current"]])
    assert apply_retention(db, lease, plan)["status"] == "nothing_to_delete"
    assert retention_state(db) == before                    # both guards kept, no boundary
    db.execute("UPDATE window_sweep_closures SET fingerprint=%s WHERE window_start=0", (fp0,))
    db.commit()
    # pending repair of window 300 keeps Z's guard; X's retires with the fenced boundary
    pending_dirty(db, 300)
    plan = plan_retention(db, T_GUARD, RPOLICY)
    assert (guards(plan)["from"], guards(plan)["to"], guards(plan)["windows"],
            guards(plan)["blocked"]) == (0, 300, [0], [300, ["pending_repair"]])
    assert apply_retention(db, lease, plan)["status"] == "applied"
    assert read(db, "SELECT event_id FROM event_identity_guards") == [(eid(Z),)]
    assert read(db, "SELECT retired_before FROM identity_guard_horizons") == [(300,)]
    (run,) = read(db, "SELECT cutoff, from_window, to_window, counts_before->>"
                      "'event_identity_guards', counts_after->>'event_identity_guards' "
                      "FROM retention_runs WHERE kind='guards'")
    assert run == (600, 0, 300, "2", "1")
    db.execute("UPDATE dirty_windows SET status='done' WHERE window_start=300")
    db.commit()
    assert apply_retention(db, lease, plan_retention(db, T_GUARD, RPOLICY))["status"] \
        == "applied"
    assert read(db, "SELECT count(*) FROM event_identity_guards") == [(0,)]
    assert read(db, "SELECT retired_before FROM identity_guard_horizons") == [(600,)]
    # after retirement the identity is still not new: its window is behind both boundaries
    with pytest.raises(psycopg.errors.RaiseException, match="no longer retained"):
        raw_insert(db, X, A)
    db.rollback()
    World(db).seed({X: A})                                   # re-delivery: quarantined
    assert events(db) == [eid(Q)]
    assert [(e, w, r) for e, _, w, r in quarantined(db)] == [
        (eid(X), 0, ["guard_retired", "payload_expired"])]
    # guards and boundaries are not editable outside a retention run
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE identity_guard_horizons SET retired_before=0")
    db.rollback()
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM canonical_actions")
    db.rollback()


# =============================================================================================
# The named Step 1 tests, 30-day derived-history expiry under
# holds and pending repair context, late deliveries routed to quarantine (never new), the
# fenced maintenance cycle (logical vs physical bytes, bounded plain VACUUM, 20% free
# low-water pause of ingestion) and its CLI. # 
import io  # noqa: E402
import json  # noqa: E402
from contextlib import redirect_stdout  # noqa: E402

from psycopg.conninfo import make_conninfo  # noqa: E402

from ton_sentinel.config import evaluate_action_window_policy  # noqa: E402
from ton_sentinel.contracts import Change, grid_cell_state  # noqa: E402
from ton_sentinel.contracts import WindowKey as Cell  # noqa: E402
from ton_sentinel.db.ledger import IngestionPaused  # noqa: E402
from ton_sentinel.db.work import (  # noqa: E402
    freeze_work as _freeze, promote, promote_scores, record_qualification, rescore_window,
    score_window,
)
from ton_sentinel.maintenance import (  # noqa: E402
    admit_delivery, apply_derived_retention, hold_context, plan_derived_retention,
    release_context, run_retention_cycle,
)
from ton_sentinel.normalize import normalize, semantic_digest  # noqa: E402
from ton_sentinel.spark.streaming import process_canonical_microbatch  # noqa: E402
from tests.integration.test_revisions import epoch as source_epoch  # noqa: E402
from tests.integration.test_revisions import mono, revise  # noqa: E402
from tests.integration.test_revisions import payload as source_payload  # noqa: E402

S = (10, 11)
EV0 = (100, 200)
TOK = "jetton:A"
SCORED = 24 * 300
T_DERIVED = 30 * DAY + 600
DPOLICY = {"derived_retention_seconds": 30 * DAY, "max_windows": 10}
CPOLICY = {"event_retention_seconds": 7 * DAY, "guard_retention_seconds": 30 * DAY,
           "derived_retention_seconds": 30 * DAY, "max_windows": 10, "vacuum_max_tables": 20,
           "vacuum_max_bytes": 1 << 34, "storage_budget_bytes": 1 << 34, "min_free_percent": 20}
CUT = "sha256:" + "c" * 64          # a recovery-cut style holder of retained context
_DERIVED_TABLES = ("served_features", "baseline_inputs", "detector_results", "detector_current",
                   "alert_revisions")
# Level-1 serialized maintenance budget accepted in
# deploy/workload/acceptance.json (maintenance_seconds = 600).
MAINTENANCE_BUDGET_SECONDS = 600


def raw_insert(db, ev, v):
    """A raw canonical insert, bypassing ingestion: only the database gate decides."""
    action = normalize(source_payload(ev, v), source_epoch())
    db.execute("INSERT INTO canonical_actions(event_id,semantic_digest,action) "
               "VALUES (%s,%s,%s::jsonb)",
               (action.identity.event_id, semantic_digest(action), action.to_json()))


def quarantined(db):
    return read(db, "SELECT event_id, network, window_start, reasons FROM delivery_quarantine "
                    "ORDER BY event_id")


def guard_ids(db):
    return {r[0] for r in read(db, "SELECT event_id FROM event_identity_guards")}


def derived_rows(db):
    return {t: read(db, f"SELECT count(*) FROM {t}")[0][0] for t in _DERIVED_TABLES}


def feature_windows(db):
    return [r[0] for r in read(db, "SELECT DISTINCT window_start FROM served_features "
                                   "ORDER BY 1")]


def dirty(db, window_start, token="TON", role="target", status="pending", cause=None):
    db.execute(
        "INSERT INTO dirty_windows(network,token,window_start,role,window_end,covers_start,"
        "covers_end,dependency_epoch,reason,cause_event_id,status) VALUES "
        "('testnet',%s,%s,%s,%s,%s,%s,1,'test',%s,%s)",
        (token, window_start, role, window_start + 300, window_start, window_start + 300,
         cause or eid(X), status))
    db.commit()


def grid_row(start, token, swaps, known=None, unknown=0):
    return {"network": "testnet", "window_start": start, "window_end": start + 300,
            "token": token, "generation": "g", "swaps": swaps, "input_raw": swaps,
            "output_raw": None, "known_traders": swaps if known is None else known,
            "unknown_count": unknown, "coverage_status": "unknown", "status": "provisional",
            "provisional": True}


def qualify(db, lease, world, start, ranges=None, delta=60):
    db.commit()
    return record_qualification(
        db, lease, network="testnet", window_start=start, mode="bounded_observed",
        mapping_version="synthetic-map.v1", delta_seconds=delta,
        trace_ranges=ranges or [[start, start + 300 + delta]],
        source_epoch=json.loads(world.epoch.to_json()), synthetic=True)


def promote_rows(db, lease, start, rows):
    db.commit()
    frozen = _freeze(db, lease, [WindowKey("testnet", start, start + 300)])
    db.commit()
    assert promote(db, lease, frozen, rows) is True


def close_with(db, lease, key, rows):
    """A sweeper closure of ``key`` that promotes ``rows`` (fingerprint of its inputs)."""
    pinned = {}

    def compute(_spark, frozen):
        pinned["fingerprint"], pinned["work_id"] = fingerprint(frozen.inputs), frozen.work_id
        return rows

    def stage(connection):
        connection.execute(
            "INSERT INTO window_sweep_closures(network,window_start,reason,fingerprint,work_id) "
            "VALUES (%s,%s,'idle',%s,%s)",
            (key.network, key.window_start, pinned["fingerprint"], pinned["work_id"]))

    db.commit()
    repair_windows(db, lease, None, (key,), compute=compute, stage=stage)


def derived_world(db):
    """jetton:A: observed at window 0 (closed, so its payload can expire first), windows
    300..6900 qualified zeros, window 7200 scored with detector results and alerts, window
    7500 featured. At T_PAYLOAD the payload of window 0 expires; at T_DERIVED its features."""
    world = World(db)
    world.seed({EV0: val(end=7)})
    acknowledge(db)
    lease = acquire_transition(db, "maintenance", ttl_seconds=300)
    for start in [i * 300 for i in range(24)] + [SCORED, SCORED + 300]:
        assert qualify(db, lease, world, start) is True
    close_with(db, lease, W0, [grid_row(0, TOK, 1)])
    promote_rows(db, lease, SCORED, [grid_row(SCORED, TOK, 12, 6)])
    promote_rows(db, lease, SCORED + 300, [grid_row(SCORED + 300, TOK, 3, 3)])
    rescore_window(db, lease, "testnet", TOK, SCORED)
    db.commit()
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    assert horizon(db) == 600
    return world, lease


class BrokerBatch:
    """The two Kafka columns a microbatch reads; product checks run before any Spark work."""
    columns = ["key", "value"]

    def __init__(self, rows):
        self.rows = rows

    def select(self, *_columns):
        return self

    def limit(self, _n):
        return self

    def collect(self):
        return self.rows


# ------------------------------------------------------------------- Step 1 named tests
def test_action_trace_clocks(db):
    """Action-end and trace-completion clocks decide independently; a late observation
    never renews an event's analytical age."""
    lease = covered_world(db)                    # X@0 (end 11), Z@300, Q@600; 0 and 300 closed
    # action-end clock at T = 7 days + 300 s: C_event = 300
    at = 7 * DAY + 300
    just_before = admit_work(db, (W0,), at, POLICY)
    assert (just_before.admitted, just_before.event_cutoff) == (False, 300)
    assert just_before.reasons == ((W0, "before_event_cutoff"),)
    assert admit_work(db, (W3,), at, POLICY).admitted is True        # window start == C_event
    # trace-completion clock: a zero needs trace ranges swept to window_end + delta (H);
    # existing payload alone (Q in window 600) never certifies coverage
    world = World(db)
    with pytest.raises(ValueError, match="cover"):
        qualify(db, lease, world, 600, ranges=[[600, 920]])           # H stops before 960
    assert read(db, "SELECT count(*) FROM action_window_qualification") == [(0,)]
    unqualified = dict(canonical=True, qualified=False, pending=False, touched=True,
                       in_universe=True)
    assert grid_cell_state(feature_swaps=1, **unqualified) == "partial"
    assert grid_cell_state(feature_swaps=None, **dict(unqualified, touched=False)) == "missing"
    no_mapping = evaluate_action_window_policy(None, 600, 900)        # no supported delta
    assert (no_mapping["eligible_zero"], no_mapping["reason"]) == (False, "mapping_unavailable")
    # an action before C_event whose trace completes inside H is still expired
    assert qualify(db, lease, world, 0) is True                       # trace side is fine...
    assert admit_work(db, (W0,), at, POLICY).admitted is False        # ...the action is not
    db.execute("UPDATE dirty_windows SET status='done' WHERE status='pending'")
    db.commit()
    # analytical age is the action-end clock: X and Z were observed today, still expire
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    assert events(db) == [eid(Q)]
    # a newly observed old action (trace complete, action before the horizon) is quarantined
    World(db).seed({R: val(end=12)})
    assert events(db) == [eid(Q)]
    assert [(e, w, r) for e, _, w, r in quarantined(db)] == [(eid(R), 0, ["payload_expired"])]


def test_holds_and_leading_context(db):
    """Pinned admitted input and its leading context survive expiry; admission checks the
    actual retained payload and leading context, not dates alone."""
    lease = covered_world(db)
    admission = admit_work(db, (W3,), NOW, POLICY)   # target 300, leading context from 0
    assert admission.admitted is True
    hold_admission(db, lease, admission)
    pinned_payload_ids = {eid(X), eid(Z)}
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "nothing_to_delete"
    remaining_payload_ids = set(events(db))
    assert pinned_payload_ids <= remaining_payload_ids
    assert release_hold(db, lease, admission) is True
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    # dates alone admit window 0 at NOW (C_event = 0); the actual payload is gone
    expired_window_admission = admit_work(db, (W0,), NOW, POLICY)
    assert expired_window_admission.event_cutoff == 0
    assert expired_window_admission.admitted is False
    assert expired_window_admission.reasons == ((W0, "payload_not_retained"),)
    # leading context: a real derived run deletes the features of 0 and 300; window 600's
    # payload is retained and its dates pass, but its leading context no longer exists
    assert apply_derived_retention(db, lease, plan_derived_retention(
        db, T_DERIVED, DPOLICY))["status"] == "applied"
    assert horizon(db, "derived_features") == 600 and feature_windows(db) == []
    leading = admit_work(db, (W6,), NOW, POLICY)
    assert (leading.admitted, leading.status) == (False, "missing")
    assert leading.reasons == ((W6, "baseline_context_not_retained"),)


def test_expired_delivery_never_new(db):
    """After payload expiry a re-delivered or newly seen old identity is quarantined, never
    counted new; a broker replay of its old envelope is rejected; admit_delivery agrees."""
    lease = covered_world(db)
    (x_envelope,) = read(db, "SELECT delivery_envelope FROM outbox WHERE event_id=%s",
                         (eid(X),))[0]
    (incarnation, replay_epoch), = read(
        db, "SELECT topic_incarnation, replay_epoch FROM outbox_delivery_config")
    x_generation = read(db, "SELECT accepted_generation FROM canonical_actions "
                            "WHERE event_id=%s", (eid(X),))[0][0]
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    guards_before = read(db, "SELECT * FROM event_identity_guards ORDER BY event_id")
    (generation_before,) = read(db, "SELECT generation FROM canonical_generation")[0]
    # one corroborated range: X re-delivered, R new but old, S new and retained
    World(db).seed({X: A, R: val(end=12), S: val(end=911)})
    assert events(db) == sorted([eid(Q), eid(S)])                   # only S is new
    assert read(db, "SELECT event_id FROM outbox ORDER BY event_id") == sorted(
        [(eid(Q),), (eid(S),)])
    assert sorted(r[0] for r in read(db, "SELECT event_id FROM scan_membership")) == sorted(
        [eid(Q), eid(S)])
    assert [(e, n, w, r) for e, n, w, r in quarantined(db)] == sorted([
        (eid(X), "testnet", 0, ["identity_guarded", "payload_expired"]),
        (eid(R), "testnet", 0, ["payload_expired"])])
    (row,) = read(db, "SELECT range_id IS NOT NULL, semantic_digest, proposed_action->>'action_end',"
                      " lease_owner FROM delivery_quarantine WHERE event_id=%s", (eid(X),))
    assert row == (True, World(db).digest(X, A), "11", "seed")
    assert read(db, "SELECT * FROM event_identity_guards ORDER BY event_id") == guards_before
    # only S consumed an accepted generation: the refused identities were never counted new
    assert read(db, "SELECT generation FROM canonical_generation") == [(generation_before + 1,)]
    assert read(db, "SELECT accepted_generation FROM canonical_actions WHERE event_id=%s",
                (eid(S),)) == [(generation_before + 1,)]
    # the quarantine is an immutable audit record
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE delivery_quarantine SET reasons='[]'::jsonb")
    db.rollback()
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM delivery_quarantine")
    db.rollback()
    # admit_delivery reads the same retained state, without writing
    before = retention_state(db)
    x_change = Change("revision", eid(X), "redelivery", x_generation,
                      (Cell("testnet", "TON", 0),), ())
    refused = admit_delivery(db, x_change)
    assert (refused.admitted, refused.status, refused.reasons) == (
        False, "backfill_only", ("identity_guarded", "payload_expired", "stale_generation"))
    s_change = Change("revision", eid(S), "redelivery", 9, (Cell("testnet", "TON", 900),), ())
    assert admit_delivery(db, s_change).admitted is True
    assert retention_state(db) == before
    # a broker replay of X's old envelope is rejected before any serving write
    batch = BrokerBatch([{"key": eid(X).encode(), "value": json.dumps(x_envelope).encode()}])
    stream = acquire_transition(db, "stream")
    before = retention_state(db)
    with pytest.raises(ValueError, match="not an accepted canonical ledger action"):
        process_canonical_microbatch(db, stream, None, batch, topic_incarnation=incarnation,
                                     replay_epoch=replay_epoch)
    db.rollback()
    assert retention_state(db) == before


def test_guard_expiry_retires_epochs(db):
    """Payload expiry preserves guards; a pending/ambiguous send blocks expiry of its event
    and a recovery-style hold keeps the guards it references; after retirement the old
    identity is refused (backfill only) by admit_delivery and by ingestion."""
    lease = covered_world(db, close_windows=(W0, W3, W6))
    db.execute("UPDATE outbox SET publication_state='ambiguous', acknowledged_at=NULL, "
               "published_at=NULL, publication_attempts=1 WHERE event_id=%s", (eid(Q),))
    db.commit()
    assert apply_retention(db, lease, plan_retention(db, T_GUARD, RPOLICY))["status"] \
        == "applied"
    assert events(db) == [eid(Q)]                     # Q's ambiguous send keeps its payload
    assert guard_ids(db) == {eid(X), eid(Z)}          # payload expiry preserved both guards
    assert horizon(db) == 600
    # a hold (as a recovery cut takes) on window 300 references Z's guard
    assert hold_context(db, lease, CUT, "testnet", 300, 600) is True
    assert hold_context(db, lease, CUT, "testnet", 300, 600) is False     # idempotent
    pending_replay_guard_ids = {eid(Z)}
    plan = plan_retention(db, T_GUARD, RPOLICY)
    assert (guards(plan)["from"], guards(plan)["to"], guards(plan)["blocked"]) == (
        0, 300, [300, ["admitted_work_hold"]])
    assert apply_retention(db, lease, plan)["status"] == "applied"
    remaining_guard_ids = guard_ids(db)
    assert pending_replay_guard_ids <= remaining_guard_ids == {eid(Z)}
    assert eid(Q) not in remaining_guard_ids and eid(Q) in events(db)
    assert read(db, "SELECT retired_before FROM identity_guard_horizons") == [(300,)]
    # a hold can never resurrect a retired identity
    with pytest.raises(ValueError, match="no longer retained"):
        hold_context(db, lease, "sha256:" + "d" * 64, "testnet", 0, 300)
    assert release_context(db, lease, CUT) is True
    assert release_context(db, lease, CUT) is False
    assert apply_retention(db, lease, plan_retention(db, T_GUARD, RPOLICY))["status"] \
        == "applied"
    assert guard_ids(db) == set()
    assert read(db, "SELECT retired_before FROM identity_guard_horizons") == [(600,)]
    # after epoch retirement the old identity is never new
    retired_epoch_change = Change("revision", eid(X), "replay", 1, (Cell("testnet", "TON", 0),),
                                  ())
    refused = admit_delivery(db, retired_epoch_change)
    assert refused.admitted is False
    assert refused.reasons == ("guard_retired", "payload_expired")
    World(db).seed({X: A, Z: val(end=311)})
    assert events(db) == [eid(Q)]
    assert [(e, w, r) for e, _, w, r in quarantined(db)] == sorted([
        (eid(X), 0, ["guard_retired", "payload_expired"]),
        (eid(Z), 300, ["guard_retired", "payload_expired"])])


# ------------------------------------------------------------------ revision late delivery
def test_revision_into_an_expired_window_is_quarantined_not_raised(db):
    lease = covered_world(db)
    assert apply_retention(db, lease, plan_retention(db, T_PAYLOAD, RPOLICY))["status"] \
        == "applied"
    world = World(db)
    evidence = world.candidate({Q: val(end=12)}, {6: {7}})   # Q would move to window 0
    auth = world.authorization(mono(revise(Q, val(end=12), src=2, prev=1)))
    before = retention_state(db)
    decide_revision(db, lease, evidence, auth)               # no raw database exception
    db.commit()
    assert read(db, "SELECT subject, scope, reason, status FROM revision_quarantine") == [
        (eid(Q), "event", "expired_identity", "open")]
    after = retention_state(db)
    # quarantine suppresses Q's eligibility (canonical_current, dirty rows) as for every
    # quarantined event; no revision, payload, guard, horizon or served row is written
    assert {k: v for k, v in after.items() if k not in ("canonical_current", "dirty_windows")} \
        == {k: v for k, v in before.items() if k not in ("canonical_current", "dirty_windows")}
    assert read(db, "SELECT action->>'action_end' FROM canonical_actions") == [("611",)]


# ------------------------------------------------------------------- derived history expiry
def test_derived_history_expires_after_30_days_behind_the_payload(db):
    _, lease = derived_world(db)
    detector_before = read(db, "SELECT window_start, detector_id, result_revision, stale "
                               "FROM detector_current ORDER BY 1, 2")
    alerts_before = read(db, "SELECT window_start, detector_id, alert_revision, state "
                             "FROM alert_revisions ORDER BY 1, 2, 3")
    assert detector_before and alerts_before
    # a score of window 7500 is computed now and promoted only after the retention run
    paused_baseline, paused_results = score_window(db, "testnet", TOK, SCORED + 300)
    db.commit()
    # whole windows only: at 30 days + 299 s window 0 has not fully aged out
    young = plan_derived_retention(db, T_DERIVED - 301, DPOLICY)
    (section,) = young["derived"]
    assert (section["from"], section["to"], section["cutoff"], section["windows"]) == (
        0, 0, 0, [])
    plan = plan_derived_retention(db, T_DERIVED, DPOLICY)
    (section,) = plan["derived"]
    assert (section["network"], section["from"], section["to"], section["cutoff"],
            section["windows"], section["blocked"], section["bounded"]) == (
        "testnet", 0, 600, 600, [0], None, False)
    assert section["rows"] == {"served_features": 1, "baseline_inputs": 0,
                               "detector_results": 0, "detector_current": 0,
                               "alert_revisions": 0}
    counts_before = derived_rows(db)
    result = apply_derived_retention(db, lease, plan)
    assert result["status"] == "applied"
    assert feature_windows(db) == [SCORED, SCORED + 300]
    assert derived_rows(db) == dict(counts_before,
                                    served_features=counts_before["served_features"] - 1)
    assert horizon(db, "derived_features") == 600
    (run,) = read(db, "SELECT kind, network, now_at, cutoff, from_window, to_window, windows, "
                      "counts_before->>'served_features', counts_after->>'served_features', "
                      "deleted->>'served_features', lease_owner FROM retention_runs "
                      "WHERE kind='derived'")
    assert run == ("derived", "testnet", T_DERIVED, 600, 0, 600, [0],
                   str(counts_before["served_features"]),
                   str(counts_before["served_features"] - 1), "1", "maintenance")
    (same,) = read(db, "SELECT r.txid %% 4294967296 = h.xmin::text::bigint FROM "
                       "retention_runs r, retained_context_horizons h WHERE r.kind='derived' "
                       "AND h.data_class='derived_features'")
    assert same == (True,)
    # retained scores and alerts stay exactly as they were (not silently invalidated) ...
    assert read(db, "SELECT window_start, detector_id, result_revision, stale "
                    "FROM detector_current ORDER BY 1, 2") == detector_before
    assert read(db, "SELECT window_start, detector_id, alert_revision, state "
                    "FROM alert_revisions ORDER BY 1, 2, 3") == alerts_before
    # ... but they are non-recomputable: admission refuses, rescoring cannot pin new input
    refused = admit_work(db, (WindowKey("testnet", SCORED, SCORED + 300),), NOW, POLICY)
    assert refused.reasons == ((WindowKey("testnet", SCORED, SCORED + 300),
                                "baseline_context_not_retained"),)
    with pytest.raises(psycopg.errors.RaiseException, match="no longer retained"):
        rescore_window(db, lease, "testnet", TOK, SCORED)
    db.rollback()
    # the score paused before the retention run is rejected when it is promoted after it
    assert promote_scores(db, lease, paused_baseline, paused_results) is False
    assert derived_rows(db) == dict(counts_before,
                                    served_features=counts_before["served_features"] - 1)
    # expired derived history never re-enters as new rows
    with pytest.raises(psycopg.errors.RaiseException, match="no longer retained"):
        replace_features(db, lease, [grid_row(0, TOK, 1)], windows=[W0])
    db.rollback()
    # payload goes first: a later cutoff never passes the payload horizon; retry idempotent
    later = plan_derived_retention(db, T_DERIVED + 3000, DPOLICY)
    assert (later["derived"][0]["from"], later["derived"][0]["to"]) == (600, 600)
    assert apply_derived_retention(db, lease, later)["status"] == "nothing_to_delete"
    assert apply_derived_retention(db, lease, plan)["status"] == "already_applied"
    # detector configuration is preserved: policies are never expired or deleted
    assert read(db, "SELECT count(*) FROM detector_policies") == [(1,)]
    assert read(db, "SELECT count(*) FROM active_detector_policy") == [(1,)]
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM detector_policies")
    db.rollback()
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM detector_results")
    db.rollback()
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("DELETE FROM baseline_inputs")
    db.rollback()


def test_derived_expiry_never_deletes_held_or_pending_repair_context(db):
    _, lease = derived_world(db)
    before = derived_rows(db)
    assert hold_context(db, lease, CUT, "testnet", 0, 300) is True
    plan = plan_derived_retention(db, T_DERIVED, DPOLICY)
    assert (plan["derived"][0]["to"], plan["derived"][0]["blocked"]) == (
        0, [0, ["admitted_work_hold"]])
    assert apply_derived_retention(db, lease, plan)["status"] == "nothing_to_delete"
    assert derived_rows(db) == before
    assert release_context(db, lease, CUT) is True
    # unadmitted pending repair of window 7200 needs its leading baseline context from 0
    dirty(db, SCORED, token=TOK, cause=eid(EV0))
    plan = plan_derived_retention(db, T_DERIVED, DPOLICY)
    assert (plan["derived"][0]["to"], plan["derived"][0]["blocked"]) == (
        0, [0, ["pending_repair"]])
    assert apply_derived_retention(db, lease, plan)["status"] == "nothing_to_delete"
    assert feature_windows(db) == [0, SCORED, SCORED + 300]
    db.execute("UPDATE dirty_windows SET status='done' WHERE window_start=%s", (SCORED,))
    db.commit()
    assert apply_derived_retention(db, lease, plan_derived_retention(
        db, T_DERIVED, DPOLICY))["status"] == "applied"
    assert feature_windows(db) == [SCORED, SCORED + 300]


# --------------------------------------------------------------------- the maintenance cycle
def test_cycle_reports_logical_and_physical_bytes_separately_and_vacuums_within_budget(db):
    covered_world(db)
    report = run_retention_cycle(db, "maintenance", T_PAYLOAD, dict(CPOLICY,
                                                                    vacuum_max_tables=1))
    assert report["status"] == "applied"
    assert report["retention"]["status"] == "applied"
    assert report["derived"]["status"] == "nothing_to_delete"
    logical = report["logical"]
    assert logical["deleted"]["canonical_actions"] == 2
    assert (logical["before"]["canonical_actions"], logical["after"]["canonical_actions"]) == (
        3, 1)
    physical = report["physical"]
    for stage in ("before", "after_delete", "after_vacuum"):
        assert physical[stage]["schema_bytes"] > 0
        assert physical[stage]["tables"]["canonical_actions"]["bytes"] > 0
    assert type(physical["wal_bytes"]) is int and physical["wal_bytes"] > 0
    # deletion leaves dead tuples; only plain VACUUM runs, within the table budget
    dead = physical["after_delete"]["tables"]["canonical_actions"]["dead_tuples"]
    assert dead >= 2
    vacuum = report["vacuum"]
    assert vacuum["mode"] == "VACUUM (ANALYZE)" and len(vacuum["vacuumed"]) == 1
    assert set(vacuum["skipped"].values()) <= {"over_table_budget", "no_dead_tuples",
                                               "over_byte_budget"}
    (table,) = vacuum["vacuumed"]
    assert physical["after_vacuum"]["tables"][table]["dead_tuples"] == 0
    assert report["storage"]["status"] == "ok" and report["ingestion"] == "running"
    assert report["seconds"] < MAINTENANCE_BUDGET_SECONDS
    (fence,) = read(db, "SELECT DISTINCT lease_fence FROM retention_runs")[0]
    assert report["lease"] == {"owner": "maintenance", "fence": fence}
    # a byte budget smaller than every table skips vacuum entirely
    again = run_retention_cycle(db, "maintenance", T_PAYLOAD + 300,
                                dict(CPOLICY, vacuum_max_bytes=1))
    assert again["vacuum"]["vacuumed"] == []
    assert set(again["vacuum"]["skipped"].values()) <= {"over_byte_budget", "no_dead_tuples"}


def test_low_free_space_pauses_ingestion_until_retention_recovers(db):
    covered_world(db)
    used = run_retention_cycle(db, "maintenance", T_PAYLOAD, CPOLICY)["storage"]["used_bytes"]
    tight = dict(CPOLICY, storage_budget_bytes=used * 10 // 9)      # ~10% free < 20%
    report = run_retention_cycle(db, "maintenance", T_PAYLOAD + 300, tight)
    assert report["storage"]["status"] == "low_water" and report["ingestion"] == "paused"
    (pause,) = read(db, "SELECT reason, budget_bytes, released_at IS NULL FROM ingestion_pauses")
    assert pause == ("low_water", used * 10 // 9, True)
    before = retention_state(db)
    with pytest.raises(IngestionPaused, match="low_water"):
        World(db).seed({S: val(end=911)})
    db.rollback()
    assert retention_state(db) == before
    # still low: the same pause stays (no second row); room again: ingestion resumes
    run_retention_cycle(db, "maintenance", T_PAYLOAD + 600, tight)
    assert len(read(db, "SELECT 1 FROM ingestion_pauses")) == 1
    resumed = run_retention_cycle(db, "maintenance", T_PAYLOAD + 900, CPOLICY)
    assert resumed["ingestion"] == "running"
    assert read(db, "SELECT released_at IS NOT NULL FROM ingestion_pauses") == [(True,)]
    World(db).seed({S: val(end=911)})
    assert eid(S) in events(db)


def test_maintenance_cli_runs_one_fenced_cycle(db, tmp_path):
    from ton_sentinel.cli import main

    covered_world(db)
    policy = tmp_path / "retention.json"
    policy.write_text(json.dumps(CPOLICY))
    dsn = make_conninfo(os.environ["TON_SENTINEL_TEST_DSN"],
                        options=f"-c search_path={db.schema_name}")
    db.commit()
    out = io.StringIO()
    with redirect_stdout(out):
        code = main(["maintenance", "--dsn", dsn, "--policy-file", str(policy), "--owner",
                     "maintenance", "--now", str(T_PAYLOAD)])
    assert code == 0
    report = json.loads(out.getvalue())
    assert report["status"] == "applied" and report["logical"]["deleted"]["canonical_actions"] == 2
    assert os.environ["TON_SENTINEL_TEST_DSN"].split("@")[0] not in out.getvalue()
    assert events(db) == [eid(Q)] and horizon(db) == 600


def test_cycle_under_backlog_stays_bounded_within_the_maintenance_budget(db):
    """24 expired, covered windows in backlog; max_windows 8 per cycle: three bounded cycles
    clear it, each within the measured level-1 maintenance budget."""
    world = World(db)
    backlog = {(20 + i, 60 + i): val(end=i * 300 + 5) for i in range(24)}
    world.seed(dict(list(backlog.items())[:12]))
    world.seed(dict(list(backlog.items())[12:]))
    acknowledge(db)
    lease = acquire_transition(db, "maintenance", ttl_seconds=300)
    for i in range(24):
        close(db, lease, win(i * 300))
    at = 7 * DAY + 24 * 300
    policy = dict(CPOLICY, max_windows=8)
    sizes, seconds = [], []
    for _ in range(4):
        report = run_retention_cycle(db, "maintenance", at, policy)
        runs = [r for r in report["retention"]["runs"] if r["kind"] == "payload"]
        sizes.append(sum(len(read(db, "SELECT windows FROM retention_runs WHERE run_id=%s",
                                  (r["run_id"],))[0][0]) for r in runs))
        seconds.append(report["seconds"])
    assert sizes == [8, 8, 8, 0]
    assert events(db) == [] and horizon(db) == 24 * 300
    assert max(seconds) < MAINTENANCE_BUDGET_SECONDS
