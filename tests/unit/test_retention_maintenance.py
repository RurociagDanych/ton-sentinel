"""Bounded retention horizon arithmetic, policy and plan validation.

Pure decisions only; the real-PostgreSQL behaviour (deletion, atomic horizon, holds, guards,
rollback) is in tests/integration/test_retention.py. Expected values are written by hand.
"""
import pytest

from ton_sentinel import maintenance
from ton_sentinel.maintenance import (
    Step, apply_retention, hold_admission, next_horizon, plan_digest, retention_policy,
    whole_window_cutoff,
)

DAY = 86400
POLICY = {"event_retention_seconds": 7 * DAY, "guard_retention_seconds": 30 * DAY,
          "max_windows": 10}


class NoDatabase:
    """A connection that fails the test on any use: validation must happen first."""

    def __getattr__(self, name):
        pytest.fail(f"database touched before validation: {name}")


# ----------------------------------------------------------------------------- policy
def test_policy_names_exactly_the_retention_keys_without_defaults():
    assert retention_policy(dict(POLICY)) == (7 * DAY, 30 * DAY, 10)
    for broken in ({k: v for k, v in POLICY.items() if k != "max_windows"},
                   dict(POLICY, extra=1), [], None):
        with pytest.raises(ValueError, match="retention policy must name exactly"):
            retention_policy(broken)


@pytest.mark.parametrize("key,value", [
    ("event_retention_seconds", 0), ("event_retention_seconds", -1),
    ("event_retention_seconds", 1.5), ("guard_retention_seconds", True),
    ("max_windows", 0), ("max_windows", "10")])
def test_policy_values_are_positive_integers(key, value):
    with pytest.raises(ValueError, match=key):
        retention_policy(dict(POLICY, **{key: value}))


def test_guards_never_expire_before_the_payload_they_guard():
    with pytest.raises(ValueError, match="guard_retention_seconds"):
        retention_policy(dict(POLICY, guard_retention_seconds=7 * DAY - 1))
    assert retention_policy(dict(POLICY, guard_retention_seconds=7 * DAY))[1] == 7 * DAY


# ------------------------------------------------------------------------ cutoff window
def test_cutoff_counts_only_whole_windows_that_ended_before_it():
    assert whole_window_cutoff(7 * DAY + 600, 7 * DAY) == 600      # 0 and 300 expired
    assert whole_window_cutoff(7 * DAY + 599, 7 * DAY) == 300      # 300 straddles: kept
    assert whole_window_cutoff(7 * DAY + 299, 7 * DAY) == 0
    assert whole_window_cutoff(DAY, 7 * DAY) == 0                  # nothing expired yet


# ------------------------------------------------------------------------ next horizon
def test_horizon_advances_over_covered_windows_up_to_the_cutoff():
    assert next_horizon(0, 900, [0, 300, 600, 900, 1200], [], 10) == Step(
        to=900, windows=(0, 300, 600), blocked=None, bounded=False)


def test_first_blocker_stops_the_horizon_and_names_every_reason_there():
    blockers = [(600, 900, "pending_repair"), (300, 600, "coverage_not_proven"),
                (300, 1200, "admitted_work_hold")]
    assert next_horizon(0, 1500, [0, 300, 600], blockers, 10) == Step(
        to=300, windows=(0,), blocked=(300, ("admitted_work_hold", "coverage_not_proven")),
        bounded=False)


def test_blocker_straddling_the_current_horizon_blocks_at_once():
    assert next_horizon(600, 1500, [600, 900], [(0, 900, "admitted_work_hold")], 10) == Step(
        to=600, windows=(), blocked=(600, ("admitted_work_hold",)), bounded=False)


def test_blockers_below_the_horizon_or_at_or_after_the_cutoff_are_ignored():
    blockers = [(0, 600, "pending_repair"), (900, 1200, "coverage_not_proven")]
    assert next_horizon(600, 900, [600], blockers, 10) == Step(
        to=900, windows=(600,), blocked=None, bounded=False)


def test_bounded_run_stops_before_the_first_data_window_over_the_limit():
    assert next_horizon(0, 3000, [0, 300, 1200, 2400], [], 2) == Step(
        to=1200, windows=(0, 300), blocked=None, bounded=True)
    # empty windows between covered ones cost nothing; exactly the limit is not bounded
    assert next_horizon(0, 3000, [0, 2400], [], 2) == Step(
        to=3000, windows=(0, 2400), blocked=None, bounded=False)


def test_nothing_expired_keeps_the_horizon():
    assert next_horizon(600, 600, [600], [(600, 900, "pending_repair")], 10) == Step(
        to=600, windows=(), blocked=None, bounded=False)
    assert next_horizon(900, 600, [], [], 10) == Step(
        to=900, windows=(), blocked=None, bounded=False)


@pytest.mark.parametrize("args", [
    (1, 900, [], [], 1), (0, 901, [], [], 1), (0, 900, [301], [], 1),
    (-300, 900, [], [], 1), (0, 900, [], [], 0), (0, 900, [], [(300, 300, "x")], 1)])
def test_horizon_inputs_are_whole_windows(args):
    with pytest.raises(ValueError):
        next_horizon(*args)


# --------------------------------------------------------------------------- plans
def _plan():
    body = {"kind": "retention", "now": 7 * DAY + 600, "policy": dict(POLICY),
            "payload": [], "guards": []}
    return dict(body, digest=plan_digest(body))


def test_plan_digest_is_deterministic_and_covers_every_field():
    plan = _plan()
    body = {k: v for k, v in plan.items() if k != "digest"}
    assert plan_digest(body) == plan["digest"]
    assert plan["digest"].startswith("sha256:") and len(plan["digest"]) == 71
    assert plan_digest(dict(body, now=body["now"] + 1)) != plan["digest"]
    assert plan_digest(dict(body, payload=[{"network": "testnet"}])) != plan["digest"]


def test_apply_rejects_a_tampered_or_foreign_plan_before_the_database():
    plan = _plan()
    with pytest.raises(ValueError, match="plan"):
        apply_retention(NoDatabase(), None, dict(plan, now=plan["now"] + 1))
    with pytest.raises(ValueError, match="plan"):
        apply_retention(NoDatabase(), None, {"digest": plan["digest"]})
    with pytest.raises(ValueError, match="plan"):
        apply_retention(NoDatabase(), None, "not a plan")


def test_only_admitted_work_takes_a_retention_hold():
    with pytest.raises(ValueError, match="admitted"):
        hold_admission(NoDatabase(), None, "not an admission")


def test_reasons_are_a_closed_vocabulary():
    assert maintenance.PAYLOAD_REASONS == frozenset({
        "coverage_not_proven", "coverage_not_current", "pending_repair",
        "admitted_work_hold", "open_quarantine", "unacknowledged_delivery"})
    assert maintenance.GUARD_REASONS == frozenset({
        "coverage_not_proven", "coverage_not_current", "pending_repair",
        "admitted_work_hold", "open_quarantine"})
