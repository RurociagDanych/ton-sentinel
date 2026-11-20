"""Admit_work(db, windows, now, policy) -> Admission (real PostgreSQL)."""

from datetime import datetime, timezone

import pytest

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.db.serving import WindowKey
from ton_sentinel.db.work import Admission, admit_work, freeze_work
from tests.integration.test_repair_fencing import (  # noqa: F401  (fixture + helpers)
    db, dirty, promotions, served, world_with_correction,
)

DAY = 86400
NOW = 10 * DAY
POLICY = {"event_retention_seconds": 7 * DAY, "repair_margin_seconds": 600,
          "baseline_lookback_seconds": 7 * DAY, "derived_retention_seconds": 30 * DAY,
          "max_windows": 1000, "max_actions": 1_000_000, "max_input_bytes": 1_000_000_000,
          "max_baseline_rows": 1_000_000}
# C_repair = ceil_to_window(259200 + 600) = 259800


def win(start, network="testnet"):
    return WindowKey(network, start, start + 300)


def state(db):
    db.commit()
    epoch = db.execute("SELECT epoch FROM dependency_epoch").fetchone()[0]
    db.commit()
    return served(db), dirty(db), promotions(db), epoch


def test_admits_whole_closed_window_at_repair_cutoff_and_pins_context(db):
    world_with_correction(db)
    before = state(db)
    admission = admit_work(db, (win(259800),), NOW, POLICY)
    assert isinstance(admission, Admission)
    assert admission.admitted is True
    assert admission.status == "admitted"
    assert admission.reasons == ()
    assert admission.windows == (win(259800),)
    assert (admission.now, admission.event_cutoff, admission.repair_cutoff) == (
        864000, 259200, 259800)
    assert admission.dependency_epoch == 0
    assert admission.active_epoch is not None and "synthetic:source" in admission.active_epoch
    assert admission.baseline_spans == ((win(259800), 259800 - 7 * DAY, 259800),)
    assert state(db) == before


def test_window_cutoffs_are_exact_at_their_boundaries(db):
    world_with_correction(db)
    margin = admit_work(db, (win(259500),), NOW, POLICY)
    assert (margin.admitted, margin.status) == (False, "unrepairable")
    assert margin.reasons == ((win(259500), "inside_repair_margin"),)
    expired = admit_work(db, (win(258900),), NOW, POLICY)
    assert expired.reasons == ((win(258900), "before_event_cutoff"),)
    # unaligned T: C_event = 259300, the straddling window [259200, 259500) is not whole
    straddle = admit_work(db, (win(259200),), NOW + 100, dict(POLICY, repair_margin_seconds=0))
    assert straddle.reasons == ((win(259200), "before_event_cutoff"),)
    first = admit_work(db, (win(259500),), NOW + 100, dict(POLICY, repair_margin_seconds=0))
    assert first.admitted is True and first.repair_cutoff == 259500


def test_open_window_is_deferred_and_closed_window_end_is_admitted(db):
    world_with_correction(db)
    open_ = admit_work(db, (win(NOW - 200),), NOW, POLICY)
    assert (open_.admitted, open_.status) == (False, "deferred")
    assert open_.reasons == ((win(NOW - 200), "window_open"),)
    closed = admit_work(db, (win(NOW - 300),), NOW, POLICY)
    assert closed.admitted is True
    aware = admit_work(db, (win(NOW - 300),), datetime.fromtimestamp(NOW, timezone.utc), POLICY)
    assert aware.admitted is True and aware.now == NOW


def test_time_move_is_admitted_on_both_sides_or_not_at_all_without_writes(db):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    decide_revision(db, lease, evidence, auth)
    db.commit()
    before = state(db)
    # old side (window 0) is far before C_event at T = NOW, new side would be admissible alone
    move = admit_work(db, (win(600), win(0)), 7 * DAY + 900, dict(POLICY, repair_margin_seconds=0))
    assert move.admitted is False and move.status == "unrepairable"
    assert move.reasons == ((win(0), "before_event_cutoff"), (win(600), "before_event_cutoff"))
    one_side = admit_work(db, (win(600), win(0)), 7 * DAY + 300,
                          dict(POLICY, repair_margin_seconds=0))
    assert one_side.admitted is False
    assert one_side.reasons == ((win(0), "before_event_cutoff"),)
    with pytest.raises(ValueError, match="not admitted"):
        freeze_work(db, lease, one_side)
    assert state(db) == before
    assert {r[3] for r in before[1]} == {"pending"}
    both = admit_work(db, (win(600), win(0)), 7 * DAY, dict(POLICY, repair_margin_seconds=0))
    assert both.admitted is True and both.windows == (win(0), win(600))
    db.commit()
    frozen = freeze_work(db, lease, both)
    assert frozen.targets == (win(0), win(600)) and frozen.epoch == both.dependency_epoch


def test_mixed_open_and_expired_windows_report_unrepairable_with_every_reason(db):
    world_with_correction(db)
    mixed = admit_work(db, (win(NOW), win(0)), NOW, POLICY)
    assert (mixed.admitted, mixed.status) == (False, "unrepairable")
    assert mixed.reasons == ((win(0), "before_event_cutoff"), (win(NOW), "window_open"))


@pytest.mark.parametrize("policy", [
    {"event_retention_seconds": 7 * DAY, "baseline_lookback_seconds": 7 * DAY},
    dict(POLICY, extra=1),
    dict(POLICY, repair_margin_seconds=-1),
    dict(POLICY, repair_margin_seconds=True),
    dict(POLICY, event_retention_seconds=0),
    dict(POLICY, baseline_lookback_seconds=301),
    "not-a-dict",
])
def test_policy_is_explicit_and_fails_closed(db, policy):
    with pytest.raises(ValueError):
        admit_work(db, (win(259800),), NOW, policy)


@pytest.mark.parametrize("windows", [
    (), (win(259800), win(259800)), (WindowKey("testnet", 259800, 260000),),
    (WindowKey("othernet", 259800, 260100),), [win(259800)],
])
def test_windows_must_be_a_nonempty_tuple_of_distinct_whole_windows(db, windows):
    with pytest.raises(ValueError):
        admit_work(db, windows, NOW, POLICY)


@pytest.mark.parametrize("now", [-1, True, 1.5, datetime(2030, 1, 1)])
def test_now_must_be_seconds_or_an_aware_timestamp(db, now):
    with pytest.raises(ValueError):
        admit_work(db, (win(259800),), now, POLICY)
