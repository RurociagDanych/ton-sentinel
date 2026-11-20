"""Unit contract: the sweeper closes windows only through canonical repair."""

from __future__ import annotations

import inspect

import pytest

from datetime import datetime, timezone

from ton_sentinel.contracts import Lease
from ton_sentinel.db.serving import WindowKey
from ton_sentinel.spark import stream
from ton_sentinel.spark.stream import Snapshot, SweepPolicy, classify_windows, sweep_windows

NOW = 10_000
ADMISSION = {"event_retention_seconds": 7 * 86400, "repair_margin_seconds": 0,
             "baseline_lookback_seconds": 86400, "derived_retention_seconds": 30 * 86400,
             "max_windows": 1000, "max_actions": 1_000_000, "max_input_bytes": 1_000_000_000,
             "max_baseline_rows": 1_000_000}
POLICY = SweepPolicy(idle_sweep_seconds=600, watermark_seconds=60, max_windows=10,
                     admission=ADMISSION)
K0, K1, K2, K3 = (WindowKey("testnet", s, s + 300) for s in (0, 300, 600, 900))
FP = "fp-a"


def snap(**kw):
    base = dict(canonical={}, served=frozenset(), candidates={}, closures={}, dirty=frozenset())
    base.update(kw)
    return Snapshot(**base)


def reasons(snapshot, now=NOW, policy=POLICY):
    return {k: r for k, r in classify_windows(snapshot, now, policy)}


def test_dirty_window_is_classified_even_without_canonical_actions():
    s = snap(dirty=frozenset({("testnet", 0)}), served=frozenset({("testnet", 0)}))
    assert reasons(s) == {K0: "dirty"}


def test_idle_window_waits_for_the_configured_sweep_and_never_uses_candidates():
    s = snap(canonical={("testnet", 0): (1, FP)})
    assert reasons(s, now=300 + 599) == {}
    assert reasons(s, now=300 + 600) == {K0: "idle"}


def test_evicted_window_needs_a_candidate_older_than_the_watermark():
    s = snap(canonical={("testnet", 0): (1, FP)}, candidates={("testnet", 0): 1})
    assert reasons(s, now=300 + 59) == {}
    assert reasons(s, now=300 + 60) == {K0: "evicted"}


def test_late_window_has_more_canonical_actions_than_the_candidate_or_a_moved_fingerprint():
    served = frozenset({("testnet", 0), ("testnet", 300)})
    s = snap(canonical={("testnet", 0): (2, FP), ("testnet", 300): (1, "fp-new")},
             candidates={("testnet", 0): 1, ("testnet", 300): 1}, served=served,
             closures={("testnet", 300): "fp-old"})
    assert reasons(s) == {K0: "late", K1: "late"}


def test_a_closed_window_is_not_repaired_twice_for_the_same_canonical_inputs():
    s = snap(canonical={("testnet", 0): (1, FP)}, served=frozenset({("testnet", 0)}),
             candidates={("testnet", 0): 5}, closures={("testnet", 0): FP})
    assert reasons(s) == {}


def test_open_or_unserved_matching_windows_are_left_alone():
    s = snap(canonical={("testnet", 9900): (1, FP)})
    assert reasons(s) == {}
    served_ok = snap(canonical={("testnet", 0): (1, FP)}, served=frozenset({("testnet", 0)}),
                     candidates={("testnet", 0): 1})
    assert reasons(served_ok) == {}


def test_a_served_window_that_was_never_closed_is_swept_as_idle_after_the_idle_sweep():
    # Windows served by the canonical stream (no fast-path candidate) used to be skipped
    # forever, so live windows never got a sweep closure (retention coverage stays unproven).
    s = snap(canonical={("testnet", 0): (1, FP)}, served=frozenset({("testnet", 0)}))
    assert reasons(s, now=300 + 599) == {}
    assert reasons(s, now=300 + 600) == {K0: "idle"}
    closed = snap(canonical={("testnet", 0): (1, FP)}, served=frozenset({("testnet", 0)}),
                  closures={("testnet", 0): FP})
    assert reasons(closed, now=300 + 600) == {}


def test_priority_and_deterministic_order_and_bound():
    s = snap(canonical={("testnet", 0): (1, FP), ("testnet", 300): (1, FP),
                        ("testnet", 600): (1, FP)},
             candidates={("testnet", 300): 1}, dirty=frozenset({("testnet", 600), ("testnet", 0)}))
    out = classify_windows(s, NOW, POLICY)
    assert out == [(K0, "dirty"), (K1, "evicted"), (K2, "dirty")]
    small = SweepPolicy(600, 60, 2, ADMISSION)
    assert classify_windows(s, NOW, small) == [(K0, "dirty"), (K1, "evicted")]


@pytest.mark.parametrize("kw", [
    dict(idle_sweep_seconds=0), dict(idle_sweep_seconds=-1), dict(watermark_seconds=0),
    dict(max_windows=0), dict(max_windows=10_001), dict(admission={"x": 1}),
    dict(idle_sweep_seconds=True)])
def test_policy_is_validated(kw):
    base = dict(idle_sweep_seconds=600, watermark_seconds=60, max_windows=10, admission=ADMISSION)
    base.update(kw)
    with pytest.raises(ValueError):
        SweepPolicy(**base)


class _Conn:
    class info:
        class transaction_status:
            name = "IDLE"

    def transaction(self):
        import contextlib
        return contextlib.nullcontext()


class _Admission:
    def __init__(self, key, admitted=True, status="admitted"):
        self.windows, self.admitted, self.status = (key,), admitted, status


def test_every_closure_goes_through_admit_then_repair_and_nothing_else(monkeypatch):
    calls = []
    lease = Lease("sweeper", 3, datetime(2030, 1, 1, tzinfo=timezone.utc))
    s = snap(canonical={("testnet", 0): (1, FP), ("testnet", 300): (1, FP)},
             dirty=frozenset({("testnet", 0)}), candidates={("testnet", 300): 1})
    monkeypatch.setattr(stream, "read_snapshot", lambda conn: s)

    def admit(conn, windows, now, policy):
        calls.append(("admit", windows, now, policy))
        return _Admission(windows[0], admitted=windows[0] != K1, status="unrepairable")

    def repair(conn, lse, spark, work, **kw):
        calls.append(("repair", work.windows, lse, kw["stage"] is not None))
        return object()

    monkeypatch.setattr(stream, "admit_work", admit)
    monkeypatch.setattr(stream, "repair_windows", repair)
    report = sweep_windows(_Conn(), lease, "spark", NOW, POLICY)
    assert [c[0] for c in calls] == ["admit", "repair", "admit"]
    assert calls[1] == ("repair", (K0,), lease, True)
    assert report.repaired == ((K0, "dirty"),)
    assert report.refused == ((K1, "unrepairable"),)


def _two_idle_windows(monkeypatch, failure):
    calls = []
    s = snap(canonical={("testnet", 0): (1, FP), ("testnet", 300): (1, FP)})
    monkeypatch.setattr(stream, "read_snapshot", lambda conn: s)
    monkeypatch.setattr(stream, "admit_work",
                        lambda conn, windows, now, policy: _Admission(windows[0]))

    def repair(conn, lse, spark, work, **kw):
        calls.append(work.windows)
        raise failure

    monkeypatch.setattr(stream, "repair_windows", repair)
    return calls


def test_a_lost_sweeper_lease_stops_the_sweep_instead_of_reporting_stale_pins(monkeypatch):
    from ton_sentinel.db.fencing import LeaseRejected

    lease = Lease("sweeper", 3, datetime(2030, 1, 1, tzinfo=timezone.utc))
    calls = _two_idle_windows(monkeypatch, LeaseRejected("lease expired"))
    with pytest.raises(LeaseRejected):
        sweep_windows(_Conn(), lease, "spark", NOW, POLICY)
    assert calls == [(K0,)]


def test_pins_that_keep_moving_are_still_reported_as_stale_pins(monkeypatch):
    lease = Lease("sweeper", 3, datetime(2030, 1, 1, tzinfo=timezone.utc))
    calls = _two_idle_windows(monkeypatch, RuntimeError("repair promotion stayed stale"))
    report = sweep_windows(_Conn(), lease, "spark", NOW, POLICY)
    assert calls == [(K0,), (K1,)]
    assert report.repaired == ()
    assert report.refused == ((K0, "stale_pins"), (K1, "stale_pins"))


def test_sweeper_source_never_writes_serving_or_candidate_tables_directly():
    source = inspect.getsource(stream.sweep_windows) + inspect.getsource(stream.read_snapshot)
    for forbidden in ("replace_features", "INSERT INTO served_features",
                      "UPDATE stream_candidate_windows", "INSERT INTO stream_candidate_windows",
                      "UPDATE dirty_windows", "promote("):
        assert forbidden not in source


def test_sweep_each_closes_every_due_window_under_its_own_fresh_lease(monkeypatch):
    # One sweep of several windows outlives the 300 s lease ceiling, so the production
    # monitor takes a fresh lease per window; a refused old window must not block newer ones.
    s = snap(canonical={("testnet", 0): (1, FP), ("testnet", 300): (1, FP)},
             served=frozenset({("testnet", 0), ("testnet", 300)}))
    monkeypatch.setattr(stream, "read_snapshot", lambda conn: s)
    leases, closed = [], []

    def acquire():
        lease = Lease("sweeper", len(leases) + 1, datetime(2030, 1, 1, tzinfo=timezone.utc))
        leases.append(lease)
        return lease

    def admit(conn, windows, now, policy):
        return _Admission(windows[0], admitted=windows[0] != K0, status="unrepairable")

    def repair(conn, lse, spark, work, **kw):
        closed.append((work.windows, lse.fence))
        return object()

    monkeypatch.setattr(stream, "admit_work", admit)
    monkeypatch.setattr(stream, "repair_windows", repair)
    report = stream.sweep_each(_Conn(), acquire, "spark", NOW, POLICY)
    assert report.refused == ((K0, "unrepairable"),)
    assert report.repaired == ((K1, "idle"),)
    assert closed == [((K1,), 2)] and len(leases) == 2
