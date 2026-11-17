"""Unit contract: whole-window admission checks retained context and resource bounds.

``admit_work`` is exercised against a scripted read-only connection that answers the queries
admission is allowed to make and fails on anything else (a write, or an unknown read).
Expected cutoffs, spans and usage are computed by hand in each test.
Integration counterpart (real PostgreSQL): tests/integration/test_retention.py.
"""
import contextlib
from datetime import datetime, timezone

import pytest

from ton_sentinel.contracts import Lease
from ton_sentinel.db.serving import WindowKey
from ton_sentinel.db.work import (
    Admission, admit_windows, admit_work, freeze_work, record_refusal,
)

DAY = 86400
NOW = 40 * DAY
POLICY = {"event_retention_seconds": 7 * DAY, "repair_margin_seconds": 0,
          "baseline_lookback_seconds": DAY, "derived_retention_seconds": 30 * DAY,
          "max_windows": 4, "max_actions": 10, "max_input_bytes": 1000,
          "max_baseline_rows": 50}
LEASE = Lease("repair", 7, datetime(2030, 1, 1, tzinfo=timezone.utc))
W = 33 * DAY                       # admissible: C_event = 33 days, C_repair = 33 days


def win(start, network="testnet"):
    return WindowKey(network, start, start + 300)


class _NoDatabase:
    def __getattr__(self, name):
        raise AssertionError(f"database touched: {name}")


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class ReadOnly:
    """Scripted read-only connection; any write or unexpected statement fails the test."""

    def __init__(self, *, detector_lookback=7 * DAY, horizons=(), inputs=None, baseline=0):
        self.detector_lookback = detector_lookback
        self.horizons = horizons
        self.inputs = inputs or {}
        self.baseline = baseline
        self.statements = []

    def transaction(self):
        return contextlib.nullcontext()

    def execute(self, sql, params=None):
        self.statements.append(sql)
        head = sql.lstrip().split(None, 1)[0].upper()
        if head not in ("SELECT", "WITH"):
            raise AssertionError("admission must not write: " + sql)
        if "FROM dependency_epoch" in sql:
            return _Rows([(3,)])
        if "FROM active_source_epoch" in sql:
            return _Rows([])
        if "active_detector_policy" in sql:
            return _Rows([(self.detector_lookback,)])
        if "FROM retained_context_horizons" in sql:
            return _Rows(list(self.horizons))
        if "octet_length" in sql:
            return _Rows([self.inputs.get(params["s"], (0, 0))])
        if "FROM served_features" in sql:
            return _Rows([(self.baseline,)])
        raise AssertionError("unexpected admission query: " + sql)


# ------------------------------------------------------------------------- policy contract
@pytest.mark.parametrize("missing", ["derived_retention_seconds", "max_windows", "max_actions",
                                     "max_input_bytes", "max_baseline_rows"])
def test_context_and_resource_budget_are_required_policy_keys(missing):
    policy = {k: v for k, v in POLICY.items() if k != missing}
    with pytest.raises(ValueError, match="admission policy must name exactly"):
        admit_work(_NoDatabase(), (win(W),), NOW, policy)


@pytest.mark.parametrize("name", ["derived_retention_seconds", "max_windows", "max_actions",
                                  "max_input_bytes", "max_baseline_rows"])
@pytest.mark.parametrize("value", [0, -1, True, 1.0, "5", None])
def test_budget_values_are_positive_integers_checked_before_the_database(name, value):
    with pytest.raises(ValueError):
        admit_work(_NoDatabase(), (win(W),), NOW, dict(POLICY, **{name: value}))


# ------------------------------------------------------------------------ whole admission
def test_whole_closed_window_with_context_and_budget_is_admitted_and_reports_usage():
    conn = ReadOnly(inputs={W: (3, 400)}, baseline=12)
    admission = admit_work(conn, (win(W),), NOW, POLICY)
    assert type(admission) is Admission
    assert (admission.admitted, admission.status, admission.reasons) == (True, "admitted", ())
    # leading context: the longer of the repair lookback (1 day) and the detector lookback (7)
    assert admission.lookback_seconds == 7 * DAY
    assert admission.baseline_spans == ((win(W), W - 7 * DAY, W),)
    assert admission.derived_cutoff == NOW - 30 * DAY
    assert admission.usage == (("actions", 3), ("baseline_rows", 12), ("input_bytes", 400),
                               ("windows", 1))
    assert admission.budget == (("actions", 10), ("baseline_rows", 50), ("input_bytes", 1000),
                                ("windows", 4))
    assert admission.digest.startswith("sha256:") and len(admission.digest) == 71


def test_usage_exactly_at_every_budget_limit_is_admitted():
    windows = tuple(win(W + 300 * i) for i in range(4))
    conn = ReadOnly(inputs={W: (10, 1000)}, baseline=50)
    admission = admit_work(conn, windows, NOW, POLICY)
    assert admission.admitted is True
    assert dict(admission.usage) == {"actions": 10, "input_bytes": 1000, "baseline_rows": 50,
                                     "windows": 4}


@pytest.mark.parametrize("limit,inputs,baseline,windows,reason", [
    ("max_actions", (11, 10), 0, 1, "over_budget:actions"),
    ("max_input_bytes", (1, 1001), 0, 1, "over_budget:input_bytes"),
    ("max_baseline_rows", (1, 10), 51, 1, "over_budget:baseline_rows"),
    ("max_windows", (1, 10), 0, 5, "over_budget:windows"),
])
def test_oversize_work_is_refused_as_a_whole_with_the_exceeded_bound(limit, inputs, baseline,
                                                                     windows, reason):
    targets = tuple(win(W + 300 * i) for i in range(windows))
    conn = ReadOnly(inputs={W: inputs}, baseline=baseline)
    admission = admit_work(conn, targets, NOW, POLICY)
    assert (admission.admitted, admission.status) == (False, "unrepairable")
    # a set-level bound refuses every window of the unit: no window is admitted alone
    assert admission.reasons == tuple((key, reason) for key in targets)


def test_target_payload_older_than_the_retained_horizon_is_unrepairable():
    conn = ReadOnly(horizons=[("testnet", "canonical_payload", W + 300)])
    admission = admit_work(conn, (win(W), win(W + 300)), NOW, POLICY)
    assert (admission.admitted, admission.status) == (False, "unrepairable")
    assert admission.reasons == ((win(W), "payload_not_retained"),)
    assert admission.context_horizons == (("testnet", "canonical_payload", W + 300),)


def test_leading_baseline_context_not_retained_is_missing_and_refuses_the_unit():
    # derived features retained only from W - 6 days; window W needs W - 7 days (detector)
    conn = ReadOnly(horizons=[("testnet", "derived_features", W - 6 * DAY)])
    admission = admit_work(conn, (win(W),), NOW, POLICY)
    assert (admission.admitted, admission.status) == (False, "missing")
    assert admission.reasons == ((win(W), "baseline_context_not_retained"),)


def test_detector_lookback_is_a_context_dependency_even_when_repair_lookback_is_short():
    # repair lookback 1 day alone fits; the active detector needs 7 days of prior windows
    conn = ReadOnly(detector_lookback=DAY,
                    horizons=[("testnet", "derived_features", W - 6 * DAY)])
    assert admit_work(conn, (win(W),), NOW, POLICY).admitted is True
    conn = ReadOnly(detector_lookback=7 * DAY,
                    horizons=[("testnet", "derived_features", W - 6 * DAY)])
    assert admit_work(conn, (win(W),), NOW, POLICY).status == "missing"


def test_leading_context_older_than_derived_retention_is_missing_by_policy():
    # derived retention 30 days: cutoff = NOW - 30 days = 10 days; span of W' starts W' - 7 days
    start = 17 * DAY - 300                # span start 10 days - 300 s: one window too old
    admission = admit_work(ReadOnly(), (win(start),), NOW,
                           dict(POLICY, event_retention_seconds=24 * DAY))
    assert admission.reasons == ((win(start), "baseline_context_expired"),)
    assert admission.status == "missing"
    ok = admit_work(ReadOnly(), (win(start + 300),), NOW,
                    dict(POLICY, event_retention_seconds=24 * DAY))
    assert ok.admitted is True


def test_horizons_of_another_network_do_not_apply():
    conn = ReadOnly(horizons=[("mainnet", "canonical_payload", NOW)])
    assert admit_work(conn, (win(W),), NOW, POLICY).admitted is True


def test_unrepairable_outranks_missing_outranks_deferred():
    conn = ReadOnly(horizons=[("testnet", "derived_features", W)])
    # W: baseline missing; NOW - 200: open (deferred)
    assert admit_work(conn, (win(W), win(NOW - 200)), NOW, POLICY).status == "missing"
    conn = ReadOnly(horizons=[("testnet", "derived_features", W),
                              ("testnet", "canonical_payload", W + 300)])
    mixed = admit_work(conn, (win(W), win(W + 300 * 10)), NOW, POLICY)
    assert mixed.status == "unrepairable"
    assert mixed.reasons == ((win(W), "payload_not_retained"),
                             (win(W + 3000), "baseline_context_not_retained"))
    assert admit_work(ReadOnly(), (win(NOW - 200),), NOW, POLICY).status == "deferred"


def test_admission_digest_changes_with_any_decided_fact():
    base = admit_work(ReadOnly(), (win(W),), NOW, POLICY)
    assert base.digest == admit_work(ReadOnly(), (win(W),), NOW, POLICY).digest
    others = [
        admit_work(ReadOnly(), (win(W),), NOW + 300, POLICY),
        admit_work(ReadOnly(inputs={W: (1, 5)}), (win(W),), NOW, POLICY),
        admit_work(ReadOnly(baseline=1), (win(W),), NOW, POLICY),
        admit_work(ReadOnly(), (win(W),), NOW, dict(POLICY, max_actions=11)),
        admit_work(ReadOnly(horizons=[("testnet", "canonical_payload", 0)]), (win(W),), NOW,
                   POLICY),
    ]
    assert len({a.digest for a in others} | {base.digest}) == len(others) + 1


# ------------------------------------------------------------------- refusal is recorded
def test_record_refusal_rejects_non_refusals_before_the_database():
    admitted = admit_work(ReadOnly(), (win(W),), NOW, POLICY)
    deferred = admit_work(ReadOnly(), (win(NOW - 200),), NOW, POLICY)
    for value in (admitted, deferred, None, "unrepairable"):
        with pytest.raises(ValueError):
            record_refusal(_NoDatabase(), LEASE, value)


def test_admit_windows_validates_like_admit_work_before_the_database():
    with pytest.raises(ValueError):
        admit_windows(_NoDatabase(), LEASE, (win(W),), NOW, {"max_windows": 1})
    with pytest.raises(ValueError):
        admit_windows(_NoDatabase(), LEASE, [win(W)], NOW, POLICY)


def test_freeze_work_refuses_a_refused_admission_before_the_database():
    refused = admit_work(ReadOnly(inputs={W: (11, 1)}), (win(W),), NOW, POLICY)
    with pytest.raises(ValueError, match="not admitted"):
        freeze_work(_NoDatabase(), LEASE, refused)
