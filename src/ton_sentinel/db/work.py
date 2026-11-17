"""Frozen repair inputs and fenced, atomic replacement promotion.

``freeze_work`` pins, under the shared lease, everything a repair computation reads: the
current canonical actions of the target windows, the dependency epoch, the coverage rows, the
active source epoch (universe/config identity), the trailing-baseline feature rows and the
pending dirty rows. The result is an immutable, digest-checked value; a computation runs on it
outside any transaction. ``promote`` re-reads those pins inside one transaction that holds the
lease row and the dependency-epoch row, and replaces the windows only if nothing moved.
A stale, superseded or repeated promotion returns ``False`` and writes nothing.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field

from ton_sentinel.contracts import (
    GRID_DIRECTION, BaselineInput, DetectorPolicy, GridCell, Lease, RevisionPolicy,
    baseline_span, grid_cell_state,
)
from ton_sentinel.contracts import WindowKey as DirtyKey
from ton_sentinel.db.fencing import LeaseRejected, validate_transition
from ton_sentinel.db.serving import WindowKey, _validated, replace_features

_ROW_FIELDS = ("targets", "inputs", "coverage", "baseline", "dirty")


def _tuples(rows) -> tuple:
    return tuple(tuple(row) for row in rows)


def _digest(owner, fence, epoch, targets, inputs, coverage, active_epoch, baseline, dirty,
            lookback) -> str:
    body = json.dumps(
        [owner, fence, epoch, [[t.network, t.window_start, t.window_end] for t in targets],
         inputs, coverage, active_epoch, baseline, dirty, lookback],
        sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(body.encode("ascii")).hexdigest()


@dataclass(frozen=True)
class FrozenWork:
    """Immutable pins of one repair computation. Build it with ``freeze_work``."""

    work_id: str
    owner: str
    fence: int
    epoch: int
    targets: tuple
    inputs: tuple          # (event_id, accepted_generation, semantic_digest, action json text)
    coverage: tuple
    active_epoch: str | None
    baseline: tuple
    dirty: tuple           # (network, token, window_start, role, dirty_generation)
    lookback: int
    input_digest: str = field(default="")

    def __post_init__(self):
        for name in _ROW_FIELDS:
            if type(getattr(self, name)) is not tuple:
                raise ValueError(f"{name} must be a tuple")
        if self._compute() != self.input_digest:
            raise ValueError("frozen work digest does not match its pinned inputs")

    def _compute(self) -> str:
        return _digest(self.owner, self.fence, self.epoch, self.targets, self.inputs,
                       self.coverage, self.active_epoch, self.baseline, self.dirty, self.lookback)

    @classmethod
    def create(cls, *, owner, fence, epoch, targets, inputs, coverage, active_epoch, baseline,
               dirty, lookback) -> "FrozenWork":
        if isinstance(active_epoch, dict):
            active_epoch = json.dumps(active_epoch, sort_keys=True, separators=(",", ":"))
        parts = dict(owner=owner, fence=fence, epoch=epoch, targets=tuple(targets),
                     inputs=_tuples(inputs), coverage=_tuples(coverage),
                     active_epoch=active_epoch, baseline=_tuples(baseline),
                     dirty=_tuples(dirty), lookback=lookback)
        return cls(work_id=str(uuid.uuid4()), input_digest=_digest(**parts), **parts)


def _targets(windows) -> tuple:
    keys = []
    for key in windows:
        if (type(key) is not WindowKey or key.network not in ("mainnet", "testnet")
                or type(key.window_start) is not int or type(key.window_end) is not int
                or key.window_start < 0 or key.window_end != key.window_start + 300):
            raise ValueError("repair targets must be valid five-minute WindowKey values")
        keys.append(key)
    if not keys:
        raise ValueError("repair needs at least one target window")
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate repair target window")
    return tuple(sorted(keys))


def _snapshot(connection, lease: Lease, targets: tuple, lookback: int) -> FrozenWork:
    """Read every pin. Caller holds the lease row lock inside one transaction."""
    epoch = connection.execute("SELECT epoch FROM dependency_epoch WHERE singleton").fetchone()[0]
    inputs, coverage, baseline, dirty = [], [], [], []
    for network in sorted({t.network for t in targets}):
        mine = [t for t in targets if t.network == network]
        starts = [t.window_start for t in mine]
        for key in mine:
            inputs.extend(connection.execute(
                "SELECT event_id,accepted_generation,semantic_digest,action::text FROM ("
                " SELECT event_id,accepted_generation,semantic_digest,action FROM canonical_current"
                "  WHERE network=%(n)s AND disposition='active' AND NOT eligibility_suppressed"
                " UNION ALL"
                " SELECT ca.event_id,ca.accepted_generation,ca.semantic_digest,ca.action"
                "  FROM canonical_actions ca WHERE ca.action->'identity'->>'network'=%(n)s"
                "  AND NOT EXISTS (SELECT 1 FROM canonical_current cc WHERE cc.event_id=ca.event_id)"
                ") cur WHERE (action->>'action_end')::bigint >= %(s)s"
                " AND (action->>'action_end')::bigint < %(e)s ORDER BY event_id COLLATE \"C\"",
                {"n": network, "s": key.window_start, "e": key.window_end}).fetchall())
            coverage.extend(connection.execute(
                "SELECT network,window_start,window_end,status,eligible_zero,reason,lineage::text,"
                "epoch::text FROM action_window_coverage WHERE network=%s AND window_start < %s "
                "AND window_end > %s ORDER BY window_start,window_end",
                (network, key.window_end, key.window_start)).fetchall())
            dirty.extend(connection.execute(
                "SELECT network,token,window_start,role,dirty_generation FROM dirty_windows "
                "WHERE network=%s AND window_start=%s AND role='target' AND status='pending' "
                "ORDER BY token COLLATE \"C\"", (network, key.window_start)).fetchall())
        baseline.extend(connection.execute(
            "SELECT network,window_start,window_end,token,generation,swaps,input_raw::text,"
            "output_raw::text,known_traders,unknown_count,coverage_status,status,provisional,"
            "write_fence FROM served_features WHERE network=%s AND window_start >= %s "
            "AND window_start < %s AND NOT (window_start = ANY(%s)) "
            "ORDER BY window_start,token COLLATE \"C\"",
            (network, min(starts) - lookback, max(t.window_end for t in mine), starts)).fetchall())
    active = connection.execute(
        "SELECT epoch::text FROM active_source_epoch WHERE singleton").fetchone()
    return FrozenWork.create(
        owner=lease.owner, fence=lease.fence, epoch=epoch, targets=targets, inputs=inputs,
        coverage=coverage, active_epoch=None if active is None else active[0],
        baseline=baseline, dirty=dirty, lookback=lookback)


_ADMISSION_POLICY_KEYS = frozenset({
    "event_retention_seconds", "repair_margin_seconds", "baseline_lookback_seconds",
    "derived_retention_seconds", "max_windows", "max_actions", "max_input_bytes",
    "max_baseline_rows"})
_BUDGET_KEYS = (("actions", "max_actions"), ("baseline_rows", "max_baseline_rows"),
                ("input_bytes", "max_input_bytes"), ("windows", "max_windows"))
_UNREPAIRABLE = frozenset({"before_event_cutoff", "inside_repair_margin",
                           "payload_not_retained"})
_MISSING = frozenset({"baseline_context_expired", "baseline_context_not_retained"})
_REFUSED = ("missing", "unrepairable")


@dataclass(frozen=True)
class Admission:
    """Outcome of ``admit_work``: whole-window repair admission plus its context pins.

    ``admitted`` is all-or-nothing over ``windows`` (both sides of a time move together).
    ``status`` is ``admitted``; ``deferred`` (only open windows); ``missing`` (leading
    baseline context is no longer retained); or ``unrepairable`` (a window is before the
    event cutoff, inside the repair margin, its payload is no longer retained, or the unit
    exceeds a resource bound). ``unrepairable`` outranks ``missing`` outranks ``deferred``.
    ``reasons`` holds sorted ``(WindowKey, reason)`` pairs; a set-level resource bound
    (``over_budget:<dimension>``) is reported for every window of the unit.
    """

    admitted: bool
    status: str
    reasons: tuple
    windows: tuple
    now: int
    event_cutoff: int
    repair_cutoff: int
    dependency_epoch: int
    active_epoch: str | None
    baseline_spans: tuple
    derived_cutoff: int = 0
    lookback_seconds: int = 0
    context_horizons: tuple = ()     # (network, data_class, retained_from) read at admission
    usage: tuple = ()                # sorted (dimension, value) actually measured
    budget: tuple = ()               # sorted (dimension, limit) from the explicit policy

    @property
    def digest(self) -> str:
        body = json.dumps(
            [self.admitted, self.status, [[k.network, k.window_start, r] for k, r in self.reasons],
             [[k.network, k.window_start] for k in self.windows], self.now, self.event_cutoff,
             self.repair_cutoff, self.dependency_epoch, self.active_epoch,
             [[k.network, k.window_start, a, b] for k, a, b in self.baseline_spans],
             self.derived_cutoff, self.lookback_seconds, [list(h) for h in self.context_horizons],
             [list(u) for u in self.usage], [list(b) for b in self.budget]],
            sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return "sha256:" + hashlib.sha256(body.encode("ascii")).hexdigest()


def _seconds(name: str, value, *, positive: bool, window: bool = False) -> int:
    if type(value) is not int or value < 0 or (positive and value == 0):
        raise ValueError(f"{name} must be a {'positive' if positive else 'nonnegative'} integer")
    if window and value % 300:
        raise ValueError(f"{name} must be whole five-minute windows")
    return value


def _admission_now(now) -> int:
    from datetime import datetime

    if isinstance(now, datetime):
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("admission time must be timezone-aware")
        return int(now.timestamp())
    return _seconds("admission time", now, positive=False)


def _status(reasons) -> str:
    names = {reason for _, reason in reasons}
    if not names:
        return "admitted"
    if names & _UNREPAIRABLE or any(n.startswith("over_budget:") for n in names):
        return "unrepairable"
    if names & _MISSING:
        return "missing"
    return "deferred"


_INPUT_SIZE_SQL = (
    "SELECT count(*), coalesce(sum(octet_length(action::text)), 0) FROM ("
    " SELECT action FROM canonical_current"
    "  WHERE network=%(n)s AND disposition='active' AND NOT eligibility_suppressed"
    " UNION ALL"
    " SELECT ca.action FROM canonical_actions ca WHERE ca.action->'identity'->>'network'=%(n)s"
    "  AND NOT EXISTS (SELECT 1 FROM canonical_current cc WHERE cc.event_id=ca.event_id)"
    ") cur WHERE (action->>'action_end')::bigint >= %(s)s"
    " AND (action->>'action_end')::bigint < %(e)s")


def admit_work(connection, windows, now, policy) -> Admission:
    """Decide whether repair of ``windows`` may start at ``now``; never writes.

    Spec section 9 clock contract on the action-end clock: ``C_event = now - retention``;
    ``C_repair = ceil_to_window(C_event + repair_margin)``. A window is admitted only if it
    is whole and closed (``window_end <= now``), starts at or after ``C_repair``, its
    payload is still retained, and its leading baseline context (the longer of the repair
    lookback and the active detector lookback, clipped at time 0) is inside the derived
    retention and still retained (``retained_context_horizons``). The unit must also fit
    the explicit resource budget (windows, canonical input actions and payload bytes, and
    leading baseline feature rows), measured on the current data. If anything fails, nothing
    is admitted. ``policy`` must name every value explicitly; there are no defaults.
    """
    if type(windows) is not tuple:
        raise ValueError("admission windows must be a tuple of WindowKey values")
    targets = _targets(windows)
    if type(policy) is not dict or set(policy) != _ADMISSION_POLICY_KEYS:
        raise ValueError("admission policy must name exactly: "
                         + ", ".join(sorted(_ADMISSION_POLICY_KEYS)))
    retention = _seconds("event_retention_seconds", policy["event_retention_seconds"],
                         positive=True)
    margin = _seconds("repair_margin_seconds", policy["repair_margin_seconds"], positive=False)
    lookback = _seconds("baseline_lookback_seconds", policy["baseline_lookback_seconds"],
                        positive=True, window=True)
    derived = _seconds("derived_retention_seconds", policy["derived_retention_seconds"],
                       positive=True)
    budget = tuple((dimension, _seconds(key, policy[key], positive=True))
                   for dimension, key in _BUDGET_KEYS)
    at = _admission_now(now)
    event_cutoff = at - retention
    repair_cutoff = -(-(event_cutoff + margin) // 300) * 300
    derived_cutoff = at - derived
    with connection.transaction():          # read-only; a savepoint inside a caller's tx
        dependency_epoch = connection.execute(
            "SELECT epoch FROM dependency_epoch WHERE singleton").fetchone()[0]
        active = connection.execute(
            "SELECT epoch::text FROM active_source_epoch WHERE singleton").fetchone()
        detector_lookback = connection.execute(
            "SELECT (p.body->>'lookback_seconds')::bigint FROM active_detector_policy a "
            "JOIN detector_policies p USING (policy_id) WHERE a.singleton").fetchone()[0]
        networks = {t.network for t in targets}
        horizons = tuple(sorted(
            (row[0], row[1], row[2]) for row in connection.execute(
                "SELECT network, data_class, retained_from FROM retained_context_horizons"
            ).fetchall() if row[0] in networks))
        span = max(lookback, detector_lookback)
        actions = size = rows = 0
        for network in sorted(networks):
            mine = [t for t in targets if t.network == network]
            for key in mine:
                count, octets = connection.execute(
                    _INPUT_SIZE_SQL,
                    {"n": network, "s": key.window_start, "e": key.window_end}).fetchone()
                actions, size = actions + count, size + octets
            rows += connection.execute(
                "SELECT count(*) FROM served_features WHERE network=%s AND window_start >= %s "
                "AND window_start < %s AND NOT (window_start = ANY(%s))",
                (network, min(t.window_start for t in mine) - span,
                 max(t.window_end for t in mine), [t.window_start for t in mine])).fetchone()[0]
    retained = {(n, c): v for n, c, v in horizons}
    reasons = []
    for key in targets:
        leading = max(0, key.window_start - span)
        payload_from = retained.get((key.network, "canonical_payload"))
        derived_from = retained.get((key.network, "derived_features"))
        if key.window_start < event_cutoff:
            reasons.append((key, "before_event_cutoff"))
        elif key.window_start < repair_cutoff:
            reasons.append((key, "inside_repair_margin"))
        elif key.window_end > at:
            reasons.append((key, "window_open"))
        elif payload_from is not None and key.window_start < payload_from:
            reasons.append((key, "payload_not_retained"))
        elif leading < derived_cutoff:
            reasons.append((key, "baseline_context_expired"))
        elif derived_from is not None and leading < derived_from:
            reasons.append((key, "baseline_context_not_retained"))
    usage = (("actions", actions), ("baseline_rows", rows), ("input_bytes", size),
             ("windows", len(targets)))
    for (dimension, used), (_, limit) in zip(usage, budget):
        if used > limit:
            reasons.extend((key, "over_budget:" + dimension) for key in targets)
    reasons.sort(key=lambda item: (item[0], item[1]))
    return Admission(
        admitted=not reasons, status=_status(reasons), reasons=tuple(reasons), windows=targets,
        now=at, event_cutoff=event_cutoff, repair_cutoff=repair_cutoff,
        dependency_epoch=dependency_epoch, active_epoch=None if active is None else active[0],
        baseline_spans=tuple((key, key.window_start - span, key.window_start)
                             for key in targets),
        derived_cutoff=derived_cutoff, lookback_seconds=span, context_horizons=horizons,
        usage=usage, budget=budget)


def record_refusal(connection, lease: Lease, admission: Admission) -> bool:
    """Durably record a ``missing``/``unrepairable`` admission, one row per window.

    The only write is the immutable ``window_admission_refusals`` audit row(s), under the
    lease; no work state changes (dirty rows stay pending). Recording the same decision
    again writes nothing and returns ``False``.
    """
    if type(admission) is not Admission or admission.status not in _REFUSED:
        raise ValueError("only a missing or unrepairable Admission is recorded")
    _idle(connection, "record_refusal")
    digest = admission.digest
    work = json.dumps([[k.network, k.window_start, k.window_end] for k in admission.windows])
    written = False
    with connection.transaction():
        validate_transition(connection, lease)
        for key in admission.windows:
            own = [reason for k, reason in admission.reasons if k == key]
            status = _status([(key, r) for r in own])
            if status not in _REFUSED:
                status = admission.status
            written |= connection.execute(
                "INSERT INTO window_admission_refusals(admission_digest,network,window_start,"
                "window_end,status,reasons,work_windows,decided_at,event_cutoff,repair_cutoff,"
                "dependency_epoch,usage,budget,lease_owner,lease_fence) VALUES "
                "(%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s) "
                "ON CONFLICT DO NOTHING RETURNING 1",
                (digest, key.network, key.window_start, key.window_end, status,
                 json.dumps(own or ["work_unit_refused"]), work, admission.now,
                 admission.event_cutoff, admission.repair_cutoff, admission.dependency_epoch,
                 json.dumps(dict(admission.usage)), json.dumps(dict(admission.budget)),
                 lease.owner, lease.fence)).fetchone() is not None
    return written


def admit_windows(connection, lease: Lease, windows, now, policy) -> Admission:
    """``admit_work`` plus the durable record of a ``missing``/``unrepairable`` refusal."""
    admission = admit_work(connection, windows, now, policy)
    if admission.status in _REFUSED:
        record_refusal(connection, lease, admission)
    return admission


def _within_budget(frozen: "FrozenWork", admission: Admission) -> None:
    limits = dict(admission.budget)
    used = {"actions": len(frozen.inputs), "baseline_rows": len(frozen.baseline),
            "input_bytes": sum(len(row[3].encode("utf-8")) for row in frozen.inputs),
            "windows": len(frozen.targets)}
    over = sorted(name for name, value in used.items() if value > limits[name])
    if over:
        raise ValueError("frozen work exceeds the admitted resource budget: " + ", ".join(over))


def freeze_work(connection, lease: Lease, windows, *,
                policy: RevisionPolicy | None = None) -> FrozenWork:
    """Freeze the inputs of a repair of ``windows``; raises ``LeaseRejected`` if not current.

    ``windows`` is a window sequence or an ``Admission``; a refused admission freezes nothing.
    Work frozen from an ``Admission`` must still fit its admitted resource budget (inputs can
    grow between admission and freezing); otherwise ``ValueError`` and nothing is returned.
    """
    admission = None
    if type(windows) is Admission:
        if not windows.admitted:
            raise ValueError("work not admitted: " + windows.status)
        admission, windows = windows, windows.windows
    targets = _targets(windows)
    policy = RevisionPolicy() if policy is None else policy
    if type(policy) is not RevisionPolicy:
        raise ValueError("RevisionPolicy required")
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("freeze_work requires an idle connection")
    with connection.transaction():
        validate_transition(connection, lease)
        frozen = _snapshot(connection, lease, targets, policy.baseline_lookback_seconds)
    if admission is not None:
        _within_budget(frozen, admission)
    return frozen


@dataclass(frozen=True)
class Replacement:
    """Feature rows computed from one ``FrozenWork`` (and only from it)."""

    work_id: str
    input_digest: str
    rows: tuple


def compute_replacement(spark, frozen: FrozenWork) -> Replacement:
    """Recompute the frozen target windows from the frozen pins, outside any transaction.

    Reads no database table: actions and coverage come from ``frozen``. A target window
    without a pinned coverage row of exactly its bounds is computed as ``unknown``.
    """
    if type(frozen) is not FrozenWork:
        raise ValueError("FrozenWork required")
    from ton_sentinel.spark.batch import compute_features

    exact = {(row[0], row[1], row[2]): (row[3], bool(row[4])) for row in frozen.coverage}
    coverage = [(t.network, t.window_start, t.window_end,
                 *exact.get((t.network, t.window_start, t.window_end), ("unknown", False)))
                for t in frozen.targets]
    actions_df = spark.createDataFrame([(row[3],) for row in frozen.inputs],
                                       "action_json string")
    coverage_df = spark.createDataFrame(
        coverage, "network string, window_start long, window_end long, status string, "
                  "eligible_zero boolean")
    rows = compute_features(spark, actions_df, coverage_df, {"schema_version": 1}).collect()
    ordered = sorted((row.asDict() for row in rows),
                     key=lambda r: (r["network"], r["window_start"], r["token"]))
    return Replacement(frozen.work_id, frozen.input_digest, tuple(ordered))


def promote(connection, lease: Lease, frozen: FrozenWork, features, *,
            commit_check: Callable | None = None, stage: Callable | None = None,
            writer: Callable | None = None) -> bool:
    """Replace the frozen windows with ``features`` if the pins are still current.

    Returns ``True`` only when this call promoted. Stale lease, moved dependency epoch,
    changed coverage/universe/baseline/inputs or an already promoted work return ``False``
    with nothing written. ``features`` may be a ``Replacement`` of this frozen work.
    The same transaction admits to the token universe every token that first appears here
    with an eligible swap. ``stage(connection)`` runs inside the promotion
    transaction after the pin check and
    before the replacement (e.g. a caller's batch audit row), so it commits only together
    with the promotion. ``writer`` is the serving replacement function (default
    ``replace_features``). ``commit_check`` runs after every write, inside the transaction.
    """
    if type(frozen) is not FrozenWork:
        raise ValueError("FrozenWork required")
    if type(features) is Replacement:
        if (features.work_id, features.input_digest) != (frozen.work_id, frozen.input_digest):
            raise ValueError("replacement was computed from a different frozen work")
        features = features.rows
    rows = [_validated(row) for row in features]
    for row in rows:
        if WindowKey(row["network"], row["window_start"], row["window_end"]) not in frozen.targets:
            raise ValueError("feature rows include a window outside the frozen targets")
    if type(lease) is not Lease or (lease.owner, lease.fence) != (frozen.owner, frozen.fence):
        return False
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("promote requires an idle connection")
    with connection.transaction():
        try:
            validate_transition(connection, lease)
        except LeaseRejected:
            return False
        connection.execute("SELECT epoch FROM dependency_epoch WHERE singleton FOR UPDATE")
        if _snapshot(connection, lease, frozen.targets, frozen.lookback).input_digest \
                != frozen.input_digest:
            return False
        claimed = connection.execute(
            "INSERT INTO repair_promotions(work_id,owner,fence,dependency_epoch,input_digest,"
            "windows) VALUES (%s,%s,%s,%s,%s,%s::jsonb) ON CONFLICT (work_id) DO NOTHING "
            "RETURNING 1",
            (frozen.work_id, frozen.owner, frozen.fence, frozen.epoch, frozen.input_digest,
             json.dumps([[t.network, t.window_start, t.window_end] for t in frozen.targets]))
        ).fetchone()
        if claimed is None:
            return False
        if stage is not None:
            stage(connection)
        (replace_features if writer is None else writer)(
            connection, lease, rows, windows=frozen.targets)
        _admit_universe(connection, frozen.work_id, rows)
        for network, token, start, role, generation in frozen.dirty:
            connection.execute(
                "UPDATE dirty_windows SET status='done',updated_at=clock_timestamp() "
                "WHERE network=%s AND token=%s AND window_start=%s AND role=%s "
                "AND dirty_generation=%s AND status='pending'",
                (network, token, start, role, generation))
        if commit_check is not None:
            commit_check(connection)
    return True


def repair_windows(connection, lease: Lease, spark, work, *, max_attempts: int = 3,
                   max_actions: int | None = None, stage: Callable | None = None,
                   writer: Callable | None = None, compute: Callable | None = None,
                   policy: RevisionPolicy | None = None) -> Replacement:
    """Freeze, compute outside the slot and promote one bounded work unit.

    ``work`` is a window sequence or an admitted ``Admission``. A promotion refused because
    the pins moved is retried from fresh inputs, at most ``max_attempts`` times; a lost or
    expired lease raises ``LeaseRejected`` at once. Returns the promoted ``Replacement``.
    """
    if type(max_attempts) is not int or not 1 <= max_attempts <= 10:
        raise ValueError("max_attempts must be 1..10")
    compute = compute_replacement if compute is None else compute
    for _ in range(max_attempts):
        frozen = freeze_work(connection, lease, work, policy=policy)
        if max_actions is not None and len(frozen.inputs) > max_actions:
            raise ValueError("accepted ledger snapshot exceeds max_actions")
        result = compute(spark, frozen)
        if promote(connection, lease, frozen, result, stage=stage, writer=writer):
            return result
        with connection.transaction():
            validate_transition(connection, lease)       # LeaseRejected: stop, do not retry
    raise RuntimeError("repair promotion stayed stale after %d fresh attempts" % max_attempts)


# --------------------------------------------------------------------------------------------
# Data-derived token universe, qualified coverage, exact baseline grid and inputs.
def _admit_universe(connection, work_id: str, rows) -> None:
    """Admit, inside the promotion transaction, tokens first seen here with an eligible swap.

    Admission is immutable: the first canonical promotion fixes ``admitted_window_start``
    (the earliest window of that promotion with an eligible swap of the token).
    """
    first: dict = {}
    for row in rows:
        if row["swaps"] > 0:
            key = (row["network"], row["token"])
            first[key] = min(first.get(key, row["window_start"]), row["window_start"])
    for network, token in sorted(first):
        if connection.execute("SELECT 1 FROM token_universe WHERE network=%s AND token=%s",
                              (network, token)).fetchone() is not None:
            continue
        version = connection.execute(
            "UPDATE token_universe_version SET version=version+1 WHERE singleton "
            "RETURNING version").fetchone()[0]
        connection.execute(
            "INSERT INTO token_universe(network,token,admitted_window_start,universe_version,"
            "admitted_work_id) VALUES (%s,%s,%s,%s,%s)",
            (network, token, first[(network, token)], version, work_id))


def _idle(connection, name: str) -> None:
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError(f"{name} requires an idle connection")


def _grid_scope(network, token, window_start) -> None:
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    if type(token) is not str or not token or token != token.strip():
        raise ValueError("token must be nonempty text")
    if type(window_start) is not int or window_start < 0 or window_start % 300:
        raise ValueError("window_start must be a five-minute UTC boundary")


_ORIGINS = (None, "live", "recorded", "synthetic")


def record_qualification(connection, lease: Lease, *, network: str, window_start: int,
                         mode: str, mapping_version: str, delta_seconds: int, trace_ranges,
                         source_epoch: dict, synthetic: bool, certification=None,
                         policy: RevisionPolicy | None = None, origin: str | None = None) -> bool:
    """Record qualified coverage of one whole action window; returns ``False`` if identical
    evidence already exists, raises ``ValueError`` on conflicting or invalid evidence.

    The evidence must name the active source epoch. Recording it is a coverage change
    (spec 7.3 step 4): in the same transaction it advances the dependency epoch (in-flight
    frozen work becomes stale) and dirties the ``baseline_dependents`` of every universe
    token admitted at or before the window. ``origin`` records where the observations
    came from: ``live``, ``recorded`` or ``synthetic``; ``None`` leaves it unrecorded.
    """
    from ton_sentinel.config import qualify_action_window

    if origin not in _ORIGINS:
        raise ValueError("origin must be live, recorded, synthetic or None")
    if origin is not None and (origin == "synthetic") != (synthetic is True):
        raise ValueError("synthetic evidence has origin synthetic and nothing else")

    _grid_scope(network, "-", window_start)
    if type(mapping_version) is not str or not mapping_version.strip():
        raise ValueError("mapping_version must be nonempty text")
    if type(source_epoch) is not dict or not source_epoch:
        raise ValueError("source_epoch must be the active source epoch object")
    evidence = qualify_action_window(
        window_start=window_start, delta_seconds=delta_seconds, trace_ranges=trace_ranges,
        mode=mode, synthetic=synthetic, certification=certification)
    policy = RevisionPolicy() if policy is None else policy
    if type(policy) is not RevisionPolicy:
        raise ValueError("RevisionPolicy required")
    _idle(connection, "record_qualification")
    wanted = (evidence["mode"], mapping_version, evidence["delta_seconds"],
              evidence["trace_ranges"], source_epoch, evidence["synthetic"],
              evidence["certification"], origin)
    with connection.transaction():
        validate_transition(connection, lease)
        active = connection.execute(
            "SELECT epoch FROM active_source_epoch WHERE singleton FOR SHARE").fetchone()
        if active is None or active[0] != source_epoch:
            raise ValueError("qualification evidence is not for the active source epoch")
        existing = connection.execute(
            "SELECT mode,mapping_version,delta_seconds,trace_ranges,source_epoch,synthetic,"
            "certification,origin FROM action_window_qualification "
            "WHERE network=%s AND window_start=%s",
            (network, window_start)).fetchone()
        if existing is not None:
            if tuple(existing) == wanted:
                return False
            raise ValueError("conflicting qualification for an already qualified window")
        epoch = connection.execute(
            "UPDATE dependency_epoch SET epoch=epoch+1 WHERE singleton RETURNING epoch"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO action_window_qualification(network,window_start,window_end,mode,"
            "mapping_version,delta_seconds,trace_ranges,source_epoch,synthetic,certification,"
            "dependency_epoch,lease_owner,lease_fence,origin) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s::jsonb,%s,%s,%s,%s)",
            (network, window_start, evidence["window_end"], evidence["mode"], mapping_version,
             evidence["delta_seconds"], json.dumps(evidence["trace_ranges"]),
             json.dumps(source_epoch), evidence["synthetic"],
             None if certification is None else json.dumps(certification), epoch,
             lease.owner, lease.fence, origin))
        tokens = connection.execute(
            "SELECT token FROM token_universe WHERE network=%s AND admitted_window_start<=%s "
            "ORDER BY token COLLATE \"C\"", (network, window_start)).fetchall()
        for (token,) in tokens:
            key = DirtyKey(network, token, window_start)
            span = key.dependents_span(policy)
            connection.execute(
                "INSERT INTO dirty_windows(network,token,window_start,role,window_end,"
                "covers_start,covers_end,dependency_epoch,reason,cause_event_id) "
                "VALUES (%s,%s,%s,'baseline_dependents',%s,%s,%s,%s,'coverage_qualified',%s) "
                "ON CONFLICT (network,token,window_start,role) DO UPDATE SET "
                "dirty_generation=dirty_windows.dirty_generation+1,"
                "dependency_epoch=EXCLUDED.dependency_epoch,reason=EXCLUDED.reason,"
                "cause_event_id=EXCLUDED.cause_event_id,status='pending',"
                "updated_at=clock_timestamp()",
                (network, token, window_start, key.window_end, span[0], span[1], epoch,
                 f"coverage:{network}:{window_start}"))
    return True


def qualifiable_windows(start: int, end: int, *, as_of: int, delta_seconds: int) -> list:
    """Five-minute windows of ``[start, end)`` whose end plus ``delta`` lies at or before the
    observed source frontier ``as_of`` (spec 5.3 coverage of ``[s, e + delta)``)."""
    for name, value in (("start", start), ("end", end), ("as_of", as_of),
                        ("delta_seconds", delta_seconds)):
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a nonnegative integer")
    if start % 300 or end % 300 or start >= end:
        raise ValueError("start and end must be five-minute boundaries with start < end")
    return [s for s in range(start, end, 300) if s + 300 + delta_seconds <= as_of]


def qualify_live_windows(connection, lease: Lease, *, network: str, start: int, end: int,
                         as_of: int, delta_seconds: int, mapping_version: str,
                         origin: str = "live", policy: RevisionPolicy | None = None) -> list:
    """Record ``bounded_observed`` qualification for the windows of one accepted live range.

    Production caller of ``record_qualification``. A window is qualified only when an
    accepted, corroborated source range of the ACTIVE source epoch covers it and the source
    frontier read at fetch time (``as_of``) is at least ``delta_seconds`` past its end. The
    trace-completion evidence is the fetched range ``[start, as_of)``. Windows that already
    carry a qualification are left alone (evidence is immutable). Returns the newly qualified
    window starts. Never synthetic; the source epoch is read, never changed.
    """
    windows = qualifiable_windows(start, end, as_of=as_of, delta_seconds=delta_seconds)
    _idle(connection, "qualify_live_windows")
    with connection.transaction():
        active = connection.execute(
            "SELECT epoch FROM active_source_epoch WHERE singleton").fetchone()
        if active is None or active[0] is None:
            raise ValueError("no active source epoch to qualify against")
        epoch = active[0]
        todo = []
        for window_start in windows:
            if connection.execute(
                    "SELECT 1 FROM action_window_qualification WHERE network=%s "
                    "AND window_start=%s", (network, window_start)).fetchone() is not None:
                continue
            covered = connection.execute(
                "SELECT 1 FROM source_ranges WHERE status='accepted' AND corroborated "
                "AND epoch=%s::jsonb AND start_inclusive<=%s AND end_exclusive>=%s "
                "AND epoch->>'network'=%s LIMIT 1",
                (json.dumps(epoch), window_start, window_start + 300, network)).fetchone()
            if covered is not None:
                todo.append(window_start)
    done = []
    for window_start in todo:
        if record_qualification(
                connection, lease, network=network, window_start=window_start,
                mode="bounded_observed", mapping_version=mapping_version,
                delta_seconds=delta_seconds, trace_ranges=[[start, as_of]],
                source_epoch=epoch, synthetic=False, policy=policy, origin=origin):
            done.append(window_start)
    return done


_GRID_SQL = """
WITH g AS (SELECT generate_series(%(f)s::bigint, %(u)s::bigint, 300) AS ws),
feat AS (
    SELECT window_start AS ws, swaps, known_traders, unknown_count, generation, write_fence
    FROM served_features
    WHERE network=%(n)s AND token=%(t)s AND window_start BETWEEN %(f)s AND %(u)s),
wf AS (
    SELECT window_start AS ws, max(write_fence) AS mf FROM served_features
    WHERE network=%(n)s AND window_start BETWEEN %(f)s AND %(u)s GROUP BY window_start),
pbest AS (
    SELECT DISTINCT ON (ws) ws, fence, work_id FROM (
        SELECT (w->>1)::bigint AS ws, p.fence, p.work_id::text AS work_id, p.promoted_at
        FROM repair_promotions p CROSS JOIN LATERAL jsonb_array_elements(p.windows) w
        WHERE w->>0 = %(n)s AND (w->>1)::bigint BETWEEN %(f)s AND %(u)s) x
    ORDER BY ws, fence DESC, promoted_at DESC, work_id),
q AS (
    SELECT window_start AS ws, mode, mapping_version, synthetic
    FROM action_window_qualification
    WHERE network=%(n)s AND window_start BETWEEN %(f)s AND %(u)s
      AND source_epoch = (SELECT epoch FROM active_source_epoch WHERE singleton)),
d AS (
    SELECT DISTINCT window_start AS ws FROM dirty_windows
    WHERE network=%(n)s AND token=%(t)s AND role='target' AND status='pending'
      AND window_start BETWEEN %(f)s AND %(u)s),
touch AS (
    SELECT DISTINCT (action->>'action_end')::bigint / 300 * 300 AS ws FROM (
        SELECT action FROM canonical_current WHERE network=%(n)s AND disposition='active'
        UNION ALL
        SELECT ca.action FROM canonical_actions ca
        WHERE ca.action->'identity'->>'network'=%(n)s
          AND NOT EXISTS (SELECT 1 FROM canonical_current cc WHERE cc.event_id=ca.event_id)
    ) a
    WHERE (action->'input'->>'asset' = %(t)s OR action->'output'->>'asset' = %(t)s)
      AND (action->>'action_end')::bigint >= %(f)s
      AND (action->>'action_end')::bigint < %(u)s + 300)
SELECT g.ws, feat.swaps, feat.known_traders, feat.unknown_count, feat.generation,
       feat.write_fence, wf.mf, pbest.fence, pbest.work_id, q.mode, q.mapping_version,
       q.synthetic, d.ws IS NOT NULL, touch.ws IS NOT NULL
FROM g LEFT JOIN feat USING (ws) LEFT JOIN wf USING (ws) LEFT JOIN pbest USING (ws)
LEFT JOIN q USING (ws) LEFT JOIN d USING (ws) LEFT JOIN touch USING (ws)
ORDER BY g.ws
"""


def _grid_cell(network, token, admitted, fact) -> GridCell:
    (ws, swaps, known, unknown, generation, fence, max_fence, promo_fence, work_id, mode,
     mapping_version, synthetic, pending, touched) = fact
    has_row = swaps is not None
    canonical = promo_fence is not None and (max_fence is None or promo_fence >= max_fence)
    state = grid_cell_state(
        feature_swaps=swaps, canonical=canonical, qualified=mode is not None, pending=pending,
        touched=touched, in_universe=admitted is not None and ws >= admitted)
    values = (swaps, known, unknown) if has_row else (None, None, None)
    revision = f"feature:{generation}:{fence}:{work_id or '-'}" if has_row else None
    if state == "zero" and not has_row:
        values = (0, 0, 0)
        revision = (f"promotion:{work_id}" if canonical
                    else f"qualification:{mapping_version}")
    if state in ("missing", "pre_universe"):
        values, revision = (None, None, None), None
    eligible = state in ("observed", "zero")
    return GridCell(network, token, GRID_DIRECTION, ws, state, *values, revision,
                    mode if eligible else None, bool(synthetic) if eligible else False)


def _read_baseline(connection, network, token, window_start, policy) -> BaselineInput:
    """Read the grid inside the caller's transaction; writes nothing."""
    first, _ = baseline_span(policy, window_start)
    epoch = connection.execute("SELECT epoch FROM dependency_epoch WHERE singleton").fetchone()[0]
    active = connection.execute(
        "SELECT epoch::text FROM active_source_epoch WHERE singleton").fetchone()
    universe = connection.execute(
        "SELECT admitted_window_start, universe_version FROM token_universe "
        "WHERE network=%s AND token=%s", (network, token)).fetchone()
    admitted = None if universe is None else universe[0]
    facts = connection.execute(
        _GRID_SQL, {"n": network, "t": token, "f": first, "u": window_start}).fetchall()
    cells = [_grid_cell(network, token, admitted, fact) for fact in facts]
    return BaselineInput.create(
        network=network, token=token, window_start=window_start, policy=policy,
        universe_version=0 if universe is None else universe[1],
        admitted_window_start=admitted, dependency_epoch=epoch,
        active_epoch=None if active is None else active[0], current=cells[-1],
        cells=tuple(cells[:-1]))


def _policy(policy) -> DetectorPolicy:
    from ton_sentinel.config import DETECTOR_POLICY_V1

    policy = DETECTOR_POLICY_V1 if policy is None else policy
    if type(policy) is not DetectorPolicy:
        raise ValueError("DetectorPolicy required")
    return policy


def build_baseline_input(connection, network: str, token: str, window_start: int, *,
                         policy: DetectorPolicy | None = None) -> BaselineInput:
    """Build the exact qualified baseline input of one scored token window (read-only).

    All reads share one REPEATABLE READ snapshot, so the same database state always yields
    the same ``BaselineInput`` (and digest).
    """
    _grid_scope(network, token, window_start)
    policy = _policy(policy)
    _idle(connection, "build_baseline_input")
    with connection.transaction():
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        return _read_baseline(connection, network, token, window_start, policy)


def pin_baseline_input(connection, lease: Lease, baseline: BaselineInput, *,
                       policy: DetectorPolicy | None = None) -> bool:
    """Persist ``baseline`` if it is still exactly what the database yields now.

    Under the lease (which serializes every writer of grid inputs) the input is rebuilt and
    compared by digest; a moved input returns ``False`` and writes nothing. Pinning the same
    digest twice is a no-op that returns ``True``.
    """
    if type(baseline) is not BaselineInput:
        raise ValueError("BaselineInput required")
    policy = _policy(policy)
    if baseline.policy_id != policy.identity:
        raise ValueError("baseline input was built under a different detector policy")
    _idle(connection, "pin_baseline_input")
    with connection.transaction():
        validate_transition(connection, lease)
        connection.execute("SELECT epoch FROM dependency_epoch WHERE singleton FOR UPDATE")
        fresh = _read_baseline(connection, baseline.network, baseline.token,
                               baseline.window_start, policy)
        if fresh.digest != baseline.digest:
            return False
        _insert_pin(connection, lease, baseline)
    return True


def _insert_pin(connection, lease: Lease, baseline: BaselineInput) -> None:
    record = baseline.to_record()
    columns = sorted(record)
    values = [json.dumps(record[name]) if name in ("cells", "current_cell") else record[name]
              for name in columns]
    connection.execute(
        "INSERT INTO baseline_inputs(" + ",".join(columns) + ",lease_owner,lease_fence) "
        "VALUES (" + ",".join(["%s"] * len(columns)) + ",%s,%s) "
        "ON CONFLICT (input_digest) DO NOTHING", (*values, lease.owner, lease.fence))


def close_baseline_dependents(connection, lease: Lease, network: str, token: str,
                              window_start: int, dirty_generation: int) -> bool:
    """Mark one pending ``baseline_dependents`` row done, only when the closure rule holds.

    Rule (also enforced by a trigger from migration 021): the row's own target window is not
    pending, and every scored window in its covers span that was pinned before the change has
    a non-pending re-pin at or after the change's dependency epoch. Returns ``False`` without
    writing when the row is absent, of another generation, already done or not closable.
    """
    _grid_scope(network, token, window_start)
    if type(dirty_generation) is not int or dirty_generation < 1:
        raise ValueError("dirty_generation must be a positive integer")
    _idle(connection, "close_baseline_dependents")
    with connection.transaction():
        validate_transition(connection, lease)
        return _close_dependents_locked(connection, network, token, window_start,
                                        dirty_generation)


def _close_dependents_locked(connection, network: str, token: str, window_start: int,
                             dirty_generation: int) -> bool:
    """The closure rule of ``close_baseline_dependents`` inside the caller's lease
    transaction (score promotion calls it after its own re-pin)."""
    row = connection.execute(
        "SELECT dependency_epoch,covers_start,covers_end FROM dirty_windows "
        "WHERE network=%s AND token=%s AND window_start=%s AND role='baseline_dependents' "
        "AND status='pending' AND dirty_generation=%s FOR UPDATE",
        (network, token, window_start, dirty_generation)).fetchone()
    if row is None:
        return False
    epoch, covers_start, covers_end = row
    if connection.execute(
            "SELECT 1 FROM dirty_windows WHERE network=%s AND token=%s AND window_start=%s "
            "AND role='target' AND status='pending'",
            (network, token, window_start)).fetchone() is not None:
        return False
    stale = connection.execute(
        "SELECT 1 FROM baseline_inputs b WHERE b.network=%(n)s AND b.token=%(t)s "
        "AND b.window_start >= %(cs)s AND b.window_start < %(ce)s "
        "AND b.dependency_epoch < %(e)s AND NOT EXISTS (SELECT 1 FROM baseline_inputs r "
        " WHERE r.network=b.network AND r.token=b.token AND r.window_start=b.window_start "
        " AND r.dependency_epoch >= %(e)s AND r.status <> 'pending') LIMIT 1",
        {"n": network, "t": token, "cs": covers_start, "ce": covers_end, "e": epoch}
    ).fetchone()
    if stale is not None:
        return False
    connection.execute(
        "UPDATE dirty_windows SET status='done',updated_at=clock_timestamp() "
        "WHERE network=%s AND token=%s AND window_start=%s AND role='baseline_dependents' "
        "AND dirty_generation=%s AND status='pending'",
        (network, token, window_start, dirty_generation))
    return True


# --------------------------------------------------------------------------------------------
# Fenced score promotion, revision-fenced alert state and score recomputation.
#
# A score is computed by ``detectors.score_baseline`` from one ``BaselineInput`` outside any
# transaction (``score_window``). ``promote_scores`` then, in one transaction under the lease
# and the dependency-epoch row lock, rebuilds the input, refuses (``False``, nothing written)
# if it moved or the active detector policy changed, pins it, writes the next result revision
# of both detectors, moves the current pointer, records the alert transition and closes every
# ``baseline_dependents`` row the re-pin completes. Relevant input/config changes mark current
# results stale (and pend active alerts) inside their own transaction (migration 022
# triggers); ``rescore_stale`` recomputes them.
_DETECTOR_KEY = ("network", "token", "direction", "window_start", "detector_id")
_KEY_WHERE = " AND ".join(f"{name}=%s" for name in _DETECTOR_KEY)


def active_detector_policy(connection) -> DetectorPolicy:
    """The registered detector policy that score promotion currently accepts."""
    from ton_sentinel.config import detector_policy

    body = connection.execute(
        "SELECT p.body FROM active_detector_policy a JOIN detector_policies p "
        "USING (policy_id) WHERE a.singleton").fetchone()[0]
    return detector_policy(body)


def register_detector_policy(connection, lease: Lease, policy: DetectorPolicy) -> bool:
    """Register ``policy`` (immutable); ``False`` if that identity is already registered."""
    if type(policy) is not DetectorPolicy:
        raise ValueError("DetectorPolicy required")
    _idle(connection, "register_detector_policy")
    with connection.transaction():
        validate_transition(connection, lease)
        return connection.execute(
            "INSERT INTO detector_policies(policy_id,version,body) VALUES (%s,%s,%s::jsonb) "
            "ON CONFLICT (policy_id) DO NOTHING RETURNING 1",
            (policy.identity, policy.version, policy.to_json())).fetchone() is not None


def activate_detector_policy(connection, lease: Lease, policy: DetectorPolicy) -> bool:
    """Make a registered ``policy`` the active one (a config change).

    In the same transaction the dependency epoch advances and every current result becomes
    stale with its active alert pending (trigger). ``False`` if it is already active.
    """
    if type(policy) is not DetectorPolicy:
        raise ValueError("DetectorPolicy required")
    _idle(connection, "activate_detector_policy")
    with connection.transaction():
        validate_transition(connection, lease)
        if connection.execute("SELECT 1 FROM detector_policies WHERE policy_id=%s",
                              (policy.identity,)).fetchone() is None:
            raise ValueError("detector policy is not registered")
        current = connection.execute(
            "SELECT policy_id FROM active_detector_policy WHERE singleton FOR UPDATE"
        ).fetchone()[0]
        if current == policy.identity:
            return False
        connection.execute("UPDATE dependency_epoch SET epoch=epoch+1 WHERE singleton")
        connection.execute(
            "UPDATE active_detector_policy SET policy_id=%s,changed_at=clock_timestamp() "
            "WHERE singleton", (policy.identity,))
    return True


def score_window(connection, network: str, token: str, window_start: int) -> tuple:
    """Build the input of one scored window under the active policy and score it.

    Returns ``(BaselineInput, results)`` and writes nothing. The computation reads only the
    built input, so ``promote_scores`` can later check that input against the database.
    """
    from ton_sentinel.detectors import score_baseline

    _grid_scope(network, token, window_start)
    _idle(connection, "score_window")
    with connection.transaction():
        policy = active_detector_policy(connection)
    baseline = build_baseline_input(connection, network, token, window_start, policy=policy)
    return baseline, score_baseline(baseline, policy)


def _alert_transition(result: dict, latest_state: str | None) -> str | None:
    """Next alert state for a freshly promoted result; ``None`` writes no revision."""
    if result["triggered"]:
        return "active"
    if result["status"] == "pending":
        return "pending" if latest_state == "active" else None
    if latest_state in ("active", "pending"):
        return "retracted" if result["status"] == "scored" else "inactive"
    return None


def promote_scores(connection, lease: Lease, baseline: BaselineInput, results) -> bool:
    """Promote both detector results of ``baseline`` if their input is still current.

    ``results`` must equal ``score_baseline(baseline, active policy)`` (``ValueError``
    otherwise). Returns ``False`` with nothing written when the lease is lost, the active
    policy is not the input's, or the rebuilt input differs (the input moved). Re-promoting
    an unchanged, fresh input writes nothing and returns ``True``.
    """
    from ton_sentinel.config import detector_policy
    from ton_sentinel.detectors import score_baseline

    if type(baseline) is not BaselineInput:
        raise ValueError("BaselineInput required")
    if type(results) is not tuple or len(results) != 2:
        raise ValueError("results must be the two detector results of score_baseline")
    if type(lease) is not Lease:
        return False
    _idle(connection, "promote_scores")
    with connection.transaction():
        try:
            validate_transition(connection, lease)
        except LeaseRejected:
            return False
        connection.execute("SELECT epoch FROM dependency_epoch WHERE singleton FOR UPDATE")
        policy_id, body = connection.execute(
            "SELECT a.policy_id, p.body FROM active_detector_policy a "
            "JOIN detector_policies p USING (policy_id) WHERE a.singleton FOR SHARE OF a"
        ).fetchone()
        if baseline.policy_id != policy_id:
            return False
        policy = detector_policy(body)
        if tuple(results) != score_baseline(baseline, policy):
            raise ValueError("results were not computed by score_baseline from this input")
        fresh = _read_baseline(connection, baseline.network, baseline.token,
                               baseline.window_start, policy)
        if fresh.digest != baseline.digest:
            return False
        _insert_pin(connection, lease, baseline)
        for result in results:
            _promote_result(connection, lease, baseline, result)
        for start, generation in connection.execute(
                "SELECT window_start, dirty_generation FROM dirty_windows "
                "WHERE network=%s AND token=%s AND role='baseline_dependents' "
                "AND status='pending' AND covers_start <= %s AND %s < covers_end "
                "ORDER BY window_start FOR UPDATE",
                (baseline.network, baseline.token, baseline.window_start,
                 baseline.window_start)).fetchall():
            _close_dependents_locked(connection, baseline.network, baseline.token, start,
                                     generation)
    return True


def _promote_result(connection, lease: Lease, baseline: BaselineInput, result: dict) -> None:
    key = (baseline.network, baseline.token, baseline.direction, baseline.window_start,
           result["detector"])
    current = connection.execute(
        "SELECT c.result_revision, c.stale, r.input_digest FROM detector_current c "
        "JOIN detector_results r USING (network,token,direction,window_start,detector_id,"
        "result_revision) WHERE " + " AND ".join(f"c.{n}=%s" for n in _DETECTOR_KEY)
        + " FOR UPDATE OF c", key).fetchone()
    if current is not None and not current[1] and current[2] == baseline.digest:
        return                                     # unchanged, fresh input: nothing to do
    revision = connection.execute(
        "SELECT coalesce(max(result_revision), 0) + 1 FROM detector_results WHERE "
        + _KEY_WHERE, key).fetchone()[0]
    connection.execute(
        "INSERT INTO detector_results(network,token,direction,window_start,detector_id,"
        "result_revision,input_digest,policy_id,dependency_epoch,status,triggered,score,"
        "result,lease_owner,lease_fence) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
        "%s::numeric,%s::jsonb,%s,%s)",
        (*key, revision, baseline.digest, baseline.policy_id, baseline.dependency_epoch,
         result["status"], result["triggered"], result["score"],
         json.dumps(result, sort_keys=True), lease.owner, lease.fence))
    if current is None:
        connection.execute(
            "INSERT INTO detector_current(network,token,direction,window_start,detector_id,"
            "result_revision,lookback_seconds) VALUES (%s,%s,%s,%s,%s,%s,%s)",
            (*key, revision, baseline.lookback_seconds))
    else:
        connection.execute(
            "UPDATE detector_current SET result_revision=%s,lookback_seconds=%s,stale=false,"
            "stale_epoch=NULL,stale_reason=NULL,updated_at=clock_timestamp() WHERE "
            + _KEY_WHERE, (revision, baseline.lookback_seconds, *key))
    latest = connection.execute(
        "SELECT alert_revision, state, result_revision, input_digest FROM alert_revisions "
        "WHERE " + _KEY_WHERE + " ORDER BY alert_revision DESC LIMIT 1", key).fetchone()
    state = _alert_transition(result, None if latest is None else latest[1])
    if state is None:
        return
    if state == "pending":                         # keeps the invalidated result as lineage
        result_revision, digest = latest[2], latest[3]
    else:
        result_revision, digest = revision, baseline.digest
    connection.execute(
        "INSERT INTO alert_revisions(network,token,direction,window_start,detector_id,"
        "alert_revision,state,result_revision,input_digest,dependency_epoch,reason,"
        "lease_owner,lease_fence) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (*key, 1 if latest is None else latest[0] + 1, state, result_revision, digest,
         baseline.dependency_epoch,
         "rescored:" + result["status"] + (":triggered" if result["triggered"] else ""),
         lease.owner, lease.fence))


def rescore_window(connection, lease: Lease, network: str, token: str, window_start: int, *,
                   max_attempts: int = 3) -> tuple:
    """Score one window from a fresh input and promote it; a moved input is retried from
    fresh inputs at most ``max_attempts`` times. A lost lease raises ``LeaseRejected``."""
    if type(max_attempts) is not int or not 1 <= max_attempts <= 10:
        raise ValueError("max_attempts must be 1..10")
    for _ in range(max_attempts):
        baseline, results = score_window(connection, network, token, window_start)
        if promote_scores(connection, lease, baseline, results):
            return results
        with connection.transaction():
            validate_transition(connection, lease)          # LeaseRejected: stop, no retry
    raise RuntimeError("score promotion stayed stale after %d fresh attempts" % max_attempts)


def rescore_stale(connection, lease: Lease, *, network: str | None = None,
                  limit: int = 100) -> list:
    """Recompute up to ``limit`` stale scored windows (oldest first), then try to close every
    pending ``baseline_dependents`` row (``close_baseline_dependents``; rows whose span holds
    no pinned score close here). Returns the recomputed ``(network, token, window_start)``."""
    if network is not None and network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    if type(limit) is not int or not 1 <= limit <= 10000:
        raise ValueError("limit must be 1..10000")
    _idle(connection, "rescore_stale")
    with connection.transaction():
        keys = connection.execute(
            "SELECT DISTINCT network, window_start, token COLLATE \"C\" FROM detector_current "
            "WHERE stale AND (%(n)s::text IS NULL OR network=%(n)s) "
            "ORDER BY network, window_start, 3 LIMIT %(l)s",
            {"n": network, "l": limit}).fetchall()
    done = []
    for net, start, token in keys:
        rescore_window(connection, lease, net, token, start)
        done.append((net, token, start))
    with connection.transaction():
        pending = connection.execute(
            "SELECT network, token, window_start, dirty_generation FROM dirty_windows "
            "WHERE role='baseline_dependents' AND status='pending' "
            "AND (%(n)s::text IS NULL OR network=%(n)s) "
            "ORDER BY network, window_start, token COLLATE \"C\" LIMIT %(l)s",
            {"n": network, "l": limit}).fetchall()
    for net, token, start, generation in pending:
        close_baseline_dependents(connection, lease, net, token, start, generation)
    return done


_UNSCORED_WHERE = (
    "FROM served_features s WHERE (%(n)s::text IS NULL OR s.network=%(n)s) AND NOT EXISTS ("
    "SELECT 1 FROM detector_current c WHERE c.network=s.network AND c.token=s.token "
    "AND c.window_start=s.window_start)")


def _count(connection, sql: str, params: dict) -> int | None:
    """One bounded count in its own transaction; ``None`` when unreadable (never 0)."""
    import psycopg

    try:
        with connection.transaction():
            return connection.execute(sql, params).fetchone()[0]
    except psycopg.Error:
        return None


def score_pending(connection, lease: Lease, *, network: str | None, limit: int) -> dict:
    """Score windows that have no result yet, then rescore stale ones, at most ``limit`` windows.

    Served windows with no ``detector_current`` row are scored first (oldest first), then
    ``rescore_stale`` takes the remaining budget. Every write goes through ``promote_scores``
    (via ``rescore_window``); this function only reads. A lost lease raises ``LeaseRejected``
    and stops the run. Backlog counts are ``None`` when unreadable.
    """
    if network is not None and network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    if type(limit) is not int or not 1 <= limit <= 10000:
        raise ValueError("limit must be 1..10000")
    _idle(connection, "score_pending")
    scope = {"n": network, "l": limit}
    with connection.transaction():
        keys = connection.execute(
            "SELECT s.network, s.token, s.window_start " + _UNSCORED_WHERE
            + " ORDER BY s.window_start, s.network, s.token COLLATE \"C\" LIMIT %(l)s",
            scope).fetchall()
    first = refused = 0
    for net, token, start in keys:
        try:
            rescore_window(connection, lease, net, token, start)
            first += 1
        except LeaseRejected:
            raise
        except RuntimeError:                       # input kept moving: counted, left unscored
            refused += 1
    rescored = 0
    if limit - len(keys) > 0:
        rescored = len(rescore_stale(connection, lease, network=network, limit=limit - len(keys)))
    return {
        "first_scored": first,
        "rescored": rescored,
        "remaining_unscored": _count(connection, "SELECT count(*) " + _UNSCORED_WHERE, scope),
        "remaining_stale": _count(
            connection,
            "SELECT count(*) FROM (SELECT DISTINCT network, window_start, token FROM "
            "detector_current WHERE stale AND (%(n)s::text IS NULL OR network=%(n)s)) d", scope),
        "refused": refused,
    }
