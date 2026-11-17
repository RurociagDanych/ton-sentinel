"""Bounded retention and identity-guard retirement.

``plan_retention`` reads, never writes. ``apply_retention`` re-derives the plan inside one
transaction that holds the shared transition slot (the lease row), and only if the result is
unchanged deletes, in that same transaction: the expired canonical payload of proven-covered
windows (leaving compact identity guards) and the guards whose coverage is proven and whose
guard retention has passed. The retained horizon of each class advances in that transaction,
behind an audit run written before the first delete. Anything that fails rolls back all of it.

A window's data is *proven covered* when the sweeper's canonical closure
(``window_sweep_closures``) was taken on exactly the inputs that exist now: the current
canonical inputs for payload, the guarded identities for guards. Pending repair, an active
hold of admitted work, an open quarantine and (for payload) an unacknowledged delivery block
the window. Horizons move contiguously from the oldest window; the first blocked window stops
them, so nothing at or after it is touched.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from ton_sentinel.db.fencing import validate_transition

WINDOW = 300
POLICY_KEYS = frozenset({"event_retention_seconds", "guard_retention_seconds", "max_windows"})
PAYLOAD_REASONS = frozenset({
    "coverage_not_proven", "coverage_not_current", "pending_repair", "admitted_work_hold",
    "open_quarantine", "unacknowledged_delivery"})
GUARD_REASONS = PAYLOAD_REASONS - {"unacknowledged_delivery"}
_PLAN_KEYS = frozenset({"kind", "now", "policy", "payload", "guards", "digest"})
_NETWORKS = ("mainnet", "testnet")
_PAYLOAD_TABLES = ("canonical_actions", "canonical_current", "canonical_revisions", "outbox",
                   "scan_membership", "event_identity_guards")


# ------------------------------------------------------------------------- pure decisions
def retention_policy(policy) -> tuple:
    """``(event_retention_seconds, guard_retention_seconds, max_windows)``; no defaults."""
    if type(policy) is not dict or set(policy) != POLICY_KEYS:
        raise ValueError("retention policy must name exactly: " + ", ".join(sorted(POLICY_KEYS)))
    for key in sorted(POLICY_KEYS):
        if type(policy[key]) is not int or policy[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    if policy["guard_retention_seconds"] < policy["event_retention_seconds"]:
        raise ValueError("guard_retention_seconds must not be shorter than the payload retention")
    return (policy["event_retention_seconds"], policy["guard_retention_seconds"],
            policy["max_windows"])


def whole_window_cutoff(now: int, retention: int) -> int:
    """Exclusive end of the whole windows that ended at or before ``now - retention``."""
    return max(0, (now - retention) // WINDOW * WINDOW)


def _window(name: str, value) -> int:
    if type(value) is not int or value < 0 or value % WINDOW:
        raise ValueError(f"{name} must be a nonnegative whole five-minute window")
    return value


@dataclass(frozen=True)
class Step:
    to: int              # new horizon: oldest window still retained (exclusive deletion end)
    windows: tuple       # data windows in [start, to): the ones this step deletes
    blocked: tuple | None  # (window, sorted reasons) that stopped the horizon, if any
    bounded: bool        # stopped by max_windows, not by a blocker or the cutoff


def next_horizon(start, cutoff_window, data_windows, blockers, max_windows) -> Step:
    """Advance a horizon from ``start`` over whole windows before ``cutoff_window``.

    ``blockers`` are ``(from, until, reason)`` spans; the first window at or after ``start``
    inside any span stops the horizon there. At most ``max_windows`` data windows are covered
    per step (empty windows cost nothing).
    """
    start = _window("start", start)
    cutoff_window = _window("cutoff window", cutoff_window)
    if type(max_windows) is not int or max_windows <= 0:
        raise ValueError("max_windows must be a positive integer")
    data = sorted({_window("data window", w) for w in data_windows})
    spans = []
    for low, high, reason in blockers:
        _window("blocker start", low)
        if type(high) is not int or high <= low:
            raise ValueError("blocker span must end after it starts")
        spans.append((low, high, reason))
    if cutoff_window <= start:
        return Step(start, (), None, False)
    hits = [max(low, start) for low, high, _ in spans
            if high > start and max(low, start) < cutoff_window]
    blocked_at = min(hits) if hits else None
    limit = cutoff_window if blocked_at is None else blocked_at
    covered = [w for w in data if start <= w < limit]
    if len(covered) > max_windows:
        return Step(covered[max_windows], tuple(covered[:max_windows]), None, True)
    blocked = None
    if blocked_at is not None:
        blocked = (blocked_at, tuple(sorted({reason for low, high, reason in spans
                                             if low <= blocked_at < high})))
    return Step(limit, tuple(covered), blocked, False)


def plan_digest(body: dict) -> str:
    text = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return "sha256:" + hashlib.sha256(text.encode("ascii")).hexdigest()


def _checked_plan(plan) -> dict:
    if type(plan) is not dict or set(plan) != _PLAN_KEYS or plan.get("kind") != "retention":
        raise ValueError("not a retention plan from plan_retention")
    body = {k: v for k, v in plan.items() if k != "digest"}
    if plan_digest(body) != plan["digest"]:
        raise ValueError("retention plan digest does not match its content")
    return plan


def _idle(connection, name: str) -> None:
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError(f"{name} requires an idle connection")


def _now(now) -> int:
    from ton_sentinel.db.work import _admission_now

    return _admission_now(now)


# ------------------------------------------------------------------------- database reads
_PAYLOAD_ROWS_SQL = (
    "SELECT event_id, trace_id, w FROM ("
    " SELECT event_id, action->'identity'->>'trace_id' AS trace_id,"
    "  canonical_payload_window(action) AS w FROM canonical_actions"
    "  WHERE action->'identity'->>'network'=%(n)s"
    " UNION ALL SELECT event_id, trace_id, canonical_payload_window(action)"
    "  FROM canonical_current WHERE network=%(n)s"
    " UNION ALL SELECT event_id, action->'identity'->>'trace_id',"
    "  canonical_payload_window(action) FROM canonical_revisions"
    "  WHERE action->'identity'->>'network'=%(n)s) p")

# The sweeper's view of a window's current inputs (spark.stream.read_snapshot), one network.
_CURRENT_INPUTS_SQL = (
    "SELECT canonical_payload_window(action), event_id, accepted_generation, semantic_digest"
    " FROM (SELECT event_id, accepted_generation, semantic_digest, action FROM canonical_current"
    "  WHERE network=%(n)s AND disposition='active' AND NOT eligibility_suppressed"
    " UNION ALL SELECT ca.event_id, ca.accepted_generation, ca.semantic_digest, ca.action"
    "  FROM canonical_actions ca WHERE ca.action->'identity'->>'network'=%(n)s"
    "  AND NOT EXISTS (SELECT 1 FROM canonical_current cc WHERE cc.event_id=ca.event_id)) cur")


def _networks(connection) -> list:
    rows = connection.execute(
        "SELECT network FROM canonical_current"
        " UNION SELECT action->'identity'->>'network' FROM canonical_actions"
        " UNION SELECT action->'identity'->>'network' FROM canonical_revisions"
        " UNION SELECT network FROM retained_context_horizons WHERE data_class='canonical_payload'"
        " UNION SELECT network FROM event_identity_guards"
        " UNION SELECT network FROM identity_guard_horizons").fetchall()
    return sorted({row[0] for row in rows if row[0] in _NETWORKS})


def _horizon(connection, network: str) -> int:
    row = connection.execute(
        "SELECT retained_from FROM retained_context_horizons "
        "WHERE network=%s AND data_class='canonical_payload'", (network,)).fetchone()
    return 0 if row is None else row[0]


def _guard_horizon(connection, network: str) -> int:
    row = connection.execute("SELECT retired_before FROM identity_guard_horizons "
                             "WHERE network=%s", (network,)).fetchone()
    return 0 if row is None else row[0]


def _closures(connection, network: str) -> dict:
    return dict(connection.execute(
        "SELECT window_start, fingerprint FROM window_sweep_closures WHERE network=%s",
        (network,)).fetchall())


def _coverage_blockers(closures: dict, inputs: dict, windows) -> list:
    from ton_sentinel.spark.stream import fingerprint

    spans = []
    for w in sorted(set(windows)):
        if w not in closures:
            spans.append((w, w + WINDOW, "coverage_not_proven"))
        elif closures[w] != fingerprint(inputs.get(w, [])):
            spans.append((w, w + WINDOW, "coverage_not_current"))
    return spans


def _work_blockers(connection, network: str) -> list:
    spans = [(min(ws, cs), max(we, ce), "pending_repair") for ws, we, cs, ce in connection.execute(
        "SELECT window_start, window_end, covers_start, covers_end FROM dirty_windows "
        "WHERE network=%s AND status='pending'", (network,)).fetchall()]
    spans += [(max(0, low // WINDOW * WINDOW), high, "admitted_work_hold")
              for low, high in connection.execute(
                  "SELECT hold_from, hold_until FROM retention_holds "
                  "WHERE network=%s AND released_at IS NULL", (network,)).fetchall()]
    return spans


def _open_traces(connection, network: str) -> set:
    return {row[0] for row in connection.execute(
        "SELECT DISTINCT trace_id FROM revision_quarantine WHERE network=%s AND status='open'",
        (network,)).fetchall()}


def _section(network, start, cutoff, step: Step, events) -> dict:
    return {"network": network, "from": start, "to": step.to, "cutoff": cutoff,
            "windows": list(step.windows),
            "blocked": None if step.blocked is None else [step.blocked[0],
                                                          list(step.blocked[1])],
            "bounded": step.bounded, "events": sorted(events)}


def _payload_section(connection, network, at, retention, max_windows) -> dict:
    start = _horizon(connection, network)
    cutoff = whole_window_cutoff(at, retention)
    rows = connection.execute(_PAYLOAD_ROWS_SQL, {"n": network}).fetchall()
    windows_of, traces = {}, {}
    for event_id, trace_id, w in rows:
        windows_of.setdefault(event_id, set()).add(w)
        traces.setdefault(trace_id, set()).add(w)
    data = {w for ws in windows_of.values() for w in ws}
    inputs = {}
    for w, event_id, generation, digest in connection.execute(
            _CURRENT_INPUTS_SQL, {"n": network}).fetchall():
        inputs.setdefault(w, []).append((event_id, generation, digest))
    blockers = _coverage_blockers(_closures(connection, network), inputs,
                                  [w for w in data if start <= w < cutoff])
    blockers += _work_blockers(connection, network)
    for trace_id in _open_traces(connection, network) & set(traces):
        blockers += [(w, w + WINDOW, "open_quarantine") for w in traces[trace_id]]
    unacked = {row[0] for row in connection.execute(
        "SELECT event_id FROM outbox WHERE publication_state <> 'acknowledged'").fetchall()}
    for event_id in unacked & set(windows_of):
        blockers += [(w, w + WINDOW, "unacknowledged_delivery") for w in windows_of[event_id]]
    step = next_horizon(start, cutoff, data, blockers, max_windows)
    events = [e for e, ws in windows_of.items() if max(ws) < step.to] if step.to > start else []
    return _section(network, start, cutoff, step, events)


def _guard_section(connection, network, at, retention, max_windows) -> dict:
    start = _guard_horizon(connection, network)
    cutoff = whole_window_cutoff(at, retention)
    limit = max(start, min(cutoff, _horizon(connection, network)))   # payload gone first
    rows = connection.execute(
        "SELECT event_id, trace_id, window_start, last_window, windows, eligible, "
        "accepted_generation, semantic_digest FROM event_identity_guards WHERE network=%s",
        (network,)).fetchall()
    inputs, touched, traces = {}, set(), {}
    for event_id, trace_id, window_start, _, windows, eligible, generation, digest in rows:
        touched.update(windows)
        traces.setdefault(trace_id, set()).update(windows)
        if eligible:
            inputs.setdefault(window_start, []).append((event_id, generation, digest))
    blockers = _coverage_blockers(_closures(connection, network), inputs,
                                  [w for w in touched if start <= w < limit])
    blockers += _work_blockers(connection, network)
    for trace_id in _open_traces(connection, network) & set(traces):
        blockers += [(w, w + WINDOW, "open_quarantine") for w in traces[trace_id]]
    step = next_horizon(start, limit, [row[3] for row in rows], blockers, max_windows)
    events = [row[0] for row in rows if row[3] < step.to] if step.to > start else []
    return _section(network, start, cutoff, step, events)


def _compute(connection, at: int, policy: dict) -> dict:
    retention, guard_retention, max_windows = retention_policy(policy)
    networks = _networks(connection)
    body = {"kind": "retention", "now": at, "policy": dict(policy),
            "payload": [_payload_section(connection, n, at, retention, max_windows)
                        for n in networks],
            "guards": [_guard_section(connection, n, at, guard_retention, max_windows)
                       for n in networks]}
    return dict(body, digest=plan_digest(body))


def plan_retention(connection, now, policy: dict) -> dict:
    """Decide, without writing, what a retention run at ``now`` may delete."""
    retention_policy(policy)
    at = _now(now)
    with connection.transaction():          # read-only; a savepoint inside a caller's tx
        return _compute(connection, at, policy)


# ------------------------------------------------------------------------- the transition
def _run_id(plan_id: str, kind: str, network: str) -> str:
    return plan_digest({"run": [plan_id, kind, network]})


def _counts(connection, network: str) -> dict:
    by_event = ("SELECT count(*) FROM {t} x JOIN canonical_actions ca USING (event_id) "
                "WHERE ca.action->'identity'->>'network'=%s")
    return {
        "canonical_actions": connection.execute(
            "SELECT count(*) FROM canonical_actions WHERE action->'identity'->>'network'=%s",
            (network,)).fetchone()[0],
        "canonical_current": connection.execute(
            "SELECT count(*) FROM canonical_current WHERE network=%s", (network,)).fetchone()[0],
        "canonical_revisions": connection.execute(
            "SELECT count(*) FROM canonical_revisions WHERE action->'identity'->>'network'=%s",
            (network,)).fetchone()[0],
        "outbox": connection.execute(by_event.format(t="outbox"), (network,)).fetchone()[0],
        "scan_membership": connection.execute(
            by_event.format(t="scan_membership"), (network,)).fetchone()[0],
        "event_identity_guards": connection.execute(
            "SELECT count(*) FROM event_identity_guards WHERE network=%s",
            (network,)).fetchone()[0],
    }


def _open_run(connection, lease, plan, kind, section) -> str:
    run_id = _run_id(plan["digest"], kind, section["network"])
    connection.execute(
        "INSERT INTO retention_runs(run_id,kind,network,now_at,cutoff,from_window,to_window,"
        "windows,plan_digest,counts_before,lease_owner,lease_fence) VALUES "
        "(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s)",
        (run_id, kind, section["network"], plan["now"], section["cutoff"], section["from"],
         section["to"], json.dumps(section["windows"]), plan["digest"],
         json.dumps(_counts(connection, section["network"])), lease.owner, lease.fence))
    return run_id


def _close_run(connection, run_id: str, network: str, deleted: dict) -> None:
    connection.execute(
        "UPDATE retention_runs SET counts_after=%s::jsonb, deleted=%s::jsonb WHERE run_id=%s",
        (json.dumps(_counts(connection, network)), json.dumps(deleted), run_id))


def _guard_rows(connection, events) -> list:
    """Compact identity of each event about to lose its payload."""
    guards = []
    for event_id in events:
        current = connection.execute(
            "SELECT network, trace_id, action_id, accepted_generation, disposition, "
            "semantic_digest, source_revision, eligibility_suppressed, "
            "canonical_payload_window(action) FROM canonical_current WHERE event_id=%s",
            (event_id,)).fetchone()
        if current is None:
            current = connection.execute(
                "SELECT action->'identity'->>'network', action->'identity'->>'trace_id', "
                "action->'identity'->>'action_id', accepted_generation, 'active', "
                "semantic_digest, NULL::bigint, false, canonical_payload_window(action) "
                "FROM canonical_actions WHERE event_id=%s", (event_id,)).fetchone()
        history = connection.execute(
            "SELECT canonical_payload_window(action), semantic_digest FROM canonical_actions "
            "WHERE event_id=%(e)s UNION ALL SELECT canonical_payload_window(action), "
            "semantic_digest FROM canonical_revisions WHERE event_id=%(e)s",
            {"e": event_id}).fetchall()
        (network, trace_id, action_id, generation, disposition, digest, source_revision,
         suppressed, window_start) = current
        windows = sorted({window_start} | {w for w, _ in history})
        superseded = sorted({d for _, d in history} - {digest})
        guards.append((event_id, network, trace_id, action_id, window_start, max(windows),
                       json.dumps(windows), generation, disposition,
                       disposition == "active" and not suppressed, digest,
                       json.dumps(superseded), source_revision))
    return guards


def _delete_payload(connection, lease, plan, section) -> None:
    network, events = section["network"], section["events"]
    run_id = _open_run(connection, lease, plan, "payload", section)
    for guard in _guard_rows(connection, events):
        connection.execute(
            "INSERT INTO event_identity_guards(event_id,network,trace_id,action_id,window_start,"
            "last_window,windows,accepted_generation,disposition,eligible,semantic_digest,"
            "superseded_digests,source_revision,payload_run) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb,%s,%s)", (*guard, run_id))
    deleted = {}
    deleted["outbox"] = connection.execute(
        "DELETE FROM outbox WHERE event_id = ANY(%s) AND publication_state='acknowledged'",
        (events,)).rowcount
    if connection.execute("SELECT count(*) FROM outbox WHERE event_id = ANY(%s)",
                          (events,)).fetchone()[0]:
        raise RuntimeError("an unacknowledged delivery appeared under the transition slot")
    for table in ("scan_membership", "canonical_revisions", "canonical_current",
                  "canonical_actions"):
        deleted[table] = connection.execute(
            f"DELETE FROM {table} WHERE event_id = ANY(%s)", (events,)).rowcount
    connection.execute(
        "INSERT INTO retained_context_horizons(network,data_class,retained_from) "
        "VALUES (%s,'canonical_payload',%s) ON CONFLICT (network,data_class) DO UPDATE "
        "SET retained_from=EXCLUDED.retained_from, advanced_at=clock_timestamp()",
        (network, section["to"]))
    _close_run(connection, run_id, network, deleted)


def _retire_guards(connection, lease, plan, section) -> None:
    network = section["network"]
    run_id = _open_run(connection, lease, plan, "guards", section)
    deleted = {"event_identity_guards": connection.execute(
        "DELETE FROM event_identity_guards WHERE network=%s AND event_id = ANY(%s)",
        (network, section["events"])).rowcount}
    connection.execute(
        "INSERT INTO identity_guard_horizons(network,retired_before) VALUES (%s,%s) "
        "ON CONFLICT (network) DO UPDATE SET retired_before=EXCLUDED.retired_before, "
        "advanced_at=clock_timestamp()", (network, section["to"]))
    _close_run(connection, run_id, network, deleted)


def _runs(connection, run_ids) -> list:
    return [{"run_id": r[0], "kind": r[1], "network": r[2], "from": r[3], "to": r[4],
             "deleted": r[5]} for r in connection.execute(
        "SELECT run_id, kind, network, from_window, to_window, deleted FROM retention_runs "
        "WHERE run_id = ANY(%s) ORDER BY kind, network", (run_ids,)).fetchall()]


def apply_retention(connection, lease, plan: dict) -> dict:
    """Carry out ``plan`` under the lease, all in one transaction, or not at all.

    Returns ``{"status", "plan", "runs"}``: ``applied``; ``already_applied`` (a retry of a
    plan that committed: nothing more is deleted); ``nothing_to_delete``; or ``stale`` (the
    plan no longer matches the data once the transition slot is held: nothing is written).
    A stale or superseded lease raises ``LeaseRejected``; any failure rolls everything back.
    """
    plan = _checked_plan(plan)
    _idle(connection, "apply_retention")
    work = [("payload", s) for s in plan["payload"] if s["to"] > s["from"]]
    work += [("guards", s) for s in plan["guards"] if s["to"] > s["from"]]
    run_ids = [_run_id(plan["digest"], kind, s["network"]) for kind, s in work]
    with connection.transaction():
        validate_transition(connection, lease)          # the shared slot: serializes all work
        if not work:
            return {"status": "nothing_to_delete", "plan": plan["digest"], "runs": []}
        done = _runs(connection, run_ids)
        if done:
            if len(done) != len(run_ids):
                raise RuntimeError("retention plan was only partly recorded")
            return {"status": "already_applied", "plan": plan["digest"], "runs": done}
        if _compute(connection, plan["now"], plan["policy"])["digest"] != plan["digest"]:
            return {"status": "stale", "plan": plan["digest"], "runs": []}
        for kind, section in work:
            (_delete_payload if kind == "payload" else _retire_guards)(
                connection, lease, plan, section)
        return {"status": "applied", "plan": plan["digest"], "runs": _runs(connection, run_ids)}


# ------------------------------------------------------------------------- admitted work
def _admitted(admission):
    from ton_sentinel.db.work import Admission

    if type(admission) is not Admission or not admission.admitted:
        raise ValueError("only admitted work takes or releases a retention hold")
    return admission


def hold_admission(connection, lease, admission) -> tuple:
    """Hold, under the lease, the payload and leading context of admitted work.

    Re-checks under the transition slot that the admitted windows and their leading context
    are still retained (retention may have run since admission); raises ``ValueError``
    otherwise. Idempotent: returns ``((admission_digest, network), ...)``.
    """
    admission = _admitted(admission)
    _idle(connection, "hold_admission")
    digest, held = admission.digest, []
    with connection.transaction():
        validate_transition(connection, lease)
        for network in sorted({k.network for k in admission.windows}):
            mine = [k for k in admission.windows if k.network == network]
            first = min(k.window_start for k in mine)
            low = max(0, (first - admission.lookback_seconds) // WINDOW * WINDOW)
            derived = connection.execute(
                "SELECT retained_from FROM retained_context_horizons "
                "WHERE network=%s AND data_class='derived_features'", (network,)).fetchone()
            if (first < _horizon(connection, network)
                    or first < _guard_horizon(connection, network)
                    or (derived is not None and low < derived[0])):
                raise ValueError("admitted context is no longer retained; admit again")
            connection.execute(
                "INSERT INTO retention_holds(admission_digest,network,hold_from,hold_until,"
                "work_windows,lease_owner,lease_fence) VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s) "
                "ON CONFLICT (admission_digest,network) DO NOTHING",
                (digest, network, low, max(k.window_end for k in mine),
                 json.dumps([[k.network, k.window_start, k.window_end] for k in mine]),
                 lease.owner, lease.fence))
            held.append((digest, network))
    return tuple(held)


def release_hold(connection, lease, admission) -> bool:
    """Release the holds of finished admitted work; ``False`` if none was active."""
    admission = _admitted(admission)
    _idle(connection, "release_hold")
    with connection.transaction():
        validate_transition(connection, lease)
        return connection.execute(
            "UPDATE retention_holds SET released_at=clock_timestamp() "
            "WHERE admission_digest=%s AND released_at IS NULL RETURNING 1",
            (admission.digest,)).fetchone() is not None


# ============================================================================================
# Generic context holds, 30-day derived-history retention,
# late-delivery classification, storage/vacuum budgets and the fenced maintenance cycle.
# 
_DIGEST_PREFIX = "sha256:"


def _holder(holder) -> str:
    if (type(holder) is not str or not holder.startswith(_DIGEST_PREFIX)
            or len(holder) != 71 or any(c not in "0123456789abcdef" for c in holder[7:])):
        raise ValueError("a hold names its holder by a sha256 digest")
    return holder


def hold_context(connection, lease, holder: str, network: str, hold_from: int,
                 hold_until: int) -> bool:
    """Hold, under the lease, retained context ``[hold_from, hold_until)`` of ``network``
    for a holder that is not an admission (for example a recovery cut manifest digest).

    Retention never deletes payload, guards or derived history inside an active hold. A hold
    can never reach below the retired identity-guard boundary (nothing is retained there
    any more): ``ValueError``. Returns ``True`` when the hold is new, ``False`` when this
    holder already held it.
    """
    holder = _holder(holder)
    if network not in _NETWORKS:
        raise ValueError("network must be exactly mainnet or testnet")
    low, high = _window("hold_from", hold_from), _window("hold_until", hold_until)
    if high <= low:
        raise ValueError("a hold must end after it starts")
    _idle(connection, "hold_context")
    with connection.transaction():
        validate_transition(connection, lease)
        if low < _guard_horizon(connection, network):
            raise ValueError("held context is no longer retained")
        return connection.execute(
            "INSERT INTO retention_holds(admission_digest,network,hold_from,hold_until,"
            "work_windows,lease_owner,lease_fence) VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s) "
            "ON CONFLICT (admission_digest,network) DO NOTHING RETURNING 1",
            (holder, network, low, high, json.dumps([[network, low, high]]), lease.owner,
             lease.fence)).fetchone() is not None


def release_context(connection, lease, holder: str) -> bool:
    """Release every active hold of ``holder``; ``False`` if none was active."""
    holder = _holder(holder)
    _idle(connection, "release_context")
    with connection.transaction():
        validate_transition(connection, lease)
        return connection.execute(
            "UPDATE retention_holds SET released_at=clock_timestamp() "
            "WHERE admission_digest=%s AND released_at IS NULL RETURNING 1",
            (holder,)).fetchone() is not None


# ------------------------------------------------------------------ derived-history retention
DERIVED_POLICY_KEYS = frozenset({"derived_retention_seconds", "max_windows"})
DERIVED_REASONS = frozenset({"pending_repair", "admitted_work_hold"})
# Delete order follows the foreign keys: alerts and current pointers reference results,
# results reference their pinned baseline input; features are referenced by nothing.
_DERIVED_TABLES = ("alert_revisions", "detector_current", "detector_results", "baseline_inputs",
                   "served_features")
_DERIVED_PLAN_KEYS = frozenset({"kind", "now", "policy", "derived", "digest"})


def derived_policy(policy) -> tuple:
    """``(derived_retention_seconds, max_windows)``; no defaults."""
    if type(policy) is not dict or set(policy) != DERIVED_POLICY_KEYS:
        raise ValueError("derived retention policy must name exactly: "
                         + ", ".join(sorted(DERIVED_POLICY_KEYS)))
    for key in sorted(DERIVED_POLICY_KEYS):
        if type(policy[key]) is not int or policy[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    return policy["derived_retention_seconds"], policy["max_windows"]


def _derived_networks(connection) -> list:
    rows = connection.execute(
        "SELECT network FROM served_features UNION SELECT network FROM baseline_inputs"
        " UNION SELECT network FROM detector_results"
        " UNION SELECT network FROM retained_context_horizons"
        "  WHERE data_class='derived_features'").fetchall()
    return sorted({row[0] for row in rows if row[0] in _NETWORKS})


def _derived_horizon(connection, network: str) -> int:
    row = connection.execute(
        "SELECT retained_from FROM retained_context_horizons "
        "WHERE network=%s AND data_class='derived_features'", (network,)).fetchone()
    return 0 if row is None else row[0]


def _derived_blockers(connection, network: str) -> list:
    """Pending repair needs its target windows and their whole leading baseline context
    (active detector lookback); active holds keep their span."""
    lookback = connection.execute(
        "SELECT (p.body->>'lookback_seconds')::bigint FROM active_detector_policy a "
        "JOIN detector_policies p USING (policy_id) WHERE a.singleton").fetchone()[0]
    spans = [(max(0, min(ws, cs) - lookback) // WINDOW * WINDOW, max(we, ce), "pending_repair")
             for ws, we, cs, ce in connection.execute(
                 "SELECT window_start, window_end, covers_start, covers_end FROM dirty_windows "
                 "WHERE network=%s AND status='pending'", (network,)).fetchall()]
    spans += [(max(0, low // WINDOW * WINDOW), high, "admitted_work_hold")
              for low, high in connection.execute(
                  "SELECT hold_from, hold_until FROM retention_holds "
                  "WHERE network=%s AND released_at IS NULL", (network,)).fetchall()]
    return spans


def _derived_rows(connection, network: str, below: int | None = None) -> dict:
    out = {}
    for table in _DERIVED_TABLES:
        if below is None:
            sql, args = f"SELECT count(*) FROM {table} WHERE network=%s", (network,)
        else:
            sql = f"SELECT count(*) FROM {table} WHERE network=%s AND window_start < %s"
            args = (network, below)
        out[table] = connection.execute(sql, args).fetchone()[0]
    return out


def _derived_section(connection, network, at, retention, max_windows) -> dict:
    start = _derived_horizon(connection, network)
    cutoff = whole_window_cutoff(at, retention)
    limit = max(start, min(cutoff, _horizon(connection, network)))   # payload goes first
    data = {row[0] for row in connection.execute(
        "SELECT window_start FROM served_features WHERE network=%(n)s"
        " UNION SELECT window_start FROM baseline_inputs WHERE network=%(n)s"
        " UNION SELECT window_start FROM detector_results WHERE network=%(n)s",
        {"n": network}).fetchall()}
    step = next_horizon(start, limit, data, _derived_blockers(connection, network), max_windows)
    rows = (_derived_rows(connection, network, step.to) if step.to > start
            else {table: 0 for table in _DERIVED_TABLES})
    return {"network": network, "from": start, "to": step.to, "cutoff": cutoff,
            "windows": list(step.windows),
            "blocked": None if step.blocked is None else [step.blocked[0],
                                                          list(step.blocked[1])],
            "bounded": step.bounded, "rows": rows}


def _compute_derived(connection, at: int, policy: dict) -> dict:
    retention, max_windows = derived_policy(policy)
    body = {"kind": "derived_retention", "now": at, "policy": dict(policy),
            "derived": [_derived_section(connection, n, at, retention, max_windows)
                        for n in _derived_networks(connection)]}
    return dict(body, digest=plan_digest(body))


def plan_derived_retention(connection, now, policy: dict) -> dict:
    """Decide, without writing, which whole windows of derived history (served features,
    pinned baselines, detector results, alerts) a run at ``now`` may delete: older than the
    derived retention, already behind the payload horizon, outside every active hold and
    every pending repair's target and leading baseline context; contiguous and bounded."""
    derived_policy(policy)
    at = _now(now)
    with connection.transaction():
        return _compute_derived(connection, at, policy)


def _checked_derived_plan(plan) -> dict:
    if (type(plan) is not dict or set(plan) != _DERIVED_PLAN_KEYS
            or plan.get("kind") != "derived_retention"):
        raise ValueError("not a derived retention plan from plan_derived_retention")
    body = {k: v for k, v in plan.items() if k != "digest"}
    if plan_digest(body) != plan["digest"]:
        raise ValueError("derived retention plan digest does not match its content")
    return plan


def _delete_derived(connection, lease, plan, section) -> None:
    network, to = section["network"], section["to"]
    run_id = _run_id(plan["digest"], "derived", network)
    connection.execute(
        "INSERT INTO retention_runs(run_id,kind,network,now_at,cutoff,from_window,to_window,"
        "windows,plan_digest,counts_before,lease_owner,lease_fence) VALUES "
        "(%s,'derived',%s,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s)",
        (run_id, network, plan["now"], section["cutoff"], section["from"], to,
         json.dumps(section["windows"]), plan["digest"],
         json.dumps(_derived_rows(connection, network)), lease.owner, lease.fence))
    deleted = {table: connection.execute(
        f"DELETE FROM {table} WHERE network=%s AND window_start < %s", (network, to)).rowcount
        for table in _DERIVED_TABLES}
    connection.execute(
        "INSERT INTO retained_context_horizons(network,data_class,retained_from) "
        "VALUES (%s,'derived_features',%s) ON CONFLICT (network,data_class) DO UPDATE "
        "SET retained_from=EXCLUDED.retained_from, advanced_at=clock_timestamp()",
        (network, to))
    connection.execute(
        "UPDATE retention_runs SET counts_after=%s::jsonb, deleted=%s::jsonb WHERE run_id=%s",
        (json.dumps(_derived_rows(connection, network)), json.dumps(deleted), run_id))


def apply_derived_retention(connection, lease, plan: dict) -> dict:
    """Carry out a derived plan under the lease in one transaction, or not at all; same
    statuses as ``apply_retention``. Retained scores keep their pinned input (they are not
    invalidated); their recomputation is refused once their leading context is gone."""
    plan = _checked_derived_plan(plan)
    _idle(connection, "apply_derived_retention")
    work = [s for s in plan["derived"] if s["to"] > s["from"]]
    run_ids = [_run_id(plan["digest"], "derived", s["network"]) for s in work]
    with connection.transaction():
        validate_transition(connection, lease)
        if not work:
            return {"status": "nothing_to_delete", "plan": plan["digest"], "runs": []}
        done = _runs(connection, run_ids)
        if done:
            if len(done) != len(run_ids):
                raise RuntimeError("derived retention plan was only partly recorded")
            return {"status": "already_applied", "plan": plan["digest"], "runs": done}
        if _compute_derived(connection, plan["now"], plan["policy"])["digest"] \
                != plan["digest"]:
            return {"status": "stale", "plan": plan["digest"], "runs": []}
        for section in work:
            _delete_derived(connection, lease, plan, section)
        return {"status": "applied", "plan": plan["digest"], "runs": _runs(connection, run_ids)}


# ------------------------------------------------------------------------ late deliveries
DELIVERY_REASONS = frozenset({"identity_guarded", "stale_generation", "payload_expired",
                              "guard_retired"})


@dataclass(frozen=True)
class DeliveryAdmission:
    """Whether a delivery may enter the additive path; refused deliveries are backfill only."""

    event_id: str
    admitted: bool
    status: str
    reasons: tuple

    def __post_init__(self):
        if (type(self.reasons) is not tuple
                or any(r not in DELIVERY_REASONS for r in self.reasons)
                or self.admitted is not (not self.reasons)
                or self.status != ("admitted" if self.admitted else "backfill_only")):
            raise ValueError("inconsistent delivery admission")


def _expiry(starts, generation, payload_from, guard_retired_before, guard_generation) -> tuple:
    reasons = set()
    if guard_generation is not None:
        reasons.add("identity_guarded")
        if generation is not None and generation <= guard_generation:
            reasons.add("stale_generation")
    if any(s < payload_from for s in starts):
        reasons.add("payload_expired")
    if any(s < guard_retired_before for s in starts):
        reasons.add("guard_retired")
    return tuple(sorted(reasons))


def delivery_reasons(change, *, payload_from: int, guard_retired_before: int,
                     guard_generation) -> tuple:
    """Pure classification of a delivered change against the retained state."""
    from ton_sentinel.contracts import Change

    if type(change) is not Change or change.kind not in ("revision", "retraction"):
        raise ValueError("not a delivery: only admitted revisions/retractions are delivered")
    windows = change.old_windows + change.new_windows
    if not windows:
        raise ValueError("a delivery names its original contribution windows")
    return _expiry([w.window_start for w in windows], change.accepted_generation,
                   payload_from, guard_retired_before, guard_generation)


def _retained_identity(connection, event_id: str, network: str) -> tuple:
    guard = connection.execute(
        "SELECT coalesce(accepted_generation, 0) FROM event_identity_guards WHERE event_id=%s",
        (event_id,)).fetchone()
    return (_horizon(connection, network), _guard_horizon(connection, network),
            None if guard is None else guard[0])


def admit_delivery(connection, change) -> DeliveryAdmission:
    """Check a delivery before first-insert classification, without writing: a guarded
    identity, an old generation, an expired payload window or a window behind the retired
    guard boundary is never new (backfill only)."""
    delivery_reasons(change, payload_from=0, guard_retired_before=0, guard_generation=None)
    networks = {w.network for w in change.old_windows + change.new_windows}
    if len(networks) != 1:
        raise ValueError("a delivery belongs to exactly one network")
    (network,) = networks
    with connection.transaction():
        payload_from, guard_before, guard_generation = _retained_identity(
            connection, change.event_id, network)
    reasons = delivery_reasons(change, payload_from=payload_from,
                               guard_retired_before=guard_before,
                               guard_generation=guard_generation)
    return DeliveryAdmission(event_id=change.event_id, admitted=not reasons,
                             status="backfill_only" if reasons else "admitted",
                             reasons=reasons)


def late_delivery_reasons(connection, event_id: str, network: str, window_start: int) -> tuple:
    """``(reasons, payload_from, guard_retired_before)`` for a newly observed source action
    (no accepted generation yet), read inside the caller's transaction."""
    payload_from, guard_before, guard_generation = _retained_identity(
        connection, event_id, network)
    return (_expiry([window_start], None, payload_from, guard_before, guard_generation),
            payload_from, guard_before)


# ----------------------------------------------------------------- storage and vacuum budgets
CYCLE_POLICY_KEYS = frozenset({
    "event_retention_seconds", "guard_retention_seconds", "derived_retention_seconds",
    "max_windows", "vacuum_max_tables", "vacuum_max_bytes", "storage_budget_bytes",
    "min_free_percent"})
_STORAGE_TABLES = (
    "canonical_actions", "canonical_current", "canonical_revisions", "outbox", "scan_membership",
    "event_identity_guards", "action_observations", "served_features", "baseline_inputs",
    "detector_results", "detector_current", "alert_revisions", "dirty_windows",
    "retention_runs", "retention_holds", "delivery_quarantine", "window_sweep_closures")
VACUUM_MODE = "VACUUM (ANALYZE)"       # plain, concurrent-safe; never VACUUM FULL


def cycle_policy(policy) -> dict:
    """Validate the full maintenance-cycle policy (no defaults) and split it."""
    if type(policy) is not dict or set(policy) != CYCLE_POLICY_KEYS:
        raise ValueError("maintenance cycle policy must name exactly: "
                         + ", ".join(sorted(CYCLE_POLICY_KEYS)))
    for key in sorted(CYCLE_POLICY_KEYS):
        if type(policy[key]) is not int or policy[key] <= 0:
            raise ValueError(f"{key} must be a positive integer")
    if policy["min_free_percent"] >= 100:
        raise ValueError("min_free_percent must be below 100")
    if policy["derived_retention_seconds"] < policy["event_retention_seconds"]:
        raise ValueError("derived_retention_seconds must not be shorter than the payload "
                         "retention")
    retention = {k: policy[k] for k in POLICY_KEYS}
    retention_policy(retention)
    return {"retention": retention,
            "derived": {k: policy[k] for k in DERIVED_POLICY_KEYS},
            "vacuum_max_tables": policy["vacuum_max_tables"],
            "vacuum_max_bytes": policy["vacuum_max_bytes"],
            "storage_budget_bytes": policy["storage_budget_bytes"],
            "min_free_percent": policy["min_free_percent"]}


def storage_pressure(used_bytes: int, budget_bytes: int, min_free_percent: int) -> dict:
    """Low water when less than ``min_free_percent`` of the data budget is free."""
    from decimal import ROUND_FLOOR, Decimal

    if type(used_bytes) is not int or used_bytes < 0:
        raise ValueError("used_bytes must be a nonnegative integer")
    if type(budget_bytes) is not int or budget_bytes <= 0:
        raise ValueError("budget_bytes must be a positive integer")
    if type(min_free_percent) is not int or not 0 < min_free_percent < 100:
        raise ValueError("min_free_percent must be an integer in 1..99")
    free = max(0, budget_bytes - used_bytes)
    percent = (Decimal(free) * 100 / Decimal(budget_bytes)).quantize(
        Decimal("0.01"), rounding=ROUND_FLOOR)
    return {"used_bytes": used_bytes, "budget_bytes": budget_bytes, "free_bytes": free,
            "free_percent": str(percent), "min_free_percent": min_free_percent,
            "status": "ok" if free * 100 >= min_free_percent * budget_bytes else "low_water"}


def choose_vacuum(tables: dict, *, max_tables: int, max_bytes: int) -> tuple:
    """Tables to vacuum this cycle: most dead tuples first, at most ``max_tables`` tables
    whose total relation size fits ``max_bytes``. Returns ``(chosen, skipped_reasons)``."""
    for name, value in (("max_tables", max_tables), ("max_bytes", max_bytes)):
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    chosen, skipped, used = [], {}, 0
    for name in sorted(tables, key=lambda t: (-tables[t]["dead_tuples"], t)):
        info = tables[name]
        if info["dead_tuples"] <= 0:
            skipped[name] = "no_dead_tuples"
        elif len(chosen) >= max_tables:
            skipped[name] = "over_table_budget"
        elif used + info["bytes"] > max_bytes:
            skipped[name] = "over_byte_budget"
        else:
            chosen.append(name)
            used += info["bytes"]
    return chosen, skipped


def storage_report(connection) -> dict:
    """Physical storage of the application schema, reported apart from logical row counts:
    total relation bytes (heap, indexes, TOAST), live/dead tuples per retention table and
    the cluster WAL position. Statistics are flushed first so dead tuples are current."""
    _idle(connection, "storage_report")
    connection.execute("SELECT pg_stat_force_next_flush()")
    connection.commit()
    with connection.transaction():
        connection.execute("SET LOCAL stats_fetch_consistency = 'none'")
        schema_bytes = connection.execute(
            "SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0)::bigint FROM pg_class c "
            "WHERE c.relnamespace = current_schema()::regnamespace AND c.relkind = 'r'"
        ).fetchone()[0]
        stats = {name: (live, dead) for name, live, dead in connection.execute(
            "SELECT relname, n_live_tup, n_dead_tup FROM pg_stat_user_tables "
            "WHERE schemaname = current_schema()").fetchall()}
        tables = {}
        for table in _STORAGE_TABLES:
            size = connection.execute("SELECT pg_total_relation_size(to_regclass(%s))",
                                      (table,)).fetchone()[0]
            live, dead = stats.get(table, (0, 0))
            tables[table] = {"bytes": int(size or 0), "live_tuples": int(live),
                             "dead_tuples": int(dead)}
        wal = connection.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0]
    return {"schema_bytes": int(schema_bytes), "tables": tables, "wal_lsn": wal}


def _vacuum(connection, tables) -> None:
    from psycopg import sql

    if not tables:
        return
    _idle(connection, "vacuum")
    schema = connection.execute("SELECT current_schema()").fetchone()[0]
    connection.commit()
    connection.autocommit = True
    try:
        for table in tables:
            connection.execute(sql.SQL(VACUUM_MODE + " {}").format(sql.Identifier(schema, table)))
    finally:
        connection.autocommit = False


def _logical_counts(connection) -> dict:
    with connection.transaction():
        return {table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in _STORAGE_TABLES}


def _set_pause(connection, lease, pressure: dict) -> str:
    with connection.transaction():
        validate_transition(connection, lease)
        active = connection.execute(
            "SELECT pause_id FROM ingestion_pauses WHERE released_at IS NULL FOR UPDATE"
        ).fetchone()
        if pressure["status"] == "low_water":
            if active is None:
                connection.execute(
                    "INSERT INTO ingestion_pauses(reason,used_bytes,budget_bytes,"
                    "min_free_percent,lease_owner,lease_fence) VALUES "
                    "('low_water',%s,%s,%s,%s,%s)",
                    (pressure["used_bytes"], pressure["budget_bytes"],
                     pressure["min_free_percent"], lease.owner, lease.fence))
            return "paused"
        if active is not None:
            connection.execute("UPDATE ingestion_pauses SET released_at=clock_timestamp() "
                               "WHERE pause_id=%s", (active[0],))
        return "running"


def run_retention_cycle(connection, owner: str, now, policy: dict) -> dict:
    """One scheduled maintenance cycle in the shared fenced slot.

    Takes the transition lease as ``owner`` (fencing out any earlier owner, whose next
    transition is refused, never corrupted), then: payload expiry and guard retirement
    (``apply_retention``), derived-history expiry (``apply_derived_retention``), bounded
    plain VACUUM within the table and byte budgets, and the low-water check: below
    ``min_free_percent`` free of ``storage_budget_bytes`` ingestion is paused (a durable
    pause that ``accept_scan`` honours) until a later cycle finds room again. Nothing is
    deleted to make room. Logical deletions and physical bytes are reported separately.
    """
    import time

    from ton_sentinel.db.fencing import acquire_transition

    parsed = cycle_policy(policy)
    at = _now(now)
    _idle(connection, "run_retention_cycle")
    started = time.monotonic()
    lease = acquire_transition(connection, owner, ttl_seconds=300)
    logical_before = _logical_counts(connection)
    before = storage_report(connection)
    retention = apply_retention(connection, lease,
                                plan_retention(connection, at, parsed["retention"]))
    derived = apply_derived_retention(connection, lease,
                                      plan_derived_retention(connection, at, parsed["derived"]))
    after_delete = storage_report(connection)
    chosen, skipped = choose_vacuum(after_delete["tables"],
                                    max_tables=parsed["vacuum_max_tables"],
                                    max_bytes=parsed["vacuum_max_bytes"])
    _vacuum(connection, chosen)
    after_vacuum = storage_report(connection)
    pressure = storage_pressure(after_vacuum["schema_bytes"], parsed["storage_budget_bytes"],
                                parsed["min_free_percent"])
    ingestion = _set_pause(connection, lease, pressure)
    with connection.transaction():
        wal_bytes = int(connection.execute(
            "SELECT pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)",
            (after_vacuum["wal_lsn"], before["wal_lsn"])).fetchone()[0])
    deleted = {}
    for result in (retention, derived):
        if result["status"] == "applied":
            for run in result["runs"]:
                for table, count in (run["deleted"] or {}).items():
                    deleted[table] = deleted.get(table, 0) + count
    statuses = {retention["status"], derived["status"]}
    status = ("applied" if "applied" in statuses else
              "stale" if "stale" in statuses else
              "already_applied" if "already_applied" in statuses else "nothing_to_delete")
    return {"status": status, "now": at, "lease": {"owner": lease.owner, "fence": lease.fence},
            "retention": retention, "derived": derived,
            "logical": {"before": logical_before, "after": _logical_counts(connection),
                        "deleted": deleted},
            "physical": {"before": before, "after_delete": after_delete,
                         "after_vacuum": after_vacuum, "wal_bytes": wal_bytes},
            "vacuum": {"mode": VACUUM_MODE, "vacuumed": chosen, "skipped": skipped},
            "storage": pressure, "ingestion": ingestion,
            "seconds": round(time.monotonic() - started, 3)}
