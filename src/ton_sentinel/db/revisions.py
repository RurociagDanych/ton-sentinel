"""Admit only ordered, authorized canonical revisions; quarantine everything else.

Policy:

* Evidence is one corroborated, fenced full-range walk (two consecutive agreeing walks)
  plus full trace key-set lookups that agree on both passes. Anything weaker is a
  candidate that stays quarantined; it never retracts or rewrites accepted data.
* A changed digest, a changed trace key set, or a disappearance is admitted only under an
  explicit authorization naming the exact candidate digest, the generation it was decided
  against, and the same source epoch. Monotonic authority additionally needs a source
  revision above the accepted one; only an audited owner resolution may retract or restore
  a previously superseded digest.
* A trace key-set change is resolved as one unit: every member is admitted in the same
  transaction or none is, and the old entries stay (but ineligible) until it resolves.
* Every admitted change or eligibility flip dirties its old and new windows plus the
  trailing-baseline dependents and advances the dependency epoch, in the same transaction.
"""
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass

from ton_sentinel.contracts import (
    Action, ActionIdentity, Authorization, Change, RevisionPolicy, SourceEpoch,
    WindowKey, parse_authorization,
)
from ton_sentinel.db.fencing import validate_transition
from ton_sentinel.normalize import contributions, normalize, semantic_digest


@dataclass
class _Row:
    event_id: str
    action_id: str
    generation: int | None
    disposition: str
    digest: str
    action: dict
    source_revision: int | None
    history: frozenset

    @property
    def action_end(self) -> int:
        return self.action["action_end"]


def _wkey(key: WindowKey):
    return (key.token, key.window_start)


class _Decision:
    """One fenced decision transaction; owns the dependency-epoch bump and the change list."""

    def __init__(self, connection, range_id, range_row, previous_range_id, observations,
                 epoch: SourceEpoch, auth: Authorization | None, policy: RevisionPolicy):
        self.db = connection
        self.range_id = range_id
        self.previous_range_id = previous_range_id
        self.start, self.end = range_row[1], range_row[2]
        self.epoch = epoch
        self.auth = auth
        self.policy = policy
        self.observations = observations      # event_id -> (Action, digest)
        self.changes: list[Change] = []
        self._epoch_value: int | None = None
        self.last_generation: int | None = None

    def event_id(self, trace_id: str, action_id: str) -> str:
        return ActionIdentity(self.epoch.network, trace_id, action_id).event_id

    # ------------------------------------------------------------------ plumbing
    def bump_epoch(self) -> int:
        if self._epoch_value is None:
            self._epoch_value = self.db.execute(
                "UPDATE dependency_epoch SET epoch=epoch+1 WHERE singleton RETURNING epoch"
            ).fetchone()[0]
        return self._epoch_value

    def allocate_generation(self) -> int:
        generation = self.db.execute(
            "UPDATE canonical_generation SET generation=generation+1 WHERE singleton "
            "RETURNING generation").fetchone()[0]
        self.last_generation = generation
        return generation

    def windows(self, action_json: dict | None, disposition: str = "active") -> set:
        if action_json is None or disposition != "active":
            return set()
        action = Action.from_json(json.dumps(action_json))
        return {WindowKey(action.epoch.network, c.token, c.window_start)
                for c in contributions(action)}

    def mark_dirty(self, windows, cause: str, reason: str) -> None:
        epoch = self.bump_epoch()
        for key in sorted(windows, key=_wkey):
            for role, span in (("target", (key.window_start, key.window_end)),
                               ("baseline_dependents", key.dependents_span(self.policy))):
                self.db.execute(
                    "INSERT INTO dirty_windows(network,token,window_start,role,window_end,"
                    "covers_start,covers_end,dependency_epoch,reason,cause_event_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (network,token,window_start,role) DO UPDATE SET "
                    "dirty_generation=dirty_windows.dirty_generation+1,"
                    "dependency_epoch=EXCLUDED.dependency_epoch,reason=EXCLUDED.reason,"
                    "cause_event_id=EXCLUDED.cause_event_id,status='pending',"
                    "updated_at=clock_timestamp()",
                    (key.network, key.token, key.window_start, role, key.window_end,
                     span[0], span[1], epoch, reason, cause))

    # ------------------------------------------------------------------- evidence
    def lookup(self, trace_id: str):
        """Key set both passes agree on, or None (missing/incomplete/unclassified/changed)."""
        sets = []
        for range_id in (self.previous_range_id, self.range_id):
            row = self.db.execute(
                "SELECT state,action_keys FROM trace_lookup_evidence "
                "WHERE range_id=%s AND trace_id=%s ORDER BY observation_id DESC LIMIT 1",
                (range_id, trace_id)).fetchone()
            if row is None or row[0] != "classified_complete" or type(row[1]) is not list:
                return None
            sets.append(frozenset(row[1]))
        return sets[0] if sets[0] == sets[1] else None

    def load_trace(self, trace_id: str) -> dict:
        """Current rows of one trace (seeded once from the first-acceptance ledger), locked."""
        network = self.epoch.network
        prefix = ("ton-sentinel:event:v1:"
                  + json.dumps([network, trace_id], separators=(",", ":"))[:-1] + ",")
        for event_id, generation, digest, action in self.db.execute(
                "SELECT ca.event_id,ca.accepted_generation,ca.semantic_digest,ca.action "
                "FROM canonical_actions ca WHERE starts_with(ca.event_id,%s) AND NOT EXISTS "
                "(SELECT 1 FROM canonical_current cc WHERE cc.event_id=ca.event_id)",
                (prefix,)).fetchall():
            self.db.execute(
                "INSERT INTO canonical_current(event_id,network,trace_id,action_id,"
                "accepted_generation,disposition,semantic_digest,action) "
                "VALUES (%s,%s,%s,%s,%s,'active',%s,%s::jsonb)",
                (event_id, network, trace_id,
                 ActionIdentity.from_event_id(event_id).action_id, generation, digest,
                 json.dumps(action)))
        rows = {}
        for (event_id, action_id, generation, disposition, digest, action,
             source_revision) in self.db.execute(
                "SELECT event_id,action_id,accepted_generation,disposition,semantic_digest,"
                "action,source_revision FROM canonical_current "
                "WHERE network=%s AND trace_id=%s ORDER BY event_id FOR UPDATE",
                (network, trace_id)).fetchall():
            history = {d for (d,) in self.db.execute(
                "SELECT semantic_digest FROM canonical_revisions WHERE event_id=%s",
                (event_id,)).fetchall()}
            base = self.db.execute(
                "SELECT semantic_digest FROM canonical_actions WHERE event_id=%s",
                (event_id,)).fetchone()
            if base is not None:
                history.add(base[0])
            rows[action_id] = _Row(event_id, action_id, generation, disposition, digest,
                                   action, source_revision, frozenset(history))
        return rows

    # -------------------------------------------------------------------- judging
    def expired(self, event_id: str, action) -> bool:
        """The new contribution of ``action`` lies in an expired window, or
        its identity is held by a guard; it is quarantined, never inserted (backfill only)."""
        from ton_sentinel.maintenance import late_delivery_reasons

        return bool(late_delivery_reasons(self.db, event_id, action.identity.network,
                                          action.action_end // 300
                                          * 300)[0])

    def judge(self, kind: str, row: _Row | None, digest: str | None, event_id: str,
              action=None):
        """Return a rejection reason, or None when this member is authorized and ordered."""
        auth = self.auth
        retraction = kind == "retract"
        unauthorized = "unauthorized_disappearance" if retraction else "unauthorized_change"
        grant = None if auth is None else auth.grants.get(event_id)
        if (auth is None or grant is None or auth.epoch != self.epoch
                or grant.action != kind
                or (not retraction and grant.digest != digest)
                or (retraction and auth.contract != "owner_resolution")
                or (row is not None
                    and Action.from_json(json.dumps(row.action)).epoch != auth.epoch)):
            return unauthorized
        if kind != "add" and grant.expected_generation != row.generation:
            return "stale_authorization"
        if auth.contract == "source_monotonic_revision" and kind == "revise":
            if digest in row.history and digest != row.digest:
                return "superseded_digest"
            baseline = (row.source_revision if row.source_revision is not None
                        else grant.previous_source_revision)
            if baseline is None:
                return "no_ordered_baseline"
            if (row.source_revision is not None
                    and grant.previous_source_revision not in (None, row.source_revision)):
                return "stale_authorization"
            if grant.source_revision == baseline:
                return "equal_revision_conflict"
            if grant.source_revision < baseline:
                return "stale_revision"
        if action is not None and self.expired(event_id, action):
            return "expired_identity"
        return None

    # ----------------------------------------------------------------- dispositions
    def apply(self, kind: str, trace_id: str, action_id: str, row: _Row | None,
              candidate) -> str:
        event_id = self.event_id(trace_id, action_id)
        grant = self.auth.grants[event_id]
        old_windows = self.windows(row.action if row else None,
                                   row.disposition if row else "active")
        generation = self.allocate_generation()
        if kind == "retract":
            digest, action_json, source_revision = row.digest, row.action, row.source_revision
        else:
            action, digest = candidate
            action_json = json.loads(action.to_json())
            source_revision = grant.source_revision
        self.db.execute(
            "INSERT INTO canonical_revisions(event_id,accepted_generation,change_kind,"
            "semantic_digest,action,previous_generation,previous_digest,source_revision,"
            "reason,contract,authority_id,authority_grant,range_id) "
            "VALUES (%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)",
            (event_id, generation, "retraction" if kind == "retract" else "revision", digest,
             json.dumps(action_json), None if row is None else row.generation,
             None if row is None else row.digest, source_revision, self.auth.contract,
             self.auth.contract, self.auth.authority_id, self.auth.to_json(), self.range_id))
        if row is None:
            self.db.execute(
                "INSERT INTO canonical_current(event_id,network,trace_id,action_id,"
                "accepted_generation,disposition,semantic_digest,action,source_revision) "
                "VALUES (%s,%s,%s,%s,%s,'active',%s,%s::jsonb,%s)",
                (event_id, self.epoch.network, trace_id, action_id, generation, digest,
                 json.dumps(action_json), source_revision))
        else:
            self.db.execute(
                "UPDATE canonical_current SET accepted_generation=%s,disposition=%s,"
                "semantic_digest=%s,action=%s::jsonb,source_revision=%s WHERE event_id=%s",
                (generation, "retracted" if kind == "retract" else "active", digest,
                 json.dumps(action_json), source_revision, event_id))
        new_windows = set() if kind == "retract" else self.windows(action_json)
        self.mark_dirty(old_windows | new_windows, event_id, "revision")
        self.changes.append(Change(
            "retraction" if kind == "retract" else "revision", event_id,
            self.auth.contract, generation, tuple(sorted(old_windows, key=_wkey)),
            tuple(sorted(new_windows, key=_wkey))))
        return event_id

    def quarantine(self, subject: str, scope: str, trace_id: str, reason: str, content) -> None:
        digest = "sha256:" + hashlib.sha256(
            json.dumps(content, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        existing = self.db.execute(
            "SELECT reason,content_digest FROM revision_quarantine "
            "WHERE subject=%s AND status='open' FOR UPDATE", (subject,)).fetchone()
        if existing is not None and tuple(existing) == (reason, digest):
            return
        if existing is None:
            self.db.execute(
                "INSERT INTO revision_quarantine(subject,scope,network,trace_id,reason,"
                "content_digest,range_id) VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (subject, scope, self.epoch.network, trace_id, reason, digest, self.range_id))
        else:
            self.db.execute(
                "UPDATE revision_quarantine SET reason=%s,content_digest=%s,range_id=%s "
                "WHERE subject=%s AND status='open'",
                (reason, digest, self.range_id, subject))
        self.changes.append(Change("quarantine", subject, reason, None, (), ()))

    def resolve(self, subject: str) -> None:
        row = self.db.execute(
            "UPDATE revision_quarantine SET status='resolved',resolved_at=clock_timestamp(),"
            "resolved_generation=%s WHERE subject=%s AND status='open' RETURNING subject",
            (self.last_generation, subject)).fetchone()
        if row is not None:
            self.changes.append(Change("cleared", subject, "resolved", None, (), ()))

    # ---------------------------------------------------------------------- traces
    def process_trace(self, trace_id: str) -> None:
        rows = self.load_trace(trace_id)
        if not rows:
            return                      # a first-seen trace is a range insert, not a revision
        trace_subject = f"trace:{self.epoch.network}:{trace_id}"
        obs = {action.identity.action_id: (action, digest)
               for action, digest in self.observations.values()
               if action.identity.trace_id == trace_id}
        look = self.lookup(trace_id)
        active = {a for a, r in rows.items() if r.disposition == "active"}
        in_range = {a for a in active if self.start <= rows[a].action_end < self.end}
        absent = in_range - set(obs)
        changed = {a for a in obs if a in active and obs[a][1] != rows[a].digest}
        new = {a for a in obs if a not in active}
        signature = [sorted((a, d) for a, (_, d) in obs.items()),
                     None if look is None else sorted(look)]
        if look is None:
            if changed or new or absent:
                self.quarantine(trace_subject, "trace", trace_id, "incomplete_trace", signature)
            return
        added = new | (look - active)
        removed = active - look
        contradiction = bool(
            (set(obs) - look) or (removed & set(obs)) or ((look - active) - set(obs))
            or any(a not in in_range for a in removed) or (absent - removed))
        if contradiction:
            self.quarantine(trace_subject, "trace", trace_id, "incomplete_trace", signature)
        elif added or removed:
            self.replace_trace(trace_id, trace_subject, rows, obs, look, added, removed,
                               changed, signature)
        elif changed:
            for action_id in sorted(changed, key=lambda a: rows[a].event_id):
                row, candidate = rows[action_id], obs[action_id]
                reason = self.judge("revise", row, candidate[1], row.event_id,
                                    action=candidate[0])
                if reason:
                    self.quarantine(row.event_id, "event", trace_id, reason, candidate[1])
                else:
                    self.apply("revise", trace_id, action_id, row, candidate)
                    self.resolve(row.event_id)
        else:
            self.resolve(trace_subject)
            for row in rows.values():
                self.resolve(row.event_id)

    def replace_trace(self, trace_id, trace_subject, rows, obs, look, added, removed,
                      changed, signature) -> None:
        """Resolve a whole trace key set: every member is admitted, or none is."""
        members = ([(a, "add" if a not in rows else "revise") for a in added]
                   + [(a, "revise") for a in changed] + [(a, "retract") for a in removed])
        members.sort(key=lambda m: self.event_id(trace_id, m[0]))
        if (self.auth is None or self.auth.epoch != self.epoch
                or self.auth.trace_sets.get(trace_id) != tuple(sorted(look))):
            reason = "unauthorized_trace_replacement" if added else "unauthorized_disappearance"
        else:
            reason = None
            for action_id, kind in members:
                reason = self.judge(kind, rows.get(action_id),
                                    None if kind == "retract" else obs[action_id][1],
                                    self.event_id(trace_id, action_id),
                                    action=None if kind == "retract" else obs[action_id][0])
                if reason:
                    break
        if reason:
            self.quarantine(trace_subject, "trace", trace_id, reason, signature)
            return
        applied = [self.apply(kind, trace_id, action_id, rows.get(action_id),
                              None if kind == "retract" else obs[action_id])
                   for action_id, kind in members]
        for subject in [trace_subject, *applied]:
            self.resolve(subject)

    def sync_suppression(self, trace_ids) -> None:
        """Eligibility is suppressed exactly while an open quarantine covers the event."""
        for trace_id in sorted(trace_ids):
            opened = {s for (s,) in self.db.execute(
                "SELECT subject FROM revision_quarantine WHERE status='open' "
                "AND network=%s AND trace_id=%s", (self.epoch.network, trace_id)).fetchall()}
            trace_open = f"trace:{self.epoch.network}:{trace_id}" in opened
            for event_id, action, disposition, suppressed in self.db.execute(
                    "SELECT event_id,action,disposition,eligibility_suppressed "
                    "FROM canonical_current WHERE network=%s AND trace_id=%s "
                    "ORDER BY event_id FOR UPDATE", (self.epoch.network, trace_id)).fetchall():
                want = trace_open or event_id in opened
                if want == suppressed:
                    continue
                self.db.execute(
                    "UPDATE canonical_current SET eligibility_suppressed=%s WHERE event_id=%s",
                    (want, event_id))
                self.mark_dirty(self.windows(action, disposition), event_id,
                                "eligibility_suppressed" if want else "eligibility_restored")


def _load_evidence(connection, evidence):
    """Return (range_row, previous_range_id, epoch) for a proven corroborated walk, or None."""
    from ton_sentinel.source.scan import validate_corroboration_proof

    row = connection.execute(
        "SELECT status,start_inclusive,end_exclusive,epoch,corroborated FROM source_ranges "
        "WHERE range_id=%s FOR SHARE", (evidence.range_id,)).fetchone()
    if row is None:
        raise ValueError("unknown scan range")
    if row[0] not in ("complete", "accepted") or not row[4]:
        return None
    group = connection.execute(
        "SELECT g.group_id,w.walk_no FROM corroboration_groups g JOIN corroboration_walks w "
        "ON w.group_id=g.group_id AND w.range_id=g.accepted_range_id "
        "WHERE g.accepted_range_id=%s AND g.status IN ('corroborated','accepted')",
        (evidence.range_id,)).fetchone()
    if group is None or not validate_corroboration_proof(
            connection, str(group[0]), selected_range_id=evidence.range_id):
        return None
    previous = connection.execute(
        "SELECT range_id FROM corroboration_walks WHERE group_id=%s AND walk_no=%s",
        (group[0], group[1] - 1)).fetchone()
    if previous is None:
        return None
    epoch = SourceEpoch.from_json(json.dumps(row[3], ensure_ascii=True, separators=(",", ":")))
    active = connection.execute(
        "SELECT epoch FROM active_source_epoch WHERE singleton=true FOR SHARE").fetchone()
    if active is None or active[0] != json.loads(epoch.to_json()):
        raise ValueError("explicit audited source epoch transition required")
    return row, previous[0], epoch


def decide_revision(connection, lease, evidence, authorization: dict | None = None, *,
                    policy: RevisionPolicy | None = None,
                    commit_check: Callable | None = None) -> tuple[Change, ...]:
    """Decide every revision candidate in one corroborated, fenced full-range walk.

    Returns the newly effective changes (admitted, retracted, quarantined or cleared).
    An identical retry returns ``()`` and writes nothing. Incomplete or uncorroborated
    evidence returns ``()`` without writing. A malformed authorization raises
    ``RevisionRejected`` before any state is read; a stale or expired lease raises
    ``LeaseRejected``. ``commit_check`` runs inside the transaction, after every write.
    """
    from ton_sentinel.source.scan import WalkEvidence

    if type(evidence) is not WalkEvidence:
        raise ValueError("persisted WalkEvidence required")
    policy = RevisionPolicy() if policy is None else policy
    if type(policy) is not RevisionPolicy:
        raise ValueError("RevisionPolicy required")
    auth = parse_authorization(authorization)
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("decide_revision requires an idle connection")
    with connection.transaction():
        validate_transition(connection, lease)
        if not evidence.complete:
            return ()
        loaded = _load_evidence(connection, evidence)
        if loaded is None:
            return ()
        range_row, previous_range_id, epoch = loaded
        observations = {}
        for (payload,) in connection.execute(
                "SELECT payload FROM action_observations WHERE range_id=%s ORDER BY ordinal",
                (evidence.range_id,)).fetchall():
            action = normalize(payload, epoch)
            if not range_row[1] <= action.action_end < range_row[2]:
                raise ValueError("action_end outside immutable scan range")
            observations[action.identity.event_id] = (action, semantic_digest(action))
        decision = _Decision(connection, evidence.range_id, range_row, previous_range_id,
                             observations, epoch, auth, policy)
        traces = {a.identity.trace_id for a, _ in observations.values()}
        for table in ("canonical_current", "canonical_actions"):
            for (event_id,) in connection.execute(
                    f"SELECT event_id FROM {table} WHERE "
                    "(action->>'action_end')::bigint >= %s AND "
                    "(action->>'action_end')::bigint < %s",
                    (range_row[1], range_row[2])).fetchall():
                identity = ActionIdentity.from_event_id(event_id)
                if identity.network == epoch.network:
                    traces.add(identity.trace_id)
        for trace_id in sorted(traces):
            decision.process_trace(trace_id)
        decision.sync_suppression(traces)
        if commit_check is not None:
            commit_check(connection)
    return tuple(decision.changes)


_AUDIT_KEYS = frozenset({"authority_id", "reason", "audit_start", "audit_end"})


def open_source_epoch(connection, lease, epoch: SourceEpoch, audit: dict) -> bool:
    """Start an audited source epoch (source upgrade); never a cross-provider comparison.

    ``audit`` names the approving ``authority_id``, a ``reason`` and the approved bounded
    before/after audit window ``[audit_start, audit_end)`` in whole five-minute windows.
    In one fenced transaction: record the immutable transition, switch the active source
    epoch and advance the dependency epoch (frozen work of the old epoch can no longer
    promote). Re-opening the active epoch is a no-op returning ``False``. Values accepted
    under the old epoch are never revised by the new provider's ordering: such changes stay
    quarantined (see ``_Decision.judge``) until separately resolved.
    """
    if type(epoch) is not SourceEpoch:
        raise ValueError("SourceEpoch required")
    if type(audit) is not dict or set(audit) != _AUDIT_KEYS:
        raise ValueError("audit must name exactly: " + ", ".join(sorted(_AUDIT_KEYS)))
    for name in ("authority_id", "reason"):
        if type(audit[name]) is not str or not audit[name].strip():
            raise ValueError(f"audit {name} must be nonempty text")
    start, end = audit["audit_start"], audit["audit_end"]
    if (type(start) is not int or type(end) is not int or start < 0 or start % 300
            or end % 300 or end <= start):
        raise ValueError("audit window must be whole five-minute windows with start < end")
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("open_source_epoch requires an idle connection")
    new = json.loads(epoch.to_json())
    with connection.transaction():
        validate_transition(connection, lease)
        active = connection.execute(
            "SELECT epoch FROM active_source_epoch WHERE singleton=true FOR UPDATE").fetchone()
        if active is None or active[0] is None:
            raise ValueError("no active source epoch to upgrade from")
        if active[0] == new:
            return False
        if active[0].get("network") != epoch.network:
            raise ValueError("a source upgrade never switches network")
        dependency = connection.execute(
            "UPDATE dependency_epoch SET epoch=epoch+1 WHERE singleton RETURNING epoch"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO source_epoch_transitions(previous_epoch,new_epoch,authority_id,reason,"
            "audit_start,audit_end,dependency_epoch,lease_owner,lease_fence) "
            "VALUES (%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,%s,%s)",
            (json.dumps(active[0]), epoch.to_json(), audit["authority_id"], audit["reason"],
             start, end, dependency, lease.owner, lease.fence))
        connection.execute("UPDATE active_source_epoch SET epoch=%s::jsonb WHERE singleton=true",
                           (epoch.to_json(),))
        validate_transition(connection, lease)
    return True
