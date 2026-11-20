"""Only ordered, authorized canonical revisions are admitted (real PostgreSQL)."""

import base64
import json
import os
import threading
import uuid
from dataclasses import dataclass, field

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import accept_corroborated_range, acquire_transition, migrate
from ton_sentinel.normalize import normalize, semantic_digest
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.scan import ScanPolicy, corroborate_range, walk_evidence
from ton_sentinel.source.trace import lookup_and_persist_trace

LOOKBACK = 7 * 86400
WINDOW = 300


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "revisions_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, topic="revisions-test", topic_incarnation=schema)
            connection.schema_name = schema
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def b64(i):
    return base64.b64encode(bytes([i]) * 32).decode("ascii")


def eid(ev):
    return "ton-sentinel:event:v1:" + json.dumps(["testnet", b64(ev[0]), b64(ev[1])],
                                                 separators=(",", ":"))


def trace_subject(t):
    return "trace:testnet:" + b64(t)


def subject_text(subject):
    return trace_subject(subject[1]) if subject[0] == "trace" else eid(subject)


def val(end=11, i_asset="TON", i_amt="100", o_asset="jetton:A", o_amt="25"):
    return (end, i_asset, i_amt, o_asset, o_amt)


A, B, C = val(), val(end=605), val(o_asset="jetton:B")


def payload(ev, v):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet", "trace_id": b64(ev[0]), "action_id": b64(ev[1]),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": v[0], "status": "success", "eligible": True, "route_proven": True,
        "route": [v[1], v[3]],
        "input": {"asset": v[1], "raw_amount": v[2]},
        "output": {"asset": v[3], "raw_amount": v[4]},
    }


def epoch(source="synthetic:source"):
    return SourceEpoch("testnet", source, "synthetic:filter", "synthetic:build", "synthetic:policy")


def policy():
    return ScanPolicy(start=0, end=100000, page_size=4, max_pages=8, max_actions=32)


class Pages:
    def __init__(self, entries):
        self.entries = entries

    def fetch_page(self, source_epoch, start, end, offset, limit):
        return self.entries[offset:offset + limit]


class Traces:
    """Fake read-only trace endpoint keyed by trace ID."""
    def __init__(self, spec):
        self.spec = spec

    def fetch_trace(self, source_epoch, trace_id, include_actions=True):
        kind = self.spec
        info = {"trace_state": "complete",
                "classification_state": "unclassified" if kind == "unclassified" else "classified"}
        body = {"trace_id": trace_id, "is_incomplete": kind == "incomplete",
                "trace_info": info}
        if isinstance(kind, (set, frozenset)):
            body["actions"] = [{"action_id": b64(a)} for a in sorted(kind)]
        return {"traces": [body]}


class World:
    def __init__(self, db):
        self.db = db
        self.epoch = epoch()
        self.initial = {}

    def seed(self, values):
        group = corroborate_range(self.db, Pages([payload(ev, v) for ev, v in values.items()]),
                                  self.epoch, policy())
        lease = acquire_transition(self.db, "seed")
        assert accept_corroborated_range(self.db, lease, group)
        self.initial = dict(values)

    def candidate(self, obs, lookups):
        """Corroborate ``obs`` and persist per-pass trace lookups.

        ``lookups[t]`` is a set of action indexes, "incomplete", "unclassified", None
        (no lookup stored, e.g. a 404) or ("pair", spec_pass1, spec_pass2).
        """
        group = corroborate_range(self.db, Pages([payload(ev, v) for ev, v in obs.items()]),
                                  self.epoch, policy())
        walks = self.db.execute(
            "SELECT range_id FROM corroboration_walks WHERE group_id=%s ORDER BY walk_no",
            (group,)).fetchall()
        assert len(walks) == 2
        for index, (range_id,) in enumerate(walks):
            for t, spec in lookups.items():
                if spec is None:
                    continue
                if isinstance(spec, tuple) and spec[0] == "pair":
                    spec = spec[1 + index]
                lookup_and_persist_trace(self.db, Traces(spec), str(range_id), b64(t))
        self.db.commit()
        selected = self.db.execute(
            "SELECT accepted_range_id FROM corroboration_groups WHERE group_id=%s",
            (group,)).fetchone()[0]
        self.db.commit()
        return walk_evidence(self.db, str(selected))

    def current_generation(self, ev):
        row = self.db.execute("SELECT accepted_generation FROM canonical_current "
                              "WHERE event_id=%s", (eid(ev),)).fetchone()
        if row is None:
            row = self.db.execute("SELECT accepted_generation FROM canonical_actions "
                                  "WHERE event_id=%s", (eid(ev),)).fetchone()
        self.db.commit()
        return None if row is None else row[0]

    def digest(self, ev, v):
        return semantic_digest(normalize(payload(ev, v), self.epoch))

    def authorization(self, spec):
        if spec is None:
            return None
        grants = []
        for g in spec["grants"]:
            ev = g["ev"]
            item = {"event_id": eid(ev), "action": g["action"]}
            if g["action"] != "retract":
                item["digest"] = self.digest(ev, g["value"])
            if g["action"] != "add":
                gen = self.current_generation(ev)
                item["expected_generation"] = (
                    gen if g.get("gen", "current") == "current" else (gen or 0) + 1000)
            if g.get("src") is not None:
                item["source_revision"] = g["src"]
            if g.get("prev") is not None:
                item["previous_source_revision"] = g["prev"]
            grants.append(item)
        out = {"contract": spec["contract"], "authority_id": "test-authority",
               "epoch": json.loads((self.epoch if spec.get("epoch_ok", True)
                                    else epoch("synthetic:other")).to_json()),
               "grants": grants}
        if spec.get("trace_sets"):
            out["trace_sets"] = {b64(t): [b64(a) for a in sorted(s)]
                                 for t, s in spec["trace_sets"].items()}
        return out


def mono(*grants, **kw):
    return {"contract": "source_monotonic_revision", "grants": list(grants), **kw}


def owner(*grants, **kw):
    return {"contract": "owner_resolution", "grants": list(grants), **kw}


def revise(ev, value, **kw):
    return {"ev": ev, "action": "revise", "value": value, **kw}


def add(ev, value, **kw):
    return {"ev": ev, "action": "add", "value": value, **kw}


def retract(ev, **kw):
    return {"ev": ev, "action": "retract", **kw}


def windows_of(value):
    start = value[0] // WINDOW * WINDOW
    return {(token, start) for token in {value[1], value[3]}}


def dirty_rows(windows):
    rows = set()
    for token, start in windows:
        rows.add((token, start, "target", start, start + WINDOW))
        rows.add((token, start, "baseline_dependents", start + WINDOW,
                  start + LOOKBACK + WINDOW))
    return rows


@dataclass
class Row:
    disp: str
    value: tuple
    src: int | None
    hist: set = field(default_factory=set)


class Oracle:
    """Expected policy from card / spec 6.3 + 7.3, with no product imports."""
    def __init__(self, seeded):
        self.cur = {ev: Row("active", v, None) for ev, v in seeded.items()}
        self.open = {}
        self.dirty = set()
        self.epoch = 0
        self.generations = 0
        self.suppressed = set()

    def judge(self, ev, kind, value, auth, grant_spec, is_stale):
        retraction = kind == "retract"
        unauthorized = "unauthorized_disappearance" if retraction else "unauthorized_change"
        if auth is None or not auth.get("epoch_ok", True) or grant_spec is None:
            return unauthorized
        if grant_spec["action"] != kind:
            return unauthorized
        if not retraction and grant_spec["value"] != value:
            return unauthorized
        if retraction and auth["contract"] != "owner_resolution":
            return unauthorized
        row = self.cur.get(ev)
        if kind != "add" and is_stale:
            return "stale_authorization"
        if auth["contract"] == "source_monotonic_revision" and not retraction:
            if kind == "revise" and value in row.hist and value != row.value:
                return "superseded_digest"
            if kind == "revise":
                baseline = row.src if row.src is not None else grant_spec.get("prev")
                if baseline is None:
                    return "no_ordered_baseline"
                if row.src is not None and grant_spec.get("prev") not in (None, row.src):
                    return "stale_authorization"
                if grant_spec["src"] == baseline:
                    return "equal_revision_conflict"
                if grant_spec["src"] < baseline:
                    return "stale_revision"
        return None

    def apply(self, ev, kind, value, auth, grant_spec, changes, touched):
        row = self.cur.get(ev)
        windows = set()
        if row is not None and row.disp == "active":
            windows |= windows_of(row.value)
        if kind == "retract":
            row.disp = "retracted"
        else:
            if row is None:
                row = self.cur[ev] = Row("active", value, None)
            else:
                if row.value != value:
                    row.hist.add(row.value)
                row.disp, row.value = "active", value
            row.src = grant_spec.get("src")
            windows |= windows_of(value)
        self.generations += 1
        self.dirty |= dirty_rows(windows)
        touched["applied"] = True
        changes.append(("retraction" if kind == "retract" else "revision",
                        eid(ev), auth["contract"]))

    def quarantine(self, subject, reason, content, changes):
        previous = self.open.get(subject)
        if previous == (reason, content):
            return
        self.open[subject] = (reason, content)
        changes.append(("quarantine", subject_text(subject), reason))

    def resolve(self, subject, changes):
        if subject in self.open:
            del self.open[subject]
            changes.append(("cleared", subject_text(subject), "resolved"))

    def step(self, cand, auth, stale_flags):
        """cand: obs {ev: value}, lookups {t: set|None}, range (s,e). Returns changes."""
        s, e = 0, 100000
        changes, touched = [], {"applied": False}
        by_trace = {}
        for ev in cand["obs"]:
            by_trace.setdefault(ev[0], {})[ev[1]] = cand["obs"][ev]
        traces = set(by_trace) | {ev[0] for ev, r in self.cur.items() if s <= r.value[0] < e}
        grants = {} if auth is None else {g["ev"]: g for g in auth["grants"]}
        for t in sorted(traces):
            known = {ev[1] for ev in self.cur if ev[0] == t}
            if not known:
                continue
            active = {a for a in known if self.cur[(t, a)].disp == "active"}
            obs = by_trace.get(t, {})
            look = cand["lookups"].get(t)
            if isinstance(look, tuple):
                look = look[1] if look[1] == look[2] and isinstance(look[1], (set, frozenset)) else None
            if not isinstance(look, (set, frozenset)):
                look = None
            absent = {a for a in active if s <= self.cur[(t, a)].value[0] < e and a not in obs}
            changed = {a for a in obs if a in active and obs[a] != self.cur[(t, a)].value}
            added = {a for a in obs if a not in active}
            tsig = (tuple(sorted(obs.items())),
                    None if look is None else tuple(sorted(look)))
            subj = ("trace", t)
            if look is None:
                if changed or added or absent:
                    self.quarantine(subj, "incomplete_trace", tsig, changes)
                continue
            added_ids = added | (look - active)
            removed = active - look
            contradiction = (
                bool(set(obs) - look) or bool(removed & set(obs))
                or bool((look - active) - set(obs))
                or any(not s <= self.cur[(t, a)].value[0] < e for a in removed)
                or bool(absent - removed))
            if contradiction:
                self.quarantine(subj, "incomplete_trace", tsig, changes)
                continue
            if added_ids or removed:
                members = ([(a, "add" if a not in known else "revise", obs[a]) for a in added_ids]
                           + [(a, "revise", obs[a]) for a in changed]
                           + [(a, "retract", None) for a in removed])
                members.sort(key=lambda m: eid((t, m[0])))
                reason = None
                if auth is None or not auth.get("epoch_ok", True) or \
                        auth.get("trace_sets", {}).get(t) != look:
                    reason = ("unauthorized_trace_replacement" if added_ids
                              else "unauthorized_disappearance")
                else:
                    for a, kind, v in members:
                        reason = self.judge((t, a), kind, v, auth, grants.get((t, a)),
                                            stale_flags.get((t, a), False))
                        if reason:
                            break
                if reason:
                    self.quarantine(subj, reason, tsig, changes)
                else:
                    for a, kind, v in members:
                        self.apply((t, a), kind, v, auth, grants[(t, a)], changes, touched)
                    for sub in [subj] + [(t, a) for a, _, _ in members]:
                        self.resolve(sub, changes)
            elif changed:
                for a in sorted(changed, key=lambda x: eid((t, x))):
                    ev = (t, a)
                    reason = self.judge(ev, "revise", obs[a], auth, grants.get(ev),
                                        stale_flags.get(ev, False))
                    if reason:
                        self.quarantine(ev, reason, obs[a], changes)
                    else:
                        self.apply(ev, "revise", obs[a], auth, grants[ev], changes, touched)
                        self.resolve(ev, changes)
            else:
                for sub in [subj] + [(t, a) for a in known]:
                    self.resolve(sub, changes)
        # derived suppression: any open quarantine on the event or its trace
        flipped = False
        for ev, row in self.cur.items():
            want = ev in self.open or ("trace", ev[0]) in self.open
            if want != (ev in self.suppressed):
                flipped = True
                (self.suppressed.add if want else self.suppressed.discard)(ev)
                if row.disp == "active":
                    self.dirty |= dirty_rows(windows_of(row.value))
        if touched["applied"] or flipped:
            self.epoch += 1
        return changes


def ev_of(event_id):
    trace, action = json.loads(event_id[len("ton-sentinel:event:v1:"):])[1:]
    return tuple(next(i for i in range(256) if b64(i) == x) for x in (trace, action))


def readback(db):
    db.commit()
    cur = {}
    for event_id, action in db.execute("SELECT event_id,action FROM canonical_actions"):
        cur[ev_of(event_id)] = ["active", action, None, False]
    for event_id, disp, action, src, supp in db.execute(
            "SELECT event_id,disposition,action,source_revision,eligibility_suppressed "
            "FROM canonical_current"):
        cur[ev_of(event_id)] = [disp, action, src, supp]
    state = {}
    for ev, (disp, action, src, supp) in cur.items():
        value = (action["action_end"], action["input"]["asset"], action["input"]["raw_amount"],
                 action["output"]["asset"], action["output"]["raw_amount"])
        state[ev] = (disp, value, src, supp)
    opened = {s: r for s, r in db.execute(
        "SELECT subject,reason FROM revision_quarantine WHERE status='open'")}
    dirty = {(token, start, role, cs, ce) for token, start, role, cs, ce in db.execute(
        "SELECT token,window_start,role,covers_start,covers_end FROM dirty_windows")}
    epoch_now = db.execute("SELECT epoch FROM dependency_epoch").fetchone()[0]
    generation = db.execute("SELECT generation FROM canonical_generation").fetchone()[0]
    db.commit()
    return state, opened, dirty, epoch_now, generation


def snapshot_untouchable(db):
    db.commit()
    rows = (db.execute("SELECT event_id,semantic_digest,action,accepted_generation "
                       "FROM canonical_actions ORDER BY event_id").fetchall(),
            db.execute("SELECT event_id,payload,accepted_generation FROM outbox "
                       "ORDER BY event_id").fetchall())
    db.commit()
    return rows


def run_scenario(db, seed, steps, lookback=LOOKBACK):
    from ton_sentinel.db.revisions import decide_revision

    world = World(db)
    world.seed(seed)
    oracle = Oracle(seed)
    base_epoch = readback(db)[3]
    base_generation = readback(db)[4]
    audit = snapshot_untouchable(db)
    results = []
    for step in steps:
        obs = step["obs"]
        evidence = world.candidate(obs, step.get("lookups", {}))
        stale = {g["ev"]: g.get("gen", "current") != "current"
                 for g in (step.get("auth") or {"grants": []})["grants"]}
        auth = world.authorization(step.get("auth"))
        lease = acquire_transition(db, "decider")
        changes = decide_revision(db, lease, evidence, auth)
        expected = oracle.step({"obs": obs, "lookups": step.get("lookups", {})},
                               step.get("auth"), stale)
        got = sorted((c.kind, c.event_id, c.reason) for c in changes)
        assert got == sorted(expected), (got, sorted(expected))
        state, opened, dirty, epoch_now, generation = readback(db)
        assert {ev: (r.disp, r.value) for ev, r in oracle.cur.items()} == \
            {ev: (s[0], s[1]) for ev, s in state.items()}
        assert {ev: r.src for ev, r in oracle.cur.items()} == {ev: s[2] for ev, s in state.items()}
        assert {ev for ev in state if state[ev][3]} == oracle.suppressed
        assert opened == {subject_text(k): v[0] for k, v in oracle.open.items()}
        assert dirty == oracle.dirty
        assert epoch_now - base_epoch == oracle.epoch
        assert generation - base_generation == oracle.generations
        # canonical audit rows and outbox are never retracted or rewritten
        assert snapshot_untouchable(db) == audit
        results.append(changes)
    return world, oracle, results


X = (1, 2)
Y = (1, 3)
Z = (4, 5)
ONLY_X = {1: {2}}

SCENARIOS = {
    "unordered_no_authorization": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}}]),
    "unordered_stable_after_repeat_still_quarantined": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}},
        {"obs": {X: B}, "lookups": {1: {2}}}]),
    "unordered_new_candidate_replaces_quarantine": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}},
        {"obs": {X: C}, "lookups": {1: {2}}}]),
    "unordered_grant_for_other_digest": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}},
         "auth": mono(revise(X, C, src=2, prev=1))}]),
    "unordered_authority_epoch_mismatch": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}},
         "auth": mono(revise(X, B, src=2, prev=1), epoch_ok=False)}]),
    "unordered_no_attested_baseline": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2))}]),
    "unordered_unrelated_grant_only": ({X: A, Z: A}, [
        {"obs": {X: B, Z: A}, "lookups": {1: {2}, 4: {5}},
         "auth": mono(revise(Z, A, src=2, prev=1))}]),
    "stale_revision_below_baseline": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=5, prev=4))},
        {"obs": {X: C}, "lookups": {1: {2}}, "auth": mono(revise(X, C, src=3))}]),
    "stale_attested_baseline_mismatch": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=5, prev=4))},
        {"obs": {X: C}, "lookups": {1: {2}}, "auth": mono(revise(X, C, src=9, prev=2))}]),
    "stale_authorization_generation": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}},
         "auth": mono(revise(X, B, src=2, prev=1, gen="stale"))}]),
    "stale_A_cannot_roll_back_B": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {X: A}, "lookups": {1: {2}}, "auth": mono(revise(X, A, src=3))}]),
    "equal_revision_different_digest": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {X: C}, "lookups": {1: {2}}, "auth": mono(revise(X, C, src=2))}]),
    "equal_revision_with_attested_equal_baseline": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=1, prev=1))}]),
    "accept_monotonic_time_move": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))}]),
    "accept_monotonic_token_move": ({X: A}, [
        {"obs": {X: C}, "lookups": {1: {2}}, "auth": mono(revise(X, C, src=2, prev=1))}]),
    "accept_monotonic_amount_only": ({X: A}, [
        {"obs": {X: val(o_amt="26")}, "lookups": {1: {2}},
         "auth": mono(revise(X, val(o_amt="26"), src=2, prev=1))}]),
    "accept_monotonic_chain_then_stale": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {X: C}, "lookups": {1: {2}}, "auth": mono(revise(X, C, src=3))},
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=4))}]),
    "accept_owner_resolution_without_source_revision": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": owner(revise(X, B))}]),
    "owner_resolution_may_restore_superseded_digest": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {X: A}, "lookups": {1: {2}}, "auth": owner(revise(X, A))}]),
    "quarantine_then_authorized_retry_resolves": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}},
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))}]),
    "reobserving_accepted_value_clears_quarantine": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}},
        {"obs": {X: A}, "lookups": {1: {2}}}]),
    "identical_reobservation_is_noop": ({X: A}, [
        {"obs": {X: A}, "lookups": {1: {2}}}]),
    "unrelated_traces_move_independently": ({X: A, Z: A}, [
        {"obs": {X: B, Z: C}, "lookups": {1: {2}, 4: {5}},
         "auth": mono(revise(X, B, src=2, prev=1))}]),
    "incomplete_trace_flag_with_valid_grant": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: "incomplete"},
         "auth": mono(revise(X, B, src=2, prev=1))}]),
    "unclassified_trace_with_valid_grant": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: "unclassified"},
         "auth": mono(revise(X, B, src=2, prev=1))}]),
    "missing_trace_lookup_404_is_not_evidence": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: None}, "auth": mono(revise(X, B, src=2, prev=1))}]),
    "trace_lookup_changes_between_passes": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: ("pair", {2}, {2, 3})},
         "auth": mono(revise(X, B, src=2, prev=1))}]),
    "trace_lookup_omits_observed_candidate": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: set()},
         "auth": owner(revise(X, B))}]),
    "disappearance_without_authorization": ({X: A}, [
        {"obs": {}, "lookups": {1: set()}}]),
    "disappearance_with_monotonic_contract_is_not_removal_authority": ({X: A}, [
        {"obs": {}, "lookups": {1: set()},
         "auth": mono(retract(X), trace_sets={1: set()})}]),
    "disappearance_authorized_by_owner": ({X: A}, [
        {"obs": {}, "lookups": {1: set()},
         "auth": owner(retract(X), trace_sets={1: set()})}]),
    "disappearance_owner_stale_generation": ({X: A}, [
        {"obs": {}, "lookups": {1: set()},
         "auth": owner(retract(X, gen="stale"), trace_sets={1: set()})}]),
    "disappearance_with_incomplete_lookup": ({X: A}, [
        {"obs": {}, "lookups": {1: "incomplete"}, "auth": owner(retract(X), trace_sets={1: set()})}]),
    "disappearance_with_404_lookup": ({X: A}, [
        {"obs": {}, "lookups": {}, "auth": owner(retract(X), trace_sets={1: set()})}]),
    "disappearance_lookup_still_lists_key": ({X: A}, [
        {"obs": {}, "lookups": {1: {2}}, "auth": owner(retract(X), trace_sets={1: set()})}]),
    "disappearance_then_reappearance_clears_quarantine": ({X: A}, [
        {"obs": {}, "lookups": {1: set()}},
        {"obs": {X: A}, "lookups": {1: {2}}}]),
    "reappearance_after_retraction_needs_authority": ({X: A}, [
        {"obs": {}, "lookups": {1: set()}, "auth": owner(retract(X), trace_sets={1: set()})},
        {"obs": {X: A}, "lookups": {1: {2}}}]),
    "reappearance_after_retraction_with_owner_authority": ({X: A}, [
        {"obs": {}, "lookups": {1: set()}, "auth": owner(retract(X), trace_sets={1: set()})},
        {"obs": {X: A}, "lookups": {1: {2}}, "auth": owner(revise(X, A), trace_sets={1: {2}})}]),
    "reappearance_after_retraction_with_newer_source_revision": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {}, "lookups": {1: set()}, "auth": owner(retract(X), trace_sets={1: set()})},
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=3), trace_sets={1: {2}})}]),
    "reappearance_after_retraction_with_equal_source_revision": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {}, "lookups": {1: set()}, "auth": owner(retract(X), trace_sets={1: set()})},
        {"obs": {X: B}, "lookups": {1: {2}},
         "auth": mono(revise(X, B, src=2), trace_sets={1: {2}})}]),
    "keyset_addition_without_authorization": ({X: A}, [
        {"obs": {X: A, Y: A}, "lookups": {1: {2, 3}}}]),
    "keyset_addition_authorized_atomically": ({X: A}, [
        {"obs": {X: A, Y: A}, "lookups": {1: {2, 3}},
         "auth": owner(add(Y, A), trace_sets={1: {2, 3}})}]),
    "keyset_addition_needs_trace_set_grant": ({X: A}, [
        {"obs": {X: A, Y: A}, "lookups": {1: {2, 3}}, "auth": owner(add(Y, A))}]),
    "keyset_addition_trace_set_disagrees_with_lookup": ({X: A}, [
        {"obs": {X: A, Y: A}, "lookups": {1: {2, 3}},
         "auth": owner(add(Y, A), trace_sets={1: {2}})}]),
    "keyset_replace_add_and_revise_with_monotonic": ({X: A}, [
        {"obs": {X: B, Y: A}, "lookups": {1: {2, 3}},
         "auth": mono(revise(X, B, src=2, prev=1), add(Y, A, src=1), trace_sets={1: {2, 3}})}]),
    "keyset_replace_one_member_stale_blocks_whole_set": ({X: A}, [
        {"obs": {X: B, Y: A}, "lookups": {1: {2, 3}},
         "auth": mono(revise(X, B, src=1, prev=1), add(Y, A, src=1), trace_sets={1: {2, 3}})}]),
    "keyset_replace_add_member_ungranted_blocks_whole_set": ({X: A}, [
        {"obs": {X: B, Y: A}, "lookups": {1: {2, 3}},
         "auth": mono(revise(X, B, src=2, prev=1), trace_sets={1: {2, 3}})}]),
    "keyset_replace_swap_ids_owner": ({X: A}, [
        {"obs": {Y: B}, "lookups": {1: {3}},
         "auth": owner(add(Y, B), retract(X), trace_sets={1: {3}})}]),
    "keyset_replace_swap_ids_missing_retract": ({X: A}, [
        {"obs": {Y: B}, "lookups": {1: {3}},
         "auth": owner(add(Y, B), trace_sets={1: {3}})}]),
    "keyset_removal_one_of_two_without_authorization": ({X: A, Y: B}, [
        {"obs": {X: A}, "lookups": {1: {2}}}]),
    "keyset_removal_one_of_two_authorized": ({X: A, Y: B}, [
        {"obs": {X: A}, "lookups": {1: {2}}, "auth": owner(retract(Y), trace_sets={1: {2}})}]),
    "keyset_change_incomplete_lookup": ({X: A}, [
        {"obs": {X: A, Y: A}, "lookups": {1: "incomplete"},
         "auth": owner(add(Y, A), trace_sets={1: {2, 3}})}]),
    "keyset_new_id_not_observed_in_range": ({X: A}, [
        {"obs": {X: A}, "lookups": {1: {2, 3}},
         "auth": owner(add(Y, A), trace_sets={1: {2, 3}})}]),
    "keyset_quarantine_suppresses_old_entries_then_resolves": ({X: A, Y: B}, [
        {"obs": {X: A, Y: B, (1, 6): C}, "lookups": {1: {2, 3, 6}}},
        {"obs": {X: A, Y: B}, "lookups": {1: {2, 3}}}]),
    "keyset_quarantine_then_authorized_retry": ({X: A}, [
        {"obs": {X: A, Y: A}, "lookups": {1: {2, 3}}},
        {"obs": {X: A, Y: A}, "lookups": {1: {2, 3}},
         "auth": owner(add(Y, A), trace_sets={1: {2, 3}})}]),
    "keyset_replacement_leaves_other_traces_alone": ({X: A, Z: A}, [
        {"obs": {X: A, Y: A, Z: B}, "lookups": {1: {2, 3}, 4: {5}},
         "auth": owner(add(Y, A), trace_sets={1: {2, 3}})}]),
    "retry_of_accepted_revision_is_noop": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))}]),
    "retry_of_quarantine_is_noop": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1, gen="stale"))},
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1, gen="stale"))}]),
    "retry_with_new_reason_updates_quarantine": ({X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}},
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=1, prev=1))}]),
}


@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_policy_oracle_agreement(db, name):
    seed, steps = SCENARIOS[name]
    run_scenario(db, seed, steps)


def test_unordered_change_never_wins_even_after_two_full_passes(db):
    world, oracle, (changes,) = run_scenario(db, {X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}}])
    assert [(c.kind, c.reason) for c in changes] == [("quarantine", "unauthorized_change")]
    row = db.execute("SELECT disposition,eligibility_suppressed,"
                     "action->'input'->>'raw_amount',action_end FROM ("
                     "SELECT *,(action->>'action_end')::bigint AS action_end "
                     "FROM canonical_current) q").fetchone()
    assert row == ("active", True, "100", 11)
    assert db.execute("SELECT count(*) FROM canonical_revisions").fetchone()[0] == 0


def test_authorized_revision_moves_event_and_marks_both_windows_and_dependents(db):
    run_scenario(db, {X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))}])
    rows = db.execute(
        "SELECT window_start,role,covers_start,covers_end FROM dirty_windows "
        "WHERE token='TON' ORDER BY window_start,role").fetchall()
    assert rows == [
        (0, "baseline_dependents", 300, LOOKBACK + 300), (0, "target", 0, 300),
        (600, "baseline_dependents", 900, LOOKBACK + 600 + 300), (600, "target", 600, 900)]
    assert db.execute("SELECT epoch FROM dependency_epoch").fetchone()[0] == 1


def test_token_move_dirties_old_and_new_token_windows(db):
    run_scenario(db, {X: A}, [
        {"obs": {X: C}, "lookups": {1: {2}}, "auth": mono(revise(X, C, src=2, prev=1))}])
    targets = {r for r in db.execute(
        "SELECT token,window_start FROM dirty_windows WHERE role='target'")}
    assert targets == {("TON", 0), ("jetton:A", 0), ("jetton:B", 0)}


def test_revision_history_is_immutable_and_current_pointer_is_unique(db):
    world, _, _ = run_scenario(db, {X: A}, [
        {"obs": {X: B}, "lookups": {1: {2}}, "auth": mono(revise(X, B, src=2, prev=1))}])
    rows = db.execute("SELECT accepted_generation,change_kind,previous_generation,"
                      "source_revision,contract FROM canonical_revisions").fetchall()
    assert rows == [(2, "revision", 1, 2, "source_monotonic_revision")]
    for sql in ("UPDATE canonical_revisions SET reason='x'", "DELETE FROM canonical_revisions"):
        with pytest.raises(psycopg.Error):
            db.execute(sql)
        db.rollback()


def test_direct_sql_cannot_retract_or_rewrite_current_without_a_revision(db):
    world = World(db)
    world.seed({X: A})
    from ton_sentinel.db.revisions import decide_revision
    decide_revision(db, acquire_transition(db, "d"),
                    world.candidate({X: A}, {1: {2}}))
    for sql in (
            "UPDATE canonical_current SET disposition='retracted'",
            "UPDATE canonical_current SET semantic_digest='sha256:forged'",
            "DELETE FROM canonical_current",
            "UPDATE canonical_current SET accepted_generation=accepted_generation+9"):
        with pytest.raises(psycopg.Error):
            db.execute(sql)
        db.rollback()
    db.execute("UPDATE canonical_current SET eligibility_suppressed=true")
    db.rollback()


def test_trace_keysets_view_replaces_whole_set_atomically_for_concurrent_readers(db):
    from ton_sentinel.db.revisions import decide_revision

    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: A, Y: A}, {1: {2, 3}})
    auth = world.authorization(owner(add(Y, A), trace_sets={1: {2, 3}}))
    seen = {}

    def during(tx):
        with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as reader:
            reader.execute(f'SET search_path TO "{db.schema_name}"')
            seen["inside"] = reader.execute(
                "SELECT action_ids FROM trace_keysets WHERE trace_id=%s", (b64(1),)).fetchone()
        assert tx.execute("SELECT action_ids FROM trace_keysets WHERE trace_id=%s",
                          (b64(1),)).fetchone()[0] == sorted([b64(2), b64(3)])

    decide_revision(db, acquire_transition(db, "d"), evidence, auth, commit_check=during)
    assert seen["inside"][0] == [b64(2)]
    db.commit()
    assert db.execute("SELECT action_ids FROM trace_keysets WHERE trace_id=%s",
                      (b64(1),)).fetchone()[0] == sorted([b64(2), b64(3)])


def test_failure_inside_decision_rolls_back_everything(db):
    from ton_sentinel.db.revisions import decide_revision

    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: B}, {1: {2}})
    auth = world.authorization(mono(revise(X, B, src=2, prev=1)))
    before = readback(db)
    audit = snapshot_untouchable(db)

    def explode(_tx):
        raise RuntimeError("boom after all writes")

    with pytest.raises(RuntimeError):
        decide_revision(db, acquire_transition(db, "d"), evidence, auth, commit_check=explode)
    db.rollback()
    assert readback(db) == before
    assert snapshot_untouchable(db) == audit
    assert db.execute("SELECT count(*) FROM canonical_revisions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM canonical_current").fetchone()[0] == 0


def test_expired_or_superseded_lease_is_rejected_without_writes(db):
    from ton_sentinel.db.fencing import LeaseRejected
    from ton_sentinel.db.revisions import decide_revision

    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: B}, {1: {2}})
    auth = world.authorization(mono(revise(X, B, src=2, prev=1)))
    stale = acquire_transition(db, "old-worker")
    acquire_transition(db, "new-worker")
    before = readback(db)
    with pytest.raises(LeaseRejected):
        decide_revision(db, stale, evidence, auth)
    db.rollback()
    expired = acquire_transition(db, "short", ttl_seconds=1)
    db.execute("SELECT pg_sleep(1.2)")
    db.commit()
    with pytest.raises(LeaseRejected):
        decide_revision(db, expired, evidence, auth)
    db.rollback()
    assert readback(db) == before


def test_unproven_evidence_returns_nothing_and_writes_nothing(db):
    from ton_sentinel.db.revisions import decide_revision
    from ton_sentinel.source.scan import WalkEvidence, scan_range

    world = World(db)
    world.seed({X: A})
    before = readback(db)
    lease = acquire_transition(db, "d")
    incomplete = WalkEvidence(str(uuid.uuid4()), False, 0, 0, "partial")
    assert decide_revision(db, lease, incomplete, None) == ()
    single = scan_range(db, Pages([payload(X, B)]), world.epoch, policy())
    lease = acquire_transition(db, "d")
    assert decide_revision(db, lease, walk_evidence(db, single), None) == ()
    assert readback(db) == before
    with pytest.raises(ValueError):
        decide_revision(db, lease, "not evidence", None)
    db.rollback()


def test_malformed_authorization_fails_closed_without_writes(db):
    from ton_sentinel.contracts import RevisionRejected
    from ton_sentinel.db.revisions import decide_revision

    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: B}, {1: {2}})
    before = readback(db)
    with pytest.raises(RevisionRejected):
        decide_revision(db, acquire_transition(db, "d"), evidence, {"contract": "nope"})
    db.rollback()
    assert readback(db) == before


def test_generations_are_one_per_accepted_change_and_none_for_quarantine(db):
    world, oracle, _ = run_scenario(db, {X: A, Z: A}, [
        {"obs": {X: B, Z: B}, "lookups": {1: {2}, 4: {5}},
         "auth": mono(revise(X, B, src=2, prev=1))},
        {"obs": {X: C, Z: B}, "lookups": {1: {2}, 4: {5}},
         "auth": mono(revise(X, C, src=3), revise(Z, B, src=2, prev=1))}])
    assert db.execute("SELECT accepted_generation FROM canonical_revisions "
                      "ORDER BY accepted_generation").fetchall() == [(3,), (4,), (5,)]
    assert db.execute("SELECT generation FROM canonical_generation").fetchone()[0] == 5


def test_baseline_dependents_follow_configured_lookback(db):
    from ton_sentinel.contracts import RevisionPolicy
    from ton_sentinel.db.revisions import decide_revision

    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: A}, {1: {2}})
    evidence = world.candidate({X: B}, {1: {2}})
    decide_revision(db, acquire_transition(db, "d"), evidence, None,
                    policy=RevisionPolicy(baseline_lookback_seconds=3600))
    rows = db.execute("SELECT covers_start,covers_end FROM dirty_windows "
                      "WHERE role='baseline_dependents' AND token='TON'").fetchall()
    assert rows == [(300, 3900)]


def test_ingest_entrypoint_acquires_fenced_lease_and_decides(db):
    from ton_sentinel.ingest import decide_corrections

    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: B}, {1: {2}})
    auth = world.authorization(mono(revise(X, B, src=2, prev=1)))
    changes = decide_corrections(db, owner="corrector", range_id=evidence.range_id,
                                 authorization=auth)
    assert [(c.kind, c.event_id) for c in changes] == [("revision", eid(X))]
    assert decide_corrections(db, owner="corrector", range_id=evidence.range_id,
                              authorization=auth) == ()


def test_two_concurrent_deciders_never_double_apply(db):
    from ton_sentinel.db.fencing import LeaseRejected
    from ton_sentinel.db.revisions import decide_revision

    world = World(db)
    world.seed({X: A})
    evidence = world.candidate({X: B}, {1: {2}})
    auth = world.authorization(mono(revise(X, B, src=2, prev=1)))
    outcomes = []
    barrier = threading.Barrier(2)

    def worker(name):
        with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as conn:
            conn.execute(f'SET search_path TO "{db.schema_name}"')
            lease = acquire_transition(conn, name)
            barrier.wait()
            try:
                outcomes.append(len(decide_revision(conn, lease, evidence, auth)))
            except LeaseRejected:
                outcomes.append("rejected")

    threads = [threading.Thread(target=worker, args=(n,)) for n in ("w1", "w2")]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert db.execute("SELECT count(*) FROM canonical_revisions").fetchone()[0] == 1
    assert sorted(map(str, outcomes)) == ["1", "rejected"]


def test_migration_003_repair_replays_idempotently_and_keeps_state(db):
    world = World(db)
    world.seed({X: A})
    migrate(db)
    migrate(db)
    assert db.execute("SELECT epoch FROM dependency_epoch").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 1
