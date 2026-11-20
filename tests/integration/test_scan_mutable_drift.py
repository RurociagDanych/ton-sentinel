"""Mutable-source drift limits, invisible mutation and fail-closed sources (real PostgreSQL)."""

import base64
import math
import os
import uuid

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import accept_corroborated_range, acquire_transition, migrate
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.coverage import action_window_coverage
from ton_sentinel.source.scan import ScanPolicy, corroborate_range, scan_range


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "drift_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, topic="drift-test", topic_incarnation=schema)
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def epoch():
    return SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                       "synthetic:build", "synthetic:policy")


def action(index, action_end=15):
    """Distinct actions that may share one action_end timestamp."""
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet",
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": action_end, "status": "success", "eligible": True,
        "route_proven": True, "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }


class Walks:
    """Transport serving one list per walk; counts every request."""
    def __init__(self, walks):
        self.walks = list(walks)
        self.walk = -1
        self.requests = []

    def fetch_page(self, source_epoch, start, end, offset, limit):
        self.requests.append((start, end, offset, limit))
        if offset == 0:
            self.walk += 1
        walk = self.walks[min(self.walk, len(self.walks) - 1)]
        return walk[offset:offset + limit]


def policy(page_size=2, max_requests=60, max_pages=20):
    return ScanPolicy(start=10, end=20, page_size=page_size, max_pages=max_pages,
                      max_actions=page_size * max_pages, max_bytes=1_000_000,
                      max_seconds=30, max_requests=max_requests)


def count(db, table):
    return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def group_state(db, group_id):
    return db.execute("SELECT status,walk_count,accepted_range_id FROM corroboration_groups "
                      "WHERE group_id=%s", (group_id,)).fetchone()


@pytest.mark.parametrize("descending", [False, True])
@pytest.mark.parametrize("page_size", [1, 2, 3, 6])
def test_mutable_drift_limits_repeat_timestamps_both_sorts_and_page_sizes(
        db, page_size, descending):
    items = [action(i) for i in range(1, 7)]
    first, second = (list(reversed(items)), items) if descending else (items, list(reversed(items)))
    transport = Walks([first, second])
    pol = policy(page_size)
    group = corroborate_range(db, transport, epoch(), pol)
    db.commit()
    # sort order is not part of the key set: two agreeing walks corroborate
    status, walks, accepted = group_state(db, group)
    assert (status, walks) == ("corroborated", 2) and accepted is not None
    # fixed bound: exactly two walks of ceil(6/ps)+1 requests (empty/short terminator)
    pages_per_walk = math.floor(6 / page_size) + 1
    assert len(transport.requests) == 2 * pages_per_walk <= pol.max_requests
    assert {(r[0], r[1], r[3]) for r in transport.requests} == {(10, 20, page_size)}
    lease = acquire_transition(db, "drift-bounds")
    assert accept_corroborated_range(db, lease, group)
    assert count(db, "canonical_actions") == count(db, "outbox") == 6


def test_mutable_drift_mismatches_reset_and_stop_at_three_walks_pending(db):
    a, b, c = [action(1)], [action(1), action(2)], [action(3)]
    transport = Walks([a, b, c, a, a])
    group = corroborate_range(db, transport, epoch(), policy())
    assert group_state(db, group) == ("pending", 3, None)
    assert transport.walk == 2
    lease = acquire_transition(db, "drift-pending")
    assert not accept_corroborated_range(db, lease, group)
    db.commit()
    assert count(db, "canonical_actions") == count(db, "outbox") == count(db, "scan_membership") == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM source_ranges WHERE status='accepted'"
                      ).fetchone()[0] == 0


def test_mutable_drift_exhausted_request_budget_remains_pending(db):
    transport = Walks([[action(1), action(2)], [action(1), action(2)]])
    group = corroborate_range(db, transport, epoch(), policy(page_size=1, max_requests=3))
    status, _, accepted = group_state(db, group)
    assert status == "pending" and accepted is None
    assert len(transport.requests) <= 3
    assert count(db, "canonical_actions") == 0 and count(db, "outbox") == 0


def test_invisible_mutation_not_certified_and_later_visible_sweep_discovers_it(db):
    truth = [action(1), action(2)]
    hidden_view = [action(1)]
    group = corroborate_range(db, Walks([hidden_view, hidden_view]), epoch(), policy())
    lease = acquire_transition(db, "invisible")
    assert accept_corroborated_range(db, lease, group)
    db.commit()
    accepted_keys = {r[0] for r in db.execute("SELECT event_id FROM canonical_actions").fetchall()}
    assert len(accepted_keys) == 1 and len(truth) == 2
    db.commit()
    coverage = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    db.commit()
    assert coverage.status == "missing" and coverage.eligible_zero is False
    assert {r[0] for r in db.execute("SELECT status FROM coverage_ranges").fetchall()} == {
        "bounded_observed"}
    # the range's own status is bounded-observed, never certified
    assert db.execute("SELECT count(*) FROM coverage_ranges WHERE status='certified'"
                      ).fetchone()[0] == 0

    sweep = corroborate_range(db, Walks([truth, truth]), epoch(), policy())
    assert accept_corroborated_range(db, acquire_transition(db, "sweep"), sweep)
    db.commit()
    assert count(db, "canonical_actions") == count(db, "outbox") == 2
    db.commit()
    after = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    assert after.status == "missing" and after.eligible_zero is False
    assert db.execute("SELECT count(*) FROM coverage_ranges WHERE status='certified'"
                      ).fetchone()[0] == 0


class Failing:
    def __init__(self, exc, ok_pages=0):
        self.exc, self.ok_pages, self.requests = exc, ok_pages, 0

    def fetch_page(self, source_epoch, start, end, offset, limit):
        self.requests += 1
        if self.requests > self.ok_pages:
            raise self.exc
        return [action(1)][offset:offset + limit]


@pytest.mark.parametrize("failure", [
    ValueError("source rejected authentication"),
    ValueError("source schema or filter mismatch"),
    OSError("connection reset"),
])
def test_source_fail_closed_stops_without_fallback_or_acceptance(db, failure):
    transport = Failing(failure)
    range_id = scan_range(db, transport, epoch(), policy(page_size=1))
    db.commit()
    assert transport.requests >= 1
    status = db.execute("SELECT status FROM source_ranges WHERE range_id=%s", (range_id,)).fetchone()[0]
    assert status != "complete" and status != "accepted"
    assert count(db, "canonical_actions") == count(db, "outbox") == count(db, "scan_membership") == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM coverage_ranges WHERE status<>'pending'"
                      ).fetchone()[0] == 0


def test_competing_range_promotions_serialize_without_duplicates_or_deadlock(db):
    import threading

    shared = action(1)
    first = corroborate_range(db, Walks([[shared, action(2)]] * 2), epoch(), policy())
    second = corroborate_range(db, Walks([[shared, action(3)]] * 2), epoch(), policy())
    lease = acquire_transition(db, "competitor")
    db.commit()
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    db.commit()
    results, errors = {}, []

    def promote(name, group):
        try:
            with psycopg.connect(dsn, options=f"-c search_path={schema}") as conn:
                results[name] = accept_corroborated_range(conn, lease, group)
        except Exception as exc:  # pragma: no cover - failure reported below
            errors.append(exc)

    threads = [threading.Thread(target=promote, args=("a", first)),
               threading.Thread(target=promote, args=("b", second))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert not any(t.is_alive() for t in threads) and not errors, errors
    assert results["a"] and results["b"]
    assert count(db, "canonical_actions") == count(db, "outbox") == 3
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    assert db.execute("SELECT count(*) FROM source_ranges WHERE status='accepted'"
                      ).fetchone()[0] == 2
