"""Admission rules in real PostgreSQL: historical start fit, absent fields, certified off."""
import base64
import os
import uuid

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import migrate
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.scan import ScanPolicy, scan_range


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "admission_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, topic="admission-test", topic_incarnation=schema)
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def epoch():
    return SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                       "synthetic:build", "synthetic:policy")


def action(index, action_end=15):
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


class Source:
    """Serves a 'latest' action (end 1000) for any window that includes it, never otherwise."""

    def __init__(self, entries):
        self.entries, self.requests = entries, []

    def fetch_page(self, source_epoch, start, end, offset, limit):
        self.requests.append((start, end, offset, limit))
        inside = [e for e in self.entries if start <= e["action_end"] < end]
        return inside[offset:offset + limit]


def rows(db, table):
    return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def test_historical_start_over_load_budget_fails_without_latest_fallback(db):
    # Authorized load = 2 actions; the explicit historical window holds 3.
    history = [action(i, 12) for i in (1, 2, 3)]
    latest = action(9, 1000)
    source = Source(history + [latest])
    pol = ScanPolicy(start=10, end=20, page_size=2, max_pages=1, max_actions=2)
    range_id = scan_range(db, source, epoch(), pol)
    db.commit()
    status, reason = db.execute("SELECT status,reason FROM source_ranges WHERE range_id=%s",
                                (range_id,)).fetchone()
    assert status == "incomplete" and reason == "ValueError"  # sanitized failure class
    # Only the explicit window was ever requested: no silent move to latest.
    assert source.requests and {(r[0], r[1]) for r in source.requests} == {(10, 20)}
    assert rows(db, "canonical_actions") == rows(db, "outbox") == 0
    assert db.execute("SELECT count(*) FROM source_ranges WHERE status='accepted'"
                      ).fetchone()[0] == 0


def test_historical_start_within_budget_scans_exactly_the_requested_window(db):
    source = Source([action(1, 12), action(9, 1000)])
    pol = ScanPolicy(start=10, end=20, page_size=2, max_pages=3, max_actions=4)
    range_id = scan_range(db, source, epoch(), pol)
    db.commit()
    assert {(r[0], r[1]) for r in source.requests} == {(10, 20)}
    assert db.execute("SELECT start_inclusive,end_exclusive,status FROM source_ranges "
                      "WHERE range_id=%s", (range_id,)).fetchone() == (10, 20, "complete")
    assert db.execute("SELECT count(*) FROM action_observations WHERE range_id=%s",
                      (range_id,)).fetchone()[0] == 1


@pytest.mark.parametrize("bad", [
    dict(start=-1, end=20), dict(start=20, end=20), dict(start=30, end=20),
    dict(start=10, end=2**63),
])
def test_unbounded_or_inverted_historical_window_is_rejected_before_any_row(db, bad):
    with pytest.raises(ValueError):
        ScanPolicy(page_size=2, max_pages=2, max_actions=4, **bad)
    assert rows(db, "source_ranges") == 0


@pytest.mark.parametrize("missing", ["status", "eligible", "route_proven", "action_end"])
def test_absent_finality_or_success_field_quarantines_in_pg(db, missing):
    broken = {k: v for k, v in action(2).items() if k != missing}
    source = Source([action(1), broken]) if missing != "action_end" else None
    if source is None:
        class Raw:
            requests = []

            def fetch_page(self, e, s, en, offset, limit):
                return [action(1), broken][offset:offset + limit]
        source = Raw()
    pol = ScanPolicy(start=10, end=20, page_size=2, max_pages=3, max_actions=6)
    range_id = scan_range(db, source, epoch(), pol)
    db.commit()
    assert db.execute("SELECT status FROM source_ranges WHERE range_id=%s",
                      (range_id,)).fetchone()[0] == "incomplete"
    assert db.execute("SELECT count(*) FROM scan_quarantine WHERE range_id=%s",
                      (range_id,)).fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM action_observations WHERE range_id=%s",
                      (range_id,)).fetchone()[0] == 1
    assert rows(db, "canonical_actions") == rows(db, "outbox") == 0


def test_certified_coverage_cannot_be_stored(db):
    range_id = scan_range(db, Source([action(1)]), epoch(),
                          ScanPolicy(start=10, end=20, page_size=2, max_pages=3, max_actions=6))
    db.commit()
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("INSERT INTO coverage_ranges(range_id,status) VALUES (%s,'certified')",
                   (range_id,))
    db.rollback()
    assert db.execute("SELECT count(*) FROM coverage_ranges WHERE status='certified'"
                      ).fetchone()[0] == 0
