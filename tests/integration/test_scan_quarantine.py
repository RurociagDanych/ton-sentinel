"""Durable quarantine for malformed local scan proposals in real PostgreSQL."""
import base64
import json
import os
import uuid

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import accept_scan, acquire_transition, migrate
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.scan import (
    ScanPolicy, resume_corroboration, resume_scan, scan_range, walk_evidence,
)


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "quarantine_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, topic="quarantine-test", topic_incarnation=schema)
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def epoch(source="synthetic:source"):
    return SourceEpoch("testnet", source, "synthetic:filter", "synthetic:build", "synthetic:policy")


def action(index, status="success"):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet",
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": 11, "status": status, "eligible": True,
        "route_proven": True, "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }


class Pages:
    def __init__(self, entries):
        self.entries = entries
        self.requests = 0

    def fetch_page(self, source_epoch, start, end, offset, limit):
        self.requests += 1
        return self.entries[offset:offset + limit]


def policy():
    return ScanPolicy(start=10, end=20, page_size=2, max_pages=4, max_actions=8)


def test_rejected_normalization_quarantines_page_and_preserves_valid_sibling(db):
    entries = [action(1), {**action(2), "status": "pending"}]
    transport = Pages(entries)
    range_id = scan_range(db, transport, epoch(), policy())

    assert transport.requests == 1
    assert db.execute("SELECT status FROM source_ranges WHERE range_id=%s", (range_id,)).fetchone()[0] == "incomplete"
    assert db.execute("SELECT payload FROM action_observations WHERE range_id=%s", (range_id,)).fetchall() == [(entries[0],)]
    quarantine = db.execute(
        "SELECT proposed_payload,reason,epoch,start_inclusive,end_exclusive FROM scan_quarantine WHERE range_id=%s",
        (range_id,),
    ).fetchone()
    assert quarantine == (entries[1], "status_not_success", json.loads(epoch().to_json()), 10, 20)

    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    with psycopg.connect(dsn) as reader:
        reader.execute(f'SET search_path TO "{db.execute("SELECT current_schema()").fetchone()[0]}"')
        assert reader.execute("SELECT count(*) FROM scan_quarantine WHERE range_id=%s", (range_id,)).fetchone()[0] == 1

    lease = acquire_transition(db, "quarantine-worker")
    assert accept_scan(db, lease, walk_evidence(db, range_id)) == ()
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


def test_schema_rejection_is_durable_idempotent_and_epoch_scoped(db):
    rejected = {**action(3), "unexpected": "field"}
    first = scan_range(db, Pages([rejected]), epoch(), policy())
    second = scan_range(db, Pages([rejected]), epoch("synthetic:other-source"), policy())
    assert db.execute("SELECT count(*) FROM scan_quarantine WHERE range_id=%s", (first,)).fetchone()[0] == 1
    retry = Pages([rejected])
    resume_scan(db, retry, first, epoch(), policy())
    assert retry.requests == 0
    assert db.execute("SELECT count(*) FROM scan_quarantine WHERE range_id=%s", (first,)).fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM scan_quarantine WHERE range_id=%s", (second,)).fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM action_observations WHERE range_id IN (%s,%s)", (first, second)).fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM source_ranges WHERE range_id IN (%s,%s) AND status='incomplete'", (first, second)).fetchone()[0] == 2


@pytest.mark.parametrize("entries", [
    [{**action(4), "status": "pending"}, action(5)],
    [action(6), {**action(7), "unexpected": "field"}],
    [action(8), action(9), {**action(10), "status": "pending"}, action(11)],
])
def test_public_resume_preserves_quarantined_sibling_without_refetch(db, entries):
    transport = Pages(entries)
    range_id = scan_range(db, transport, epoch(), policy())
    original = db.execute(
        "SELECT next_offset,page_count,action_count,request_count,status,reason "
        "FROM source_ranges WHERE range_id=%s", (range_id,),
    ).fetchone()
    proposals = db.execute(
        "SELECT ordinal,payload FROM action_observations WHERE range_id=%s ORDER BY ordinal",
        (range_id,),
    ).fetchall()
    quarantine = db.execute(
        "SELECT ordinal,proposed_payload,reason FROM scan_quarantine WHERE range_id=%s ORDER BY ordinal",
        (range_id,),
    ).fetchall()

    for _ in range(2):
        resume_scan(db, transport, range_id, epoch(), policy())
    assert transport.requests == (len(entries) + policy().page_size - 1) // policy().page_size
    assert db.execute(
        "SELECT next_offset,page_count,action_count,request_count,status,reason "
        "FROM source_ranges WHERE range_id=%s", (range_id,),
    ).fetchone() == original
    assert db.execute(
        "SELECT ordinal,payload FROM action_observations WHERE range_id=%s ORDER BY ordinal",
        (range_id,),
    ).fetchall() == proposals
    assert db.execute(
        "SELECT ordinal,proposed_payload,reason FROM scan_quarantine WHERE range_id=%s ORDER BY ordinal",
        (range_id,),
    ).fetchall() == quarantine
    assert original[4:] == ("incomplete", "action_quarantined")
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


def test_public_corroboration_resume_stops_at_quarantine(db):
    from ton_sentinel.source.scan import corroborate_range

    transport = Pages([{**action(12), "status": "pending"}, action(13)])
    group_id = corroborate_range(db, transport, epoch(), policy())
    child = db.execute(
        "SELECT range_id FROM corroboration_walks WHERE group_id=%s", (group_id,),
    ).fetchone()[0]
    snapshot = db.execute(
        "SELECT next_offset,page_count,action_count,request_count,status FROM source_ranges WHERE range_id=%s",
        (child,),
    ).fetchone()
    for _ in range(2):
        resume_corroboration(db, transport, group_id)
    assert transport.requests == 1
    assert db.execute(
        "SELECT next_offset,page_count,action_count,request_count,status FROM source_ranges WHERE range_id=%s",
        (child,),
    ).fetchone() == snapshot
    assert db.execute(
        "SELECT status FROM corroboration_groups WHERE group_id=%s", (group_id,),
    ).fetchone()[0] == "pending"
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


@pytest.mark.parametrize("damage", [
    "missing_rejected_prefix", "misplaced_rejection", "attempt_cursor_regression",
    "deleted_attempt_history",
])
def test_public_resume_refuses_corrupt_quarantine_evidence(db, damage):
    if damage == "missing_rejected_prefix":
        entries = [{**action(20), "status": "pending"},
                   {**action(21), "status": "pending"}]
        bounded = policy()
        range_id = scan_range(db, Pages(entries), epoch(), bounded)
        db.execute("DELETE FROM scan_quarantine WHERE range_id=%s AND ordinal=0", (range_id,))
    elif damage == "misplaced_rejection":
        entries = [action(22), {**action(23), "status": "pending"}]
        bounded = policy()
        range_id = scan_range(db, Pages(entries), epoch(), bounded)
        db.execute("UPDATE scan_quarantine SET ordinal=999 WHERE range_id=%s", (range_id,))
    elif damage == "attempt_cursor_regression":
        entries = [action(24), action(25), action(26), action(27),
                   {**action(28), "status": "pending"}]
        bounded = ScanPolicy(start=10, end=20, page_size=2, max_pages=4, max_actions=8)
        range_id = scan_range(db, Pages(entries), epoch(), bounded)
        db.execute("UPDATE scan_page_attempts SET requested_offset=0 "
                   "WHERE range_id=%s AND attempt_no=3", (range_id,))
    else:
        entries = [action(29), {**action(30), "status": "pending"}]
        bounded = policy()
        range_id = scan_range(db, Pages(entries), epoch(), bounded)
        db.execute("DELETE FROM scan_page_attempts WHERE range_id=%s", (range_id,))
        db.execute("UPDATE source_ranges SET page_count=0 WHERE range_id=%s", (range_id,))

    db.commit()
    retry = Pages(entries)
    with pytest.raises(ValueError, match="checkpoint|evidence|attempt|ordinal|terminal"):
        resume_scan(db, retry, range_id, epoch(), bounded)
    assert retry.requests == 0
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0


def test_valid_mult_page_quarantine_resume_keeps_checkpoint_unchanged(db):
    entries = [action(31), action(32), action(33), action(34),
               {**action(35), "status": "pending"}]
    bounded = ScanPolicy(start=10, end=20, page_size=2, max_pages=4, max_actions=8)
    source = Pages(entries)
    range_id = scan_range(db, source, epoch(), bounded)
    assert db.execute(
        "SELECT requested_offset FROM scan_page_attempts WHERE range_id=%s ORDER BY attempt_no",
        (range_id,),
    ).fetchall() == [(0,), (2,), (4,)]
    assert db.execute("SELECT ordinal FROM scan_quarantine WHERE range_id=%s", (range_id,)).fetchall() == [(4,)]
    checkpoint = db.execute(
        "SELECT next_offset,page_count,action_count,request_count,status,reason "
        "FROM source_ranges WHERE range_id=%s", (range_id,),
    ).fetchone()

    retry = Pages(entries)
    for _ in range(2):
        resume_scan(db, retry, range_id, epoch(), bounded)
    assert retry.requests == 0
    assert db.execute(
        "SELECT next_offset,page_count,action_count,request_count,status,reason "
        "FROM source_ranges WHERE range_id=%s", (range_id,),
    ).fetchone() == checkpoint
