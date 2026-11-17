"""Real PostgreSQL behavior for restarting an interrupted bounded scan."""
import base64
import json
import os
import time
import uuid
from pathlib import Path

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import (accept_corroborated_range, accept_scan,
                                    acquire_transition, migrate)
from ton_sentinel.outbox import configure_delivery
import ton_sentinel.source.scan as scan_module
from ton_sentinel.source.scan import (ScanPolicy, corroborate_range, scan_range,
                                      walk_evidence)


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "resume_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, topic="resume-test", topic_incarnation=schema)
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def epoch():
    return SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                       "synthetic:build", "synthetic:policy")


def action(index):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet",
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": 10 + index, "status": "success", "eligible": True,
        "route_proven": True, "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }


class Pages:
    def __init__(self, values, fail_at=None):
        self.values = values
        self.fail_at = fail_at
        self.requests = []

    def fetch_page(self, source_epoch, start, end, offset, limit):
        self.requests.append((source_epoch, start, end, offset, limit))
        if offset == self.fail_at:
            raise OSError("synthetic interrupted page")
        return self.values[offset:offset + limit]


class RepeatedPages:
    def __init__(self, values):
        self.values = values

    def fetch_page(self, source_epoch, start, end, offset, limit):
        return self.values[offset:offset + limit]


def policy(**changes):
    values = dict(start=10, end=20, page_size=1, max_pages=4, max_actions=4)
    values.update(changes)
    return ScanPolicy(**values)


def rows(db, table):
    return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def corroborate(db, values):
    bounded = policy(max_pages=8, max_actions=8, max_bytes=100_000,
                     max_seconds=30, max_requests=10)
    group_id = corroborate_range(db, RepeatedPages(values), epoch(), bounded)
    selected = db.execute(
        "SELECT accepted_range_id FROM corroboration_groups "
        "WHERE group_id=%s AND status='corroborated'", (group_id,),
    ).fetchone()
    assert selected is not None and selected[0] is not None
    db.commit()
    return group_id, str(selected[0])





def test_interrupted_range_resumes_from_durable_checkpoint_and_promotes_once(db):
    source = Pages([action(1), action(2)], fail_at=1)
    original_epoch = epoch()
    original_policy = policy()
    range_id = scan_range(db, source, original_epoch, original_policy)
    assert db.execute(
        "SELECT status FROM source_ranges WHERE range_id=%s", (range_id,)
    ).fetchone()[0] == "incomplete"
    assert rows(db, "action_observations") == 1
    assert rows(db, "canonical_actions") == 0
    assert rows(db, "outbox") == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0

    resume_scan = getattr(scan_module, "resume_scan", None)
    assert callable(resume_scan), "scan module must provide durable resume_scan"
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as restarted:
        restarted.execute(f'SET search_path TO "{schema}"')
        resumed = Pages([action(1), action(2)])
        assert resume_scan(restarted, resumed, range_id, original_epoch, original_policy) == range_id
        assert [request[3] for request in resumed.requests] == [1, 2]
        assert all(request[:3] == (original_epoch, 10, 20) for request in resumed.requests)
        assert rows(restarted, "action_observations") == 2
        assert rows(restarted, "canonical_actions") == 0
        assert rows(restarted, "outbox") == 0
        assert restarted.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0

        lease = acquire_transition(restarted, "resume-worker")
        evidence = walk_evidence(restarted, range_id)
        assert evidence.complete
        assert accept_scan(restarted, lease, evidence) == ()
        assert accept_scan(restarted, lease, evidence) == ()
        assert rows(restarted, "canonical_actions") == 0
        assert rows(restarted, "outbox") == 0
        assert restarted.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0

        group_id, selected = corroborate(restarted, [action(1), action(2)])
        assert selected != range_id
        lease = acquire_transition(restarted, "corroborated-resume-worker")
        assert accept_corroborated_range(restarted, lease, group_id)
        assert accept_corroborated_range(restarted, lease, group_id)
        assert rows(restarted, "canonical_actions") == 2
        assert rows(restarted, "outbox") == 2
        assert restarted.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    assert rows(db, "canonical_actions") == 2
    assert rows(db, "outbox") == 2
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20


def test_resume_rejects_changed_epoch_or_bounds_and_exhausted_budget_stays_incomplete(db):
    original_epoch = epoch()
    original_policy = policy(max_pages=1, max_actions=1)
    range_id = scan_range(db, Pages([action(1)]), original_epoch, original_policy)
    resume_scan = getattr(scan_module, "resume_scan", None)
    assert callable(resume_scan), "scan module must provide durable resume_scan"

    with pytest.raises(ValueError, match="immutable"):
        resume_scan(db, Pages([action(1)]), range_id, original_epoch,
                    policy(max_pages=1, max_actions=1, end=21))
    changed_epoch = SourceEpoch("testnet", "synthetic:source-v2", "synthetic:filter",
                                "synthetic:build", "synthetic:policy")
    with pytest.raises(ValueError, match="immutable"):
        resume_scan(db, Pages([action(1)]), range_id, changed_epoch, original_policy)

    limited = Pages([action(1)])
    assert resume_scan(db, limited, range_id, original_epoch, original_policy) == range_id
    status, reason = db.execute(
        "SELECT status,reason FROM source_ranges WHERE range_id=%s", (range_id,)
    ).fetchone()
    assert (status, reason) == ("incomplete", "ValueError")
    assert rows(db, "action_observations") == 1
    assert rows(db, "canonical_actions") == 0
    assert rows(db, "outbox") == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0


def test_failed_fetch_attempts_are_durable_and_exhaust_across_resumes(db):
    source = Pages([], fail_at=0)
    bounded = policy(max_pages=2, max_actions=2)
    range_id = scan_range(db, source, epoch(), bounded)
    assert db.execute(
        "SELECT page_count,next_offset FROM source_ranges WHERE range_id=%s", (range_id,)
    ).fetchone() == (1, 0)
    db.commit()
    for _ in range(3):
        scan_module.resume_scan(db, source, range_id, epoch(), bounded)
    assert len(source.requests) == 2
    assert not walk_evidence(db, range_id).complete


def test_page_attempt_is_committed_before_transport_is_called(db):
    schema = db.execute("SELECT current_schema()").fetchone()[0]

    class InspectAttempt(Pages):
        def fetch_page(self, *args):
            with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as reader:
                reader.execute(f'SET search_path TO "{schema}"')
                assert reader.execute("SELECT page_count FROM source_ranges").fetchone() == (1,)
            return super().fetch_page(*args)

    source = InspectAttempt([])
    range_id = scan_range(db, source, epoch(), policy())
    assert walk_evidence(db, range_id).complete


@pytest.mark.parametrize("damage", ["oversized", "normalization", "non_bounded"])
def test_rejected_page_bytes_remain_charged_and_cannot_be_refetched(db, damage):
    raw = action(1)
    if damage == "normalization":
        raw["status"] = "invalid"
    page = [raw] if damage != "non_bounded" else {"unexpected": raw}
    encoded_values = page if isinstance(page, list) else [page]
    byte_count = sum(len(json.dumps(value, ensure_ascii=True, separators=(",", ":"),
                                    sort_keys=True).encode("ascii"))
                     for value in encoded_values)
    bounded = policy(max_bytes=byte_count - 1 if damage == "oversized" else byte_count)

    class RejectedPage(Pages):
        def fetch_page(self, *args):
            self.requests.append(args)
            return page

    source = RejectedPage([])
    range_id = scan_range(db, source, epoch(), bounded)
    assert db.execute(
        "SELECT page_count,bytes_used,action_count,next_offset FROM source_ranges WHERE range_id=%s",
        (range_id,),
    ).fetchone() == (1, byte_count, 0, 0)
    db.commit()
    for _ in range(2):
        scan_module.resume_scan(db, source, range_id, epoch(), bounded)
    assert len(source.requests) == 1
    assert not walk_evidence(db, range_id).complete


def test_failed_fetch_time_is_charged_and_exhaustion_is_durable(db):
    class SlowFailure(Pages):
        def fetch_page(self, *args):
            time.sleep(1.02)
            return super().fetch_page(*args)

    source = SlowFailure([], fail_at=0)
    bounded = policy(max_seconds=1)
    range_id = scan_range(db, source, epoch(), bounded)
    assert db.execute("SELECT elapsed_ms FROM source_ranges WHERE range_id=%s",
                      (range_id,)).fetchone()[0] >= 1000
    db.commit()
    scan_module.resume_scan(db, source, range_id, epoch(), bounded)
    assert len(source.requests) == 1


@pytest.mark.parametrize("damage", ["offset", "missing", "payload", "pages", "actions"])
def test_resume_rejects_inconsistent_checkpoint_before_fetch(db, damage):
    bounded = policy()
    range_id = scan_range(db, Pages([action(1), action(2)], fail_at=1), epoch(), bounded)
    statements = {
        "offset": "UPDATE source_ranges SET next_offset=2 WHERE range_id=%s",
        "missing": "DELETE FROM action_observations WHERE range_id=%s",
        "payload": "UPDATE action_observations SET payload=jsonb_set(payload,'{output,raw_amount}',"
                   "'\"99\"') WHERE range_id=%s",
        "pages": "UPDATE source_ranges SET page_count=0 WHERE range_id=%s",
        "actions": "UPDATE source_ranges SET action_count=0 WHERE range_id=%s",
    }
    db.execute(statements[damage], (range_id,))
    db.commit()
    source = Pages([action(1), action(2)])
    with pytest.raises(ValueError, match="checkpoint|evidence"):
        scan_module.resume_scan(db, source, range_id, epoch(), bounded)
    assert source.requests == []
    assert rows(db, "canonical_actions") == rows(db, "outbox") == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0


@pytest.mark.parametrize("damage", ["missing", "payload", "ordinal", "offset", "pages"])
def test_acceptance_revalidates_completed_evidence_in_its_transaction(db, damage):
    _, range_id = corroborate(db, [action(1), action(2)])
    evidence = walk_evidence(db, range_id)
    assert evidence.complete
    statements = {
        "missing": "DELETE FROM action_observations WHERE range_id=%s AND ordinal=0",
        "payload": "UPDATE action_observations SET payload=jsonb_set(payload,'{output,raw_amount}',"
                   "'\"99\"') WHERE range_id=%s AND ordinal=0",
        "ordinal": "UPDATE action_observations SET ordinal=7 WHERE range_id=%s AND ordinal=0",
        "offset": "UPDATE source_ranges SET next_offset=3 WHERE range_id=%s",
        "pages": "UPDATE source_ranges SET page_count=1 WHERE range_id=%s",
    }
    db.execute(statements[damage], (range_id,))
    db.commit()
    lease = acquire_transition(db, "accept-worker")
    with pytest.raises(ValueError, match="checkpoint|evidence"):
        accept_scan(db, lease, evidence)
    assert rows(db, "canonical_actions") == rows(db, "outbox") == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
    assert db.execute("SELECT epoch FROM active_source_epoch WHERE singleton=true").fetchone()[0] is None


def test_migration_adopts_baseline_complete_scan_without_bare_acceptance_and_is_idempotent(db):
    schema = "baseline_resume_" + uuid.uuid4().hex
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    migration001 = Path(__file__).resolve().parents[2] / "src/ton_sentinel/db/migrations/001_ledger.sql"
    range_id = str(uuid.uuid4())
    baseline_epoch = epoch()
    try:
        with psycopg.connect(dsn) as legacy:
            legacy.execute(f'SET search_path TO "{schema}"')
            legacy.execute(migration001.read_text())
            legacy.execute(
                "INSERT INTO source_ranges(range_id,start_inclusive,end_exclusive,epoch,status,"
                "page_count,action_count) VALUES (%s,10,20,%s::jsonb,'complete',3,2)",
                (range_id, baseline_epoch.to_json()),
            )
            legacy.execute(
                "INSERT INTO coverage_ranges(range_id,status) VALUES (%s,'bounded_observed')",
                (range_id,),
            )
            with legacy.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO action_observations(range_id,ordinal,payload) "
                    "VALUES (%s,%s,%s::jsonb)",
                    [(range_id, ordinal, json.dumps(action(index), separators=(",", ":")))
                     for ordinal, index in enumerate((1, 2))],
                )
            legacy.commit()

            # The rows above use only baseline-001 columns and counts: complete,
            # 3 fetched pages, 2 contiguous observations, and no current manifest.
            migrate(legacy)
            migrate(legacy)
            assert legacy.execute(
                "SELECT status,page_count,action_count,scan_policy,legacy_resume "
                "FROM source_ranges WHERE range_id=%s", (range_id,),
            ).fetchone() == ("complete", 3, 2, None, True)
            assert legacy.execute(
                "SELECT count(*) FROM action_observations WHERE range_id=%s", (range_id,),
            ).fetchone()[0] == 2
            assert rows(legacy, "canonical_actions") == 0
            assert rows(legacy, "outbox") == 0
            assert legacy.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
            legacy.commit()

            evidence = walk_evidence(legacy, range_id)
            assert (evidence.complete, evidence.pages, evidence.actions) == (True, 3, 2)
            lease = acquire_transition(legacy, "legacy-migration-worker")
            assert accept_scan(legacy, lease, evidence) == ()
            assert accept_scan(legacy, lease, evidence) == ()
            assert rows(legacy, "canonical_actions") == 0
            assert rows(legacy, "outbox") == 0
            assert legacy.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0

            legacy.commit()
            configure_delivery(legacy, topic="migrated-actions", topic_incarnation=schema)
            group_id, _ = corroborate(legacy, [action(3), action(4)])
            lease = acquire_transition(legacy, "corroborated-migration-worker")
            assert accept_corroborated_range(legacy, lease, group_id)
            assert accept_corroborated_range(legacy, lease, group_id)
            assert rows(legacy, "canonical_actions") == 2
            assert rows(legacy, "outbox") == 2
            assert legacy.execute(
                "SELECT count(*) FROM scan_membership sm JOIN source_ranges sr USING(range_id) "
                "WHERE sr.status='accepted'",
            ).fetchone()[0] == 2
            assert legacy.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def test_migration_rerun_does_not_adopt_new_scan_with_missing_manifest(db):
    source = Pages([action(1), action(2)], fail_at=1)
    bounded = policy()
    range_id = scan_range(db, source, epoch(), bounded)
    db.execute("UPDATE source_ranges SET scan_policy=NULL WHERE range_id=%s", (range_id,))
    db.commit()

    migrate(db)
    migrate(db)
    assert db.execute(
        "SELECT status,scan_policy,legacy_resume FROM source_ranges WHERE range_id=%s",
        (range_id,),
    ).fetchone() == ("incomplete", None, False)

    resume_scan = getattr(scan_module, "resume_scan", None)
    assert callable(resume_scan)
    retry_source = Pages([action(1), action(2)])
    with pytest.raises(ValueError, match="immutable|manifest"):
        resume_scan(db, retry_source, range_id, epoch(), bounded)
    assert retry_source.requests == []
