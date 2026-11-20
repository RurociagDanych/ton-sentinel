"""Durable real-PostgreSQL corroboration of complete mutable-source walks."""

import base64
import os
import uuid

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import acquire_transition, migrate
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.scan import ScanPolicy, corroborate_range


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "corroboration_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, topic="corroboration-test", topic_incarnation=schema)
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


class FullWalks:
    def __init__(self, walks, on_fetch=None):
        self.walks = list(walks)
        self.walk_index = -1
        self.offset = 0
        self.fail_at_walk: int | None = None
        self.on_fetch = on_fetch

    def fetch_page(self, source_epoch, start, end, offset, limit):
        if self.on_fetch:
            self.on_fetch(self.walk_index + (offset == 0))
        if offset == 0:
            self.walk_index += 1
            self.offset = 0
        if self.walk_index == self.fail_at_walk and offset > 0:
            raise RuntimeError("synthetic partial walk")
        page = self.walks[self.walk_index]
        result = page[offset:offset + limit]
        self.offset += len(result)
        return result


def policy():
    return ScanPolicy(start=10, end=20, page_size=2, max_pages=4,
                      max_actions=8, max_bytes=100_000, max_seconds=30,
                      max_requests=20)


@pytest.mark.parametrize(("walks", "accepted"), [
    ([[action(1)], [action(1)]], True),
    ([[action(1)], [action(2)], [action(2)]], True),
    ([[action(1)], [action(2)], [action(1)]], False),
    ([[action(1), action(2)], [action(1), action(3)]], False),
    ([[], []], True),
])
def test_only_two_consecutive_identical_complete_key_sets_are_accepted(db, walks, accepted):
    transport = FullWalks(walks)
    group_id = corroborate_range(db, transport, epoch(), policy())
    evidence = db.execute(
        "SELECT status,walk_count,accepted_range_id FROM corroboration_groups WHERE group_id=%s",
        (group_id,),
    ).fetchone()
    lease = acquire_transition(db, "worker-corroboration")
    from ton_sentinel.db.ledger import accept_corroborated_range
    promoted = accept_corroborated_range(db, lease, group_id)
    independent = db.execute(
        "SELECT status,walk_count,accepted_range_id FROM corroboration_groups WHERE group_id=%s",
        (group_id,),
    ).fetchone()
    assert independent[0] == ("accepted" if accepted else "pending")
    assert independent[1] == len(walks)
    assert (independent[2] is not None) is accepted
    assert bool(promoted) is accepted
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == (
        len(walks[-1]) if accepted else 0
    )


def test_partial_walk_is_not_empty_and_cannot_be_used_as_a_match(db):
    transport = FullWalks([[action(1)], [action(2), action(3)]])
    transport.fail_at_walk = 1
    group_id = corroborate_range(db, transport, epoch(), policy())
    state = db.execute(
        "SELECT status,walk_count,accepted_range_id FROM corroboration_groups WHERE group_id=%s",
        (group_id,),
    ).fetchone()
    assert state == ("pending", 2, None)
    assert db.execute(
        "SELECT r.status FROM corroboration_walks w JOIN source_ranges r USING(range_id) "
        "WHERE w.group_id=%s ORDER BY w.walk_no DESC LIMIT 1", (group_id,),
    ).fetchone()[0] == "incomplete"
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0


def test_all_pass_budgets_are_cumulative_and_persisted(db):
    transport = FullWalks([[action(1)], [action(1)]])
    constrained = ScanPolicy(start=10, end=20, page_size=1, max_pages=3,
                             max_actions=3, max_bytes=100_000, max_seconds=30,
                             max_requests=3)
    group_id = corroborate_range(db, transport, epoch(), constrained)
    row = db.execute(
        "SELECT request_count,page_count,action_count,bytes_used,elapsed_ms "
        "FROM corroboration_groups WHERE group_id=%s", (group_id,),
    ).fetchone()
    assert row[0] <= 3
    assert row[1] <= 3
    assert row[2] <= 3
    assert row[3] <= 100_000
    assert row[4] <= 30_000


def test_child_manifest_is_linked_and_fail_closed_before_transport(db):
    def assert_registered(walk_no):
        row = db.execute(
            "SELECT r.corroborated,w.walk_no,g.walk_count FROM source_ranges r "
            "JOIN corroboration_walks w USING(range_id) "
            "JOIN corroboration_groups g USING(group_id) WHERE w.walk_no=%s",
            (walk_no,),
        ).fetchone()
        assert row == (False, walk_no, walk_no)
        range_id = db.execute(
            "SELECT range_id FROM corroboration_walks WHERE walk_no=%s", (walk_no,)
        ).fetchone()[0]
        db.execute("SAVEPOINT reject_unproven_corroboration")
        with pytest.raises(psycopg.errors.RaiseException, match="cannot be corroborated"):
            db.execute("UPDATE source_ranges SET corroborated=true WHERE range_id=%s", (range_id,))
        db.execute("ROLLBACK TO SAVEPOINT reject_unproven_corroboration")
        db.execute("RELEASE SAVEPOINT reject_unproven_corroboration")

    transport = FullWalks([[action(1)], [action(1)]], on_fetch=assert_registered)
    group_id = corroborate_range(db, transport, epoch(), policy())
    first_range = db.execute(
        "SELECT range_id FROM corroboration_walks WHERE group_id=%s AND walk_no=1",
        (group_id,),
    ).fetchone()[0]
    lease = acquire_transition(db, "worker-child-gate")
    from ton_sentinel.db.ledger import accept_scan
    from ton_sentinel.source.scan import walk_evidence
    assert accept_scan(db, lease, walk_evidence(db, str(first_range))) == ()
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0


def test_completed_child_is_processed_after_fresh_connection_restart(db, monkeypatch):
    import ton_sentinel.source.scan as scan_module

    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    original = scan_module._walk_keys
    raised = False
    lease = acquire_transition(db, "worker-crash-gate")

    def crash_before_key_set(connection, range_id, source_epoch, start, end):
        nonlocal raised
        if not raised:
            raised = True
            connection.commit()
            evidence = scan_module.walk_evidence(connection, range_id)
            assert evidence.complete
            from ton_sentinel.db.ledger import accept_scan
            assert accept_scan(connection, lease, evidence) == ()
            raise RuntimeError("crash after child scan commit")
        return original(connection, range_id, source_epoch, start, end)

    monkeypatch.setattr(scan_module, "_walk_keys", crash_before_key_set)
    transport = FullWalks([[action(1)], [action(1)]])
    with pytest.raises(RuntimeError, match="crash after child scan commit"):
        corroborate_range(db, transport, epoch(), policy())
    group_id = db.execute(
        "SELECT group_id FROM corroboration_groups ORDER BY created_at DESC LIMIT 1"
    ).fetchone()[0]
    first = db.execute(
        "SELECT range_id FROM corroboration_walks WHERE group_id=%s AND walk_no=1",
        (group_id,),
    ).fetchone()[0]
    before = db.execute(
        "SELECT request_count,page_count,action_count,bytes_used,elapsed_ms "
        "FROM source_ranges WHERE range_id=%s", (first,),
    ).fetchone()

    with psycopg.connect(dsn) as restarted:
        restarted.execute(f'SET search_path TO "{schema}"')
        monkeypatch.setattr(scan_module, "_walk_keys", original)
        scan_module.resume_corroboration(
            restarted, FullWalks([[action(1)]]), str(group_id)
        )
        after = restarted.execute(
            "SELECT request_count,page_count,action_count,bytes_used,elapsed_ms "
            "FROM corroboration_groups WHERE group_id=%s", (group_id,),
        ).fetchone()
        assert after[0] >= before[0]
        assert after[1] >= before[1]
        assert after[2] >= before[2]
        assert after[3] >= before[3]
        assert after[4] >= before[4]
        assert restarted.execute(
            "SELECT walk_count,status FROM corroboration_groups WHERE group_id=%s",
            (group_id,),
        ).fetchone() == (2, "corroborated")


def test_accept_scan_never_promotes_an_unlinked_complete_walk(db):
    from ton_sentinel.db.ledger import accept_scan
    from ton_sentinel.source.scan import scan_range, walk_evidence

    range_id = scan_range(db, FullWalks([[action(9)]]), epoch(), policy())
    lease = acquire_transition(db, "worker-unlinked-range")
    assert accept_scan(db, lease, walk_evidence(db, range_id)) == ()
    assert db.execute("SELECT status FROM source_ranges WHERE range_id=%s",
                      (range_id,)).fetchone()[0] == "complete"
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


def test_database_rejects_fabricated_single_walk_terminal_proof(db):
    import dataclasses
    import json

    from ton_sentinel.source.scan import scan_range

    group_id = str(uuid.uuid4())
    range_id = scan_range(db, FullWalks([[]]), epoch(), policy())
    db.execute(
        "INSERT INTO corroboration_groups(group_id,start_inclusive,end_exclusive,epoch,"
        "scan_policy,status,walk_count) VALUES (%s,10,20,%s::jsonb,%s::jsonb,'pending',1)",
        (group_id, epoch().to_json(), json.dumps(dataclasses.asdict(policy()))),
    )
    db.execute("INSERT INTO corroboration_walks(group_id,walk_no,range_id,scan_policy,complete,key_set) "
               "SELECT %s,1,%s,scan_policy,true,'[]'::jsonb FROM source_ranges WHERE range_id=%s",
               (group_id, range_id, range_id))
    db.commit()
    db.execute("SAVEPOINT reject_synthetic_terminal_group")
    with pytest.raises(psycopg.errors.RaiseException):
        db.execute("UPDATE corroboration_groups SET status='corroborated',accepted_range_id=%s "
                   "WHERE group_id=%s", (range_id, group_id))
    db.execute("ROLLBACK TO SAVEPOINT reject_synthetic_terminal_group")
    db.execute("RELEASE SAVEPOINT reject_synthetic_terminal_group")


@pytest.mark.parametrize("walks", [
    [[action(1)], [action(1)]],
    [[action(1)], [action(2)], [action(2)]],
])
def test_resume_rechecks_persisted_pair_after_commit_before_decision(db, monkeypatch, walks):
    import ton_sentinel.source.scan as scan_module

    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    original = scan_module._group_totals
    crashed = False
    target_walks = 2 if walks[0] == walks[1] else 3

    def crash_after_proof_commit(connection, group_id):
        nonlocal crashed
        totals = original(connection, group_id)
        complete = connection.execute(
            "SELECT count(*) FROM corroboration_walks WHERE group_id=%s AND complete",
            (group_id,),
        ).fetchone()[0]
        if complete >= target_walks and not crashed:
            crashed = True
            connection.commit()
            raise RuntimeError("crash after persisted key-set commit")
        return totals

    monkeypatch.setattr(scan_module, "_group_totals", crash_after_proof_commit)
    transport = FullWalks(walks)
    with pytest.raises(RuntimeError, match="crash after persisted key-set commit"):
        corroborate_range(db, transport, epoch(), policy())
    group_id = db.execute(
        "SELECT group_id FROM corroboration_groups ORDER BY created_at DESC LIMIT 1"
    ).fetchone()[0]
    assert db.execute("SELECT walk_count FROM corroboration_groups WHERE group_id=%s",
                      (group_id,)).fetchone()[0] == target_walks
    before = db.execute("SELECT request_count,page_count,action_count,bytes_used,elapsed_ms "
                        "FROM corroboration_groups WHERE group_id=%s", (group_id,)).fetchone()

    class NoFetch:
        def fetch_page(self, *args):
            pytest.fail("resume fetched a child instead of evaluating persisted adjacent proof")

    with psycopg.connect(dsn) as restarted:
        restarted.execute(f'SET search_path TO "{schema}"')
        monkeypatch.setattr(scan_module, "_group_totals", original)
        scan_module.resume_corroboration(restarted, NoFetch(), str(group_id))
        state = restarted.execute(
            "SELECT walk_count,status,request_count,page_count,action_count,bytes_used,elapsed_ms "
            "FROM corroboration_groups WHERE group_id=%s", (group_id,),
        ).fetchone()
        assert state[0:2] == (target_walks, "corroborated")
        assert state[2:] == before


def test_terminal_group_cannot_be_inserted_without_evidence(db):
    from dataclasses import asdict
    import json

    with pytest.raises(psycopg.errors.RaiseException, match="start pending"):
        db.execute(
            "INSERT INTO corroboration_groups(group_id,start_inclusive,end_exclusive,epoch,"
            "scan_policy,status) VALUES (%s,10,20,%s::jsonb,%s::jsonb,'accepted')",
            (str(uuid.uuid4()), epoch().to_json(), json.dumps(asdict(policy()))),
        )
    db.rollback()


def test_group_cannot_be_accepted_before_the_range_is_accepted(db):
    group_id = corroborate_range(db, FullWalks([[action(1)], [action(1)]]), epoch(), policy())
    with pytest.raises(psycopg.errors.RaiseException, match="accepted source range"):
        db.execute("UPDATE corroboration_groups SET status='accepted' WHERE group_id=%s", (group_id,))
    db.rollback()
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0


@pytest.mark.parametrize("walk_no", [1, 2])
def test_acceptance_rebuilds_both_walks_from_persisted_observations(db, walk_no):
    from ton_sentinel.db.ledger import accept_corroborated_range

    group_id = corroborate_range(db, FullWalks([[action(1)], [action(1)]]), epoch(), policy())
    db.execute(
        "DELETE FROM action_observations WHERE range_id=(SELECT range_id FROM "
        "corroboration_walks WHERE group_id=%s AND walk_no=%s)", (group_id, walk_no),
    )
    lease = acquire_transition(db, "recheck-both-walks")
    with pytest.raises(ValueError, match="checkpoint evidence"):
        accept_corroborated_range(db, lease, group_id)
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
    assert db.execute("SELECT epoch FROM active_source_epoch").fetchone()[0] is None
    assert db.execute("SELECT status FROM corroboration_groups WHERE group_id=%s",
                      (group_id,)).fetchone()[0] == "corroborated"


def test_corroborated_acceptance_is_atomic_and_leaves_connection_idle(db):
    from ton_sentinel.db.ledger import accept_corroborated_range

    group_id = corroborate_range(db, FullWalks([[action(1)], [action(1)]]), epoch(), policy())
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    lease = acquire_transition(db, "atomic-acceptance")
    assert accept_corroborated_range(db, lease, group_id)
    assert db.info.transaction_status.name == "IDLE"
    assert accept_corroborated_range(db, lease, group_id)
    assert db.info.transaction_status.name == "IDLE"
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as reader:
        reader.execute(f'SET search_path TO "{schema}"')
        assert reader.execute("SELECT status FROM corroboration_groups WHERE group_id=%s",
                              (group_id,)).fetchone()[0] == "accepted"
        assert reader.execute("SELECT count(*) FROM source_ranges WHERE status='accepted'").fetchone()[0] == 1
        assert reader.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 1
        assert reader.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1


@pytest.mark.parametrize("mutation", ["update", "delete"])
@pytest.mark.parametrize("walk_no", [1, 2])
def test_acceptance_locks_both_verified_observation_sets(db, monkeypatch, mutation, walk_no):
    import json
    import ton_sentinel.source.scan as scan_module
    from ton_sentinel.db.ledger import accept_corroborated_range

    group_id = corroborate_range(db, FullWalks([[action(1)], [action(1)]]), epoch(), policy())
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    target = db.execute("SELECT range_id FROM corroboration_walks WHERE group_id=%s AND walk_no=%s",
                        (group_id, walk_no)).fetchone()[0]
    lease = acquire_transition(db, "locked-proof")
    original = scan_module.validate_corroboration_proof

    def validate_then_mutate(connection, *args, **kwargs):
        result = original(connection, *args, **kwargs)
        assert result
        with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as writer:
            writer.execute(f'SET search_path TO "{schema}"')
            writer.execute("SET LOCAL lock_timeout = '100ms'")
            with pytest.raises(psycopg.errors.LockNotAvailable):
                if mutation == "update":
                    writer.execute("UPDATE action_observations SET payload=%s::jsonb WHERE range_id=%s",
                                   (json.dumps(action(2)), target))
                else:
                    writer.execute("DELETE FROM action_observations WHERE range_id=%s", (target,))
            writer.rollback()
        return result

    monkeypatch.setattr(scan_module, "validate_corroboration_proof", validate_then_mutate)
    assert accept_corroborated_range(db, lease, group_id)
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 1


@pytest.mark.parametrize("operation", ["insert", "update"])
def test_unproven_range_cannot_be_marked_accepted_directly(db, operation):
    from ton_sentinel.source.scan import scan_range

    range_id = scan_range(db, FullWalks([[action(1)]]), epoch(), policy())
    with pytest.raises(psycopg.errors.RaiseException, match="accepted range"):
        if operation == "insert":
            db.execute(
                "INSERT INTO source_ranges(range_id,start_inclusive,end_exclusive,epoch,status) "
                "VALUES (%s,10,20,%s::jsonb,'accepted')",
                (str(uuid.uuid4()), epoch().to_json()),
            )
        else:
            db.execute("UPDATE source_ranges SET status='accepted' WHERE range_id=%s", (range_id,))
    db.rollback()


def test_selected_range_cannot_skip_atomic_promotion_by_setting_status(db):
    group_id = corroborate_range(db, FullWalks([[action(1)], [action(1)]]), epoch(), policy())
    with pytest.raises(psycopg.errors.RaiseException, match="accepted range"):
        db.execute(
            "UPDATE source_ranges SET status='accepted' WHERE range_id="
            "(SELECT accepted_range_id FROM corroboration_groups WHERE group_id=%s)", (group_id,),
        )
    db.rollback()
    assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
