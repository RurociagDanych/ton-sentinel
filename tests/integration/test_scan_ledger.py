"""Real PostgreSQL behavior for durable scan proposals and fenced promotion."""

import base64
import json
import os
import threading
import time
import uuid
from pathlib import Path

import psycopg
import pytest
import ton_sentinel.db.ledger as ledger
from ton_sentinel.outbox import configure_delivery, publish_pending

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.fencing import LeaseRejected
from ton_sentinel.db.ledger import (accept_corroborated_range, accept_scan,
                                    acquire_transition, migrate)
from ton_sentinel.source.scan import (ScanPolicy, corroborate_range, scan_range,
                                      walk_evidence)


@pytest.fixture
def db(request):
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "scan_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, **getattr(request, "param", {
                "topic": "scan-test", "topic_incarnation": schema,
            }))
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def epoch():
    return SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                       "synthetic:build", "synthetic:policy")


def action(index, action_end=None, network="testnet"):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": network,
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": 10 + index if action_end is None else action_end,
        "status": "success", "eligible": True,
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
        self.requests.append((start, end, offset, limit))
        if offset == self.fail_at:
            raise OSError("synthetic second-page failure")
        return self.values[offset:offset + limit]


class RepeatedPages:
    def __init__(self, values):
        self.values = values
        self.walk = -1
        self.requests = []

    def fetch_page(self, source_epoch, start, end, offset, limit):
        self.requests.append((source_epoch, start, end, offset, limit))
        if offset == 0:
            self.walk += 1
        return self.values[offset:offset + limit]


def policy():
    return ScanPolicy(start=10, end=20, page_size=1, max_pages=4, max_actions=4)


def rows(db, table):
    return db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]


def corroborated_range(db, values):
    bounded = ScanPolicy(start=10, end=20, page_size=1, max_pages=8,
                         max_actions=8, max_bytes=100_000, max_seconds=30,
                         max_requests=10)
    group_id = corroborate_range(db, RepeatedPages(values), epoch(), bounded)
    selected = db.execute(
        "SELECT accepted_range_id FROM corroboration_groups "
        "WHERE group_id=%s AND status='corroborated'", (group_id,),
    ).fetchone()
    assert selected is not None and selected[0] is not None
    db.commit()
    return group_id, str(selected[0])


@pytest.mark.parametrize("db", [{"topic": "bound-actions", "topic_incarnation": "bound-inc",
                                "replay_epoch": 7}], indirect=True)
def test_delivery_binding_is_atomic_durable_immutable_and_migration_safe(db, monkeypatch):
    group, _ = corroborated_range(db, [action(5)])
    lease = acquire_transition(db, "binding-test")
    real_validate = ledger.validate_transition
    checks = 0

    def reject_final(connection, token):
        nonlocal checks
        checks += 1
        if checks == 2:
            assert connection.execute("SELECT delivery_envelope IS NOT NULL FROM outbox").fetchone()[0]
            raise LeaseRejected("binding final fence")
        return real_validate(connection, token)

    monkeypatch.setattr(ledger, "validate_transition", reject_final)
    with pytest.raises(LeaseRejected, match="binding final fence"):
        accept_corroborated_range(db, lease, group)
    db.commit()
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    db.commit()
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"],
                         options=f"-c search_path={schema}") as reader:
        assert rows(reader, "canonical_actions") == rows(reader, "outbox") == 0
    monkeypatch.setattr(ledger, "validate_transition", real_validate)
    assert accept_corroborated_range(db, lease, group)
    stored = db.execute(
        "SELECT event_id,payload,delivery_topic,delivery_topic_incarnation,"
        "replay_epoch,delivery_envelope FROM outbox"
    ).fetchone()
    assert stored[2:5] == ("bound-actions", "bound-inc", 7)
    assert stored[5] == {"envelope_version": 2, "event_id": stored[0],
                         "topic_incarnation": "bound-inc", "replay_epoch": 7,
                         "accepted_generation": 1, "action": stored[1]}
    db.commit()
    migrate(db)
    migrate(db)
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"],
                         options=f"-c search_path={schema}") as reader:
        assert reader.execute(
            "SELECT event_id,payload,delivery_topic,delivery_topic_incarnation,"
            "replay_epoch,delivery_envelope FROM outbox"
        ).fetchone() == stored
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        db.execute("UPDATE outbox SET delivery_envelope=jsonb_set("
                   "delivery_envelope,'{envelope_version}','1'::jsonb)")
    db.rollback()


def test_unconfigured_acceptance_rolls_back_and_retries_after_delivery_configuration(db):
    group, _ = corroborated_range(db, [action(6)])
    db.execute("DELETE FROM outbox_delivery_config WHERE singleton=true")
    db.commit()
    lease = acquire_transition(db, "unbound-test")
    with pytest.raises(ValueError, match="delivery must be configured"):
        accept_corroborated_range(db, lease, group)
    db.commit()
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    db.commit()
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"],
                         options=f"-c search_path={schema}") as reader:
        assert rows(reader, "canonical_actions") == 0
        assert rows(reader, "outbox") == 0
        assert rows(reader, "scan_membership") == 0
        assert reader.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
        assert reader.execute(
            "SELECT status FROM source_ranges ORDER BY created_at DESC LIMIT 1"
        ).fetchone()[0] == "complete"
        assert reader.execute(
            "SELECT status FROM corroboration_groups WHERE group_id=%s", (group,)
        ).fetchone()[0] == "corroborated"
    configure_delivery(db, topic="scan-test", topic_incarnation=schema)
    assert accept_corroborated_range(db, lease, group)
    assert accept_corroborated_range(db, lease, group)
    assert rows(db, "canonical_actions") == rows(db, "outbox") == 1
    stored = db.execute(
        "SELECT event_id,payload,delivery_topic,delivery_topic_incarnation,"
        "replay_epoch,delivery_envelope FROM outbox"
    ).fetchone()
    assert stored[2:5] == ("scan-test", schema, 0)
    assert stored[5] == {"envelope_version": 2, "event_id": stored[0],
                         "topic_incarnation": schema, "replay_epoch": 0,
                         "accepted_generation": 1, "action": stored[1]}
    db.commit()


def test_empty_acceptance_advances_without_delivery_intent(db):
    group, _ = corroborated_range(db, [])
    db.execute("DELETE FROM outbox_delivery_config WHERE singleton=true")
    db.commit()
    lease = acquire_transition(db, "empty-unconfigured-test")
    assert accept_corroborated_range(db, lease, group)
    assert rows(db, "canonical_actions") == rows(db, "outbox") == 0
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    assert db.execute(
        "SELECT status FROM corroboration_groups WHERE group_id=%s", (group,)
    ).fetchone()[0] == "accepted"
    db.commit()


@pytest.mark.parametrize("db", [{"topic": "accepted-actions", "topic_incarnation": "accepted-inc"}],
                         indirect=True)
def test_publication_rejects_configuration_drift_before_send(db):
    group, _ = corroborated_range(db, [action(7)])
    lease = acquire_transition(db, "drift-test")
    assert accept_corroborated_range(db, lease, group)
    db.execute("UPDATE outbox_delivery_config SET topic_incarnation='unexpected-inc'")
    db.commit()

    class NoSend:
        config = {"acks": -1}

        def send(self, *args, **kwargs):
            pytest.fail("configuration drift must not reach Kafka")

    with pytest.raises(ValueError, match="differs from immutable delivery authority"):
        publish_pending(db, NoSend(), lease)
    assert db.execute("SELECT publication_attempts FROM outbox").fetchone()[0] == 0


def test_acceptance_rejects_replacement_delivery_authority_after_config_loss(db):
    first_group, _ = corroborated_range(db, [action(8)])
    second_group, _ = corroborated_range(db, [action(9)])
    lease = acquire_transition(db, "delivery-authority-test")
    assert accept_corroborated_range(db, lease, first_group)

    db.execute("DELETE FROM outbox_delivery_config WHERE singleton=true")
    db.commit()
    with pytest.raises(ValueError, match="immutable"):
        configure_delivery(db, topic="replacement-actions", topic_incarnation="replacement-inc")

    with pytest.raises(ValueError, match="durable accepted delivery authority"):
        accept_corroborated_range(db, lease, second_group)
    assert rows(db, "canonical_actions") == 1
    assert rows(db, "outbox") == 1
    assert db.execute("SELECT status FROM corroboration_groups WHERE group_id=%s",
                      (second_group,)).fetchone()[0] == "corroborated"
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    db.commit()


def test_delivery_authority_survives_outbox_retention_and_cannot_be_replaced(db):
    group, _ = corroborated_range(db, [action(4)])
    lease = acquire_transition(db, "retention-authority-test")
    assert accept_corroborated_range(db, lease, group)
    db.execute("DELETE FROM outbox")
    db.execute("DELETE FROM outbox_delivery_config WHERE singleton=true")
    db.commit()

    with pytest.raises(ValueError, match="immutable"):
        configure_delivery(db, topic="replacement-after-retention", topic_incarnation="new-inc")
    assert db.execute(
        "SELECT topic,topic_incarnation,replay_epoch FROM outbox_delivery_authority "
        "WHERE singleton=true"
    ).fetchone() == ("scan-test", db.execute("SELECT current_schema()").fetchone()[0], 0)
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        db.execute("UPDATE outbox_delivery_authority SET topic='forged-topic'")
    db.rollback()
    with pytest.raises(psycopg.errors.RaiseException, match="immutable"):
        db.execute("DELETE FROM outbox_delivery_authority")
    db.rollback()
    next_group, _ = corroborated_range(db, [action(5)])
    with pytest.raises(ValueError, match="durable accepted delivery authority"):
        accept_corroborated_range(db, lease, next_group)
    assert rows(db, "canonical_actions") == 1
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    db.commit()


def test_migration_fails_closed_on_conflicting_config_and_bound_outbox_evidence(db):
    group, _ = corroborated_range(db, [action(6)])
    lease = acquire_transition(db, "migration-conflict-test")
    assert accept_corroborated_range(db, lease, group)
    db.execute("UPDATE outbox_delivery_config SET topic='conflicting-topic'")
    db.commit()

    with pytest.raises(psycopg.errors.RaiseException, match="conflicting outbox delivery authority"):
        migrate(db)


def test_page_two_failure_keeps_proposals_without_canonical_acceptance(db):
    range_id = scan_range(db, Pages([action(1), action(2)], fail_at=1), epoch(), policy())
    lease = acquire_transition(db, "worker-one")
    assert accept_scan(db, lease, walk_evidence(db, range_id)) == ()
    state = db.execute("SELECT status FROM source_ranges WHERE range_id=%s", (range_id,)).fetchone()[0]
    cursor = db.execute("SELECT cursor FROM ingest_state").fetchone()[0]
    assert state == "incomplete"
    assert rows(db, "action_observations") == 1
    assert rows(db, "canonical_actions") == 0
    assert rows(db, "outbox") == 0
    assert cursor == 0


def test_complete_exact_multiple_pages_need_empty_page_and_replay_is_idempotent(db):
    source = RepeatedPages([action(3), action(4)])
    group_id = corroborate_range(db, source, epoch(), ScanPolicy(
        start=10, end=20, page_size=1, max_pages=8, max_actions=8,
        max_bytes=100_000, max_seconds=30, max_requests=10,
    ))
    range_id = str(db.execute(
        "SELECT accepted_range_id FROM corroboration_groups WHERE group_id=%s",
        (group_id,),
    ).fetchone()[0])
    db.commit()
    assert [request[3] for request in source.requests] == [0, 1, 2, 0, 1, 2]
    assert source.requests[2][3] == source.requests[5][3] == 2
    assert source.walk == 1
    assert db.execute("SELECT status FROM coverage_ranges WHERE range_id=%s",
                      (range_id,)).fetchone()[0] == "bounded_observed"
    lease = acquire_transition(db, "worker-one")
    assert accept_corroborated_range(db, lease, group_id)
    assert accept_corroborated_range(db, lease, group_id)
    assert rows(db, "canonical_actions") == 2
    assert rows(db, "outbox") == 2
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20


def test_superseded_fence_cannot_promote_completed_scan(db):
    group_id, range_id = corroborated_range(db, [action(5)])
    stale = acquire_transition(db, "worker-one")
    acquire_transition(db, "worker-two")
    with pytest.raises(LeaseRejected):
        accept_corroborated_range(db, stale, group_id)
    assert rows(db, "canonical_actions") == 0
    assert rows(db, "outbox") == 0


def test_expired_lease_is_rejected_before_incomplete_scan_returns(db):
    range_id = scan_range(db, Pages([], fail_at=0), epoch(), policy())
    lease = acquire_transition(db, "worker-one")
    db.execute("UPDATE lease_state SET expires_at=clock_timestamp()-interval '1 second'")
    db.commit()
    with pytest.raises(LeaseRejected):
        accept_scan(db, lease, walk_evidence(db, range_id))
    assert rows(db, "canonical_actions") == 0
    assert rows(db, "outbox") == 0


def test_lease_expiry_during_promotion_rolls_back_even_if_caller_commits(db):
    group_id, range_id = corroborated_range(db, [action(6)])
    evidence = walk_evidence(db, range_id)
    db.execute("""
        CREATE FUNCTION expire_promotion_lease() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            PERFORM pg_sleep(1.2);
            RETURN NEW;
        END
        $$
    """)
    db.execute("""
        CREATE TRIGGER expire_promotion_lease AFTER INSERT ON canonical_actions
        FOR EACH ROW EXECUTE FUNCTION expire_promotion_lease()
    """)
    db.commit()
    lease = acquire_transition(db, "worker-one", ttl_seconds=1)

    with pytest.raises(LeaseRejected):
        accept_scan(db, lease, evidence)
    # Callers may commit incidental work after handling a rejected promotion.
    db.commit()

    schema = db.execute("SELECT current_schema()").fetchone()[0]
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as reader:
        reader.execute(f'SET search_path TO "{schema}"')
        assert rows(reader, "canonical_actions") == 0
        assert rows(reader, "outbox") == 0
        assert rows(reader, "scan_membership") == 0
        assert reader.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
        assert reader.execute("SELECT status FROM source_ranges WHERE range_id=%s",
                              (range_id,)).fetchone()[0] == "complete"
        assert reader.execute("SELECT status FROM corroboration_groups WHERE group_id=%s",
                              (group_id,)).fetchone()[0] == "corroborated"


def test_accept_scan_still_promotes_with_valid_lease(db):
    group_id, range_id = corroborated_range(db, [action(7)])
    lease = acquire_transition(db, "worker-one")

    assert accept_scan(db, lease, walk_evidence(db, range_id))
    assert rows(db, "canonical_actions") == 1
    assert rows(db, "outbox") == 1
    assert rows(db, "scan_membership") == 1
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    assert db.execute("SELECT status FROM source_ranges WHERE range_id=%s",
                      (range_id,)).fetchone()[0] == "accepted"
    assert db.execute("SELECT status FROM corroboration_groups WHERE group_id=%s",
                      (group_id,)).fetchone()[0] == "accepted"


def test_scan_enforces_immutable_half_open_action_bounds(db):
    range_id = scan_range(db, Pages([action(10, action_end=10),
                                     action(11, action_end=20)]), epoch(), policy())
    state = db.execute("SELECT status FROM source_ranges WHERE range_id=%s",
                        (range_id,)).fetchone()[0]
    assert state == "incomplete"
    assert rows(db, "action_observations") == 1
    lease = acquire_transition(db, "worker-one")
    assert accept_scan(db, lease, walk_evidence(db, range_id)) == ()
    assert rows(db, "canonical_actions") == 0


def test_acceptance_rechecks_persisted_action_bounds(db):
    range_id = scan_range(db, Pages([action(1, action_end=10)]), epoch(), policy())
    db.execute("UPDATE action_observations SET payload=jsonb_set(payload,'{action_end}','20') "
               "WHERE range_id=%s", (range_id,))
    db.commit()
    lease = acquire_transition(db, "worker-one")
    with pytest.raises(ValueError, match="outside immutable scan range"):
        accept_scan(db, lease, walk_evidence(db, range_id))
    assert rows(db, "canonical_actions") == 0


@pytest.mark.parametrize("changed", [
    SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                "synthetic:build-v2", "synthetic:policy"),
    SourceEpoch("mainnet", "synthetic:source", "synthetic:filter",
                "synthetic:build", "synthetic:policy"),
    SourceEpoch("testnet", "synthetic:source-v2", "synthetic:filter",
                "synthetic:build", "synthetic:policy"),
])
def test_promotion_rejects_unapproved_changed_source_epoch(db, changed):
    first_group, _ = corroborated_range(db, [action(1)])
    lease = acquire_transition(db, "worker-one")
    assert accept_corroborated_range(db, lease, first_group)

    changed_policy = ScanPolicy(start=10, end=20, page_size=1, max_pages=8,
                                max_actions=8, max_bytes=100_000, max_seconds=30,
                                max_requests=10)
    second_group = corroborate_range(
        db, RepeatedPages([action(2, network=changed.network)]), changed, changed_policy
    )
    lease = acquire_transition(db, "worker-two")
    with pytest.raises(ValueError, match="explicit audited source epoch transition"):
        accept_corroborated_range(db, lease, second_group)
    assert rows(db, "canonical_actions") == 1


def test_lease_is_rechecked_after_waiting_for_row_lock(db):
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    lease = acquire_transition(db, "worker-one", ttl_seconds=1)
    blocker = psycopg.connect(dsn)
    validator = psycopg.connect(dsn)
    blocker.execute(f'SET search_path TO "{schema}"')
    validator.execute(f'SET search_path TO "{schema}"')
    blocker.execute("SELECT 1 FROM lease_state WHERE singleton=true FOR UPDATE")
    validator_pid = validator.execute("SELECT pg_backend_pid()").fetchone()[0]
    outcome = []

    def validate():
        try:
            from ton_sentinel.db.fencing import validate_transition
            validate_transition(validator, lease)
            outcome.append("accepted")
        except Exception as exc:
            outcome.append(exc)

    worker = threading.Thread(target=validate)
    try:
        worker.start()
        deadline = time.monotonic() + 5
        blocked = False
        while time.monotonic() < deadline:
            wait_type = blocker.execute(
                "SELECT wait_event_type FROM pg_stat_activity WHERE pid=%s",
                (validator_pid,),
            ).fetchone()[0]
            if wait_type == "Lock":
                blocked = True
                break
            time.sleep(0.01)
        assert blocked, f"validator did not block behind the lease row lock: {outcome!r}"
        blocker.execute("SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM "
                        "(expires_at-clock_timestamp())) + 0.1)) FROM lease_state")
        blocker.commit()
        worker.join(timeout=5)
        assert not worker.is_alive(), "validator remained blocked after lock release"
        assert len(outcome) == 1 and isinstance(outcome[0], LeaseRejected)
    finally:
        if blocker.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            blocker.rollback()
        if validator.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            validator.rollback()
        blocker.close()
        validator.close()


def test_acceptance_has_durable_boundary_and_rejects_caller_transaction(db):
    group_id, range_id = corroborated_range(db, [action(1)])
    lease = acquire_transition(db, "worker-one")
    assert accept_corroborated_range(db, lease, group_id)

    schema = db.execute("SELECT current_schema()").fetchone()[0]
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as reader:
        reader.execute(f'SET search_path TO "{schema}"')
        assert rows(reader, "canonical_actions") == 1

    _, range_two = corroborated_range(db, [action(2)])
    db.execute("UPDATE ingest_state SET cursor=99")
    with pytest.raises(ValueError, match="idle connection"):
        walk_evidence(db, range_two)
    db.rollback()
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    db.commit()

    evidence_two = walk_evidence(db, range_two)
    db.execute("UPDATE ingest_state SET cursor=99")
    with pytest.raises(ValueError, match="idle connection"):
        accept_scan(db, lease, evidence_two)
    db.rollback()
    assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20


class AckProducer:
    config = {"acks": -1}

    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

        class Done:
            def get(self, timeout):
                return None
        return Done()


def _fresh(schema):
    return psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"],
                           options=f"-c search_path={schema}")


def test_rolled_back_acceptance_leaves_no_intent_for_any_publisher(db, monkeypatch):
    group, _ = corroborated_range(db, [action(7)])
    lease = acquire_transition(db, "no-intent")
    real_validate = ledger.validate_transition
    calls = 0

    def reject_final(connection, token):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise LeaseRejected("final fence")
        return real_validate(connection, token)

    monkeypatch.setattr(ledger, "validate_transition", reject_final)
    with pytest.raises(LeaseRejected):
        accept_corroborated_range(db, lease, group)
    db.commit()
    monkeypatch.setattr(ledger, "validate_transition", real_validate)
    producer = AckProducer()
    assert publish_pending(db, producer, lease) == ()
    assert producer.sent == [] and rows(db, "outbox") == 0
    db.commit()


@pytest.mark.parametrize("db", [{"topic": "durable-actions", "topic_incarnation": "durable-inc",
                                "replay_epoch": 2}], indirect=True)
def test_committed_intent_is_published_from_a_fresh_connection_with_persisted_bytes(db):
    group, _ = corroborated_range(db, [action(8)])
    lease = acquire_transition(db, "restart")
    assert accept_corroborated_range(db, lease, group)
    db.commit()
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    db.commit()
    with _fresh(schema) as restarted:
        stored = restarted.execute(
            "SELECT event_id,delivery_envelope FROM outbox").fetchone()
        restarted.commit()
        producer = AckProducer()
        assert publish_pending(restarted, producer, lease) == (stored[0],)
        topic, key, value = producer.sent[0]
        assert (topic, key) == ("durable-actions", stored[0].encode())
        assert json.loads(value) == stored[1]
        assert stored[1]["replay_epoch"] == 2 and stored[1]["envelope_version"] == 2


def test_database_rejects_outbox_rows_with_inconsistent_delivery_envelope(db):
    group, _ = corroborated_range(db, [action(9)])
    lease = acquire_transition(db, "constraint")
    assert accept_corroborated_range(db, lease, group)
    ev, payload, topic, inc, epoch_n, env = db.execute(
        "SELECT event_id,payload,delivery_topic,delivery_topic_incarnation,"
        "replay_epoch,delivery_envelope FROM outbox").fetchone()
    db.commit()
    bad_envelopes = [
        {**env, "envelope_version": 2}, {**env, "event_id": "other"},
        {**env, "topic_incarnation": "other"}, {**env, "replay_epoch": env["replay_epoch"] + 1},
        {**env, "action": {"different": True}}, {**env, "extra": 1},
    ]
    for index, bad in enumerate(bad_envelopes):
        new_id = f"forged-{index}"
        db.execute("INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                   "VALUES (%s,%s,%s::jsonb)", (new_id, new_id, json.dumps(payload)))
        with pytest.raises(psycopg.errors.CheckViolation):
            db.execute("INSERT INTO outbox(event_id,payload,delivery_topic,"
                       "delivery_topic_incarnation,replay_epoch,delivery_envelope) "
                       "VALUES (%s,%s::jsonb,%s,%s,%s,%s::jsonb)",
                       (new_id, json.dumps(payload), topic, inc, epoch_n, json.dumps(bad)))
        db.rollback()
    new_id = "partial"
    db.execute("INSERT INTO canonical_actions(event_id,semantic_digest,action) "
               "VALUES (%s,%s,%s::jsonb)", (new_id, new_id, json.dumps(payload)))
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("INSERT INTO outbox(event_id,payload,delivery_topic) VALUES (%s,%s::jsonb,%s)",
                   (new_id, json.dumps(payload), topic))
    db.rollback()



def corroborated_span(db, values, start, end):
    bounded = ScanPolicy(start=start, end=end, page_size=1, max_pages=8,
                         max_actions=8, max_bytes=100_000, max_seconds=30,
                         max_requests=10)
    group_id = corroborate_range(db, RepeatedPages(values), epoch(), bounded)
    db.commit()
    return group_id


def generation_state(db):
    value = db.execute("SELECT generation FROM canonical_generation").fetchone()[0]
    db.commit()
    return value


def test_accepted_changes_get_monotonic_generations_carried_in_the_envelope(db):
    lease = acquire_transition(db, "generation")
    assert generation_state(db) == 0
    assert accept_corroborated_range(db, lease, corroborated_span(
        db, [action(1, 11), action(2, 12)], 10, 20))
    assert accept_corroborated_range(db, lease, corroborated_span(
        db, [action(3, 21)], 20, 30))
    stored = db.execute(
        "SELECT c.event_id,c.accepted_generation,o.accepted_generation,o.delivery_envelope,"
        "(c.action->>'action_end')::int FROM canonical_actions c JOIN outbox o USING(event_id) "
        "ORDER BY c.accepted_generation").fetchall()
    db.commit()
    assert [row[1] for row in stored] == [1, 2, 3]
    assert [row[4] for row in stored] == [11, 12, 21]
    for event_id, canonical_generation, outbox_generation, envelope, _ in stored:
        assert outbox_generation == canonical_generation
        assert envelope["envelope_version"] == 2
        assert envelope["accepted_generation"] == canonical_generation
        assert envelope["event_id"] == event_id
    assert generation_state(db) == 3


def test_generation_allocation_rolls_back_with_a_failed_acceptance(db):
    lease = acquire_transition(db, "generation-rollback")
    group = corroborated_span(db, [action(1, 11), action(2, 12)], 10, 20)

    def fail(connection):
        assert connection.execute(
            "SELECT generation FROM canonical_generation").fetchone()[0] == 2
        raise RuntimeError("synthetic failure after generation allocation")

    with pytest.raises(RuntimeError, match="synthetic failure"):
        accept_corroborated_range(db, lease, group, acceptance_check=fail)
    assert generation_state(db) == 0
    assert rows(db, "canonical_actions") == 0 and rows(db, "outbox") == 0
    db.commit()
    assert accept_corroborated_range(db, lease, group)
    assert [row[0] for row in db.execute(
        "SELECT accepted_generation FROM outbox ORDER BY accepted_generation")] == [1, 2]
    db.commit()


def test_semantic_reobservation_allocates_no_generation(db):
    lease = acquire_transition(db, "generation-reobservation")
    assert accept_corroborated_range(db, lease, corroborated_span(db, [action(2, 12)], 10, 20))
    assert accept_corroborated_range(db, lease, corroborated_span(db, [action(2, 12)], 10, 30))
    assert generation_state(db) == 1
    assert rows(db, "outbox") == 1
    db.commit()


def test_concurrent_generation_allocation_is_serialized_and_commit_ordered(db):
    lease = acquire_transition(db, "generation-concurrency")
    assert accept_corroborated_range(db, lease, corroborated_span(db, [action(2, 12)], 10, 20))
    payload = db.execute("SELECT action FROM canonical_actions").fetchone()[0]
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    db.commit()
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    options = f"-c search_path={schema}"
    insert = ("INSERT INTO canonical_actions(event_id,semantic_digest,action) "
              "VALUES (%s,%s,%s::jsonb) RETURNING accepted_generation")
    with psycopg.connect(dsn, options=options) as first, \
            psycopg.connect(dsn, options=options) as second:
        first_generation = first.execute(
            insert, ("concurrent-a", "a", json.dumps(payload))).fetchone()[0]
        result = {}

        def contend():
            result["generation"] = second.execute(
                insert, ("concurrent-b", "b", json.dumps(payload))).fetchone()[0]
            second.commit()

        worker = threading.Thread(target=contend)
        worker.start()
        time.sleep(0.5)
        assert worker.is_alive(), "second allocator must wait for the first transaction"
        first.commit()
        worker.join(10)
        assert not worker.is_alive()
    assert (first_generation, result["generation"]) == (2, 3)
    assert generation_state(db) == 3


def test_database_rejects_non_monotonic_or_mutated_generations(db):
    lease = acquire_transition(db, "generation-guards")
    assert accept_corroborated_range(db, lease, corroborated_span(
        db, [action(1, 11), action(2, 12)], 10, 20))
    payload = db.execute("SELECT action FROM canonical_actions LIMIT 1").fetchone()[0]
    db.commit()
    for explicit in (1, 2, 3, 100):
        with pytest.raises(psycopg.errors.RaiseException, match="allocated"):
            db.execute("INSERT INTO canonical_actions(event_id,semantic_digest,action,"
                       "accepted_generation) VALUES ('forged','d',%s::jsonb,%s)",
                       (json.dumps(payload), explicit))
        db.rollback()
    for statement in (
            "UPDATE canonical_generation SET generation=generation-1",
            "UPDATE canonical_generation SET generation=generation",
            "UPDATE canonical_generation SET generation=generation+2",
            "UPDATE canonical_generation SET generation=0",
            "DELETE FROM canonical_generation",
            "TRUNCATE canonical_generation",
            "UPDATE canonical_actions SET accepted_generation=accepted_generation+10",
            "UPDATE canonical_actions SET accepted_generation=NULL",
            "UPDATE outbox SET accepted_generation=accepted_generation+10"):
        with pytest.raises(psycopg.errors.RaiseException, match="generation"):
            db.execute(statement)
        db.rollback()
    with pytest.raises(psycopg.errors.CheckViolation):
        db.execute("INSERT INTO canonical_generation(singleton,generation) VALUES (false,9)")
    db.rollback()
    assert generation_state(db) == 2


def test_database_checks_generation_carried_by_the_envelope(db):
    lease = acquire_transition(db, "generation-envelope")
    assert accept_corroborated_range(db, lease, corroborated_span(db, [action(2, 12)], 10, 20))
    payload, topic, inc, replay, env = db.execute(
        "SELECT payload,delivery_topic,delivery_topic_incarnation,replay_epoch,"
        "delivery_envelope FROM outbox").fetchone()
    db.commit()
    insert = ("INSERT INTO outbox(event_id,payload,delivery_topic,delivery_topic_incarnation,"
              "replay_epoch,accepted_generation,delivery_envelope) "
              "VALUES (%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb)")

    def fresh_canonical(name):
        return db.execute(
            "INSERT INTO canonical_actions(event_id,semantic_digest,action) "
            "VALUES (%s,%s,%s::jsonb) RETURNING accepted_generation",
            (name, name, json.dumps(payload))).fetchone()[0]

    def envelope(name, version, generation):
        value = {"envelope_version": version, "event_id": name,
                 "topic_incarnation": inc, "replay_epoch": replay, "action": payload}
        if generation is not None:
            value["accepted_generation"] = generation
        return json.dumps(value)

    cases = [lambda n, g: (g, envelope(n, 2, g + 1)),
             lambda n, g: (None, envelope(n, 2, g)),
             lambda n, g: (g, envelope(n, 1, None)),
             lambda n, g: (g, envelope(n, 1, g))]
    for index, case in enumerate(cases):
        name = f"forged-check-{index}"
        generation = fresh_canonical(name)
        column, body = case(name, generation)
        with pytest.raises(psycopg.errors.CheckViolation):
            db.execute(insert, (name, json.dumps(payload), topic, inc, replay, column, body))
        db.rollback()
    for index, delta in enumerate((1, -1)):
        name = f"forged-reference-{index}"
        generation = fresh_canonical(name) + delta
        with pytest.raises(psycopg.errors.RaiseException, match="canonical generation"):
            db.execute(insert, (name, json.dumps(payload), topic, inc, replay, generation,
                                envelope(name, 2, generation)))
        db.rollback()
    name = "forged-downgrade"
    fresh_canonical(name)
    with pytest.raises(psycopg.errors.RaiseException, match="canonical generation"):
        db.execute(insert, (name, json.dumps(payload), topic, inc, replay, None,
                            envelope(name, 1, None)))
    db.rollback()
    name = "consistent"
    generation = fresh_canonical(name)
    db.execute(insert, (name, json.dumps(payload), topic, inc, replay, generation,
                        envelope(name, 2, generation)))
    db.rollback()
    assert env["accepted_generation"] == 1


def test_publisher_sends_persisted_generation_envelope_and_migration_replays(db):
    lease = acquire_transition(db, "generation-publish")
    assert accept_corroborated_range(db, lease, corroborated_span(db, [action(2, 12)], 10, 20))
    db.commit()
    migrate(db)
    migrate(db)
    stored = db.execute("SELECT event_id,delivery_envelope FROM outbox").fetchone()
    db.commit()
    producer = AckProducer()
    assert publish_pending(db, producer, lease) == (stored[0],)
    assert json.loads(producer.sent[0][2]) == stored[1]
    assert stored[1]["envelope_version"] == 2 and stored[1]["accepted_generation"] == 1
