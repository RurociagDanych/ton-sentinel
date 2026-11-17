"""Real PostgreSQL + Kafka publication acceptance (opt-in via env vars)."""
import base64
import json
import os
from pathlib import Path
import threading
import uuid

import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.ledger import accept_corroborated_range, migrate
import ton_sentinel.outbox as outbox
from ton_sentinel.outbox import (advance_publication_frontier, configure_delivery,
                                 initialize_publication_origin, publish_pending)
from ton_sentinel.source.scan import ScanPolicy, corroborate_range


class RepeatedPages:
    def __init__(self, value):
        self.value = value
        self.walk = -1

    def fetch_page(self, source_epoch, start, end, offset, limit):
        if offset == 0:
            self.walk += 1
        return [self.value][offset:offset + limit]


def synthetic_action(index, action_end=15):
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


@pytest.mark.integration
def test_acknowledged_publication_survives_database_reconnect_and_advances_frontier(monkeypatch):
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    bootstrap = os.environ["TON_SENTINEL_KAFKA_BOOTSTRAP"]
    pytest.importorskip("kafka")
    from psycopg import connect
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    suffix = uuid.uuid4().hex
    topic = "outbox-" + suffix
    schema = "ob_" + suffix
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="outbox-test-" + suffix)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    incarnation = "test-incarnation-" + suffix
    try:
        with connect(dsn, options=options) as db:
            migrate(db)
            configure_delivery(db, topic=topic, topic_incarnation=incarnation)
            policy = ScanPolicy(start=10, end=20, page_size=2, max_pages=8,
                                max_actions=8, max_bytes=100_000, max_seconds=30,
                                max_requests=10)
            group = corroborate_range(
                db, RepeatedPages(synthetic_action(8)),
                SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                            "synthetic:build", "synthetic:policy"), policy,
            )
            assert db.execute("SELECT status FROM corroboration_groups WHERE group_id=%s",
                              (group,)).fetchone()[0] == "corroborated"
            db.commit()
            lease = acquire_transition(db, "outbox-test-" + suffix)
            assert accept_corroborated_range(db, lease, group)
            initialize_publication_origin(db, lease, 10)
            event = db.execute("SELECT event_id FROM outbox ORDER BY event_id LIMIT 1").fetchone()[0]
            db.commit()
            configure_delivery(db, topic=topic, topic_incarnation=incarnation)
            producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
            try:
                # Reject only at the final fence, after the SQL marker UPDATE.
                # The transaction must roll back even if the caller commits
                # after the exception; retry remains at-least-once.
                real_validate = outbox.validate_transition
                checks = 0

                def reject_after_ack(connection, token):
                    nonlocal checks
                    checks += 1
                    if checks == 4:
                        raise LeaseRejected("simulated final fence rejection")
                    return real_validate(connection, token)

                monkeypatch.setattr(outbox, "validate_transition", reject_after_ack)
                with pytest.raises(LeaseRejected):
                    publish_pending(db, producer, lease)
                monkeypatch.setattr(outbox, "validate_transition", real_validate)
                assert not db.execute(
                    "SELECT published_at IS NOT NULL FROM outbox WHERE event_id=%s",
                    (event,),
                ).fetchone()[0]
                assert db.execute(
                    "SELECT publication_state,publication_attempts,last_attempt_at "
                    "FROM outbox WHERE event_id=%s", (event,),
                ).fetchone()[0:2] == ("attempted", 1)
                db.commit()

                class FailingProducer:
                    calls = 0
                    config = {"acks": -1}

                    def send(self, *_args, **_kwargs):
                        self.calls += 1

                        class FailedFuture:
                            def get(self, timeout):
                                raise TimeoutError("simulated lost Kafka acknowledgement")

                        return FailedFuture()

                failing = FailingProducer()
                with pytest.raises(outbox.PublicationError, match="after 3 attempt"):
                    publish_pending(db, failing, lease, max_retries=2)
                assert failing.calls == 3
                assert db.execute(
                    "SELECT publication_state,publication_attempts,published_at "
                    "FROM outbox WHERE event_id=%s", (event,),
                ).fetchone() == ("ambiguous", 4, None)
                db.commit()
                with connect(dsn, options=options, autocommit=True) as publisher_db:
                    assert publish_pending(publisher_db, producer, lease) == (event,)
                producer.flush(timeout=10)
            finally:
                producer.close()
            assert advance_publication_frontier(db, lease) == 20
        # Reopen PostgreSQL to verify durable acknowledgement state/frontier.
        with connect(dsn, options=options) as reopened:
            assert reopened.execute("SELECT published_at IS NOT NULL FROM outbox WHERE event_id=%s",
                                    (event,)).fetchone()[0]
            assert reopened.execute(
                "SELECT publication_state,publication_attempts,acknowledged_at IS NOT NULL "
                "FROM outbox WHERE event_id=%s", (event,),
            ).fetchone() == ("acknowledged", 5, True)
            assert reopened.execute("SELECT end_exclusive FROM outbox_publication_frontier").fetchone()[0] == 20
        consumer = KafkaConsumer(
            topic, bootstrap_servers=bootstrap, group_id="outbox-test-" + suffix,
            auto_offset_reset="earliest", enable_auto_commit=False,
            consumer_timeout_ms=5000,
        )
        try:
            records = list(consumer)
            assert len(records) == 2
            assert [record.key.decode() for record in records] == [event, event]
            payload = json.loads(records[-1].value)
            assert records[0].value == records[1].value
            with connect(dsn, options=options) as reader:
                persisted = reader.execute("SELECT delivery_envelope FROM outbox").fetchone()[0]
            assert payload == persisted
            assert payload["event_id"] == event
            assert payload["topic_incarnation"] == incarnation
            assert payload["replay_epoch"] == 0
        finally:
            consumer.close()
    finally:
        admin.delete_topics([topic])
        admin.close()
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
def test_takeover_during_kafka_send_rejects_stale_ack_and_allows_duplicate_retry():
    """A real broker ack cannot bypass SQL lease fencing after a takeover."""
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    bootstrap = os.environ["TON_SENTINEL_KAFKA_BOOTSTRAP"]
    pytest.importorskip("kafka")
    from psycopg import connect
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    suffix = uuid.uuid4().hex
    topic = "outbox-takeover-" + suffix
    schema = "ob_takeover_" + suffix
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="outbox-takeover-" + suffix)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    entered_send = threading.Event()
    release_send = threading.Event()
    worker_result = {}
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
    try:
        with connect(dsn, options=options) as db:
            migrate(db)
            configure_delivery(db, topic=topic, topic_incarnation="takeover-" + suffix)
            policy = ScanPolicy(start=100, end=110, page_size=2, max_pages=8,
                                max_actions=8, max_bytes=100_000, max_seconds=30,
                                max_requests=10)
            group = corroborate_range(
                db, RepeatedPages(synthetic_action(88, 105)),
                SourceEpoch("testnet", "takeover:source", "takeover:filter",
                            "takeover:build", "takeover:policy"), policy,
            )
            db.commit()
            current_lease = acquire_transition(db, "publisher-0-" + suffix, ttl_seconds=1)
            assert accept_corroborated_range(db, current_lease, group)
            initialize_publication_origin(db, current_lease, 100)
            event = db.execute("SELECT event_id FROM outbox").fetchone()[0]
            db.commit()
            configure_delivery(db, topic=topic, topic_incarnation="takeover-" + suffix)
            envelope = db.execute(
                "SELECT delivery_envelope FROM outbox WHERE event_id=%s", (event,)
            ).fetchone()[0]

        # Three barrier-controlled stale acknowledgements force three distinct
        # lease generations to lose ownership while their real Kafka send waits.
        for cycle in range(3):
            entered_send = threading.Event()
            release_send = threading.Event()
            worker_result = {}

            class DelayedAck:
                """Pause inside the Future, then await a real broker ack."""
                config = {"acks": -1}

                def send(self, sent_topic, *, key, value):
                    class Future:
                        def get(self, timeout):
                            entered_send.set()
                            if not release_send.wait(timeout=15):
                                raise TimeoutError("test did not release delayed Kafka send")
                            return producer.send(
                                sent_topic, key=key, value=value
                            ).get(timeout=timeout)
                    return Future()

            stale_lease = current_lease

            def publish_as_stale_holder():
                try:
                    with connect(dsn, options=options) as stale_db:
                        try:
                            publish_pending(stale_db, DelayedAck(), stale_lease, ack_timeout=20)
                        except LeaseRejected as exc:
                            # A caller commit after rejection must not persist stale SQL.
                            stale_db.commit()
                            worker_result["error"] = exc
                except BaseException as exc:
                    worker_result["error"] = exc

            worker = threading.Thread(target=publish_as_stale_holder, daemon=True)
            worker.start()
            try:
                assert entered_send.wait(timeout=10), "publisher did not enter delayed send"
                # PostgreSQL's clock provides deterministic expiry, not a race sleep.
                with connect(dsn, options=options, autocommit=True) as clock_db:
                    clock_db.execute("SELECT pg_sleep(1.2)")
                with connect(dsn, options=options) as new_db:
                    current_lease = acquire_transition(
                        new_db, f"publisher-{cycle + 1}-" + suffix,
                        ttl_seconds=30 if cycle == 2 else 1,
                    )
            finally:
                release_send.set()
            worker.join(timeout=20)
            assert not worker.is_alive(), f"stale publisher {cycle} failed to terminate"
            assert isinstance(worker_result.get("error"), LeaseRejected), worker_result

            # Confirm each acknowledged stale send leaves the marker/frontier intact.
            with connect(dsn, options=options) as check_db:
                assert check_db.execute(
                    "SELECT publication_state,publication_attempts,published_at "
                    "FROM outbox WHERE event_id=%s", (event,),
                ).fetchone() == ("attempted", cycle + 1, None)
                assert check_db.execute(
                    "SELECT count(*) FROM outbox_publication_frontier"
                ).fetchone()[0] == 0

        with connect(dsn, options=options, autocommit=True) as retry_db:
            assert publish_pending(retry_db, producer, current_lease) == (event,)
            assert advance_publication_frontier(retry_db, current_lease) == 110
        with connect(dsn, options=options) as check_db:
            assert check_db.execute(
                "SELECT published_at IS NOT NULL FROM outbox WHERE event_id=%s", (event,)
            ).fetchone()[0] is True
            assert check_db.execute(
                "SELECT end_exclusive FROM outbox_publication_frontier WHERE singleton=true"
            ).fetchone()[0] == 110
            assert check_db.execute(
                "SELECT publication_state,publication_attempts FROM outbox WHERE event_id=%s",
                (event,),
            ).fetchone() == ("acknowledged", 4)
        consumer = KafkaConsumer(
            topic, bootstrap_servers=bootstrap, group_id="outbox-takeover-" + suffix,
            auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=5000,
        )
        try:
            records = list(consumer)
            assert len(records) == 4
            assert [record.key.decode("utf-8") for record in records] == [event] * 4
            payloads = [json.loads(record.value) for record in records]
            assert [payload["event_id"] for payload in payloads] == [event] * 4
            assert payloads == [envelope] * 4
            with connect(dsn, options=options) as fresh_db:
                assert fresh_db.execute(
                    "SELECT delivery_envelope FROM outbox WHERE event_id=%s", (event,)
                ).fetchone()[0] == envelope
        finally:
            consumer.close()
    finally:
        release_send.set()
        producer.close()
        admin.delete_topics([topic])
        admin.close()
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
def test_simultaneous_takeover_contenders_fence_every_superseded_holder():
    """Bounded repeated PostgreSQL contention fences every superseded holder."""
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    bootstrap = os.environ["TON_SENTINEL_KAFKA_BOOTSTRAP"]
    pytest.importorskip("kafka")
    from psycopg import connect
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    contender_count = 4
    rounds = 8
    suffix = uuid.uuid4().hex
    topic = "outbox-contenders-" + suffix
    schema = "ob_contenders_" + suffix
    incarnation = "contenders-" + suffix
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="outbox-contenders-" + suffix)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
    leases = []
    event_expectations = []
    workers = []
    try:
        with connect(dsn, options=options) as db:
            migrate(db)
            configure_delivery(db, topic=topic, topic_incarnation=incarnation)
            seed_lease = acquire_transition(db, "contender-seed-" + suffix)
            current_lease = seed_lease
            initialize_publication_origin(db, seed_lease, 200)

        workers = []
        for round_index in range(rounds):
            start, end = 200 + round_index * 10, 210 + round_index * 10
            with connect(dsn, options=options) as db:
                policy = ScanPolicy(start=start, end=end, page_size=2, max_pages=8,
                                    max_actions=8, max_bytes=100_000, max_seconds=30,
                                    max_requests=10)
                group = corroborate_range(
                    db, RepeatedPages(synthetic_action(round_index + 40, start + 5)),
                    SourceEpoch("testnet", "contenders:source", "contenders:filter",
                                "contenders:build", "contenders:policy"), policy,
                )
                db.commit()
                assert accept_corroborated_range(db, current_lease, group)
                event, envelope = db.execute(
                    "SELECT event_id,delivery_envelope FROM outbox "
                    "WHERE payload->>'action_end'=%s", (str(start + 5),),
                ).fetchone()
                event_expectations.append((event, envelope))

            barrier = threading.Barrier(contender_count)
            round_leases, errors = [], []
            lock = threading.Lock()

            def acquire_contender(index):
                try:
                    with connect(dsn, options=options) as contender_db:
                        barrier.wait(timeout=10)
                        lease = acquire_transition(
                            contender_db, f"contender-{round_index}-{index}-" + suffix,
                            ttl_seconds=30,
                        )
                    with lock:
                        round_leases.append(lease)
                except BaseException as exc:
                    with lock:
                        errors.append(exc)

            workers = [threading.Thread(target=acquire_contender, args=(index,))
                       for index in range(contender_count)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(timeout=20)
            assert not any(worker.is_alive() for worker in workers), "takeover worker did not terminate"
            assert not errors, f"takeover worker exceptions: {errors!r}"
            assert len(round_leases) == contender_count
            leases.extend(round_leases)

            # Resolve ownership from committed database state, not thread order.
            with connect(dsn, options=options) as readback:
                current_row = readback.execute(
                    "SELECT owner,fence,expires_at FROM lease_state WHERE singleton=true"
                ).fetchone()
            current = [lease for lease in round_leases
                       if (lease.owner, lease.fence, lease.expires_at) == current_row]
            assert len(current) == 1, f"database fence did not identify one contender: {current_row!r}"
            current_lease = current[0]
            stale_leases = [lease for lease in leases if lease != current_lease]

            for stale_lease in stale_leases:
                with connect(dsn, options=options) as stale_db:
                    with pytest.raises(LeaseRejected):
                        publish_pending(stale_db, producer, stale_lease)
                    stale_db.commit()
                    with pytest.raises(LeaseRejected):
                        advance_publication_frontier(stale_db, stale_lease)
                    stale_db.commit()
                with connect(dsn, options=options) as check_db:
                    assert check_db.execute(
                        "SELECT publication_state,publication_attempts,published_at "
                        "FROM outbox WHERE event_id=%s", (event,),
                    ).fetchone() == ("pending", 0, None)
                    for expected_event, _ in event_expectations[:-1]:
                        assert check_db.execute(
                            "SELECT publication_state,publication_attempts,published_at IS NOT NULL "
                            "FROM outbox WHERE event_id=%s", (expected_event,),
                        ).fetchone() == ("acknowledged", 1, True)
                    expected_frontier = None if round_index == 0 else start
                    assert check_db.execute(
                        "SELECT end_exclusive FROM outbox_publication_frontier WHERE singleton=true"
                    ).fetchall() == ([] if expected_frontier is None else [(expected_frontier,)])

            with connect(dsn, options=options, autocommit=True) as current_db:
                assert publish_pending(current_db, producer, current_lease) == (event,)
                assert advance_publication_frontier(current_db, current_lease) == end
            with connect(dsn, options=options) as check_db:
                for expected_event, expected_envelope in event_expectations:
                    assert check_db.execute(
                        "SELECT publication_state,publication_attempts,published_at IS NOT NULL "
                        "FROM outbox WHERE event_id=%s", (expected_event,),
                    ).fetchone() == ("acknowledged", 1, True)
                    assert check_db.execute(
                        "SELECT delivery_envelope FROM outbox WHERE event_id=%s",
                        (expected_event,),
                    ).fetchone()[0] == expected_envelope
                assert check_db.execute(
                    "SELECT end_exclusive FROM outbox_publication_frontier WHERE singleton=true"
                ).fetchone()[0] == end
        consumer = KafkaConsumer(
            topic, bootstrap_servers=bootstrap, group_id="outbox-contenders-" + suffix,
            auto_offset_reset="earliest", enable_auto_commit=False, consumer_timeout_ms=5000,
        )
        try:
            records = list(consumer)
            actual = {record.key.decode("utf-8"): json.loads(record.value) for record in records}
            expected = {event_id: envelope for event_id, envelope in event_expectations}
            assert len(records) == len(actual) == len(expected) == rounds
            assert actual == expected
        finally:
            consumer.close()
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.join(timeout=20)
        assert not any(worker.is_alive() for worker in workers), "takeover worker survived cleanup"
        producer.close()
        admin.delete_topics([topic])
        admin.close()
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
def test_frontier_origin_overlap_fencing_and_transaction_boundaries(monkeypatch):
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    from psycopg import connect

    schema = "of_" + uuid.uuid4().hex
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    try:
        with connect(dsn, options=options) as db:
            migrate(db)
            lease = acquire_transition(db, "frontier-test")
            with pytest.raises(ValueError, match="initialize_publication_origin"):
                advance_publication_frontier(db, lease)

            initialize_publication_origin(db, lease, 10)
            configure_delivery(db, topic="frontier-actions", topic_incarnation=schema)
            db.commit()

            def accepted(start, end, action_end, index):
                policy = ScanPolicy(start=start, end=end, page_size=2,
                                    max_pages=8, max_actions=8,
                                    max_bytes=100_000, max_seconds=30,
                                    max_requests=10)
                group = corroborate_range(
                    db, RepeatedPages(synthetic_action(index, action_end)),
                    SourceEpoch("testnet", "frontier:source", "frontier:filter",
                                "frontier:build", "frontier:policy"), policy,
                )
                assert accept_corroborated_range(db, lease, group)
                db.execute(
                    "UPDATE outbox SET published_at=clock_timestamp(), acknowledged_at=clock_timestamp(), publication_state='acknowledged' "
                    "WHERE event_id IN (SELECT event_id FROM scan_membership WHERE range_id="
                    "(SELECT range_id FROM source_ranges WHERE start_inclusive=%s "
                    "AND end_exclusive=%s ORDER BY created_at DESC LIMIT 1))",
                    (start, end),
                )
                db.commit()

            accepted(20, 30, 25, 20)
            assert advance_publication_frontier(db, lease) is None
            accepted(10, 15, 12, 9)
            assert advance_publication_frontier(db, lease, limit=1) == 15
            accepted(10, 20, 15, 10)
            accepted(15, 30, 20, 15)
            # Prefer the greatest eligible end even when its start is later:
            # a single repair step must bridge [15,30), not stop at 20.
            assert advance_publication_frontier(db, lease, limit=1) == 30

            # An overlap bridges the frontier; same-start choices are stable and
            # the longer acknowledged range is preferred to avoid needless steps.
            accepted(30, 35, 32, 31)
            accepted(30, 40, 35, 30)
            assert advance_publication_frontier(db, lease, limit=1) == 40

            # A takeover rejects a stale token without changing the frontier.
            stale = lease
            current_lease = acquire_transition(db, "frontier-takeover")
            with pytest.raises(LeaseRejected):
                advance_publication_frontier(db, stale)
            assert advance_publication_frontier(db, current_lease) == 40

        # Even if the caller commits after a final fence rejection, the SQL
        # transaction context has already rolled back the attempted UPSERT.
        with connect(dsn, options=options) as db:
            lease = acquire_transition(db, "expiry-test")
            policy = ScanPolicy(start=40, end=50, page_size=2, max_pages=8,
                                max_actions=8, max_bytes=100_000,
                                max_seconds=30, max_requests=10)
            group = corroborate_range(
                db, RepeatedPages(synthetic_action(40, 45)),
                SourceEpoch("testnet", "frontier:source", "frontier:filter",
                            "frontier:build", "frontier:policy"), policy,
            )
            assert accept_corroborated_range(db, lease, group)
            db.execute("UPDATE outbox SET published_at=clock_timestamp(), acknowledged_at=clock_timestamp(), publication_state='acknowledged'")
            db.commit()
            real_validate = outbox.validate_transition
            calls = 0

            def expire_on_final_check(connection, token):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise LeaseRejected("forced expiry at final fence")
                return real_validate(connection, token)

            monkeypatch.setattr(outbox, "validate_transition", expire_on_final_check)
            with pytest.raises(LeaseRejected):
                advance_publication_frontier(db, lease)
            db.commit()
        with connect(dsn, options=options) as reopened:
            assert reopened.execute(
                "SELECT end_exclusive FROM outbox_publication_frontier"
            ).fetchone()[0] == 40

        # Explicit transaction blocks also work with psycopg autocommit enabled.
        with connect(dsn, options=options, autocommit=True) as db:
            lease = acquire_transition(db, "autocommit-test")
            assert advance_publication_frontier(db, lease) == 50
    finally:
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
@pytest.mark.parametrize("ranges,should_reconcile", [
    ([(10, 15), (15, 20)], True),
    ([(10, 15), (16, 20)], False),
])
def test_migration_009_reconciles_existing_frontier_only_with_contiguous_acknowledged_history(
        ranges, should_reconcile):
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    from psycopg import connect

    schema = "legacy_ob_" + uuid.uuid4().hex
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    try:
        with connect(dsn, options=options) as db:
            migrations = Path(outbox.__file__).with_name("db") / "migrations"
            for path in sorted(migrations.glob("[0-9][0-9][0-9]_*.sql")):
                if int(path.name[:3]) > 8:
                    break
                db.execute(path.read_text())
                db.commit()
            lease = acquire_transition(db, "legacy-frontier-test")
            for index, (start, end) in enumerate(ranges, start=50):
                policy = ScanPolicy(start=start, end=end, page_size=2,
                                    max_pages=8, max_actions=8,
                                    max_bytes=100_000, max_seconds=30,
                                    max_requests=10)
                group = corroborate_range(
                    db, RepeatedPages(synthetic_action(index, start + 1)),
                    SourceEpoch("testnet", "legacy:source", "legacy:filter",
                                "legacy:build", "legacy:policy"), policy,
                )
                # Seed historical acceptance without invoking a current writer
                # against the intentionally pre-delivery-binding schema.
                from ton_sentinel.normalize import normalize, semantic_digest
                range_id = db.execute(
                    "SELECT accepted_range_id FROM corroboration_groups WHERE group_id=%s",
                    (group,),
                ).fetchone()[0]
                canonical = normalize(synthetic_action(index, start + 1), SourceEpoch(
                    "testnet", "legacy:source", "legacy:filter", "legacy:build", "legacy:policy"))
                db.execute(
                    "INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                    "VALUES (%s,%s,%s::jsonb)",
                    (canonical.identity.event_id, semantic_digest(canonical), canonical.to_json()),
                )
                db.execute("INSERT INTO outbox(event_id,payload) VALUES (%s,%s::jsonb)",
                           (canonical.identity.event_id, canonical.to_json()))
                db.execute("INSERT INTO scan_membership(range_id,event_id) VALUES (%s,%s)",
                           (range_id, canonical.identity.event_id))
                db.execute("UPDATE active_source_epoch SET epoch=(SELECT epoch FROM source_ranges "
                           "WHERE range_id=%s) WHERE singleton=true", (range_id,))
                db.execute("UPDATE ingest_state SET cursor=%s WHERE singleton=true", (end,))
                db.execute("UPDATE source_ranges SET status='accepted' WHERE range_id=%s", (range_id,))
                db.execute("UPDATE corroboration_groups SET status='accepted' WHERE group_id=%s", (group,))
                db.execute(
                    "UPDATE outbox SET published_at=clock_timestamp() "
                    "WHERE event_id IN (SELECT event_id FROM scan_membership "
                    "WHERE range_id=(SELECT range_id FROM source_ranges "
                    "WHERE start_inclusive=%s AND end_exclusive=%s "
                    "ORDER BY created_at DESC LIMIT 1))",
                    (start, end),
                )
                db.commit()
            db.execute(
                "INSERT INTO outbox_publication_frontier(singleton,end_exclusive) "
                "VALUES (true,20)"
            )
            db.commit()

            # Apply 009 against an actual schema containing only 008's frontier.
            db.execute((migrations / "009_outbox_publication_origin.sql").read_text())
            db.commit()
            # Current publication code requires the lifecycle schema. Upgrade
            # the historical rows before testing origin reconciliation.
            migrate(db)
            if should_reconcile:
                initialize_publication_origin(db, lease, 10)
                assert db.execute(
                    "SELECT origin_inclusive,reconciled_frontier_end "
                    "FROM outbox_publication_origin WHERE singleton=true"
                ).fetchone() == (10, 20)
                db.commit()
                initialize_publication_origin(db, lease, 10)
                with pytest.raises(ValueError, match="immutable"):
                    initialize_publication_origin(db, lease, 9)
            else:
                with pytest.raises(ValueError, match="gap at 15"):
                    initialize_publication_origin(db, lease, 10)
                assert db.execute(
                    "SELECT count(*) FROM outbox_publication_origin"
                ).fetchone()[0] == 0
            assert db.execute(
                "SELECT end_exclusive FROM outbox_publication_frontier"
            ).fetchone()[0] == 20
    finally:
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
def test_migration_012_upgrades_legacy_rows_and_preserves_state_on_replay():
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    from psycopg import connect
    from ton_sentinel.db.migrate import migrate

    schema = "attempt_state_" + uuid.uuid4().hex
    migrations = Path(outbox.__file__).with_name("db") / "migrations"
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    ids = ("legacy-published", "legacy-unresolved", "other-unresolved")
    try:
        with connect(dsn, options=options) as db:
            # Build a real pre-012 database, including published and unresolved
            # outbox rows, rather than simulating the migration with columns.
            for path in sorted(migrations.glob("[0-9][0-9][0-9]_*.sql")):
                if int(path.name[:3]) >= 12:
                    break
                db.execute(path.read_text())
                db.commit()
            for event_id in ids:
                db.execute(
                    "INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                    "VALUES (%s,%s,'{}'::jsonb)", (event_id, event_id),
                )
            db.execute(
                "INSERT INTO outbox(event_id,payload,published_at) VALUES "
                "(%s,'{}'::jsonb,'2024-01-02T03:04:05Z'), "
                "(%s,'{}'::jsonb,NULL), (%s,'{}'::jsonb,NULL)", ids,
            )
            db.commit()

            migrate(db)
            migrated = db.execute(
                "SELECT event_id,publication_state,publication_attempts,last_attempt_at,"
                "published_at,acknowledged_at FROM outbox ORDER BY event_id"
            ).fetchall()
            by_id = {row[0]: row for row in migrated}
            assert by_id[ids[1]] == (ids[1], "pending", 0, None, None, None)
            assert by_id[ids[2]] == (ids[2], "pending", 0, None, None, None)
            published = by_id[ids[0]]
            assert published[0:3] == (ids[0], "acknowledged", 0)
            assert published[3] is None
            assert published[4] is not None
            assert published[4] == published[5]
            db.commit()

            # The stronger lifecycle invariant rejects either one-sided marker.
            import psycopg
            with pytest.raises(psycopg.errors.CheckViolation):
                with db.transaction():
                    db.execute(
                        "UPDATE outbox SET published_at=clock_timestamp() WHERE event_id=%s",
                        (ids[1],),
                    )
            with pytest.raises(psycopg.errors.CheckViolation):
                with db.transaction():
                    db.execute(
                        "UPDATE outbox SET acknowledged_at=clock_timestamp() WHERE event_id=%s",
                        (ids[1],),
                    )

            db.execute(
                "UPDATE outbox SET publication_state='attempted',publication_attempts=3,"
                "last_attempt_at='2024-02-03T04:05:06Z' WHERE event_id=%s", (ids[1],),
            )
            db.execute(
                "UPDATE outbox SET publication_state='ambiguous',publication_attempts=4,"
                "last_attempt_at='2024-03-04T05:06:07Z' WHERE event_id=%s", (ids[2],),
            )
            # Simulate the permissive original 012 constraint on a database
            # whose lifecycle columns already exist. Replaying all migrations
            # must not infer ACK from this partial marker.
            db.execute("ALTER TABLE outbox DROP CONSTRAINT outbox_publication_ack_consistent")
            db.execute(
                "ALTER TABLE outbox ADD CONSTRAINT outbox_publication_ack_consistent "
                "CHECK ((publication_state='acknowledged') = "
                "(published_at IS NOT NULL AND acknowledged_at IS NOT NULL))"
            )
            db.execute(
                "UPDATE outbox SET published_at=clock_timestamp() WHERE event_id=%s",
                (ids[1],),
            )
            db.commit()
            before = db.execute(
                "SELECT event_id,publication_state,publication_attempts,last_attempt_at,"
                "published_at,acknowledged_at FROM outbox ORDER BY event_id"
            ).fetchall()
            migrate(db)
            after = db.execute(
                "SELECT event_id,publication_state,publication_attempts,last_attempt_at,"
                "published_at,acknowledged_at FROM outbox ORDER BY event_id"
            ).fetchall()
            replayed = {row[0]: row for row in after}
            assert replayed[ids[1]][1:3] == ("attempted", 3)
            assert replayed[ids[1]][4:6] == (None, None)
            assert replayed[ids[2]][1:3] == ("ambiguous", 4)
            before_by_id = {row[0]: row for row in before}
            assert replayed[ids[0]] == before_by_id[ids[0]]
            assert replayed[ids[1]][:4] == before_by_id[ids[1]][:4]
            assert replayed[ids[2]] == before_by_id[ids[2]]
    finally:
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
def test_migration_013_preserves_historical_unbound_outbox_intents():
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    from psycopg import connect

    schema = "legacy_binding_" + uuid.uuid4().hex
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    try:
        with connect(dsn, options=options) as db:
            migrations = Path(outbox.__file__).with_name("db") / "migrations"
            for path in sorted(migrations.glob("[0-9][0-9][0-9]_*.sql")):
                if int(path.name[:3]) > 12:
                    break
                db.execute(path.read_text())
                db.commit()
            db.execute(
                "INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                "VALUES (%s,%s,%s::jsonb)",
                ("legacy-event", "legacy-digest", json.dumps({"legacy": True})),
            )
            db.execute("INSERT INTO outbox(event_id,payload) VALUES (%s,%s::jsonb)",
                       ("legacy-event", json.dumps({"legacy": True})))
            db.commit()
            for name in ("013_outbox_delivery_binding.sql", "014_outbox_delivery_authority.sql"):
                db.execute((migrations / name).read_text())
                db.commit()
            configure_delivery(db, topic="new-topic", topic_incarnation="new-inc")
            assert db.execute(
                "SELECT delivery_topic,delivery_topic_incarnation,replay_epoch,"
                "delivery_envelope FROM outbox WHERE event_id='legacy-event'"
            ).fetchone() == (None, None, 0, None)
            db.commit()
    finally:
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
@pytest.mark.parametrize("marker", ["published_at", "acknowledged_at"])
def test_partial_acknowledgement_cannot_advance_or_reconcile_frontier(marker):
    """A published marker alone is not proof of a complete acknowledgement."""
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    from psycopg import connect

    schema = "partial_ack_" + uuid.uuid4().hex
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    try:
        with connect(dsn, options=options) as db:
            migrate(db)
            lease = acquire_transition(db, "partial-ack-test")
            configure_delivery(db, topic="partial-ack-actions", topic_incarnation=schema)
            initialize_publication_origin(db, lease, 10)
            policy = ScanPolicy(start=10, end=20, page_size=2, max_pages=8,
                                max_actions=8, max_bytes=100_000, max_seconds=30,
                                max_requests=10)
            group = corroborate_range(
                db, RepeatedPages(synthetic_action(101, 15)),
                SourceEpoch("testnet", "partial:source", "partial:filter",
                            "partial:build", "partial:policy"), policy,
            )
            assert accept_corroborated_range(db, lease, group)
            event = db.execute("SELECT event_id FROM outbox").fetchone()[0]
            # Model either partial row allowed by the previous 012 CHECK so the
            # progress queries are independently required to fail closed.
            db.execute("ALTER TABLE outbox DROP CONSTRAINT outbox_publication_ack_consistent")
            db.execute(
                f"UPDATE outbox SET {marker}=clock_timestamp() WHERE event_id=%s",
                (event,),
            )
            db.commit()

            assert advance_publication_frontier(db, lease) is None
            db.execute(
                "INSERT INTO outbox_publication_frontier(singleton,end_exclusive) "
                "VALUES (true,20)"
            )
            db.execute("DELETE FROM outbox_publication_origin")
            db.commit()
            with pytest.raises(ValueError, match="acknowledged accepted coverage"):
                initialize_publication_origin(db, lease, 10)
            assert db.execute(
                "SELECT count(*) FROM outbox_publication_origin"
            ).fetchone()[0] == 0
    finally:
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()

@pytest.mark.integration
def test_migration_016_keeps_pre_generation_bound_intents_publishable_and_replays():
    """Persisted canonical generation semantics."""
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    from psycopg import connect
    from psycopg.errors import CheckViolation

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

    schema = "legacy_generation_" + uuid.uuid4().hex
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    try:
        with connect(dsn, options=options) as db:
            migrations = Path(outbox.__file__).with_name("db") / "migrations"
            for path in sorted(migrations.glob("[0-9][0-9][0-9]_*.sql")):
                if int(path.name[:3]) > 15:
                    break
                db.execute(path.read_text())
                db.commit()
            configure_delivery(db, topic="legacy-generation", topic_incarnation="legacy-inc")
            action = {"legacy": True}
            envelope = {"envelope_version": 1, "event_id": "legacy-bound",
                        "topic_incarnation": "legacy-inc", "replay_epoch": 0,
                        "action": action}
            db.execute("INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                       "VALUES ('legacy-bound','d',%s::jsonb)", (json.dumps(action),))
            db.execute("INSERT INTO outbox(event_id,payload,delivery_topic,"
                       "delivery_topic_incarnation,replay_epoch,delivery_envelope) "
                       "VALUES ('legacy-bound',%s::jsonb,'legacy-generation','legacy-inc',0,"
                       "%s::jsonb)", (json.dumps(action), json.dumps(envelope)))
            db.commit()
            migrate(db)
            assert db.execute(
                "SELECT c.accepted_generation,o.accepted_generation,o.delivery_envelope "
                "FROM canonical_actions c JOIN outbox o USING(event_id)").fetchone() == (
                    None, None, envelope)
            assert db.execute("SELECT generation FROM canonical_generation").fetchone() == (0,)
            db.execute("INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                       "VALUES ('post-016','d',%s::jsonb)", (json.dumps(action),))
            generation = db.execute("SELECT accepted_generation FROM canonical_actions "
                                    "WHERE event_id='post-016'").fetchone()[0]
            v2 = dict(envelope, envelope_version=2, event_id="post-016",
                      accepted_generation=generation)
            db.execute("INSERT INTO outbox(event_id,payload,delivery_topic,"
                       "delivery_topic_incarnation,replay_epoch,accepted_generation,"
                       "delivery_envelope) VALUES ('post-016',%s::jsonb,'legacy-generation',"
                       "'legacy-inc',0,%s,%s::jsonb)",
                       (json.dumps(action), generation, json.dumps(v2)))
            db.commit()
            # Replaying every migration with generation-bearing rows present must
            # neither fail nor weaken the envelope CHECK.
            migrate(db)
            migrate(db)
            assert generation == 1
            db.execute("INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                       "VALUES ('post-replay','d',%s::jsonb)", (json.dumps(action),))
            with pytest.raises(CheckViolation):
                db.execute("INSERT INTO outbox(event_id,payload,delivery_topic,"
                           "delivery_topic_incarnation,replay_epoch,accepted_generation,"
                           "delivery_envelope) VALUES ('post-replay',%s::jsonb,"
                           "'legacy-generation','legacy-inc',0,NULL,%s::jsonb)",
                           (json.dumps(action), json.dumps(dict(v2, event_id="post-replay"))))
            db.rollback()
            lease = acquire_transition(db, "legacy-generation-publisher")
            producer = AckProducer()
            assert publish_pending(db, producer, lease) == ("legacy-bound", "post-016")
            assert [json.loads(value) for _, _, value in producer.sent] == [envelope, v2]
    finally:
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()
