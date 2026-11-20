"""Real PostgreSQL + Kafka acknowledgement, outage, backpressure, hole, lease."""

import base64
import os
import socket
import time
import uuid
from contextlib import contextmanager

import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.ledger import accept_corroborated_range, migrate
from ton_sentinel.outbox import (PublicationError, advance_publication_frontier,
                                 configure_delivery, initialize_publication_origin,
                                 publish_pending)
from ton_sentinel.source.scan import ScanPolicy, corroborate_range


class RepeatedPages:
    def __init__(self, value):
        self.value, self.walk = value, -1

    def fetch_page(self, source_epoch, start, end, offset, limit):
        if offset == 0:
            self.walk += 1
        return [self.value][offset:offset + limit]


def action(index, action_end):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True, "network": "testnet",
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1", "action_end": action_end,
        "status": "success", "eligible": True, "route_proven": True, "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }


@contextmanager
def env():
    from kafka import KafkaConsumer
    from kafka.admin import KafkaAdminClient, NewTopic
    from psycopg import connect
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    bootstrap = os.environ["TON_SENTINEL_KAFKA_BOOTSTRAP"]
    suffix = uuid.uuid4().hex
    topic, schema = "ack-" + suffix, "ak_" + suffix
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="ack-" + suffix)
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"

    def records():
        consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap, group_id="g-" + uuid.uuid4().hex,
                                 auto_offset_reset="earliest", enable_auto_commit=False,
                                 consumer_timeout_ms=4000)
        try:
            return [r.key.decode() for r in consumer]
        finally:
            consumer.close()

    try:
        with connect(dsn, options=options) as db:
            migrate(db)
            configure_delivery(db, topic=topic, topic_incarnation="inc-" + suffix)
            db.commit()
            yield db, bootstrap, records, (dsn, options)
    finally:
        admin.delete_topics([topic])
        admin.close()
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


def accept(db, lease, start, end, index):
    policy = ScanPolicy(start=start, end=end, page_size=2, max_pages=8, max_actions=8,
                        max_bytes=100_000, max_seconds=30, max_requests=10)
    group = corroborate_range(
        db, RepeatedPages(action(index, start + 1)),
        SourceEpoch("testnet", "ack:source", "ack:filter", "ack:build", "ack:policy"), policy)
    assert accept_corroborated_range(db, lease, group)
    db.commit()


def state(db):
    rows = db.execute("SELECT publication_state,publication_attempts,published_at IS NOT NULL "
                      "FROM outbox").fetchall()
    db.commit()
    return rows


@pytest.mark.integration
def test_broker_outage_is_ambiguous_bounded_and_recovers_after_reconnect():
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    with env() as (db, bootstrap, records, _):
        lease = acquire_transition(db, "outage")
        accept(db, lease, 10, 20, 8)
        initialize_publication_origin(db, lease, 10)
        db.commit()
        down = KafkaProducer(bootstrap_servers="127.0.0.1:1", acks="all",
                             api_version=(2, 5, 0), max_block_ms=300, retries=0)
        try:
            with pytest.raises(PublicationError, match="after 2 attempt"):
                publish_pending(db, down, lease, max_retries=1, ack_timeout=1)
        finally:
            down.close(timeout=1)
        db.commit()
        assert state(db) == [("ambiguous", 2, False)]
        assert advance_publication_frontier(db, lease) is None
        assert records() == []
        up = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
        try:
            assert len(publish_pending(db, up, lease)) == 1
        finally:
            up.close()
        assert state(db) == [("acknowledged", 3, True)]
        assert advance_publication_frontier(db, lease) == 20
        assert len(records()) == 1


@pytest.mark.integration
def test_stalled_broker_backpressure_never_falsely_acknowledges():
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    with env() as (db, bootstrap, records, _):
        lease = acquire_transition(db, "pressure")
        accept(db, lease, 10, 20, 8)
        initialize_publication_origin(db, lease, 10)
        db.commit()
        # A stalled broker: the socket accepts but never answers, so the real
        # producer blocks for metadata and send() times out (producer backpressure).
        stalled = socket.socket()
        stalled.bind(("127.0.0.1", 0))
        stalled.listen(8)
        producer = KafkaProducer(bootstrap_servers=f"127.0.0.1:{stalled.getsockname()[1]}",
                                 acks="all", retries=0, api_version=(2, 5, 0),
                                 max_block_ms=300, request_timeout_ms=1000)
        try:
            with pytest.raises(PublicationError):
                publish_pending(db, producer, lease, max_retries=1, ack_timeout=1)
        finally:
            producer.close(timeout=1)
            stalled.close()
        db.commit()
        assert state(db) == [("ambiguous", 2, False)]
        assert advance_publication_frontier(db, lease) is None


@pytest.mark.integration
def test_frontier_waits_for_earlier_unacknowledged_range_then_advances_contiguously():
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    with env() as (db, bootstrap, records, _):
        lease = acquire_transition(db, "hole")
        initialize_publication_origin(db, lease, 10)
        accept(db, lease, 20, 30, 20)
        accept(db, lease, 10, 20, 8)
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
        try:
            first = publish_pending(db, producer, lease, limit=1)
            assert len(first) == 1
            acked_range = db.execute(
                "SELECT r.start_inclusive FROM scan_membership m JOIN source_ranges r "
                "USING(range_id) WHERE m.event_id=%s", (first[0],)).fetchone()[0]
            db.commit()
            assert acked_range == 20
            assert advance_publication_frontier(db, lease) is None
            assert len(publish_pending(db, producer, lease)) == 1
        finally:
            producer.close()
        assert advance_publication_frontier(db, lease) == 30
        assert len(records()) == 2


@pytest.mark.integration
def test_expired_lease_cannot_send_or_acknowledge_and_leaves_intent_pending():
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    with env() as (db, bootstrap, records, _):
        lease = acquire_transition(db, "short", ttl_seconds=1)
        accept(db, lease, 10, 20, 8)
        time.sleep(1.5)
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
        try:
            with pytest.raises(LeaseRejected):
                publish_pending(db, producer, lease)
            producer.flush(timeout=5)
        finally:
            producer.close()
        db.rollback()
        assert state(db) == [("pending", 0, False)]
        assert records() == []


@pytest.mark.integration
def test_ack_timeout_after_send_is_ambiguous_then_duplicate_delivery_is_stable():
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    with env() as (db, bootstrap, records, _):
        lease = acquire_transition(db, "late-ack")
        accept(db, lease, 10, 20, 8)
        initialize_publication_origin(db, lease, 10)
        db.commit()
        slow = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3, linger_ms=3000)
        try:
            with pytest.raises(PublicationError, match="after 1 attempt"):
                publish_pending(db, slow, lease, max_retries=0, ack_timeout=0.2)
            assert state(db) == [("ambiguous", 1, False)]
            assert advance_publication_frontier(db, lease) is None
            slow.flush(timeout=15)
        finally:
            slow.close()
        fast = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
        try:
            assert len(publish_pending(db, fast, lease)) == 1
        finally:
            fast.close()
        assert state(db) == [("acknowledged", 2, True)]
        assert advance_publication_frontier(db, lease) == 20
        keys = records()
        assert len(keys) == 2 and keys[0] == keys[1]
