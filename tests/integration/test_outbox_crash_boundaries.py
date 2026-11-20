"""Real process kills at the three acknowledgement boundaries."""

import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
BOUNDARIES = ["before_pg_commit", "after_pg_commit", "after_kafka_ack"]


def child(boundary, dsn, options, bootstrap, topic):
    """Runs in a separate interpreter and dies abruptly at the boundary."""
    from kafka import KafkaProducer
    from psycopg import connect
    from test_outbox_ack_protocol import accept
    from ton_sentinel.db.fencing import acquire_transition
    from ton_sentinel.db.ledger import accept_corroborated_range
    from ton_sentinel.outbox import initialize_publication_origin, publish_pending

    db = connect(dsn, options=options)
    lease = acquire_transition(db, "child-" + boundary)
    if boundary == "before_pg_commit":
        from test_outbox_ack_protocol import RepeatedPages, action
        from ton_sentinel.contracts import SourceEpoch
        from ton_sentinel.source.scan import ScanPolicy, corroborate_range
        policy = ScanPolicy(start=10, end=20, page_size=2, max_pages=8, max_actions=8,
                            max_bytes=100_000, max_seconds=30, max_requests=10)
        group = corroborate_range(
            db, RepeatedPages(action(8, 11)),
            SourceEpoch("testnet", "ack:source", "ack:filter", "ack:build", "ack:policy"), policy)
        db.commit()
        from ton_sentinel.db import ledger
        real_validate = ledger.validate_transition

        def die_with_intent_written_but_uncommitted(connection, held):
            real_validate(connection, held)
            if connection.execute("SELECT count(*) FROM outbox").fetchone()[0]:
                os._exit(137)

        ledger.validate_transition = die_with_intent_written_but_uncommitted
        accept_corroborated_range(db, lease, group)
        os._exit(2)
    accept(db, lease, 10, 20, 8)
    initialize_publication_origin(db, lease, 10)
    db.commit()
    if boundary == "after_pg_commit":
        os._exit(137)
    real = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)

    class Killer:
        config = real.config

        def send(self, *args, **kwargs):
            future = real.send(*args, **kwargs)
            future.get(timeout=30)
            os._exit(137)

    publish_pending(db, Killer(), lease)
    os._exit(2)


@pytest.mark.integration
@pytest.mark.parametrize("boundary", BOUNDARIES)
def test_crash_ack_boundaries(boundary):
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    from test_outbox_ack_protocol import accept, env
    from ton_sentinel.db.fencing import acquire_transition
    from ton_sentinel.outbox import (advance_publication_frontier,
                                     initialize_publication_origin, publish_pending)

    with env() as (db, bootstrap, records, (dsn, options)):
        topic = db.execute("SELECT topic FROM outbox_delivery_config").fetchone()[0]
        db.commit()
        code = ("import sys; sys.path.insert(0, %r); sys.path.insert(0, %r); "
                "import test_outbox_crash_boundaries as m; m.child(%r, %r, %r, %r, %r)"
                % (HERE, os.path.join(HERE, "..", "..", "src"), boundary, dsn, options,
                   bootstrap, topic))
        proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True,
                              timeout=120)
        assert proc.returncode == 137, (proc.returncode, proc.stdout, proc.stderr)

        def counts():
            row = db.execute("SELECT (SELECT count(*) FROM canonical_actions),"
                             "(SELECT count(*) FROM outbox)").fetchone()
            db.commit()
            return row

        lease = acquire_transition(db, "recover-" + boundary)
        if boundary == "before_pg_commit":
            assert counts() == (0, 0)
            assert records() == []
            assert db.execute("SELECT count(*) FROM source_ranges WHERE status='accepted'"
                              ).fetchone()[0] == 0
            db.commit()
            accept(db, lease, 10, 20, 8)
            initialize_publication_origin(db, lease, 10)
            db.commit()
        else:
            assert counts() == (1, 1)
            state = db.execute("SELECT publication_state FROM outbox").fetchone()[0]
            db.commit()
            assert state == ("pending" if boundary == "after_pg_commit" else "attempted")
            assert advance_publication_frontier(db, lease) is None
            assert len(records()) == (0 if boundary == "after_pg_commit" else 1)
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
        try:
            assert len(publish_pending(db, producer, lease)) == 1
        finally:
            producer.close()
        assert counts() == (1, 1)
        assert advance_publication_frontier(db, lease) == 20
        keys = records()
        assert len(set(keys)) == 1
        assert len(keys) == (2 if boundary == "after_kafka_ack" else 1)


@pytest.mark.integration
def test_empty_range_progress():
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    from test_outbox_ack_protocol import accept, env
    from ton_sentinel.contracts import SourceEpoch
    from ton_sentinel.db.fencing import acquire_transition
    from ton_sentinel.db.ledger import accept_corroborated_range
    from ton_sentinel.outbox import (advance_publication_frontier,
                                     initialize_publication_origin, publish_pending)
    from ton_sentinel.source.scan import ScanPolicy, corroborate_range

    class NoActions:
        def fetch_page(self, source_epoch, start, end, offset, limit):
            return []

    with env() as (db, bootstrap, records, _):
        lease = acquire_transition(db, "empty")
        initialize_publication_origin(db, lease, 10)
        policy = ScanPolicy(start=10, end=20, page_size=2, max_pages=8, max_actions=8,
                            max_bytes=100_000, max_seconds=30, max_requests=10)
        group = corroborate_range(
            db, NoActions(),
            SourceEpoch("testnet", "ack:source", "ack:filter", "ack:build", "ack:policy"), policy)
        assert accept_corroborated_range(db, lease, group)
        db.commit()
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
        db.commit()
        # A complete empty range needs no Kafka send to publish its disposition.
        assert advance_publication_frontier(db, lease) == 20
        accept(db, lease, 20, 30, 20)
        assert advance_publication_frontier(db, lease) == 20
        producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
        try:
            assert len(publish_pending(db, producer, lease)) == 1
        finally:
            producer.close()
        assert advance_publication_frontier(db, lease) == 30
