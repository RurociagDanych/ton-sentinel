"""Real PostgreSQL/Spark regression for canonical broker-triggered replacement."""
import base64
import json
import os
import uuid

import psycopg
import pytest
from pyspark.sql import SparkSession

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.ledger import migrate, accept_corroborated_range
from ton_sentinel.db.serving import read_features
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.scan import ScanPolicy, corroborate_range
from ton_sentinel.spark.streaming import process_canonical_microbatch


class RepeatedPages:
    def __init__(self, value):
        self.value = value
        self.walk = -1

    def fetch_page(self, source_epoch, start, end, offset, limit):
        if offset == 0:
            self.walk += 1
        return [self.value][offset:offset + limit]


def _source_action(index, action_end):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet",
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": action_end, "status": "success", "eligible": True,
        "route_proven": True, "route": ["TON", "JETTON:CANONICAL"],
        "input": {"asset": "TON", "raw_amount": str(index * 10)},
        "output": {"asset": "JETTON:CANONICAL", "raw_amount": str(index * 3)},
    }


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("canonical-stream-regression")
               .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session
    session.stop()


@pytest.fixture

def database():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "canonical_stream_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    scoped = psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={schema}")
    try:
        with psycopg.connect(scoped) as connection:
            migrate(connection)
        yield scoped
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _accepted(connection, lease, start, end, index, action_end):
    policy = ScanPolicy(start=start, end=end, page_size=2, max_pages=8,
                        max_actions=8, max_bytes=100_000, max_seconds=30,
                        max_requests=10)
    group = corroborate_range(
        connection, RepeatedPages(_source_action(index, action_end)),
        SourceEpoch("testnet", "canonical:source", "canonical:filter",
                    "canonical:build", "canonical:policy"), policy,
    )
    connection.commit()
    assert accept_corroborated_range(connection, lease, group)


def _batch(spark, rows):
    return spark.createDataFrame(rows, "key binary, value binary")


def _audited_batch(spark, rows):
    return spark.createDataFrame(
        rows, "topic string, partition int, offset long, key binary, value binary",
    )


def _audited_row(topic, offset, event_id, action, incarnation, generation):
    envelope = {
        "envelope_version": 2, "event_id": event_id, "accepted_generation": generation,
        "topic_incarnation": incarnation, "replay_epoch": 0, "action": action,
    }
    return topic, 0, offset, event_id.encode(), json.dumps(
        envelope, separators=(",", ":"),
    ).encode()


def test_broker_triggers_recompute_complete_accepted_window_and_reject_bad_rows(spark, database):
    incarnation = "canonical-test-" + uuid.uuid4().hex
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "canonical-stream-test", ttl_seconds=300)
        configure_delivery(connection, topic="canonical-test", topic_incarnation=incarnation)
        _accepted(connection, lease, 10, 20, 11, 15)
        _accepted(connection, lease, 20, 30, 21, 25)

        events = connection.execute(
            "SELECT o.event_id,o.payload,o.accepted_generation FROM outbox o ORDER BY o.event_id"
        ).fetchall()
        connection.commit()
        assert len(events) == 2
        trigger_event, trigger_action, trigger_generation = events[-1]
        envelope = {
            "envelope_version": 2, "event_id": trigger_event,
            "accepted_generation": trigger_generation,
            "topic_incarnation": incarnation, "replay_epoch": 0,
            "action": trigger_action,
        }
        trigger = (trigger_event.encode(), json.dumps(envelope, separators=(",", ":")).encode())

        # Only the second accepted action triggers the batch, but both accepted
        # canonical actions in that five-minute window contribute to the result.
        assert process_canonical_microbatch(
            connection, lease, spark, _batch(spark, [trigger]),
            topic_incarnation=incarnation, replay_epoch=0,
        ) == 2
        before = read_features(connection)
        connection.commit()
        assert len(before) == 2
        ton = next(row for row in before if row["token"] == "TON")
        jetton = next(row for row in before if row["token"] == "JETTON:CANONICAL")
        assert ton["swaps"] == jetton["swaps"] == 2
        assert ton["input_raw"] == "320"
        assert jetton["output_raw"] == "96"
        assert ton["generation"] == jetton["generation"] == "canonical:build"
        assert ton["coverage_status"] == jetton["coverage_status"] == "unknown"
        assert ton["status"] == jetton["status"] == "provisional"
        assert ton["provisional"] is jetton["provisional"] is True

        # At-least-once broker replay performs replacement, not additive writes.
        assert process_canonical_microbatch(
            connection, lease, spark, _batch(spark, [trigger, trigger]),
            topic_incarnation=incarnation, replay_epoch=0,
        ) == 2
        assert read_features(connection) == before
        connection.commit()

        # Python equality alone conflates booleans, integers, and floats. Each
        # received payload must satisfy the typed action contract independently.
        for field, value in (("source_eligible", 1),
                             ("action_end", float(trigger_action["action_end"])),
                             ("schema_version", True)):
            malformed = dict(trigger_action, **{field: value})
            assert malformed == trigger_action
            malformed_envelope = dict(envelope, action=malformed)
            with pytest.raises(ValueError, match="broker action payload is malformed"):
                process_canonical_microbatch(
                    connection, lease, spark,
                    _batch(spark, [(trigger_event.encode(), json.dumps(malformed_envelope).encode())]),
                    topic_incarnation=incarnation, replay_epoch=0,
                )
            connection.commit()
            assert read_features(connection) == before
            connection.commit()

        mismatched = dict(trigger_action)
        mismatched["status"] = "failed"
        bad_envelope = dict(envelope, action=mismatched)
        with pytest.raises(ValueError, match="does not match"):
            process_canonical_microbatch(
                connection, lease, spark,
                _batch(spark, [(trigger_event.encode(), json.dumps(bad_envelope).encode())]),
                topic_incarnation=incarnation, replay_epoch=0,
            )
        unknown_event = "ton-sentinel:event:v1:[\"testnet\",\"unknown\",\"unknown\"]"
        unknown_envelope = dict(envelope, event_id=unknown_event)
        with pytest.raises(ValueError, match="not an accepted"):
            process_canonical_microbatch(
                connection, lease, spark,
                _batch(spark, [(unknown_event.encode(), json.dumps(unknown_envelope).encode())]),
                topic_incarnation=incarnation, replay_epoch=0,
            )
        assert read_features(connection) == before


def test_audited_empty_batches_and_batch_key_reuse_are_transactional(spark, database):
    topic = "canonical-audited"
    incarnation = "canonical-audit-test-" + uuid.uuid4().hex
    environment_id = "test-environment"
    query_id = "canonical-query"
    pipeline_epoch = "canonical-pipeline"
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "canonical-audit-test", ttl_seconds=300)
        configure_delivery(connection, topic=topic, topic_incarnation=incarnation)
        _accepted(connection, lease, 10, 20, 31, 15)
        event_id, action, generation = connection.execute(
            "SELECT o.event_id,o.payload,o.accepted_generation FROM outbox o "
            "ORDER BY o.event_id LIMIT 1"
        ).fetchone()
        connection.commit()

        def run(batch_id, rows):
            return process_canonical_microbatch(
                connection, lease, spark, _audited_batch(spark, rows),
                topic_incarnation=incarnation, replay_epoch=0,
                batch_id=batch_id, environment_id=environment_id,
                query_id=query_id, pipeline_epoch=pipeline_epoch, topic=topic,
            )

        def audit_rows():
            return connection.execute(
                "SELECT batch_id,offset_metadata,record_digest,record_count FROM stream_batches "
                "ORDER BY batch_id"
            ).fetchall()

        initial_features = read_features(connection)
        connection.commit()
        assert initial_features == []

        assert run(0, []) == 0
        connection.commit()
        first_empty_audit = audit_rows()
        assert len(first_empty_audit) == 1
        assert first_empty_audit[0][0] == 0
        assert first_empty_audit[0][1] == []
        assert first_empty_audit[0][2]
        assert first_empty_audit[0][3] == 0
        connection.commit()
        assert run(0, []) == 0
        assert audit_rows() == first_empty_audit
        assert read_features(connection) == initial_features
        connection.commit()

        valid_row = _audited_row(topic, 100, event_id, action, incarnation, generation)
        with pytest.raises(ValueError, match="conflicting stream batch audit identity reuse"):
            run(0, [valid_row])
        assert audit_rows() == first_empty_audit
        assert read_features(connection) == initial_features
        connection.commit()

        assert run(1, [valid_row]) == 2
        connection.commit()
        serving_after_nonempty = read_features(connection)
        audits_after_nonempty = audit_rows()
        assert len(serving_after_nonempty) == 2
        assert len(audits_after_nonempty) == 2
        assert audits_after_nonempty[1][3] == 1
        connection.commit()

        assert run(1, [valid_row]) == 2
        assert read_features(connection) == serving_after_nonempty
        assert audit_rows() == audits_after_nonempty
        connection.commit()
        with pytest.raises(ValueError, match="conflicting stream batch audit identity reuse"):
            run(1, [_audited_row(topic, 101, event_id, action, incarnation, generation)])
        assert read_features(connection) == serving_after_nonempty
        assert audit_rows() == audits_after_nonempty
        connection.commit()

        with pytest.raises(ValueError, match="conflicting stream batch audit identity reuse"):
            run(1, [])
        assert read_features(connection) == serving_after_nonempty
        assert audit_rows() == audits_after_nonempty
        connection.commit()

        # Delay a real audit insert past a one-second lease. The final adapter
        # validation must abort the audit, even if the caller commits afterward.
        suffix = uuid.uuid4().hex
        trigger = f"expire_audit_{suffix}"
        function = f"sleep_audit_{suffix}"
        connection.execute(
            f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS "
            "$$ BEGIN PERFORM pg_sleep(1.2); RETURN NEW; END $$"
        )
        connection.execute(
            f"CREATE TRIGGER {trigger} BEFORE INSERT ON stream_batches "
            f"FOR EACH ROW EXECUTE FUNCTION {function}()"
        )
        connection.commit()
        short_lease = acquire_transition(connection, "canonical-audit-expiry", ttl_seconds=1)
        connection.commit()
        expired_batch = 2
        try:
            with pytest.raises(RuntimeError, match="lease expired or fencing token"):
                process_canonical_microbatch(
                    connection, short_lease, spark, _audited_batch(spark, []),
                    topic_incarnation=incarnation, replay_epoch=0,
                    batch_id=expired_batch, environment_id=environment_id,
                    query_id=query_id, pipeline_epoch=pipeline_epoch, topic=topic,
                )
            connection.commit()
            assert connection.execute(
                "SELECT count(*) FROM stream_batches WHERE environment_id=%s AND query_id=%s "
                "AND pipeline_epoch=%s AND batch_id=%s",
                (environment_id, query_id, pipeline_epoch, expired_batch),
            ).fetchone()[0] == 0
            connection.commit()
            with psycopg.connect(database) as independent:
                assert independent.execute(
                    "SELECT count(*) FROM stream_batches WHERE environment_id=%s AND query_id=%s "
                    "AND pipeline_epoch=%s AND batch_id=%s",
                    (environment_id, query_id, pipeline_epoch, expired_batch),
                ).fetchone()[0] == 0
                assert read_features(independent) == serving_after_nonempty
        finally:
            connection.execute(f"DROP TRIGGER IF EXISTS {trigger} ON stream_batches")
            connection.execute(f"DROP FUNCTION IF EXISTS {function}()")
            connection.commit()


def test_broker_envelope_must_carry_the_persisted_canonical_generation(spark, database):
    """Persisted canonical generation semantics."""
    incarnation = "canonical-generation-" + uuid.uuid4().hex
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "canonical-generation-test", ttl_seconds=300)
        configure_delivery(connection, topic="canonical-generation", topic_incarnation=incarnation)
        _accepted(connection, lease, 10, 20, 41, 15)
        _accepted(connection, lease, 20, 30, 43, 25)
        stored = connection.execute(
            "SELECT event_id,delivery_envelope FROM outbox ORDER BY accepted_generation"
        ).fetchall()
        connection.commit()
        assert [row[1]["accepted_generation"] for row in stored] == [1, 2]
        event_id, persisted = stored[1]
        legacy = {key: value for key, value in persisted.items() if key != "accepted_generation"}
        legacy["envelope_version"] = 1
        forged = [legacy, dict(persisted, accepted_generation=1),
                  dict(persisted, accepted_generation=3)]
        for envelope in forged:
            with pytest.raises(ValueError, match="generation|envelope"):
                process_canonical_microbatch(
                    connection, lease, spark,
                    _batch(spark, [(event_id.encode(), json.dumps(envelope).encode())]),
                    topic_incarnation=incarnation, replay_epoch=0,
                )
            connection.commit()
            assert read_features(connection) == []
            connection.commit()
        for envelope in (dict(persisted, accepted_generation=True),
                         dict(persisted, accepted_generation=0)):
            with pytest.raises(ValueError, match="envelope"):
                process_canonical_microbatch(
                    connection, lease, spark,
                    _batch(spark, [(event_id.encode(), json.dumps(envelope).encode())]),
                    topic_incarnation=incarnation, replay_epoch=0,
                )
            connection.commit()
        assert process_canonical_microbatch(
            connection, lease, spark,
            _batch(spark, [(event_id.encode(), json.dumps(persisted).encode())]),
            topic_incarnation=incarnation, replay_epoch=0,
        ) == 2
