"""Real Kafka → canonical Spark adapter → PostgreSQL restart regression."""
from __future__ import annotations

import base64
import json
import os
import threading
import uuid
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from ton_sentinel.api import create_app
from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.ledger import migrate
from ton_sentinel.ingest import OutboxBudget, ingest_range
from ton_sentinel.outbox import configure_delivery, publish_pending
from ton_sentinel.source.client import RESTPageClient
from ton_sentinel.source.scan import ScanPolicy
import ton_sentinel.spark.streaming as streaming
from ton_sentinel.stream_cli import ConsumerConfig, run_consumer


def _source_action(index: int, action_end: int) -> dict:
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


@pytest.fixture
def database():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "canonical_kafka_" + uuid.uuid4().hex
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


def _accept(connection, start: int, end: int, index: int, action_end: int) -> None:
    action = _source_action(index, action_end)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            query = parse_qs(urlsplit(self.path).query)
            payload = [action] if query.get("offset") == ["0"] else []
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    policy = ScanPolicy(start=start, end=end, page_size=2, max_pages=8,
                        max_actions=8, max_bytes=100_000, max_seconds=30,
                        max_requests=10)
    try:
        result = ingest_range(
            connection, RESTPageClient(f"http://127.0.0.1:{server.server_port}", "/v1/actions"),
            SourceEpoch("testnet", "canonical:source", "canonical:filter",
                        "canonical:build", "canonical:policy"), policy,
            owner="canonical-kafka-" + uuid.uuid4().hex,
            budget=OutboxBudget(max_pending_bytes=1_000_000,
                                max_oldest_age_seconds=3600),
        )
        assert result.status == "accepted" and result.accepted
        assert len(result.promoted_event_ids) == 1
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def _http_json(app, path: str) -> tuple[str, dict]:
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with urlopen(f"http://127.0.0.1:{server.server_port}{path}", timeout=5) as response:
            return f"{response.status} {response.reason}", json.loads(response.read())
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_real_kafka_canonical_stream_resumes_and_retries_failed_batch(
        database, tmp_path, monkeypatch):
    bootstrap = os.environ.get("TON_SENTINEL_TEST_KAFKA_BOOTSTRAP")
    if not bootstrap:
        pytest.fail("TON_SENTINEL_TEST_KAFKA_BOOTSTRAP must name the owned disposable Kafka")
    topic = "canonical-" + uuid.uuid4().hex
    incarnation = "canonical-incarnation-" + uuid.uuid4().hex
    checkpoint = (tmp_path / "durable-canonical-checkpoint").resolve()
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15_000)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    finally:
        admin.close()
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3,
                             max_block_ms=15_000)
    try:
        with psycopg.connect(database) as connection:
            configure_delivery(connection, topic=topic, topic_incarnation=incarnation)
            _accept(connection, 10, 20, 11, 15)
            lease = acquire_transition(connection, "canonical-kafka-publish-" + uuid.uuid4().hex,
                                       ttl_seconds=300)
            assert publish_pending(connection, producer, lease, limit=10) == tuple(
                row[0] for row in connection.execute(
                    "SELECT event_id FROM outbox ORDER BY created_at,event_id"
                ).fetchall()
            )

        config = ConsumerConfig(
            dsn=database, bootstrap_servers=bootstrap, topic=topic,
            checkpoint=str(checkpoint), topic_incarnation=incarnation,
            replay_epoch=0, max_offsets_per_trigger=20,
            environment_id="local-test", query_id="canonical-features",
            pipeline_epoch="epoch-a",
        )
        processed = []
        original_process = streaming.process_canonical_microbatch

        def record_batch(*args, **kwargs):
            rows = args[3].select("partition", "offset", "key").collect()
            processed.append([(row.partition, row.offset,
                               row.key.decode("utf-8")) for row in rows])
            return original_process(*args, **kwargs)

        monkeypatch.setattr(streaming, "process_canonical_microbatch", record_batch)
        run_consumer(config)
        assert len(processed) == 1
        assert [(partition, offset) for partition, offset, _ in processed[-1]] == [(0, 0)]
        with psycopg.connect(database) as connection:
            first = connection.execute(
                "SELECT token,swaps FROM served_features ORDER BY token"
            ).fetchall()
        assert first == [("JETTON:CANONICAL", 1), ("TON", 1)]
        assert (checkpoint / "commits").is_dir()

        with psycopg.connect(database) as connection:
            _accept(connection, 20, 30, 21, 25)
            lease = acquire_transition(connection, "canonical-kafka-publish-" + uuid.uuid4().hex,
                                       ttl_seconds=300)
            assert len(publish_pending(connection, producer, lease, limit=10)) == 1
        producer.flush(timeout=15)

        # A fresh Spark query resumes the committed offsets from the same durable checkpoint.
        run_consumer(config)
        assert len(processed) == 2
        assert [(partition, offset) for partition, offset, _ in processed[-1]] == [(0, 1)]
        with psycopg.connect(database) as connection:
            rows = connection.execute(
                "SELECT token,swaps,input_raw::text,output_raw::text "
                "FROM served_features ORDER BY token"
            ).fetchall()
            assert connection.execute("SELECT count(*) FROM stream_batches").fetchone()[0] == 2
        assert rows == [
            ("JETTON:CANONICAL", 2, None, "96"),
            ("TON", 2, "320", None),
        ]

        # Changed source identity is rejected before Spark starts or writes.
        with pytest.raises(ValueError, match="checkpoint identity mismatch"):
            run_consumer(ConsumerConfig(
                dsn=database, bootstrap_servers=bootstrap or "", topic=topic,
                checkpoint=str(checkpoint), topic_incarnation=incarnation,
                replay_epoch=1, environment_id="local-test",
                query_id="canonical-features", pipeline_epoch="epoch-a",
                max_offsets_per_trigger=20,
            ))
        assert len(processed) == 2

        with psycopg.connect(database) as connection:
            _accept(connection, 30, 40, 31, 35)
            lease = acquire_transition(connection, "canonical-kafka-publish-" + uuid.uuid4().hex,
                                       ttl_seconds=300)
            assert len(publish_pending(connection, producer, lease, limit=10)) == 1
        producer.flush(timeout=15)
        committed_batches_before = {path.name for path in (checkpoint / "commits").iterdir()}
        with psycopg.connect(database) as connection:
            rows_before_failure = connection.execute(
                "SELECT token,swaps,input_raw::text,output_raw::text "
                "FROM served_features ORDER BY token"
            ).fetchall()

        original_replace = streaming.replace_features
        failures = []

        def replace_then_fail(*args, **kwargs):
            result = original_replace(*args, **kwargs)
            if not failures:
                failures.append(True)
                raise RuntimeError("injected failure after serving replacement")
            return result

        monkeypatch.setattr(streaming, "replace_features", replace_then_fail)
        with pytest.raises(Exception, match="injected failure after serving replacement"):
            run_consumer(config)
        assert failures == [True]
        assert len(processed) == 3
        failed_batch = processed[-1]
        assert [(partition, offset) for partition, offset, _ in failed_batch] == [(0, 2)]
        assert {path.name for path in (checkpoint / "commits").iterdir()} == committed_batches_before
        with psycopg.connect(database) as connection:
            assert connection.execute(
                "SELECT token,swaps,input_raw::text,output_raw::text "
                "FROM served_features ORDER BY token"
            ).fetchall() == rows_before_failure
            assert connection.execute("SELECT count(*) FROM stream_batches").fetchone()[0] == 2

        monkeypatch.setattr(streaming, "replace_features", original_replace)
        run_consumer(config)
        assert len(processed) == 4
        assert processed[-1] == failed_batch
        with psycopg.connect(database) as connection:
            rows = connection.execute(
                "SELECT token,swaps,input_raw::text,output_raw::text "
                "FROM served_features ORDER BY token"
            ).fetchall()
        assert rows == [
            ("JETTON:CANONICAL", 3, None, "189"),
            ("TON", 3, "630", None),
        ]
        with psycopg.connect(database) as connection:
            assert connection.execute(
                "SELECT pipeline_epoch,batch_id,record_count FROM stream_batches "
                "ORDER BY pipeline_epoch,batch_id"
            ).fetchall() == [("epoch-a", 0, 1), ("epoch-a", 1, 1), ("epoch-a", 2, 1)]
        app = create_app(dsn=database)
        status, payload = _http_json(app, "/status")
        assert status == "200 OK"
        assert payload["feature_rows"] == 2
        feature_status, feature_payload = _http_json(
            app, "/v1/features?" + urlencode({"network": "testnet"})
        )
        assert feature_status == "200 OK"
        assert len(feature_payload["features"]) == 2
        assert {item["swaps"] for item in feature_payload["features"]} == {3}

        # Commit PostgreSQL, then fail before foreachBatch returns to Spark.
        with psycopg.connect(database) as connection:
            _accept(connection, 40, 50, 41, 45)
            lease = acquire_transition(connection, "canonical-kafka-publish-" + uuid.uuid4().hex,
                                       ttl_seconds=300)
            assert len(publish_pending(connection, producer, lease, limit=10)) == 1
        producer.flush(timeout=15)
        recorded_process = streaming.process_canonical_microbatch
        post_commit_failures = []

        def commit_then_fail(*args, **kwargs):
            result = recorded_process(*args, **kwargs)
            args[0].commit()
            if not post_commit_failures:
                post_commit_failures.append(True)
                raise RuntimeError("injected failure after PostgreSQL commit")
            return result

        monkeypatch.setattr(streaming, "process_canonical_microbatch", commit_then_fail)
        with pytest.raises(Exception, match="injected failure after PostgreSQL commit"):
            run_consumer(config)
        assert post_commit_failures == [True]
        committed_batches_before_retry = {path.name for path in (checkpoint / "commits").iterdir()}
        monkeypatch.setattr(streaming, "process_canonical_microbatch", recorded_process)
        run_consumer(config)
        assert {path.name for path in (checkpoint / "commits").iterdir()} > committed_batches_before_retry
        with psycopg.connect(database) as connection:
            assert connection.execute("SELECT count(*) FROM stream_batches").fetchone()[0] == 4
            assert connection.execute(
                "SELECT token,swaps,input_raw::text,output_raw::text "
                "FROM served_features ORDER BY token"
            ).fetchall() == [
                ("JETTON:CANONICAL", 4, None, "312"),
                ("TON", 4, "1040", None),
            ]

        # A deliberate epoch replay gets an independent audit namespace, even
        # when Spark reuses batch IDs, and must reproduce identical serving rows.
        replay_config = replace(
            config, checkpoint=str((tmp_path / "replay-checkpoint").resolve()),
            pipeline_epoch="epoch-b", max_offsets_per_trigger=1,
        )
        run_consumer(replay_config)
        with psycopg.connect(database) as connection:
            assert connection.execute(
                "SELECT token,swaps,input_raw::text,output_raw::text "
                "FROM served_features ORDER BY token"
            ).fetchall() == [
                ("JETTON:CANONICAL", 4, None, "312"),
                ("TON", 4, "1040", None),
            ]
            assert connection.execute(
                "SELECT pipeline_epoch,batch_id FROM stream_batches "
                "GROUP BY pipeline_epoch,batch_id ORDER BY pipeline_epoch,batch_id"
            ).fetchall() == [
                ("epoch-a", 0), ("epoch-a", 1), ("epoch-a", 2), ("epoch-a", 3),
                ("epoch-b", 0), ("epoch-b", 1), ("epoch-b", 2), ("epoch-b", 3),
            ]
    finally:
        producer.close(timeout=10)
