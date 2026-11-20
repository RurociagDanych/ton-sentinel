"""Minimal local-K3s entrypoints exercising existing project code offline."""
from __future__ import annotations

import io
import json
import os
import sys
import time
from pathlib import Path

TOPIC = "ton.swap_events.v1"  # already budgeted in the chart's Kafka topic list


def _lazy(name):
    def call(*args, **kwargs):
        import importlib
        mod, attr = {"migrate": ("ton_sentinel.db", "migrate"),
                     "acquire_transition": ("ton_sentinel.db.fencing", "acquire_transition"),
                     "replace_features": ("ton_sentinel.db.serving", "replace_features"),
                     "read_features": ("ton_sentinel.db.serving", "read_features")}[name]
        return getattr(importlib.import_module(mod), attr)(*args, **kwargs)
    return call


migrate = _lazy("migrate")
acquire_transition = _lazy("acquire_transition")
replace_features = _lazy("replace_features")
read_features = _lazy("read_features")


def sample_features() -> list[dict]:
    base = {"network": "testnet", "window_start": 0, "window_end": 300, "generation": "k3s-probe",
            "swaps": 1, "known_traders": 1, "unknown_count": 0, "coverage_status": "bounded_observed",
            "status": "provisional", "provisional": True}
    return [dict(base, token="TON", input_raw="1000", output_raw=None),
            dict(base, token="jetton:A", input_raw=None, output_raw="250")]


def postgres_roundtrip(connection, owner: str) -> list[dict]:
    migrate(connection)
    lease = acquire_transition(connection, owner)
    rows = sample_features()
    replace_features(connection, lease, rows)
    got = read_features(connection, network="testnet", window_start=0, window_end=300)
    by_token = lambda items: sorted(items, key=lambda r: r["token"])  # DB collation order is not literal order
    if by_token(got) != by_token(rows):
        raise RuntimeError(f"postgres read-back mismatch: {got!r} != {rows!r}")
    return got


def sample_actions() -> list[str]:
    def action(action_id, end, in_asset, in_raw, out_asset, out_raw):
        return json.dumps({
            "schema_version": 1, "kind": "synthetic_action",
            "identity": {"network": "testnet", "trace_id": "trace-1", "action_id": action_id},
            "epoch": {"network": "testnet", "source_fingerprint": "src", "filter_fingerprint": "filter",
                      "build_fingerprint": "k3s-probe", "action_policy_fingerprint": "policy"},
            "protocol": "STON.fi", "protocol_version": "synthetic-v1", "action_end": end, "status": "success",
            "source_eligible": True, "route_proven": True, "route": [in_asset, out_asset],
            "input": {"asset": in_asset, "raw_amount": in_raw, "decimals": None, "decimal_provenance": None},
            "output": {"asset": out_asset, "raw_amount": out_raw, "decimals": None, "decimal_provenance": None},
            "trader": "wallet-1", "trader_method": "explicit", "trader_unambiguous": True,
        }, separators=(",", ":"))
    return [action("a1", 299, "TON", "1000", "jetton:A", "250"),
            action("a2", 300, "jetton:A", "250", "TON", "1000")]


SPARK_EXPECTED = {(0, "TON"): (1, 1000, None), (0, "jetton:A"): (1, None, 250),
                  (300, "jetton:A"): (1, 250, None), (300, "TON"): (1, None, 1000)}


def kafka_client_config(env) -> dict:
    bootstrap = env.get("SENTINEL_KAFKA_BOOTSTRAP")
    if not bootstrap:
        raise ValueError("SENTINEL_KAFKA_BOOTSTRAP is required")
    tls = env.get("SENTINEL_KAFKA_TLS_DIR")
    if not tls:
        raise ValueError("SENTINEL_KAFKA_TLS_DIR is required (broker demands TLS client auth)")
    return {"bootstrap_servers": bootstrap, "security_protocol": "SSL",
            "ssl_cafile": f"{tls}/kafka-ca.crt", "ssl_certfile": f"{tls}/kafka-client.crt",
            "ssl_keyfile": f"{tls}/kafka-client.key", "ssl_check_hostname": True}


def kafka_roundtrip(producer, consumer, topic, records, *, deadline_s=60):
    """Produce ``records`` [(key, json-able)] and read the same records back."""
    for key, value in records:
        producer.send(topic, key=key.encode(), value=json.dumps(value).encode()).get(timeout=30)
    producer.flush(timeout=30)
    got, end = [], time.monotonic() + deadline_s
    while len(got) < len(records) and time.monotonic() < end:
        for batch in consumer.poll(timeout_ms=500).values():
            got.extend((m.key.decode(), json.loads(m.value)) for m in batch)
    if got != list(records):
        raise RuntimeError(f"kafka round-trip mismatch: {got!r} != {list(records)!r}")
    return got


def _ingestor(env):
    import psycopg
    with psycopg.connect(env["SENTINEL_DATABASE_DSN"], connect_timeout=10) as conn:
        return {"features": postgres_roundtrip(conn, "k3s-probe")}


def _api(env):
    from ton_sentinel.api import create_app
    app = create_app(dsn=env["SENTINEL_DATABASE_DSN"])
    deadline = time.monotonic() + 120
    while True:
        out = {}
        body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/v1/features",
                             "QUERY_STRING": "network=testnet&window_start=0&window_end=300",
                             "wsgi.input": io.BytesIO()},
                            lambda status, headers, *a: out.update(status=status)))
        feats = json.loads(body).get("features", []) if out["status"].startswith("200") else []
        if sorted(f["token"] for f in feats) == ["TON", "jetton:A"]:
            return {"features": feats}
        if time.monotonic() > deadline:
            raise RuntimeError(f"api never served the probe features: {out} {body[:200]!r}")
        time.sleep(3)


def _notifier(env):
    import uuid

    from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
    cfg = kafka_client_config(env)
    admin = KafkaAdminClient(**cfg)
    try:
        admin.create_topics([NewTopic(TOPIC, 1, 1)])
    except TopicAlreadyExistsError:
        pass
    run = uuid.uuid4().hex[:8]
    records = [(f"{run}-{i}", {"n": i, "run": run}) for i in range(3)]
    consumer = KafkaConsumer(TOPIC, group_id=None, auto_offset_reset="earliest", **cfg)
    consumer.poll(timeout_ms=3000)
    producer = KafkaProducer(acks="all", **cfg)
    try:
        # Other runs' records may already be in the topic; keep only ours.
        got = _kafka_filtered(producer, consumer, records, run)
    finally:
        producer.close()
        consumer.close()
        admin.close()
    return {"records": got}


def _kafka_filtered(producer, consumer, records, run):
    class Only:
        def poll(self, timeout_ms=0):
            return {tp: [m for m in batch if m.key.decode().startswith(run)]
                    for tp, batch in consumer.poll(timeout_ms=timeout_ms).items()}
    return kafka_roundtrip(producer, Only(), TOPIC, records)


def _enable_probe_fixtures():
    """Deployment smoke harness only: its literal synthetic-v1 data is a test fixture.

    Sets the test-only switch (TON_SENTINEL_SYNTHETIC_FIXTURES) for this probe process alone; the
    product deployments never set it, so the production path keeps refusing synthetic input.
    """
    os.environ["TON_SENTINEL_SYNTHETIC_FIXTURES"] = "test-only"


def _spark(env):
    _enable_probe_fixtures()
    from pyspark.sql import SparkSession
    from ton_sentinel.spark.batch import compute_features
    spark = (SparkSession.builder.master("local[1]").appName("k3s-probe")
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1")
             .config("spark.driver.memory", env.get("SPARK_DRIVER_MEMORY", "512m")).getOrCreate())
    try:
        actions = spark.createDataFrame([(a,) for a in sample_actions()], "action_json string")
        coverage = spark.createDataFrame(
            [("testnet", 0, 300, "bounded_observed", False), ("testnet", 300, 600, "bounded_observed", False)],
            "network string, window_start long, window_end long, status string, eligible_zero boolean")
        rows = compute_features(spark, actions, coverage, {"schema_version": 1}).collect()
    finally:
        spark.stop()
    got = {(r.window_start, r.token): (r.swaps, None if r.input_raw is None else int(r.input_raw),
                                       None if r.output_raw is None else int(r.output_raw)) for r in rows}
    if got != SPARK_EXPECTED:
        raise RuntimeError(f"spark result mismatch: {got!r} != {SPARK_EXPECTED!r}")
    return {"features": sorted(f"{k}={v}" for k, v in got.items())}


STREAM_EXPECTED_FIRST = [("JETTON:CANONICAL", 1, None, "33"), ("TON", 1, "110", None)]
STREAM_EXPECTED_FINAL = [("JETTON:CANONICAL", 2, None, "96"), ("TON", 2, "320", None)]


def stream_source_action(index: int, action_end: int) -> dict:
    import base64
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True, "network": "testnet",
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": action_end, "status": "success", "eligible": True,
        "route_proven": True, "route": ["TON", "JETTON:CANONICAL"],
        "input": {"asset": "TON", "raw_amount": str(index * 10)},
        "output": {"asset": "JETTON:CANONICAL", "raw_amount": str(index * 3)},
    }


class FixedPages:
    """In-process page client over one literal action (no network, no live source)."""
    def __init__(self, action):
        self.action = action

    def fetch_page(self, source_epoch, start, end, offset, limit):
        return [self.action][offset:offset + limit]


def stream_kafka_options(env, workdir) -> list[tuple[str, str]]:
    """TLS client options for the Spark Kafka source: file paths only, no secret values."""
    tls = env.get("SENTINEL_KAFKA_TLS_DIR")
    if not tls:
        raise ValueError("SENTINEL_KAFKA_TLS_DIR is required (broker demands TLS client auth)")
    keystore = Path(workdir) / "kafka-client-keystore.pem"
    fd = os.open(keystore, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:  # Kafka's PEM keystore: private key then certificate chain
        handle.write((Path(tls) / "kafka-client.key").read_text())
        handle.write((Path(tls) / "kafka-client.crt").read_text())
    os.chmod(keystore, 0o600)
    return [("security.protocol", "SSL"),
            ("ssl.truststore.type", "PEM"), ("ssl.truststore.location", f"{tls}/kafka-ca.crt"),
            ("ssl.keystore.type", "PEM"), ("ssl.keystore.location", str(keystore))]


def stream_cli_argv(env, kafka_options, *, incarnation: str, checkpoint: str) -> list[str]:
    argv = ["--bootstrap-servers", env["SENTINEL_KAFKA_BOOTSTRAP"], "--topic", TOPIC,
            "--checkpoint", checkpoint, "--topic-incarnation", incarnation, "--replay-epoch", "0",
            "--environment-id", "k3s-local", "--query-id", "canonical-features",
            "--pipeline-epoch", "k3s-stream", "--max-offsets-per-trigger", "20"]
    for key, value in kafka_options:
        argv += ["--kafka-option", f"{key}={value}"]
    return argv  # the DSN (with password) is passed via TON_SENTINEL_DSN, never argv


def check_stream_rows(got, expected) -> None:
    if list(got) != list(expected):
        raise RuntimeError(f"stream read-back mismatch: {list(got)!r} != {list(expected)!r}")


def _stream_rows(dsn):
    import psycopg
    with psycopg.connect(dsn, connect_timeout=10) as conn:
        return [tuple(r) for r in conn.execute(
            'SELECT token,swaps,input_raw::text,output_raw::text FROM served_features '
            'ORDER BY token COLLATE "C"').fetchall()]


def _reset_topic(cfg, deadline_s=90):
    """This role owns the topic: other probes' non-envelope records would fail the strict consumer."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError
    admin = KafkaAdminClient(**cfg)
    try:
        if TOPIC in admin.list_topics():
            admin.delete_topics([TOPIC])
        end = time.monotonic() + deadline_s
        while True:
            try:
                if TOPIC not in admin.list_topics():
                    admin.create_topics([NewTopic(TOPIC, 1, 1)])
                    return
            except KafkaError:
                pass
            if time.monotonic() > end:
                raise RuntimeError(f"could not recreate topic {TOPIC}")
            time.sleep(2)
    finally:
        admin.close()


def _stream(env):
    _enable_probe_fixtures()
    import tempfile
    import uuid

    import psycopg
    from kafka import KafkaProducer

    from ton_sentinel import stream_cli
    from ton_sentinel.contracts import SourceEpoch
    from ton_sentinel.db.fencing import acquire_transition
    from ton_sentinel.ingest import OutboxBudget, ingest_range
    from ton_sentinel.outbox import configure_delivery, publish_pending
    from ton_sentinel.source.scan import ScanPolicy

    dsn = env["SENTINEL_DATABASE_DSN"]
    os.environ["TON_SENTINEL_DSN"] = dsn
    workdir = tempfile.mkdtemp(prefix="stream-", dir=env.get("TMPDIR", "/tmp"))
    options = stream_kafka_options(env, workdir)
    cfg = kafka_client_config(env)
    incarnation = "k3s-stream-" + uuid.uuid4().hex
    checkpoint = f"{env.get('SENTINEL_STREAM_CHECKPOINT', '/var/sentinel/checkpoints/stream')}/{incarnation}"
    argv = stream_cli_argv(env, options, incarnation=incarnation, checkpoint=checkpoint)
    _reset_topic(cfg)
    epoch = SourceEpoch("testnet", "k3s:source", "k3s:filter", "k3s:build", "k3s:policy")
    budget = OutboxBudget(max_pending_bytes=1_000_000, max_oldest_age_seconds=3600)
    producer = KafkaProducer(acks="all", retries=3, max_block_ms=15_000, **cfg)
    stages = []
    try:
        with psycopg.connect(dsn, connect_timeout=10) as conn:
            migrate(conn)
            configure_delivery(conn, topic=TOPIC, topic_incarnation=incarnation)
            for start, end, index, action_end, expected in ((10, 20, 11, 15, STREAM_EXPECTED_FIRST),
                                                           (20, 30, 21, 25, STREAM_EXPECTED_FINAL)):
                policy = ScanPolicy(start=start, end=end, page_size=2, max_pages=8, max_actions=8,
                                    max_bytes=100_000, max_seconds=30, max_requests=10)
                result = ingest_range(conn, FixedPages(stream_source_action(index, action_end)), epoch,
                                      policy, owner="k3s-stream-ingest", budget=budget)
                if not result.accepted:
                    raise RuntimeError(f"range {start}-{end} not accepted: {result.status}")
                lease = acquire_transition(conn, "k3s-stream-publish", ttl_seconds=300)
                sent = publish_pending(conn, producer, lease, limit=10)
                conn.commit()
                if len(sent) != 1:
                    raise RuntimeError(f"expected 1 published event, got {sent!r}")
                producer.flush(timeout=30)
                if stream_cli.main(argv) != 0:
                    raise RuntimeError("stream_cli failed")
                rows = _stream_rows(dsn)
                check_stream_rows(rows, expected)
                stages.append(rows)
            batches = conn.execute("SELECT count(*) FROM stream_batches WHERE topic=%s", (TOPIC,)).fetchone()[0]
            conn.commit()
            if batches != 2:
                raise RuntimeError(f"expected 2 stream_batches audit rows, got {batches}")
    finally:
        producer.close()
    return {"topic": TOPIC, "stages": stages, "stream_batches": batches}


ROLES = {"ingestor": _ingestor, "api": _api, "notifier": _notifier, "spark": _spark, "stream": _stream}


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0] not in ROLES:
        print(f"usage: k3s_probe {{{'|'.join(ROLES)}}} [--once]", file=sys.stderr)
        raise SystemExit(2)
    role, once = argv[0], "--once" in argv[1:]
    try:
        result = ROLES[role](os.environ)
    except Exception as exc:  # noqa: BLE001 - report and fail the pod, never fake readiness
        print(f"PROBE_FAIL role={role} {type(exc).__name__}: {exc}", flush=True)
        return 1
    print(f"PROBE_OK role={role} {json.dumps(result, sort_keys=True, default=str)}", flush=True)
    ready = Path(os.environ.get("SENTINEL_READY_FILE", "/tmp/ready"))
    ready.write_text("ok\n")
    while not once:
        time.sleep(3600)
    return 0
