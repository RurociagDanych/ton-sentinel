"""Bounded synthetic Kafka → stateful Spark → PG probe.

Not canonical ingestion, economic normalization, repair, or deployment. All endpoints
are hardcoded fixture services. Each phase starts a fresh Python/JVM process using
one durable checkpoint, so open-state restoration is exercised, not simulated.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PG_PASSWORD_ENV = "TON_SENTINEL_PG_PASSWORD"


def _libpq_quote(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'" if re.search(r"[\s'\\]", value) else value


def local_dsn() -> str:
    """DSN of the local fixture database. The password comes only from the environment
    (TON_SENTINEL_PG_PASSWORD, set in the git-ignored .env); no password is kept in the source."""
    password = os.environ.get(PG_PASSWORD_ENV, "")
    if not password:
        raise RuntimeError(f"{PG_PASSWORD_ENV} is not set; copy .env.example to .env and set it")
    return ("host=postgres dbname=feasibility user=probe password="
            f"{_libpq_quote(password)} connect_timeout=5")


def _guard(settings):
    if os.environ.get("TON_SENTINEL_LOCAL_FIXTURE") != "1":
        raise ValueError("local fixture authorization missing")
    for key in ("schema", "topic"):
        if not re.fullmatch(r"probe_[0-9a-f]{32}", settings[key]):
            raise ValueError("invalid isolated probe identifier")
    root = Path("/runtime").resolve()
    checkpoint = Path(settings["checkpoint"]).resolve()
    if root not in checkpoint.parents:
        raise ValueError("checkpoint must be inside isolated runtime")
    if Path("/evidence").resolve() not in Path(settings["evidence"]).resolve().parents:
        raise ValueError("evidence must be inside isolated evidence directory")
    fixture = settings["fixture"]
    if fixture.get("synthetic") is not True:
        raise ValueError("only tagged synthetic fixture allowed")
    if sum(map(len, fixture["phases"])) > 10000:
        raise ValueError("bounded probe input exceeded")


def _rows(schema):
    import psycopg
    from psycopg import sql
    with psycopg.connect(local_dsn()) as conn:
        rows = conn.execute(sql.SQL("SELECT token, window_start, window_end, swaps FROM {}.features ORDER BY token, window_start, window_end").format(sql.Identifier(schema))).fetchall()
    return [[token, start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z"), count] for token, start, end, count in rows]


def _worker(settings):
    _guard(settings)
    import psycopg
    from psycopg import sql
    from pyspark.sql import SparkSession, functions as F

    spark = (SparkSession.builder.master("local[1]").appName("feasibility-fixture")
             .config("spark.ui.enabled", "false")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
             .config("spark.local.dir", str(Path(settings["checkpoint"]).parent / "spark-local"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    # No runtime package resolution, disabled correctness checks, or fake source.
    incoming = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", settings["topic"])
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", "true")
                .option("maxOffsetsPerTrigger", "1000").load())
    events = incoming.select(F.from_json(F.col("value").cast("string"), "id STRING, token STRING, event_time TIMESTAMP").alias("v")).select("v.*")
    dedup = events.withWatermark("event_time", "1 minute").dropDuplicatesWithinWatermark(["id"])
    windows = dedup.groupBy("token", F.window("event_time", "5 minutes")).count()
    plan = windows._jdf.queryExecution().toString()

    def replace_batch(batch, batch_id):
        # Input is capped by this feasibility harness, not a general product sink.
        values = [(r.token, r.window.start.replace(tzinfo=timezone.utc), r.window.end.replace(tzinfo=timezone.utc), r["count"]) for r in batch.collect()]
        with psycopg.connect(local_dsn()) as conn:
            with conn.cursor() as cur:
                cur.executemany(sql.SQL("INSERT INTO {}.features VALUES (%s,%s,%s,%s) ON CONFLICT (token,window_start,window_end) DO UPDATE SET swaps=EXCLUDED.swaps").format(sql.Identifier(settings["schema"])), values)
                cur.execute(sql.SQL("INSERT INTO {}.batches VALUES (%s,%s) ON CONFLICT (batch_id) DO UPDATE SET output_rows=EXCLUDED.output_rows").format(sql.Identifier(settings["schema"])), (batch_id, len(values)))

        if values and settings.get("crash_after_sink_commit"):
            marker = Path(settings["checkpoint"]).parent / "pg-committed-before-checkpoint"
            if not marker.exists():
                marker.write_text(str(batch_id))
                # Actual abrupt driver death after committed SQL, before callback return.
                os._exit(91)

    query = (windows.writeStream.outputMode("append")
             .foreachBatch(replace_batch)
             .option("checkpointLocation", settings["checkpoint"])
             .trigger(availableNow=True).start())
    try:
        if not query.awaitTermination(150):
            raise TimeoutError("bounded Spark probe timed out")
        progress = [json.loads(p.json) for p in query.recentProgress]
        offsets = sorted((Path(settings["checkpoint"]) / "offsets").glob("[0-9]*"), key=lambda p: int(p.name))
        metadata = json.loads(offsets[-1].read_text().splitlines()[1]) if offsets else {}
        result = {"progress": progress, "checkpoint_metadata": metadata,
                  "rows": _rows(settings["schema"]), "logical_plan": plan,
                  "correctness_checks": spark.conf.get("spark.sql.streaming.statefulOperator.checkCorrectness.enabled"),
                  "state_store_provider": spark.conf.get("spark.sql.streaming.stateStore.providerClass"),
                  "spark_version": spark.version}
        Path(settings["worker_output"]).write_text(json.dumps(result, indent=2))
    finally:
        query.stop()
        spark.stop()


def run_probe(settings: dict) -> dict:
    _guard(settings)
    import psycopg
    from psycopg import sql
    from kafka import KafkaProducer
    from kafka.serializer import Serializer
    from kafka.admin import KafkaAdminClient, NewTopic
    admin = KafkaAdminClient(bootstrap_servers="kafka:9092", request_timeout_ms=15000)
    try:
        admin.create_topics([NewTopic(settings["topic"], num_partitions=1, replication_factor=1)])
    finally:
        admin.close()
    with psycopg.connect(local_dsn()) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(settings["schema"])))
        conn.execute(sql.SQL("CREATE TABLE {}.features (token text NOT NULL, window_start timestamptz NOT NULL, window_end timestamptz NOT NULL, swaps bigint NOT NULL, PRIMARY KEY(token,window_start,window_end))").format(sql.Identifier(settings["schema"])))
        conn.execute(sql.SQL("CREATE TABLE {}.batches (batch_id bigint PRIMARY KEY, output_rows bigint NOT NULL)").format(sql.Identifier(settings["schema"])))
    fixture = settings["fixture"]
    base = datetime.fromisoformat(fixture["base_time"].replace("Z", "+00:00"))
    acknowledgements, results = [], []
    crash_exit, crash_rows, replayed_rows = None, None, None
    crash_coordinate = {}
    class JsonSerializer(Serializer):
        def serialize(self, topic, headers, data):
            return json.dumps(data).encode()
    producer = KafkaProducer(bootstrap_servers="kafka:9092", acks="all", enable_idempotence=True, max_block_ms=15000,
                             compression_type="gzip", value_serializer=JsonSerializer())
    try:
        # Last two phases have no input: idle/checkpoint restart and unchanged sink.
        for index, phase in enumerate(fixture["phases"] + [[], []]):
            for event in phase:
                payload = {"id": event["id"], "token": event["token"],
                           "event_time": (base + timedelta(seconds=event["seconds"])).isoformat()}
                ack = producer.send(settings["topic"], key=event["id"].encode(), value=payload).get(timeout=15)
                acknowledgements.append([event["id"], ack.topic, ack.partition, ack.offset])
            producer.flush(timeout=15)
            worker_settings = dict(settings, worker_output=settings["evidence"] + f"-phase-{index}.json")
            worker_settings["crash_after_sink_commit"] = bool(settings.get("crash_after_sink_commit") and index == 1)
            config = Path(settings["checkpoint"]).parent / f"phase-{index}.json"
            config.write_text(json.dumps(worker_settings))
            with Path(settings["evidence"] + f"-phase-{index}.log").open("w") as log:
                process = subprocess.run([sys.executable, "-B", "-m", "ton_sentinel.feasibility", "--worker", str(config)], stdout=log, stderr=subprocess.STDOUT, timeout=190)
            if process.returncode == 91 and worker_settings["crash_after_sink_commit"]:
                crash_exit, crash_rows = process.returncode, _rows(settings["schema"])
                batch = int((Path(settings["checkpoint"]).parent / "pg-committed-before-checkpoint").read_text())
                crash_coordinate = {"crash_batch_id": batch,
                                    "crash_checkpoint_committed": (Path(settings["checkpoint"]) / "commits" / str(batch)).exists(),
                                    "crash_offset_written": (Path(settings["checkpoint"]) / "offsets" / str(batch)).exists()}
                Path(settings["evidence"] + "-crash-boundary.json").write_text(json.dumps(dict(crash_coordinate, pg_rows=crash_rows), indent=2))
                with Path(settings["evidence"] + f"-phase-{index}-replay.log").open("w") as log:
                    process = subprocess.run([sys.executable, "-B", "-m", "ton_sentinel.feasibility", "--worker", str(config)], stdout=log, stderr=subprocess.STDOUT, timeout=190)
                replayed_rows = _rows(settings["schema"])
            if process.returncode:
                raise RuntimeError(f"real Spark worker failed exit={process.returncode}; inspect {settings['evidence']}-phase-{index}.log")
            results.append(json.loads(Path(worker_settings["worker_output"]).read_text()))
            if index == 1 and crash_coordinate:
                crash_coordinate["replay_first_batch_id"] = results[-1]["progress"][0]["batchId"]
    finally:
        producer.close(timeout=10)
    progress = [p for r in results for p in r["progress"]]
    operators = [op for p in progress for op in p.get("stateOperators", [])]
    def watermark(result):
        return result["checkpoint_metadata"].get("batchWatermarkMs")
    return {"synthetic": True, "architecture": "x86_64-local-Docker-not-K3s",
            "pg_rows": results[-3]["rows"], "restart_rows": results[-1]["rows"], "idle_rows": results[-2]["rows"],
            "state_operator_count": max((len(p.get("stateOperators", [])) for p in progress), default=0),
            "restored_open_state_rows": sum(op["numRowsTotal"] for p in results[1]["progress"] for op in p.get("stateOperators", [])),
            "late_dropped": sum(op["numRowsDroppedByWatermark"] for op in operators),
            "watermark": datetime.fromtimestamp(watermark(results[-1]) / 1000, timezone.utc).isoformat().replace("+00:00", "Z"),
            "idle_watermark_before": watermark(results[-3]), "idle_watermark_after": watermark(results[-1]),
            "correctness_checks": results[-1]["correctness_checks"], "state_store_provider": results[-1]["state_store_provider"],
            "crash_exit": crash_exit, "crash_rows": crash_rows, "replayed_rows": replayed_rows, **crash_coordinate,
            "acknowledgements": acknowledgements, "phases": results,
            "checkpoint_bytes": sum(p.stat().st_size for p in Path(settings["checkpoint"]).rglob("*") if p.is_file())}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", required=True)
    args = parser.parse_args()
    _worker(json.loads(Path(args.worker).read_text()))
