"""Local envelope workload measurement (one level per invocation)."""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from ton_sentinel.feasibility import local_dsn

WINDOW_SECONDS = 300
WINDOWS_PER_DAY = 86400 // WINDOW_SECONDS
BASELINE_SECONDS = 7 * 86400
COVERAGE = ("host_kernel_k3s", "broker_cache_native", "spark_driver_executor_python",
            "state_checkpoint_spill", "pg_wal_index_vacuum", "page_cache",
            "retention_zero_grid_repair", "backlog_catchup", "serialized_maintenance")
SETUP_MARGIN_SECONDS = 2400
MAINTENANCE_SLOT = 7_202_611  # one shared advisory-lock key: corrections, walks and maintenance
DATA_ORIGIN = ("locally generated deterministic events at the envelope level rate/cardinality; "
               "no live TON source read, no network egress")


def _positive(level, key):
    value = level.get(key)
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"level {key} must be a positive finite number")
    return value


def phase_plan(level: dict) -> dict:
    """Wall-clock plan for one level; fail closed on an inconsistent level."""
    for key in ("events_per_second", "payload_bytes", "active_tokens", "active_wallets",
                "zero_grid_rows", "scan_membership", "reconciliation_per_hour",
                "corrections_per_hour", "baseline_fanout", "maintenance_seconds",
                "backlog_events", "outage_seconds", "latency_seconds", "catchup_seconds",
                "run_seconds"):
        _positive(level, key)
    steady = (level["run_seconds"] - level["outage_seconds"] - level["catchup_seconds"]
              - level["maintenance_seconds"])
    if steady < 3 * WINDOW_SECONDS:
        raise ValueError("run_seconds leaves fewer than three steady-state windows")
    if level["zero_grid_rows"] % level["active_tokens"]:
        raise ValueError("zero_grid_rows must be an integral active_tokens x windows grid")
    windows = level["zero_grid_rows"] // level["active_tokens"]
    if windows * WINDOW_SECONDS < BASELINE_SECONDS:
        raise ValueError("zero grid shorter than the 7-day baseline lookback")
    return {"steady_seconds": steady,
            "outage_seconds": level["outage_seconds"],
            "catchup_budget_seconds": level["catchup_seconds"],
            "maintenance_budget_seconds": level["maintenance_seconds"],
            "zero_grid_windows_per_token": windows,
            "retention_expiry_rows": level["active_tokens"] * WINDOWS_PER_DAY,
            "reconciliation_interval_seconds": 3600 / level["reconciliation_per_hour"],
            "correction_interval_seconds": 3600 / level["corrections_per_hour"],
            "reconciliation_range_seconds": level["scan_membership"] / level["events_per_second"],
            "max_wall_seconds": level["run_seconds"] + SETUP_MARGIN_SECONDS}


def due_events(elapsed: float, eps: float) -> int:
    return int(math.floor(max(0.0, elapsed) * eps + 1e-9))


def event_payload(seq: int, level: dict, event_time: str, rng: random.Random) -> bytes:
    """One serialized action of exactly payload_bytes (incompressible-ish padding)."""
    event = {"id": f"a{seq:012d}", "token_id": rng.randrange(level["active_tokens"]),
             "wallet_id": rng.randrange(level["active_wallets"]), "event_time": event_time, "pad": ""}
    base = len(json.dumps(event, separators=(",", ":")).encode())
    missing = level["payload_bytes"] - base
    if missing < 0:
        raise ValueError("payload_bytes too small for the event envelope")
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    event["pad"] = "".join(rng.choice(alphabet) for _ in range(missing))
    raw = json.dumps(event, separators=(",", ":")).encode()
    assert len(raw) == level["payload_bytes"]
    return raw


def k3s_system(scope_bytes: int, excluded_bytes: int) -> int:
    """K3s node usage minus pods of foreign (non kube-system) workloads sharing the node."""
    if excluded_bytes > scope_bytes or excluded_bytes < 0:
        raise ValueError("excluded pod usage exceeds K3s node usage")
    return scope_bytes - excluded_bytes


def zero_fill_starts(frontier: int, starts) -> tuple:
    """Window starts to zero-fill for one emitted feature batch, and the next frontier.

    The explicit zero grid must stay contiguous: every window from the frontier (first
    window not yet materialized) up to the newest emitted window is filled, including
    windows in which no event arrived at all (e.g. while the grid was being seeded).
    """
    values = [frontier, *starts]
    if any(type(v) is not int or v % WINDOW_SECONDS for v in values):
        raise ValueError("window starts must be integral epochs aligned to the window size")
    if not starts:
        return [], frontier
    top = max(starts)
    fill = sorted(set(starts) | set(range(frontier, top + 1, WINDOW_SECONDS)))
    return fill, max(frontier, top + WINDOW_SECONDS)


def grid_contiguity(*, distinct_windows: int, rows: int, min_epoch: int, max_epoch: int, tokens: int) -> dict:
    """Measured contiguity of the retained feature grid (every window x every token)."""
    expected = (max_epoch - min_epoch) // WINDOW_SECONDS + 1
    return {"distinct_windows": distinct_windows, "expected_windows": expected, "rows": rows,
            "expected_rows": expected * tokens, "missing_windows": expected - distinct_windows,
            "contiguous": distinct_windows == expected and rows == expected * tokens}


def latency_summary(rows, marks: dict) -> dict:
    """rows: (window_start_epoch, committed_at_epoch) of stream-accepted feature rows.

    Normal operation = windows ending before the outage, or starting after catch-up ended;
    others are outage-affected and judged by the catch-up objective instead.
    """
    normal, affected = [], []
    for start, committed in rows:
        latency = committed - (start + WINDOW_SECONDS)
        if start + WINDOW_SECONDS <= marks["outage_start"] or start >= marks["catchup_end"]:
            normal.append(latency)
        else:
            affected.append(latency)
    def pct(values, q):
        return sorted(values)[min(len(values) - 1, int(q * len(values)))] if values else None
    return {"normal_operation_max_seconds": max(normal) if normal else None,
            "normal_operation_p95_seconds": pct(normal, 0.95),
            "normal_operation_windows": len(normal),
            "outage_affected_max_seconds": max(affected) if affected else None,
            "outage_affected_windows": len(affected)}


def _num(value):
    return type(value) in (int, float) and math.isfinite(value)


def derive_coverage(harness: dict, raw: list, level: dict) -> list:
    """Categories with real evidence present; never assumed."""
    got = set()
    containers_ok = lambda name, key: all(_num(s.get("containers", {}).get(name, {}).get(key)) for s in raw)
    if raw and all(_num(s.get("k3s", {}).get("scope_memory_current")) and _num(s.get("host_kernel_bytes"))
                   and s["host_kernel_bytes"] > 0 for s in raw):
        got.add("host_kernel_k3s")
    if raw and containers_ok("kafka", "memory_current") and containers_ok("kafka", "anon") \
            and harness.get("kafka", {}).get("produced_events", 0) > 0:
        got.add("broker_cache_native")
    spark = harness.get("spark", {})
    if raw and containers_ok("app", "memory_current") and spark.get("canonical_batches", 0) > 0 \
            and spark.get("feature_batches", 0) > 0:
        got.add("spark_driver_executor_python")
    if spark.get("checkpoint_bytes", 0) > 0 and spark.get("state_rows_max", 0) > 0:
        got.add("state_checkpoint_spill")
    pg = harness.get("pg", {})
    if all(_num(pg.get(k)) and pg[k] > 0 for k in ("wal_dir_bytes", "wal_generated_bytes", "index_bytes", "vacuum_seconds")):
        got.add("pg_wal_index_vacuum")
    if raw and all(containers_ok(name, "file") for name in ("app", "kafka", "postgres")):
        got.add("page_cache")
    corrections = harness.get("corrections", {})
    if (harness.get("zero_grid", {}).get("rows_materialized", 0) >= level["zero_grid_rows"]
            and harness.get("retention", {}).get("expired_feature_rows", 0) > 0
            and corrections.get("applied", 0) > 0
            and _num(corrections.get("baseline_rows_read_max"))
            and corrections["baseline_rows_read_max"] >= level["baseline_fanout"]
            and _num(corrections.get("baseline_rows_read_min"))  # every rescoring read the full baseline
            and corrections["baseline_rows_read_min"] >= level["baseline_fanout"]):
        got.add("retention_zero_grid_repair")
    backlog = harness.get("backlog", {})
    if backlog.get("caught_up") is True and _num(backlog.get("catchup_seconds")) \
            and backlog.get("events", 0) > 0:
        got.add("backlog_catchup")
    maintenance = harness.get("maintenance", {})
    if maintenance.get("serialized") is True and _num(maintenance.get("seconds")):
        got.add("serialized_maintenance")
    return sorted(got)


def summarize_level(raw: list, harness: dict, *, level: dict, target_id: str, architecture: str,
                    capacity: dict, provenance: str) -> dict:
    """One measure_feasibility sample from host cgroup samples + in-container harness output."""
    if len(raw) < 2:
        raise ValueError("at least two host samples are required")
    storage = [s["storage"] for s in raw if isinstance(s.get("storage"), dict)]
    if not storage:
        raise ValueError("no data-volume storage measurement")
    first, last = raw[0], raw[-1]
    names = sorted(last["containers"])
    elapsed = last["mono"] - first["mono"]
    k3s_usage = lambda s: s["k3s"]["scope_usage_usec"] - s["k3s"]["excluded_usage_usec"]
    cpu_usec = sum(last["containers"][n]["usage_usec"] - first["containers"][n]["usage_usec"] for n in names)
    cpu_usec += k3s_usage(last) - k3s_usage(first)
    k3s_mem = lambda s: k3s_system(s["k3s"]["scope_memory_current"], s["k3s"]["excluded_memory_current"])
    sampled = max(sum(s["containers"][n]["memory_current"] for n in names) + k3s_mem(s) + s["host_kernel_bytes"]
                  for s in raw)
    peak_bound = (sum(last["containers"][n]["memory_peak"] for n in names)
                  + max(k3s_mem(s) for s in raw) + max(s["host_kernel_bytes"] for s in raw))
    free = min(rec["free_bytes"] / rec["total_bytes"] for st in storage for rec in st.values())
    oom = sum(last["containers"][n]["oom_kill"] for n in names) + last["k3s"]["oom_kill"] - first["k3s"]["oom_kill"]
    throttled = {n: last["containers"][n]["throttled_usec"] - first["containers"][n]["throttled_usec"] for n in names}
    return {
        "level": level["name"], "target_id": target_id, "architecture": architecture, "synthetic": False,
        "workload_data_origin": DATA_ORIGIN,
        "elapsed_seconds": elapsed,
        "events": harness["events"], "input_bytes": harness["input_bytes"],
        "full_host_footprint_bytes": max(sampled, peak_bound),
        "footprint_sampled_simultaneous_max_bytes": sampled,
        "footprint_peak_upper_bound_bytes": peak_bound,
        "data_free_fraction": free, "oom_events": oom,
        "measured_cpu_seconds": cpu_usec / 1e6,
        "measured_cpu_capacity_cores": capacity["effective_cores"],
        "cpu_throttled_seconds": {n: v / 1e6 for n, v in throttled.items()},
        "cpu_measurement_scope": ("sum of cgroup v2 cpu.stat usage_usec deltas of the compose app/kafka/postgres "
                                  "containers plus the K3s node (k3d server scope minus foreign-namespace pods) "
                                  "over the level interval; capacity = host sched_getaffinity/cgroup cpu.max "
                                  "effective cores"),
        "cpu_measurement_provenance": provenance,
        "measured_maintenance_seconds": harness["maintenance"]["seconds"],
        "measured_catchup_seconds": harness["backlog"]["catchup_seconds"],
        "measured_latency_seconds": harness["latency"]["normal_operation_max_seconds"],
        "measured_backlog_events": harness["backlog"]["events"],
        "zero_grid_rows_materialized": harness["zero_grid"]["rows_materialized"],
        "coverage": derive_coverage(harness, raw, level),
        "scope": "local x86_64 single-host measurement at an envelope level; not ARM64/target qualification",
    }



def _guard(config):
    if os.environ.get("TON_SENTINEL_LOCAL_FIXTURE") != "1":
        raise ValueError("local fixture authorization missing")
    for key in ("schema", "topic"):
        if not re.fullmatch(r"probe_[0-9a-f]{32}", config[key]):
            raise ValueError("invalid isolated probe identifier")
    if Path("/runtime").resolve() not in Path(config["checkpoint_root"]).resolve().parents:
        raise ValueError("checkpoint must be inside isolated runtime")
    if Path("/evidence").resolve() not in Path(config["output"]).resolve().parents:
        raise ValueError("output must be inside the evidence directory")


def _du(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file()) if path.exists() else 0


def run_level(config: dict) -> dict:  # pragma: no cover - exercised only against real services
    _guard(config)
    import psycopg
    from psycopg import sql
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic

    level, plan = config["level"], phase_plan(config["level"])
    schema, topic = config["schema"], config["topic"]
    root = Path(config["checkpoint_root"])
    root.mkdir(parents=True, exist_ok=True)
    progress_path = Path(config["output"]).with_suffix(".progress.json")
    marks: dict = {}
    result: dict = {"config": {k: v for k, v in config.items() if k != "level"}, "level": level,
                    "plan": plan, "marks": marks, "data_origin": DATA_ORIGIN}

    def mark(name):
        marks[name] = time.time()
        progress_path.write_text(json.dumps({"marks": marks}, indent=1))

    S = lambda text: sql.SQL(text).format(s=sql.Identifier(schema))
    tokens, wallets = level["active_tokens"], level["active_wallets"]
    eps = level["events_per_second"]

    admin = KafkaAdminClient(bootstrap_servers="kafka:9092", request_timeout_ms=15000)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1,
                                      topic_configs={"retention.ms": str(24 * 3600 * 1000)})])
    finally:
        admin.close()

    mark("setup_start")
    with psycopg.connect(local_dsn(), autocommit=True) as conn:
        conn.execute(S("CREATE SCHEMA {s}"))
        conn.execute(S("CREATE TABLE {s}.canonical (id text PRIMARY KEY, token_id int NOT NULL, wallet_id int NOT NULL, "
                       "event_time timestamptz NOT NULL, payload text NOT NULL, revision int NOT NULL DEFAULT 0)"))
        conn.execute(S("CREATE INDEX ON {s}.canonical (token_id, event_time)"))
        conn.execute(S("CREATE INDEX ON {s}.canonical (event_time)"))
        conn.execute(S("CREATE TABLE {s}.features (token_id int NOT NULL, window_start timestamptz NOT NULL, "
                       "swaps int NOT NULL, unique_traders int NOT NULL, origin smallint NOT NULL, "
                       "committed_at timestamptz NOT NULL DEFAULT clock_timestamp())"))
        conn.execute(S("CREATE TABLE {s}.scan_manifest (walk_id bigint NOT NULL, id text NOT NULL, digest text NOT NULL)"))
        wal_before = conn.execute("SELECT wal_bytes FROM pg_stat_wal").fetchone()[0]
        grid_end = math.floor(time.time() / WINDOW_SECONDS) * WINDOW_SECONDS
        windows = plan["zero_grid_windows_per_token"]
        mark("zero_grid_start")
        conn.execute(S("INSERT INTO {s}.features (token_id, window_start, swaps, unique_traders, origin) "
                       "SELECT t, to_timestamp(%s) - (w + 1) * interval '300 seconds', 0, 0, 0 "
                       "FROM generate_series(0, %s) t, generate_series(0, %s) w"),
                     (grid_end, tokens - 1, windows - 1))
        conn.execute(S("ALTER TABLE {s}.features ADD PRIMARY KEY (token_id, window_start)"))
        conn.execute(S("ANALYZE {s}.features"))
        rows = conn.execute(S("SELECT count(*) FROM {s}.features")).fetchone()[0]
        mark("zero_grid_end")
        result["zero_grid"] = {"rows_materialized": rows, "windows_per_token": windows, "tokens": tokens,
                               "seconds": marks["zero_grid_end"] - marks["zero_grid_start"],
                               "oldest_window_epoch": grid_end - windows * WINDOW_SECONDS,
                               "bytes_after_seed": conn.execute(sql.SQL("SELECT pg_total_relation_size(%s::regclass)"),
                                                                (f"{schema}.features",)).fetchone()[0]}

    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[1]").appName("workload-" + level["name"])
             .config("spark.ui.enabled", "false")
             .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.streaming.stateStore.providerClass",
                     "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider")
             .config("spark.local.dir", str(root / "spark-local"))
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark_version = spark.version
    counters = {"canonical_batches": 0, "feature_batches": 0, "canonical_rows": 0, "feature_rows": 0,
                "zero_fill_windows": 0, "zero_fill_gap_windows": 0, "state_rows_max": 0, "state_memory_bytes_max": 0}
    lock = threading.Lock()
    grid = {"frontier": int(grid_end)}  # first window after the seeded grid; survives query restarts

    def sink_canonical(batch, batch_id):
        values = [(r.id, r.token_id, r.wallet_id, r.event_time.replace(tzinfo=timezone.utc), r.raw)
                  for r in batch.collect() if r.id is not None]
        with psycopg.connect(local_dsn()) as c, c.cursor() as cur:
            cur.executemany(S("INSERT INTO {s}.canonical (id, token_id, wallet_id, event_time, payload) "
                              "VALUES (%s,%s,%s,%s,%s) ON CONFLICT (id) DO NOTHING"), values)
        with lock:
            counters["canonical_batches"] += 1
            counters["canonical_rows"] += len(values)

    def sink_features(batch, batch_id):
        values = [(r.token_id, r.window.start.replace(tzinfo=timezone.utc), r.swaps, r.unique_traders)
                  for r in batch.collect()]
        emitted = sorted({int(v[1].timestamp()) for v in values})
        with lock:
            starts, frontier = zero_fill_starts(grid["frontier"], emitted)
        with psycopg.connect(local_dsn()) as c, c.cursor() as cur:
            cur.executemany(S("INSERT INTO {s}.features (token_id, window_start, swaps, unique_traders, origin) "
                              "VALUES (%s,%s,%s,%s,1) ON CONFLICT (token_id, window_start) DO UPDATE "
                              "SET swaps=EXCLUDED.swaps, unique_traders=EXCLUDED.unique_traders"), values)
            for start in starts:  # explicit zero rows: inactive tokens and windows with no events at all
                cur.execute(S("INSERT INTO {s}.features (token_id, window_start, swaps, unique_traders, origin) "
                              "SELECT t, to_timestamp(%s), 0, 0, 2 FROM generate_series(0, %s) t ON CONFLICT DO NOTHING"),
                            (start, tokens - 1))
        with lock:
            grid["frontier"] = max(grid["frontier"], frontier)  # only after the fill committed
            counters["feature_batches"] += 1
            counters["feature_rows"] += len(values)
            counters["zero_fill_windows"] += len(starts)
            counters["zero_fill_gap_windows"] += len(set(starts) - set(emitted))

    trigger = f"{config['trigger_seconds']} seconds"

    def start_queries():
        src = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")
               .option("subscribe", topic).option("startingOffsets", "earliest")
               .option("failOnDataLoss", "true")
               .option("maxOffsetsPerTrigger", str(config["max_offsets_per_trigger"])).load())
        parsed = src.select(F.from_json(F.col("value").cast("string"),
                                        "id STRING, token_id INT, wallet_id INT, event_time TIMESTAMP").alias("v"),
                            F.col("value").cast("string").alias("raw")).select("v.*", "raw")
        canonical = (parsed.writeStream.foreachBatch(sink_canonical)
                     .option("checkpointLocation", str(root / "checkpoint-canonical"))
                     .trigger(processingTime=trigger).start())
        features = (parsed.withWatermark("event_time", f"{config['watermark_seconds']} seconds")
                    .dropDuplicatesWithinWatermark(["id"])
                    .groupBy("token_id", F.window("event_time", "5 minutes"))
                    .agg(F.count("*").alias("swaps"), F.size(F.collect_set("wallet_id")).alias("unique_traders")))
        feature_q = (features.writeStream.outputMode("append").foreachBatch(sink_features)
                     .option("checkpointLocation", str(root / "checkpoint-features"))
                     .trigger(processingTime=trigger).start())
        return [canonical, feature_q]

    def processed_offset(query):
        progress = query.lastProgress
        if progress is None:
            return None
        data = json.loads(progress.json) if hasattr(progress, "json") else progress
        end = data["sources"][0]["endOffset"]
        end = json.loads(end) if isinstance(end, str) else end
        for op in data.get("stateOperators", []):
            counters["state_rows_max"] = max(counters["state_rows_max"], op.get("numRowsTotal", 0))
            counters["state_memory_bytes_max"] = max(counters["state_memory_bytes_max"], op.get("memoryUsedBytes", 0))
        return int(end[topic]["0"])

    stop = threading.Event()
    produced = {"events": 0, "bytes": 0, "errors": 0}
    producer = KafkaProducer(bootstrap_servers="kafka:9092", acks="all", enable_idempotence=True,
                             linger_ms=50, max_block_ms=15000, compression_type=None)

    def produce():
        rng = random.Random(config["seed"])
        started = time.monotonic()
        while not stop.is_set():
            due = due_events(time.monotonic() - started, eps)
            while produced["events"] < due:
                now = datetime.now(timezone.utc).isoformat()
                value = event_payload(produced["events"], level, now, rng)
                future = producer.send(topic, key=f"a{produced['events']:012d}".encode(), value=value)
                future.add_errback(lambda _e: produced.__setitem__("errors", produced["errors"] + 1))
                produced["events"] += 1
                produced["bytes"] += len(value)
            time.sleep(0.02)
        producer.flush(timeout=30)

    corrections = {"applied": 0, "skipped": 0, "baseline_rows_read_max": 0, "baseline_rows_read_min": None,
                   "seconds_max": 0.0, "seconds_total": 0.0}
    walks = {"attempts": 0, "walks": 0, "rows_max": 0, "seconds_max": 0.0, "agreeing_attempts": 0}

    def correct():
        rng = random.Random(config["seed"] + 1)
        while not stop.wait(plan["correction_interval_seconds"]):
            t0 = time.monotonic()
            with psycopg.connect(local_dsn()) as c:
                c.execute("SELECT pg_advisory_xact_lock(%s)", (MAINTENANCE_SLOT,))
                row = c.execute(S("SELECT id, token_id, event_time FROM {s}.canonical "
                                  "WHERE event_time < now() - interval '10 minutes' ORDER BY random() LIMIT 1")).fetchone()
                if row is None:
                    corrections["skipped"] += 1
                    continue
                action, token, event_time = row
                c.execute(S("UPDATE {s}.canonical SET wallet_id = (wallet_id + %s) %% %s, revision = revision + 1 "
                            "WHERE id = %s"), (rng.randrange(1, wallets), wallets, action))
                ws = datetime.fromtimestamp(math.floor(event_time.timestamp() / WINDOW_SECONDS) * WINDOW_SECONDS,
                                            timezone.utc)
                swaps, traders = c.execute(S("SELECT count(*), count(DISTINCT wallet_id) FROM {s}.canonical "
                                             "WHERE token_id = %s AND event_time >= %s AND event_time < %s + interval '300 seconds'"),
                                           (token, ws, ws)).fetchone()
                c.execute(S("INSERT INTO {s}.features (token_id, window_start, swaps, unique_traders, origin) "
                            "VALUES (%s,%s,%s,%s,3) ON CONFLICT (token_id, window_start) DO UPDATE "
                            "SET swaps=EXCLUDED.swaps, unique_traders=EXCLUDED.unique_traders, origin=3"),
                          (token, ws, swaps, traders))
                history = c.execute(S("SELECT swaps, unique_traders FROM {s}.features WHERE token_id = %s "
                                      "AND window_start >= %s - interval '7 days' AND window_start < %s "
                                      "ORDER BY window_start"), (token, ws, ws)).fetchall()
            for column in (0, 1):
                values = [h[column] for h in history]
                if values:
                    med = statistics.median(values)
                    statistics.median([abs(v - med) for v in values])
            seconds = time.monotonic() - t0
            corrections["applied"] += 1
            corrections["baseline_rows_read_max"] = max(corrections["baseline_rows_read_max"], len(history))
            low = corrections["baseline_rows_read_min"]
            corrections["baseline_rows_read_min"] = len(history) if low is None else min(low, len(history))
            corrections["seconds_max"] = max(corrections["seconds_max"], seconds)
            corrections["seconds_total"] += seconds

    def reconcile():
        walk_id = 0
        while not stop.wait(plan["reconciliation_interval_seconds"]):
            walks["attempts"] += 1
            digests, first_walk = [], walk_id + 1
            for _ in range(3):  # at most three full walks, stop at two consecutive agreeing
                walk_id += 1
                t0 = time.monotonic()
                with psycopg.connect(local_dsn()) as c:
                    c.execute("SELECT pg_advisory_xact_lock(%s)", (MAINTENANCE_SLOT,))
                    c.execute(S("INSERT INTO {s}.scan_manifest SELECT %s, id, "
                                "md5(payload || ':' || wallet_id || ':' || revision) FROM {s}.canonical "
                                "WHERE event_time >= now() - %s * interval '1 second' ORDER BY id LIMIT %s"),
                              (walk_id, plan["reconciliation_range_seconds"], level["scan_membership"]))
                    count, digest = c.execute(S("SELECT count(*), md5(coalesce(string_agg(id || digest, ',' ORDER BY id), '')) "
                                                "FROM {s}.scan_manifest WHERE walk_id = %s"), (walk_id,)).fetchone()
                walks["walks"] += 1
                walks["rows_max"] = max(walks["rows_max"], count)
                walks["seconds_max"] = max(walks["seconds_max"], time.monotonic() - t0)
                digests.append(digest)
                if len(digests) >= 2 and digests[-1] == digests[-2]:
                    walks["agreeing_attempts"] += 1
                    break
            with psycopg.connect(local_dsn()) as c:
                c.execute("SELECT pg_advisory_xact_lock(%s)", (MAINTENANCE_SLOT,))
                c.execute(S("DELETE FROM {s}.scan_manifest WHERE walk_id < %s"), (first_walk,))

    end_consumer = KafkaConsumer(bootstrap_servers="kafka:9092")
    partition = TopicPartition(topic, 0)
    end_offset = lambda: end_consumer.end_offsets([partition])[partition]

    def wait_until(deadline, condition=lambda: False, step=2.0):
        while time.time() < deadline:
            for q in queries:
                if q.exception() is not None:
                    raise RuntimeError(f"streaming query failed: {q.exception()}")
                processed_offset(q)
            if condition():
                return True
            time.sleep(step)
        return condition()

    threads = [threading.Thread(target=f, daemon=True) for f in (produce, correct, reconcile)]
    for thread in threads:
        thread.start()
    queries = start_queries()
    mark("steady_start")
    wait_until(marks["steady_start"] + plan["steady_seconds"])
    stopped_at = [processed_offset(q) or 0 for q in queries]
    mark("outage_start")
    for q in queries:
        q.stop()
    queries = []
    wait_until(marks["outage_start"] + plan["outage_seconds"])
    restart_end = end_offset()
    backlog = restart_end - min(stopped_at)
    mark("catchup_start")
    queries = start_queries()
    limit = marks["catchup_start"] + plan["catchup_budget_seconds"] + 600
    caught = wait_until(limit, lambda: all((processed_offset(q) or -1) >= restart_end for q in queries), 1.0)
    mark("catchup_end")
    with psycopg.connect(local_dsn()) as c:
        canonical_rows_at_catchup = c.execute(S("SELECT count(*) FROM {s}.canonical")).fetchone()[0]
        canonical_bytes_at_catchup = c.execute(sql.SQL("SELECT pg_total_relation_size(%s::regclass)"),
                                               (f"{schema}.canonical",)).fetchone()[0]
    result["backlog"] = {"events": backlog, "end_offset_at_restart": restart_end, "processed_at_stop": stopped_at,
                         "caught_up": caught,
                         "catchup_seconds": (marks["catchup_end"] - marks["catchup_start"]) if caught else None,
                         "canonical_rows_at_catchup": canonical_rows_at_catchup,
                         "complete_through_restart_offset": canonical_rows_at_catchup >= restart_end}

    # Serialized maintenance in the one shared fenced slot; streaming continues, replay is finished.
    mark("maintenance_start")
    with psycopg.connect(local_dsn(), autocommit=True) as c:
        c.execute("SELECT pg_advisory_lock(%s)", (MAINTENANCE_SLOT,))
        t0 = time.monotonic()
        oldest = result["zero_grid"]["oldest_window_epoch"]
        expired = c.execute(S("DELETE FROM {s}.features WHERE window_start < to_timestamp(%s) + interval '1 day'"),
                            (oldest,)).rowcount
        t1 = time.monotonic()
        canonical_expired = c.execute(S("DELETE FROM {s}.canonical WHERE event_time < to_timestamp(%s)"),
                                      (marks["outage_start"],)).rowcount
        t2 = time.monotonic()
        for table in ("features", "canonical", "scan_manifest"):
            c.execute(sql.SQL("VACUUM (ANALYZE) {}.{}").format(sql.Identifier(schema), sql.Identifier(table)))
        t3 = time.monotonic()
        c.execute("CHECKPOINT")
        t4 = time.monotonic()
        c.execute("SELECT pg_advisory_unlock(%s)", (MAINTENANCE_SLOT,))
    mark("maintenance_end")
    per_row = (t2 - t1) / canonical_expired if canonical_expired else None
    result["maintenance"] = {"serialized": True, "slot": "pg_advisory_lock(%d) shared with corrections and walks" % MAINTENANCE_SLOT,
                             "seconds": t4 - t0, "feature_expiry_seconds": t1 - t0,
                             "canonical_expiry_seconds": t2 - t1, "vacuum_seconds": t3 - t2,
                             "checkpoint_seconds": t4 - t3,
                             "projected_daily_canonical_expiry_seconds": per_row * eps * 86400 if per_row else None,
                             "projection_note": "per-row measured canonical expiry cost x events/day; not measured at 7-day size"}
    result["retention"] = {"expired_feature_rows": expired, "expected_daily_feature_rows": plan["retention_expiry_rows"],
                           "expired_canonical_rows": canonical_expired,
                           "canonical_bytes_per_row_measured": canonical_bytes_at_catchup / canonical_rows_at_catchup
                           if canonical_rows_at_catchup else None}
    rows_per_day = eps * 86400
    if result["retention"]["canonical_bytes_per_row_measured"]:
        result["retention"]["canonical_7d_projected_bytes"] = (result["retention"]["canonical_bytes_per_row_measured"]
                                                               * rows_per_day * 7)
    # Short post-maintenance tail so post-recovery windows finalize under normal operation.
    wait_until(time.time() + config.get("tail_seconds", 2 * WINDOW_SECONDS))
    mark("tail_end")
    stop.set()
    for thread in threads:
        thread.join(timeout=120)
    for q in queries:
        processed_offset(q)
        q.stop()
    spark.stop()
    end_consumer.close()
    producer.close(timeout=10)
    mark("run_end")

    with psycopg.connect(local_dsn()) as c:
        latency_rows = [(r[0].timestamp(), r[1].timestamp()) for r in
                        c.execute(S("SELECT window_start, committed_at FROM {s}.features WHERE origin = 1")).fetchall()]
        sizes = {t: dict(zip(("total", "heap", "indexes"), c.execute(
            sql.SQL("SELECT pg_total_relation_size(%s::regclass), pg_relation_size(%s::regclass), pg_indexes_size(%s::regclass)"),
            (f"{schema}.{t}",) * 3).fetchone())) for t in ("canonical", "features", "scan_manifest")}
        wal_dir = c.execute("SELECT coalesce(sum(size), 0) FROM pg_ls_waldir()").fetchone()[0]
        wal_after = c.execute("SELECT wal_bytes FROM pg_stat_wal").fetchone()[0]
        feature_rows = c.execute(S("SELECT origin, count(*) FROM {s}.features GROUP BY origin ORDER BY origin")).fetchall()
        database = c.execute("SELECT pg_database_size(current_database())").fetchone()[0]
        distinct, total, low, high = c.execute(S(
            "SELECT count(DISTINCT window_start), count(*), extract(epoch FROM min(window_start))::bigint, "
            "extract(epoch FROM max(window_start))::bigint FROM {s}.features")).fetchone()
    result["zero_grid"]["contiguity_at_end"] = grid_contiguity(distinct_windows=distinct, rows=total,
                                                               min_epoch=low, max_epoch=high, tokens=tokens)
    result["latency"] = latency_summary(latency_rows, marks)
    result["pg"] = {"tables": sizes, "index_bytes": sum(v["indexes"] for v in sizes.values()),
                    "wal_dir_bytes": int(wal_dir), "wal_generated_bytes": int(wal_after - wal_before),
                    "vacuum_seconds": result["maintenance"]["vacuum_seconds"],
                    "feature_rows_by_origin": {str(o): n for o, n in feature_rows}, "database_bytes": database}
    result["spark"] = dict(counters, checkpoint_bytes=_du(root / "checkpoint-canonical") + _du(root / "checkpoint-features"),
                           spark_local_bytes=_du(root / "spark-local"), spark_version=spark_version)
    result["kafka"] = {"produced_events": produced["events"], "produced_bytes": produced["bytes"],
                       "send_errors": produced["errors"]}
    result["corrections"], result["reconciliation"] = corrections, walks
    result["events"], result["input_bytes"] = produced["events"], produced["bytes"]
    Path(config["output"]).write_text(json.dumps(result, indent=2, default=str))
    return result


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    run_level(json.loads(Path(args.config).read_text()))
