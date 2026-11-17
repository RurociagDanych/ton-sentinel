"""Bounded, deduplicated Spark stateful fast path with a durable checkpoint.

Candidate output is diagnostic only: canonical replacement stays authoritative. Only the
first accepted insert per ``event_id`` inside the watermark bound enters state; the bound is
not perpetual uniqueness. ``failOnDataLoss`` stays true (see ``stream_cli.kafka_reader_options``).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from ton_sentinel.db.fencing import LeaseRejected
from ton_sentinel.db.serving import WindowKey
from ton_sentinel.db.work import admit_work, repair_windows

_POLICY_KEYS = {"watermark_seconds", "max_state_rows", "max_partitions", "window_seconds"}
_MAX_BATCH_WINDOWS = 1_000


class StateBoundExceeded(RuntimeError):
    """Streaming state grew past the configured bound; the query must stop."""


@dataclass(frozen=True)
class FastPathPolicy:
    watermark_seconds: int
    max_state_rows: int
    max_partitions: int
    window_seconds: int = 300


def _bounded(policy: dict, name: str, low: int, high: int) -> int:
    value = policy[name]
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{name} must be an integer {low}..{high}")
    return value


def fast_path_policy(policy: dict) -> FastPathPolicy:
    if type(policy) is not dict or not {"watermark_seconds", "max_state_rows", "max_partitions"} <= policy.keys() \
            or not policy.keys() <= _POLICY_KEYS:
        raise ValueError("policy requires watermark_seconds, max_state_rows, max_partitions")
    window = policy.get("window_seconds", 300)
    if window != 300:
        raise ValueError("window_seconds must be 300")
    return FastPathPolicy(_bounded(policy, "watermark_seconds", 1, 86_400),
                          _bounded(policy, "max_state_rows", 1, 1_000_000),
                          _bounded(policy, "max_partitions", 1, 64), 300)


def check_partitions(shuffle_partitions: int, policy: FastPathPolicy) -> None:
    if shuffle_partitions > policy.max_partitions:
        raise ValueError("spark.sql.shuffle.partitions exceeds max_partitions")


def summarize_progress(progress: dict) -> dict:
    operators = progress.get("stateOperators") or []
    return {
        "batch_id": progress["batchId"],
        "input_rows": progress.get("numInputRows", 0),
        "state_rows": sum(op.get("numRowsTotal", 0) for op in operators),
        "state_bytes": sum(op.get("memoryUsedBytes", 0) for op in operators),
        "watermark": (progress.get("eventTime") or {}).get("watermark"),
        "late_drops": sum(op.get("numRowsDroppedByWatermark", 0) for op in operators),
    }


def enforce_state_bound(progress: dict, policy: FastPathPolicy) -> None:
    if not progress.get("stateOperators"):
        raise StateBoundExceeded("progress reports no state operators; stateful plan not active")
    rows = summarize_progress(progress)["state_rows"]
    if rows > policy.max_state_rows:
        raise StateBoundExceeded(f"state rows {rows} exceed max_state_rows {policy.max_state_rows}")


def build_fast_path(changes_df, policy: dict):
    """Kafka-shaped streaming frame -> per-(network, 5-minute window) distinct-event counts."""
    from pyspark.sql import functions as F

    policy = fast_path_policy(policy)
    events = (changes_df.select(F.from_json(
        F.col("value").cast("string"),
        "event_id STRING, action STRUCT<network: STRING, action_end: LONG>").alias("v"))
        .select(F.col("v.event_id").alias("event_id"), F.col("v.action.network").alias("network"),
                F.timestamp_seconds("v.action.action_end").alias("event_time"))
        .where("event_id IS NOT NULL AND network IS NOT NULL AND event_time IS NOT NULL"))
    deduped = (events.withWatermark("event_time", f"{policy.watermark_seconds} seconds")
               .dropDuplicatesWithinWatermark(["event_id"]))
    return deduped.groupBy("network", F.window("event_time", f"{policy.window_seconds} seconds")).count()


def explain_plan(df) -> str:
    """Analyzed logical plan of a streaming frame (physical plans exist only once started)."""
    return df._jdf.queryExecution().analyzed().toString()


@dataclass(frozen=True)
class FastPathResult:
    progress: list
    max_state_rows: int
    executed_plan: str = ""


def collect_candidate_rows(batch_df, cap: int = _MAX_BATCH_WINDOWS) -> list:
    """Collect one micro-batch of candidate windows, reading at most ``cap + 1`` rows."""
    if type(cap) is not int or cap < 1:
        raise ValueError("cap must be a positive integer")
    rows = batch_df.limit(cap + 1).collect()
    if len(rows) > cap:
        raise ValueError("candidate batch exceeds window cap")
    return rows


def run_fast_path(spark, config, policy: dict) -> FastPathResult:
    """One bounded availableNow pass resuming from the identity-bound durable checkpoint."""
    import json
    from datetime import timezone

    import psycopg

    from ton_sentinel.db.stream_batches import bind_checkpoint, checkpoint_identity
    from ton_sentinel.stream_cli import kafka_reader_options

    policy = fast_path_policy(policy)
    checkpoint = config.validate()
    check_partitions(int(spark.conf.get("spark.sql.shuffle.partitions")), policy)
    bind_checkpoint(checkpoint, checkpoint_identity(
        environment_id=config.environment_id, query_id=config.query_id,
        pipeline_epoch=config.pipeline_epoch, topic=config.topic,
        topic_incarnation=config.topic_incarnation, replay_epoch=config.replay_epoch))
    identity = (config.environment_id, config.query_id, config.pipeline_epoch)
    source = spark.readStream.format("kafka").options(**kafka_reader_options(config)).load()
    windows = build_fast_path(source, {
        "watermark_seconds": policy.watermark_seconds, "max_state_rows": policy.max_state_rows,
        "max_partitions": policy.max_partitions})
    holder = {}

    def apply_batch(batch_df, batch_id: int) -> None:
        # The previous batch's progress is the freshest state measurement available here.
        last = holder["query"].lastProgress
        if last:
            enforce_state_bound(last, policy)
        rows = collect_candidate_rows(batch_df)
        with psycopg.connect(config.dsn) as conn:
            for row in rows:     # full-replacement upsert: a replay converges, never adds
                conn.execute(
                    "INSERT INTO stream_candidate_windows (environment_id,query_id,pipeline_epoch,network,"
                    "window_start,window_end,swaps,batch_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (environment_id,query_id,pipeline_epoch,network,window_start) DO UPDATE "
                    "SET swaps=EXCLUDED.swaps, window_end=EXCLUDED.window_end, batch_id=EXCLUDED.batch_id, "
                    "updated_at=clock_timestamp()",
                    (*identity, row["network"], row["window"].start.replace(tzinfo=timezone.utc),
                     row["window"].end.replace(tzinfo=timezone.utc), row["count"], batch_id))

    query = (windows.writeStream.outputMode("append").foreachBatch(apply_batch)
             .option("checkpointLocation", str(checkpoint)).trigger(availableNow=True).start())
    holder["query"] = query
    try:
        query.awaitTermination()
        progress = [json.loads(p.json) for p in query.recentProgress]
        execution = query._jsq.streamingQuery().lastExecution()
        executed_plan = execution.executedPlan().toString() if execution is not None else ""
    except Exception as exc:
        cause = str(exc)
        if "StateBoundExceeded" in cause:
            raise StateBoundExceeded(cause.split("StateBoundExceeded:", 1)[-1].strip().splitlines()[0]) from exc
        raise
    finally:
        if query.isActive:
            query.stop()
    summaries = [summarize_progress(p) for p in progress]
    with psycopg.connect(config.dsn) as conn:
        for item in summaries:
            conn.execute(
                "INSERT INTO stream_state_metrics (environment_id,query_id,pipeline_epoch,batch_id,input_rows,"
                "state_rows,state_bytes,watermark,late_drops) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (environment_id,query_id,pipeline_epoch,batch_id) DO UPDATE SET "
                "input_rows=EXCLUDED.input_rows,state_rows=EXCLUDED.state_rows,state_bytes=EXCLUDED.state_bytes,"
                "watermark=EXCLUDED.watermark,late_drops=EXCLUDED.late_drops, recorded_at=clock_timestamp()",
                (*identity, item["batch_id"], item["input_rows"], item["state_rows"], item["state_bytes"],
                 item["watermark"], item["late_drops"]))
    for p in progress:
        if p.get("numInputRows"):
            enforce_state_bound(p, policy)
    return FastPathResult(progress, max((s["state_rows"] for s in summaries), default=0),
                          executed_plan)


# ----------------------------------------------------------------------------------------
# Sweeper. Late, evicted, idle and dirty windows are closed ONLY by canonical repair
# (admit_work -> freeze_work -> compute_replacement -> promote, via repair_windows). Candidate
# rows are read as hints for *which* window needs attention and are never copied to serving.
# Wall-clock time here selects windows to repair; it never advances or implies a Spark watermark.
_REASONS = ("dirty", "late", "evicted", "idle")
_MAX_SWEEP_WINDOWS = 10_000


def _positive(name: str, value) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


@dataclass(frozen=True)
class SweepPolicy:
    idle_sweep_seconds: int      # wall-clock age of a closed window with no candidate and no output
    watermark_seconds: int       # a candidate window this old (past its end) is evicted from state
    max_windows: int             # windows repaired per sweep
    admission: dict              # exactly the admit_work policy keys

    def __post_init__(self):
        _positive("idle_sweep_seconds", self.idle_sweep_seconds)
        _positive("watermark_seconds", self.watermark_seconds)
        if _positive("max_windows", self.max_windows) > _MAX_SWEEP_WINDOWS:
            raise ValueError("max_windows exceeds the sweep bound")
        from ton_sentinel.db.work import _ADMISSION_POLICY_KEYS
        if type(self.admission) is not dict or set(self.admission) != _ADMISSION_POLICY_KEYS:
            raise ValueError("admission policy must name exactly: "
                             + ", ".join(sorted(_ADMISSION_POLICY_KEYS)))


@dataclass(frozen=True)
class Snapshot:
    """Read-only view used to choose windows. Keys are (network, window_start)."""

    canonical: dict      # key -> (active action count, fingerprint of canonical inputs)
    served: frozenset    # keys with serving rows
    candidates: dict     # key -> candidate swaps (diagnostic only)
    closures: dict       # key -> fingerprint the window was last closed on
    dirty: frozenset     # keys with a pending target dirty row


def fingerprint(inputs) -> str:
    """Digest of (event_id, accepted_generation, semantic_digest) triples of one window."""
    body = json.dumps(sorted([i[0], i[1], i[2]] for i in inputs), separators=(",", ":"))
    return "sha256:" + hashlib.sha256(body.encode("ascii")).hexdigest()


def classify_windows(snapshot: Snapshot, now: int, policy: SweepPolicy) -> list:
    """Return ``[(WindowKey, reason)]`` sorted by window, at most ``policy.max_windows``.

    Priority dirty > late > evicted > idle. A window whose canonical inputs equal the ones it
    was last closed on is never selected again (unless dirtied), so repair happens once.
    """
    found = []
    keys = set(snapshot.canonical) | set(snapshot.dirty)
    for key in keys:
        network, start = key
        count, fp = snapshot.canonical.get(key, (0, None))
        closed = snapshot.closures.get(key)
        end = start + 300
        reason = None
        if key in snapshot.dirty:
            reason = "dirty"
        elif closed is not None and closed == fp:
            continue
        elif end > now:
            continue
        elif (closed is not None or key in snapshot.served) and (
                closed is not None or (key in snapshot.candidates
                                       and count != snapshot.candidates[key])):
            reason = "late"
        elif closed is not None:
            continue
        elif key in snapshot.served:
            # Served by the canonical stream with no fast-path candidate and never closed
            # (the live path): close it once its idle sweep time has passed, so its coverage
            # can later be proven. A matching candidate keeps the window as it is.
            if key not in snapshot.candidates and end + policy.idle_sweep_seconds <= now:
                reason = "idle"
        elif key in snapshot.candidates:
            if end + policy.watermark_seconds <= now:
                reason = "evicted"
        elif count > 0 and end + policy.idle_sweep_seconds <= now:
            reason = "idle"
        if reason is not None:
            found.append((WindowKey(network, start, end), reason))
    found.sort(key=lambda item: (item[0].window_start, item[0].network))
    return found[:policy.max_windows]


def read_snapshot(connection) -> Snapshot:
    """Read what the sweeper needs. Writes nothing."""
    canonical = {}
    rows = connection.execute(
        "SELECT network, (action->>'action_end')::bigint / 300 * 300 AS ws, event_id,"
        " accepted_generation, semantic_digest FROM ("
        " SELECT network,event_id,accepted_generation,semantic_digest,action FROM canonical_current"
        "  WHERE disposition='active' AND NOT eligibility_suppressed"
        " UNION ALL"
        " SELECT ca.action->'identity'->>'network',ca.event_id,ca.accepted_generation,"
        "  ca.semantic_digest,ca.action FROM canonical_actions ca"
        "  WHERE NOT EXISTS (SELECT 1 FROM canonical_current cc WHERE cc.event_id=ca.event_id)"
        ") cur").fetchall()
    grouped = {}
    for network, start, *triple in rows:
        grouped.setdefault((network, start), []).append(triple)
    for key, triples in grouped.items():
        canonical[key] = (len(triples), fingerprint(triples))
    served = frozenset(connection.execute(
        "SELECT DISTINCT network, window_start FROM served_features").fetchall())
    candidates = {(n, int(s)): int(c) for n, s, c in connection.execute(
        "SELECT network, extract(epoch FROM window_start)::bigint, max(swaps) "
        "FROM stream_candidate_windows GROUP BY 1,2").fetchall()}
    closures = {(n, s): f for n, s, f in connection.execute(
        "SELECT network, window_start, fingerprint FROM window_sweep_closures").fetchall()}
    dirty = frozenset(connection.execute(
        "SELECT DISTINCT network, window_start FROM dirty_windows "
        "WHERE role='target' AND status='pending'").fetchall())
    return Snapshot(canonical, served, candidates, closures, dirty)


@dataclass(frozen=True)
class SweepReport:
    repaired: tuple      # ((WindowKey, reason), ...)
    refused: tuple       # ((WindowKey, admission status or 'stale_pins'), ...)


def sweep_windows(connection, lease, spark, now: int, policy: SweepPolicy,
                  compute=None) -> SweepReport:
    """Close the windows that need it, each through admission and canonical repair."""
    from ton_sentinel.db import work as work_module

    if type(policy) is not SweepPolicy:
        raise ValueError("SweepPolicy required")
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("sweep_windows requires an idle connection")
    with connection.transaction():
        snapshot = read_snapshot(connection)
    base_compute = work_module.compute_replacement if compute is None else compute
    repaired, refused = [], []
    for key, reason in classify_windows(snapshot, now, policy):
        status = _close_window(connection, lease, spark, key, reason, now, policy, base_compute)
        if status is None:
            repaired.append((key, reason))
        else:
            refused.append((key, status))
    return SweepReport(tuple(repaired), tuple(refused))


def sweep_each(connection, acquire, spark, now: int, policy: SweepPolicy,
               compute=None) -> SweepReport:
    """``sweep_windows`` with a fresh lease from ``acquire()`` for every selected window.

    One canonical repair can take most of the 300 s lease ceiling, so a multi-window sweep
    under one lease would lose it. Selection, admission and repair are unchanged; a refused
    window is reported and the next one is still tried. A lost lease still stops the sweep.
    """
    from ton_sentinel.db import work as work_module

    if type(policy) is not SweepPolicy:
        raise ValueError("SweepPolicy required")
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("sweep_each requires an idle connection")
    with connection.transaction():
        snapshot = read_snapshot(connection)
    base_compute = work_module.compute_replacement if compute is None else compute
    repaired, refused = [], []
    for key, reason in classify_windows(snapshot, now, policy):
        status = _close_window(connection, acquire(), spark, key, reason, now, policy,
                               base_compute)
        if status is None:
            repaired.append((key, reason))
        else:
            refused.append((key, status))
    return SweepReport(tuple(repaired), tuple(refused))


def _close_window(connection, lease, spark, key, reason, now, policy, base_compute):
    """Admit then canonically repair one window. Returns None on success, else the refusal status."""
    admission = admit_work(connection, (key,), now, policy.admission)
    if not admission.admitted:
        return admission.status
    pinned = {}

    def pinning_compute(sp, frozen, _pinned=pinned):
        _pinned["work_id"] = frozen.work_id
        _pinned["fingerprint"] = fingerprint(frozen.inputs)
        return base_compute(sp, frozen)

    def record(conn):
        conn.execute(
            "INSERT INTO window_sweep_closures(network,window_start,reason,fingerprint,work_id)"
            " VALUES (%s,%s,%s,%s,%s) ON CONFLICT (network,window_start) DO UPDATE SET "
            "reason=EXCLUDED.reason, fingerprint=EXCLUDED.fingerprint, work_id=EXCLUDED.work_id,"
            " closed_at=clock_timestamp()",
            (key.network, key.window_start, reason, pinned["fingerprint"], pinned["work_id"]))

    try:
        repair_windows(connection, lease, spark, admission, stage=record, compute=pinning_compute)
    except LeaseRejected:
        raise           # a lost or expired lease stops the sweep; it is not a pin refusal
    except RuntimeError:
        return "stale_pins"
    return None


def sweep_due(db, spark, now: int, limit: int, *, lease, policy: SweepPolicy) -> int:
    """Close at most ``limit`` due windows through canonical repair; returns how many were repaired."""
    import dataclasses

    if type(policy) is not SweepPolicy:
        raise ValueError("SweepPolicy required")
    bounded = dataclasses.replace(policy, max_windows=_positive("limit", limit))
    return len(sweep_windows(db, lease, spark, now, bounded).repaired)


def close_candidate_windows(db, spark, keys: tuple, *, lease, now: int, policy: SweepPolicy) -> None:
    """Explicitly close the given windows through admission and canonical repair.

    Candidate state is never copied to serving. Raises ``RuntimeError`` if any window is refused.
    """
    from ton_sentinel.db import work as work_module

    if type(policy) is not SweepPolicy:
        raise ValueError("SweepPolicy required")
    if type(keys) is not tuple or not keys or len(keys) > policy.max_windows:
        raise ValueError("keys must be a non-empty tuple within the sweep bound")
    if db.info.transaction_status.name != "IDLE":
        raise ValueError("close_candidate_windows requires an idle connection")
    refused = []
    for key in keys:
        status = _close_window(db, lease, spark, key, "evicted", now, policy,
                               work_module.compute_replacement)
        if status is not None:
            refused.append((key, status))
    if refused:
        raise RuntimeError("window closure refused: " + ", ".join(
            f"{k.network}/{k.window_start}={status}" for k, status in refused))
