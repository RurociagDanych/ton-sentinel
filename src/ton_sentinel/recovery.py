"""Paused, fenced recovery cut: snapshot + outbox + Kafka offsets."""
import hashlib
import json
import uuid
from typing import NoReturn
from collections.abc import Callable, Mapping
from dataclasses import dataclass, fields

from ton_sentinel.contracts import Lease
from ton_sentinel.db.fencing import acquire_transition, validate_transition

MANIFEST_VERSION = 1

# Tables whose content is the PostgreSQL side of the cut; each is write-guarded while paused.
SNAPSHOT_TABLES = (
    "canonical_actions", "canonical_current", "canonical_revisions", "scan_membership",
    "source_ranges", "outbox", "outbox_publication_frontier", "outbox_publication_origin",
    "outbox_delivery_config", "outbox_delivery_authority", "canonical_generation",
    "active_source_epoch", "dependency_epoch", "ingest_state", "event_identity_guards",
    "identity_guard_horizons", "retained_context_horizons", "delivery_quarantine",
    "revision_quarantine", "scan_quarantine", "dirty_windows", "source_epoch_transitions",
    "token_universe_version", "active_detector_policy",
)
# Derived writers (repair/stream promotion, retention) are paused too but are not cut state.
GUARDED_TABLES = SNAPSHOT_TABLES + (
    "repair_promotions", "served_features", "stream_batches", "retention_runs")

_STATE_CLASS = {"pending": "pending", "attempted": "ambiguous", "ambiguous": "ambiguous",
                "acknowledged": "acknowledged"}
_SECTIONS = {
    "fence": ("owner", "fence"),
    "snapshot": ("canonical_generation", "contributions", "tables"),
    "outbox": ("members", "counts", "frontier"),
    "kafka": ("cluster_id", "topic", "topic_id", "topic_incarnation", "replay_epoch",
              "end_offsets", "log_start_offsets"),
}


class RecoveryCutRejected(RuntimeError):
    """A cut could not be captured or claimed; nothing was recorded or claimed."""
    def __init__(self, message: str, reasons=None):
        super().__init__(message)
        self.reasons = tuple(reasons) if reasons else (message,)


@dataclass(frozen=True)
class RecoveryPause:
    pause_id: int
    recovery_epoch: int
    lease: Lease
    resumed: bool


@dataclass(frozen=True)
class RecoveryCut:
    cut_id: str
    pause_id: int
    recovery_epoch: int
    manifest: dict
    digest: str


@dataclass(frozen=True)
class RecoveryPoint:
    cut_id: str
    recovery_epoch: int
    canonical_generation: int
    topic: str
    topic_incarnation: str
    topic_id: str
    end_offsets: dict
    digest: str


def _int(value) -> bool:
    return type(value) is int and value >= 0


def _idle(connection, name: str) -> None:
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError(f"{name} requires an idle connection")


def _refuse(message: str) -> NoReturn:
    raise RecoveryCutRejected(message)


def normalize_kafka_sample(sample, *, topic: str) -> dict:
    """Validate one broker offset sample; partitions become JSON keys "0".."n-1"."""
    if not isinstance(sample, Mapping):
        _refuse("incomplete: Kafka offset sample must be a mapping")
    cluster, topic_id = sample.get("cluster_id"), sample.get("topic_id")
    if type(cluster) is not str or not cluster:
        _refuse("incomplete: Kafka sample has no cluster id")
    if type(topic_id) is not str or not topic_id:
        _refuse("incomplete: Kafka sample has no topic id")
    if sample.get("topic") != topic:
        _refuse(f"mismatched: Kafka sample topic differs from delivery topic {topic!r}")
    partitions = sample.get("partitions")
    if not isinstance(partitions, Mapping) or not partitions:
        _refuse("incomplete: Kafka sample partition set is empty")
    keys = {}
    for key, offsets in partitions.items():
        if type(key) is int and key >= 0:
            number = key
        elif type(key) is str and key.isdigit() and str(int(key)) == key:
            number = int(key)
        else:
            _refuse(f"incomplete: Kafka sample partition {key!r} is invalid")
        keys[number] = offsets
    if sorted(keys) != list(range(len(keys))):
        _refuse("incomplete: Kafka sample partition set is not 0..n-1")
    normalized = {}
    for number in sorted(keys):
        offsets = keys[number]
        start = offsets.get("start") if isinstance(offsets, Mapping) else None
        end = offsets.get("end") if isinstance(offsets, Mapping) else None
        if not (_int(start) and _int(end) and start <= end):
            _refuse(f"incomplete: Kafka sample offset invalid for partition {number}")
        normalized[str(number)] = {"start": start, "end": end}
    return {"cluster_id": cluster, "topic": topic, "topic_id": topic_id,
            "partitions": normalized}


def classify_publication_state(state: str) -> str:
    """Attempted sends may have reached Kafka: they stay ambiguous, never acknowledged."""
    if state not in _STATE_CLASS:
        _refuse(f"incomplete: unknown outbox publication state {state!r}")
    return _STATE_CLASS[state]


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def manifest_digest(manifest: dict) -> str:
    return hashlib.sha256(_canonical(manifest).encode("ascii")).hexdigest()


def validate_manifest(manifest, digest) -> None:
    """Refuse a manifest that is missing a component or does not match its digest."""
    if not isinstance(manifest, dict):
        _refuse("incomplete: recovery cut manifest is not an object")
    for key in ("version", "pause_id", "recovery_epoch", *_SECTIONS):
        if key not in manifest:
            _refuse(f"incomplete: recovery cut manifest has no {key}")
    for section, keys in _SECTIONS.items():
        if not isinstance(manifest[section], dict) or any(k not in manifest[section]
                                                          for k in keys):
            _refuse(f"incomplete: recovery cut manifest {section} section is partial")
    offsets = manifest["kafka"]["end_offsets"]
    if (not isinstance(offsets, dict) or not offsets
            or sorted(offsets) != sorted(str(p) for p in range(len(offsets)))
            or not all(_int(v) for v in offsets.values())):
        _refuse("incomplete: recovery cut has no complete partition end offsets")
    if type(digest) is not str or manifest_digest(manifest) != digest:
        _refuse("incomplete: manifest digest does not match its content (partial or altered cut)")


def recovery_point_reasons(row: dict, manifest: dict, digest: str, live: dict) -> tuple:
    """Every reason this durable cut may not be claimed now; empty means claimable."""
    try:
        validate_manifest(manifest, digest)
    except RecoveryCutRejected as exc:
        return exc.reasons
    reasons = []
    k = manifest["kafka"]
    expected = {
        "pause_id": manifest["pause_id"], "recovery_epoch": manifest["recovery_epoch"],
        "lease_owner": manifest["fence"]["owner"], "lease_fence": manifest["fence"]["fence"],
        "canonical_generation": manifest["snapshot"]["canonical_generation"],
        "topic": k["topic"], "topic_incarnation": k["topic_incarnation"],
        "replay_epoch": k["replay_epoch"], "kafka_cluster_id": k["cluster_id"],
        "kafka_topic_id": k["topic_id"], "end_offsets": k["end_offsets"]}
    for key, value in expected.items():
        if row.get(key) != value:
            reasons.append(f"mismatched: cut row {key} differs from its manifest")
    if not live["pause_active"]:
        reasons.append("stale: the recovery pause was released (ingestion resumed after the cut)")
    if live["latest_epoch"] != manifest["recovery_epoch"]:
        reasons.append(f"stale: a newer recovery epoch {live['latest_epoch']} exists")
    if live["snapshot"] != manifest["snapshot"]:
        reasons.append("stale: the live PostgreSQL snapshot no longer matches the cut")
    if live["outbox"] != manifest["outbox"]:
        reasons.append("stale: the live outbox membership/frontier no longer matches the cut")
    if tuple(live["delivery"]) != (k["topic"], k["topic_incarnation"], k["replay_epoch"]):
        reasons.append("mismatched: delivery authority differs from the cut")
    kafka = live["kafka"]
    if kafka["cluster_id"] != k["cluster_id"]:
        reasons.append("mismatched: kafka cluster id changed")
    if kafka["topic_id"] != k["topic_id"]:
        reasons.append("mismatched: kafka topic id changed (topic recreated)")
    if set(kafka["partitions"]) != set(k["end_offsets"]):
        reasons.append("mismatched: kafka partition set changed")
    else:
        for partition, end in sorted(k["end_offsets"].items(), key=lambda item: int(item[0])):
            log = kafka["partitions"][partition]
            if not log["start"] <= end <= log["end"]:
                reasons.append(f"mismatched: partition {partition} offset {end} outside the "
                               f"retained log [{log['start']},{log['end']}]")
    return tuple(reasons)


def _lock_statement() -> str:
    return "LOCK TABLE " + ", ".join(GUARDED_TABLES) + " IN SHARE MODE"


def pause_for_recovery(connection, owner: str, *, ttl_seconds: int = 300) -> RecoveryPause:
    """Take the shared slot, let admitted writers finish, persist the pause and its epoch.

    An already active pause (an interrupted recovery) is resumed under the new lease with its
    original epoch, never replaced: restart from the durable pause or release it first.
    """
    if type(owner) is not str or not owner.strip():
        raise ValueError("recovery owner must be nonempty text")
    _idle(connection, "pause_for_recovery")
    lease = acquire_transition(connection, owner, ttl_seconds=ttl_seconds)
    with connection.transaction():
        validate_transition(connection, lease)
        connection.execute(_lock_statement())
        active = connection.execute(
            "SELECT pause_id, recovery_epoch FROM recovery_pauses WHERE released_at IS NULL "
            "FOR UPDATE").fetchone()
        resumed = active is not None
        if active is None:
            active = connection.execute(
                "INSERT INTO recovery_pauses(lease_owner, lease_fence) VALUES (%s, %s) "
                "RETURNING pause_id, recovery_epoch", (lease.owner, lease.fence)).fetchone()
        validate_transition(connection, lease)
    return RecoveryPause(int(active[0]), int(active[1]), lease, resumed)


def release_recovery_pause(connection, lease, pause_id: int) -> None:
    """Resume ingestion under the presented (current) fence; writers need a fresh lease."""
    if type(lease) is not Lease:
        raise ValueError("recovery lease required")
    if type(pause_id) is not int or pause_id <= 0:
        raise ValueError("recovery pause id required")
    _idle(connection, "release_recovery_pause")
    with connection.transaction():
        validate_transition(connection, lease)
        row = connection.execute(
            "UPDATE recovery_pauses SET released_at=clock_timestamp(), release_owner=%s, "
            "release_fence=%s WHERE pause_id=%s AND released_at IS NULL RETURNING pause_id",
            (lease.owner, lease.fence, pause_id)).fetchone()
        if row is None:
            raise ValueError("recovery pause is not active")
        validate_transition(connection, lease)


def _begin_snapshot(connection) -> None:
    connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
    connection.execute("SET LOCAL TimeZone = 'UTC'")


def _snapshot(connection) -> dict:
    from psycopg import sql

    generation = connection.execute(
        "SELECT generation FROM canonical_generation WHERE singleton").fetchone()
    if generation is None:
        _refuse("incomplete: canonical generation state is not initialized")
    contributions = connection.execute(
        "SELECT event_id, network, accepted_generation, semantic_digest FROM ("
        " SELECT event_id, network, accepted_generation, semantic_digest FROM canonical_current"
        "  WHERE disposition='active' AND NOT eligibility_suppressed"
        " UNION ALL SELECT ca.event_id, ca.action->'identity'->>'network', ca.accepted_generation,"
        "  ca.semantic_digest FROM canonical_actions ca WHERE NOT EXISTS"
        "  (SELECT 1 FROM canonical_current cc WHERE cc.event_id=ca.event_id)) c"
        " ORDER BY event_id COLLATE \"C\"").fetchall()
    tables = {}
    for table in SNAPSHOT_TABLES:
        rows, digest = connection.execute(sql.SQL(
            "SELECT count(*), encode(sha256(convert_to(coalesce(string_agg(r, E'\\n' "
            "ORDER BY r COLLATE \"C\"), ''), 'UTF8')), 'hex') "
            "FROM (SELECT row_to_json(x)::text AS r FROM {} x) s").format(
                sql.Identifier(table))).fetchone()
        tables[table] = {"rows": int(rows), "digest": digest}
    return {"canonical_generation": int(generation[0]),
            "contributions": [list(row) for row in contributions], "tables": tables}


def _outbox(connection) -> dict:
    members = []
    counts = {"acknowledged": 0, "ambiguous": 0, "pending": 0}
    for event_id, generation, state in connection.execute(
            "SELECT event_id, accepted_generation, publication_state FROM outbox "
            "ORDER BY event_id COLLATE \"C\"").fetchall():
        kind = classify_publication_state(state)
        counts[kind] += 1
        members.append([event_id, generation, state, kind])
    origin = connection.execute(
        "SELECT origin_inclusive FROM outbox_publication_origin WHERE singleton").fetchone()
    frontier = connection.execute(
        "SELECT end_exclusive FROM outbox_publication_frontier WHERE singleton").fetchone()
    return {"members": members, "counts": counts,
            "frontier": {"origin_inclusive": None if origin is None else origin[0],
                         "end_exclusive": None if frontier is None else frontier[0]}}


def _delivery(connection) -> tuple:
    authority = connection.execute(
        "SELECT topic, topic_incarnation, replay_epoch FROM outbox_delivery_authority "
        "WHERE singleton").fetchone()
    config = connection.execute(
        "SELECT topic, topic_incarnation, replay_epoch FROM outbox_delivery_config "
        "WHERE singleton").fetchone()
    if authority is None or config is None or tuple(authority) != tuple(config):
        _refuse("mismatched: delivery is not configured from its immutable authority")
    return tuple(authority)


def capture_recovery_cut(connection, lease, pause_id: int,
                         kafka: Callable[[str], Mapping]) -> RecoveryCut:
    """Record the paired snapshot/outbox/offset manifest of an active pause, or nothing."""
    if type(lease) is not Lease:
        raise ValueError("recovery lease required")
    if type(pause_id) is not int or pause_id <= 0:
        raise ValueError("recovery pause id required")
    if not callable(kafka):
        raise ValueError("Kafka offset reader required")
    _idle(connection, "capture_recovery_cut")
    with connection.transaction():
        _begin_snapshot(connection)
        validate_transition(connection, lease)
        pause = connection.execute(
            "SELECT recovery_epoch, released_at IS NULL FROM recovery_pauses WHERE pause_id=%s",
            (pause_id,)).fetchone()
        if pause is None or not pause[1]:
            _refuse("stale: the recovery pause is not active; pause before capturing a cut")
        if connection.execute("SELECT 1 FROM recovery_cuts WHERE pause_id=%s",
                              (pause_id,)).fetchone() is not None:
            _refuse("recovery cut already captured for this pause; never combine cuts")
        topic, incarnation, replay_epoch = _delivery(connection)
        before = normalize_kafka_sample(kafka(topic), topic=topic)
        snapshot = _snapshot(connection)
        outbox = _outbox(connection)
        after = normalize_kafka_sample(kafka(topic), topic=topic)
        if before != after:
            _refuse("Kafka offsets moved during the cut (a producer is still in flight); "
                    "nothing was recorded")
        end_offsets = {p: o["end"] for p, o in after["partitions"].items()}
        manifest = {
            "version": MANIFEST_VERSION, "pause_id": pause_id, "recovery_epoch": int(pause[0]),
            "fence": {"owner": lease.owner, "fence": lease.fence},
            "snapshot": snapshot, "outbox": outbox,
            "kafka": {"cluster_id": after["cluster_id"], "topic": topic,
                      "topic_id": after["topic_id"], "topic_incarnation": incarnation,
                      "replay_epoch": replay_epoch, "end_offsets": end_offsets,
                      "log_start_offsets": {p: o["start"]
                                            for p, o in after["partitions"].items()}},
        }
        digest = manifest_digest(manifest)
        cut_id = str(uuid.uuid4())
        connection.execute(
            "INSERT INTO recovery_cuts(cut_id,pause_id,recovery_epoch,lease_owner,lease_fence,"
            "canonical_generation,topic,topic_incarnation,replay_epoch,kafka_cluster_id,"
            "kafka_topic_id,end_offsets,outbox_members,manifest,manifest_digest) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s::jsonb,%s)",
            (cut_id, pause_id, manifest["recovery_epoch"], lease.owner, lease.fence,
             snapshot["canonical_generation"], topic, incarnation, replay_epoch,
             after["cluster_id"], after["topic_id"], _canonical(end_offsets),
             _canonical([member[:3] for member in outbox["members"]]),
             _canonical(manifest), digest))
        validate_transition(connection, lease)
    return RecoveryCut(cut_id, pause_id, manifest["recovery_epoch"], manifest, digest)


_CUT_COLUMNS = ("pause_id", "recovery_epoch", "lease_owner", "lease_fence",
                "canonical_generation", "topic", "topic_incarnation", "replay_epoch",
                "kafka_cluster_id", "kafka_topic_id", "end_offsets", "manifest",
                "manifest_digest")


def _cut_id(cut_id) -> str:
    try:
        if type(cut_id) is not str or str(uuid.UUID(cut_id)) != cut_id:
            raise ValueError
    except ValueError:
        raise ValueError("recovery cut id must be a canonical UUID string") from None
    return cut_id


def _read_cut(connection, cut_id: str) -> dict:
    row = connection.execute(
        "SELECT " + ",".join(_CUT_COLUMNS) + " FROM recovery_cuts WHERE cut_id=%s",
        (cut_id,)).fetchone()
    if row is None:
        _refuse("incomplete: unknown recovery cut")
    return dict(zip(_CUT_COLUMNS, row))


def load_recovery_cut(connection, cut_id: str) -> RecoveryCut:
    """Read back a durable cut; refuse it if its manifest is partial or altered."""
    _cut_id(cut_id)
    _idle(connection, "load_recovery_cut")
    with connection.transaction():
        row = _read_cut(connection, cut_id)
    validate_manifest(row["manifest"], row["manifest_digest"])
    return RecoveryCut(cut_id, int(row["pause_id"]), int(row["recovery_epoch"]),
                       row["manifest"], row["manifest_digest"])


def _live_state(connection, row: dict, kafka: Callable[[str], Mapping]) -> dict:
    pause = connection.execute(
        "SELECT released_at IS NULL FROM recovery_pauses WHERE pause_id=%s",
        (row["pause_id"],)).fetchone()
    latest = connection.execute("SELECT max(recovery_epoch) FROM recovery_pauses").fetchone()
    delivery = connection.execute(
        "SELECT topic, topic_incarnation, replay_epoch FROM outbox_delivery_authority "
        "WHERE singleton").fetchone()
    return {"pause_active": bool(pause and pause[0]), "latest_epoch": latest[0],
            "snapshot": _snapshot(connection), "outbox": _outbox(connection),
            "delivery": tuple(delivery) if delivery else (),
            "kafka": normalize_kafka_sample(kafka(row["topic"]), topic=row["topic"])}


def _point(cut_id: str, row: dict, manifest: dict, digest: str) -> RecoveryPoint:
    k = manifest["kafka"]
    return RecoveryPoint(cut_id, int(row["recovery_epoch"]),
                         manifest["snapshot"]["canonical_generation"], k["topic"],
                         k["topic_incarnation"], k["topic_id"],
                         {int(p): e for p, e in k["end_offsets"].items()}, digest)


def claim_recovery_point(connection, cut_id: str,
                         kafka: Callable[[str], Mapping]) -> RecoveryPoint:
    """Name a recovery point only for a complete, current and matching durable cut."""
    _cut_id(cut_id)
    if not callable(kafka):
        raise ValueError("Kafka offset reader required")
    _idle(connection, "claim_recovery_point")
    with connection.transaction():
        _begin_snapshot(connection)
        row = _read_cut(connection, cut_id)
        manifest, digest = row["manifest"], row["manifest_digest"]
        live = _live_state(connection, row, kafka)
    reasons = recovery_point_reasons(row, manifest, digest, live)
    if reasons:
        raise RecoveryCutRejected("recovery cut refused: " + "; ".join(reasons), reasons)
    return _point(cut_id, row, manifest, digest)


@dataclass(frozen=True)
class RecoveryResumption:
    cut_id: str
    pause_id: int
    recovery_epoch: int
    pipeline_epoch: str
    lease: Lease
    topic: str
    start_offsets: dict
    resolutions: dict
    duplicates: tuple
    digest: str


def resume_refusals(expected: RecoveryPoint, presented, *, cut_fence: int, lease_fence: int,
                    pipeline_epoch_used: bool) -> tuple:
    """Why a validated cut may still not be resumed by this caller; empty means resumable."""
    if type(presented) is not RecoveryPoint:
        return ("incomplete: a claimed RecoveryPoint is required to resume",)
    reasons = [f"mismatched: presented recovery point {f.name} differs from the durable cut"
               for f in fields(RecoveryPoint)
               if getattr(presented, f.name) != getattr(expected, f.name)]
    if not lease_fence > cut_fence:
        reasons.append(f"stale: resume requires a handoff lease newer than the cut fence "
                       f"{cut_fence} (presented {lease_fence})")
    if pipeline_epoch_used:
        reasons.append("stale: the pipeline epoch already belongs to a consumer lineage; "
                       "resume needs a new pipeline epoch")
    return tuple(reasons)


def _record_value(value):
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    return json.loads(value)


def _record_key(key):
    if isinstance(key, (bytes, bytearray, memoryview)):
        try:
            return bytes(key).decode("utf-8")
        except UnicodeDecodeError:
            return None
    return key if type(key) is str else None


def classify_resume_records(manifest: dict, envelopes: Mapping, records) -> tuple:
    """Resolve every ambiguous cut member against the retained log.

    Returns ``(resolutions, duplicates, reasons)``: an ambiguous member whose exact persisted
    envelope is on the log is resolved to its first copy ``(partition, offset)`` and must not
    be resent; otherwise it is ``None`` and must be redelivered. At or above the cut's end
    offsets only copies of ambiguous members may appear; anything else refuses the resume.
    """
    ends = manifest["kafka"]["end_offsets"]
    ambiguous = sorted(m[0] for m in manifest["outbox"]["members"] if m[3] == "ambiguous")
    reasons = [f"incomplete: no persisted delivery envelope for ambiguous member {e}"
               for e in ambiguous if e not in envelopes]
    found, duplicates = {}, []
    for r in sorted(records, key=lambda item: (item["partition"], item["offset"])):
        partition, offset = r["partition"], r["offset"]
        if str(partition) not in ends:
            reasons.append(f"mismatched: record on partition {partition} outside the cut")
            continue
        post_cut = offset >= ends[str(partition)]
        key = _record_key(r.get("key"))
        if key not in ambiguous or key not in envelopes:
            if post_cut:
                reasons.append(f"mismatched: post-cut record at partition {partition} offset "
                               f"{offset} is not an ambiguous member of the cut")
            continue
        try:
            same = _record_value(r.get("value")) == envelopes[key]
        except (TypeError, ValueError, UnicodeDecodeError):
            same = False
        if not same:
            if post_cut:
                reasons.append(f"mismatched: post-cut record at partition {partition} offset "
                               f"{offset} differs from the persisted envelope of {key}")
            continue
        if key in found:
            duplicates.append((partition, offset))
        else:
            found[key] = (partition, offset)
    return ({e: found.get(e) for e in ambiguous}, tuple(duplicates), tuple(reasons))


def resumed_records(resumption: RecoveryResumption, records) -> list:
    """Records the resumed consumer applies: from the cut's end offsets, each event once.

    Below E everything is already part of the cut. An ambiguous send found on the log is
    applied only at its resolved copy (never, if that copy lies below E); any other event is
    applied at its first record at or above E, and later copies are dropped.
    """
    kept, seen = [], set()
    for r in sorted(records, key=lambda item: (item["partition"], item["offset"])):
        start = resumption.start_offsets.get(r["partition"])
        if start is None or r["offset"] < start:
            continue
        key = _record_key(r.get("key"))
        resolved = resumption.resolutions.get(key)
        if resolved is not None and (r["partition"], r["offset"]) != tuple(resolved):
            continue
        if key in seen:
            continue
        seen.add(key)
        kept.append(r)
    return kept


def spark_starting_offsets(resumption: RecoveryResumption) -> str:
    """The Spark Kafka ``startingOffsets`` JSON for the resumed lineage: exactly E."""
    return json.dumps({resumption.topic: {str(p): e for p, e in
                                          sorted(resumption.start_offsets.items())}},
                      separators=(",", ":"))


def resumed_consumer_config(config, resumption: RecoveryResumption):
    """The consumer config of the resumed lineage: its new pipeline epoch, starting exactly at E."""
    from dataclasses import replace

    from ton_sentinel.stream_cli import ConsumerConfig

    if type(config) is not ConsumerConfig:
        raise ValueError("a ConsumerConfig is required")
    if type(resumption) is not RecoveryResumption:
        raise ValueError("a RecoveryResumption is required")
    if config.topic != resumption.topic:
        raise ValueError("the consumer topic differs from the resumed cut topic")
    return replace(config, pipeline_epoch=resumption.pipeline_epoch,
                   starting_offsets=spark_starting_offsets(resumption))


def check_resumed_start(connection, config) -> None:
    """Refuse explicit consumer start offsets that are not a durable resumption's exact E."""
    from ton_sentinel.stream_cli import starting_offsets_by_partition

    wanted = starting_offsets_by_partition(config)
    if wanted is None:
        return
    row = connection.execute(
        "SELECT r.start_offsets, c.topic FROM recovery_resumptions r "
        "JOIN recovery_cuts c USING (cut_id) WHERE r.pipeline_epoch=%s",
        (config.pipeline_epoch,)).fetchone()
    connection.rollback()
    if row is None:
        raise ValueError("explicit starting offsets need a durable recovery resumption for "
                         "this pipeline epoch")
    stored, topic = row
    if topic != config.topic or {int(p): e for p, e in stored.items()} != wanted:
        raise ValueError("starting offsets differ from the durable recovery resumption")


def resume_from_recovery_cut(connection, lease, point, kafka: Callable[[str], Mapping],
                             records: Callable, *, pipeline_epoch: str,
                             max_records: int = 10_000) -> RecoveryResumption:
    """Resume ingestion from a durable cut only on an exact fenced match, or change nothing."""
    if type(lease) is not Lease:
        raise ValueError("recovery handoff lease required")
    if type(point) is not RecoveryPoint:
        raise ValueError("a claimed recovery point is required")
    if not callable(kafka):
        raise ValueError("Kafka offset reader required")
    if not callable(records):
        raise ValueError("Kafka record reader required")
    if type(pipeline_epoch) is not str or not pipeline_epoch.strip() or len(pipeline_epoch) > 256:
        raise ValueError("pipeline epoch must be nonempty text of at most 256 characters")
    if type(max_records) is not int or not 1 <= max_records <= 1_000_000:
        raise ValueError("max_records must be 1..1000000")
    _cut_id(point.cut_id)
    _idle(connection, "resume_from_recovery_cut")
    with connection.transaction():
        _begin_snapshot(connection)
        validate_transition(connection, lease)  # holds the lease row: no takeover
        row = _read_cut(connection, point.cut_id)
        manifest, digest = row["manifest"], row["manifest_digest"]
        live = _live_state(connection, row, kafka)
        reasons = recovery_point_reasons(row, manifest, digest, live)
        if not reasons:
            used = connection.execute(
                "SELECT EXISTS (SELECT 1 FROM stream_batches WHERE pipeline_epoch=%s) "
                "OR EXISTS (SELECT 1 FROM recovery_resumptions WHERE pipeline_epoch=%s)",
                (pipeline_epoch, pipeline_epoch)).fetchone()[0]
            reasons = resume_refusals(_point(point.cut_id, row, manifest, digest), point,
                                      cut_fence=int(row["lease_fence"]),
                                      lease_fence=lease.fence, pipeline_epoch_used=bool(used))
        if reasons:
            raise RecoveryCutRejected("recovery resume refused: " + "; ".join(reasons), reasons)
        ambiguous = [m[0] for m in manifest["outbox"]["members"] if m[3] == "ambiguous"]
        envelopes = dict(connection.execute(
            "SELECT event_id, delivery_envelope FROM outbox WHERE event_id = ANY(%s)",
            (ambiguous,)).fetchall()) if ambiguous else {}
        partitions = live["kafka"]["partitions"]
        start = {int(p): o["start"] for p, o in partitions.items()}
        end = {int(p): o["end"] for p, o in partitions.items()}
        if sum(end[p] - start[p] for p in end) > max_records:
            _refuse("incomplete: retained Kafka log exceeds max_records; nothing was resumed")
        found = list(records(row["topic"], start, end))
        if any(not start.get(r["partition"], 0) <= r["offset"] < end.get(r["partition"], -1)
               for r in found):
            _refuse("incomplete: Kafka record reader returned records outside the sampled log")
        after = normalize_kafka_sample(kafka(row["topic"]), topic=row["topic"])
        if after != live["kafka"]:
            _refuse("Kafka offsets moved during resume validation (a producer is still in "
                    "flight); nothing was resumed")
        resolutions, duplicates, reasons = classify_resume_records(manifest, envelopes, found)
        if reasons:
            raise RecoveryCutRejected("recovery resume refused: " + "; ".join(reasons), reasons)
        connection.execute(
            "INSERT INTO recovery_resumptions(cut_id,pause_id,recovery_epoch,lease_owner,"
            "lease_fence,pipeline_epoch,start_offsets,manifest_digest,resolutions) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb)",
            (point.cut_id, row["pause_id"], row["recovery_epoch"], lease.owner, lease.fence,
             pipeline_epoch, _canonical(manifest["kafka"]["end_offsets"]), digest,
             _canonical([[e, *(c if c is not None else (None, None))]
                         for e, c in resolutions.items()])))
        released = connection.execute(
            "UPDATE recovery_pauses SET released_at=clock_timestamp(), release_owner=%s, "
            "release_fence=%s WHERE pause_id=%s AND released_at IS NULL RETURNING pause_id",
            (lease.owner, lease.fence, row["pause_id"])).fetchone()
        if released is None:
            _refuse("stale: the recovery pause is not active")
        landed = sorted(e for e, c in resolutions.items() if c is not None)
        if landed:
            connection.execute(
                "UPDATE outbox SET publication_state='acknowledged', "
                "published_at=clock_timestamp(), acknowledged_at=clock_timestamp() "
                "WHERE event_id = ANY(%s) AND publication_state IN ('attempted','ambiguous')",
                (landed,))
        validate_transition(connection, lease)
    return RecoveryResumption(point.cut_id, int(row["pause_id"]), int(row["recovery_epoch"]),
                              pipeline_epoch, lease, row["topic"], point.end_offsets,
                              resolutions, duplicates, digest)


class KafkaOffsetReader:
    """Read cluster id, topic id and per-partition [log start, exclusive end) from a broker."""
    def __init__(self, bootstrap_servers, *, client_id: str = "ton-sentinel-recovery",
                 timeout_ms: int = 10_000):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.timeout_ms = timeout_ms

    def __call__(self, topic: str) -> dict:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient

        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers,
                                 client_id=self.client_id,
                                 request_timeout_ms=self.timeout_ms)
        try:
            cluster = admin.describe_cluster()["cluster_id"]
            described = admin.describe_topics([topic])
        finally:
            admin.close()
        if len(described) != 1 or described[0].get("error_code") != 0:
            _refuse(f"mismatched: kafka topic {topic!r} is not available")
        numbers = sorted(p["partition_index"] for p in described[0]["partitions"])
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                 client_id=self.client_id, enable_auto_commit=False,
                                 request_timeout_ms=max(self.timeout_ms, 11_000))
        try:
            parts = [TopicPartition(topic, n) for n in numbers]
            start = consumer.beginning_offsets(parts)
            end = consumer.end_offsets(parts)
        finally:
            consumer.close()
        return {"cluster_id": cluster, "topic": topic, "topic_id": described[0]["topic_id"],
                "partitions": {tp.partition: {"start": start[tp], "end": end[tp]}
                               for tp in parts}}


class KafkaRecordReader:
    """Read every record of a topic in ``[start, end)`` per partition from a broker."""
    def __init__(self, bootstrap_servers, *, client_id: str = "ton-sentinel-recovery",
                 timeout_ms: int = 20_000):
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.timeout_ms = timeout_ms

    def __call__(self, topic: str, start: Mapping, end: Mapping) -> list:
        import time

        from kafka import KafkaConsumer, TopicPartition

        parts = [TopicPartition(topic, p) for p in sorted(end) if end[p] > start.get(p, 0)]
        if not parts:
            return []
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers,
                                 client_id=self.client_id, enable_auto_commit=False,
                                 request_timeout_ms=max(self.timeout_ms, 11_000))
        try:
            consumer.assign(parts)
            for tp in parts:
                consumer.seek(tp, start.get(tp.partition, 0))
            deadline = time.monotonic() + self.timeout_ms / 1000
            out = []
            while any(consumer.position(tp) < end[tp.partition] for tp in parts):
                if time.monotonic() > deadline:
                    _refuse("incomplete: the retained Kafka log could not be read in time")
                for tp, batch in consumer.poll(timeout_ms=500).items():
                    out.extend({"partition": tp.partition, "offset": r.offset, "key": r.key,
                                "value": r.value}
                               for r in batch if r.offset < end[tp.partition])
            return out
        finally:
            consumer.close()
