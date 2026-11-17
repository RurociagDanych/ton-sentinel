"""Bounded canonical microbatch adapter for broker-triggered Spark batches.

Kafka records are treated only as triggers. Their identity and action must match
an accepted canonical ledger action; feature computation always reads the full
accepted ledger population for every touched five-minute window.
"""
from __future__ import annotations

import json
import hashlib

from ton_sentinel.contracts import Action, SyntheticInputRefused
from ton_sentinel.db.fencing import validate_transition
from ton_sentinel.db.serving import WindowKey, replace_features
from ton_sentinel.db.work import compute_replacement, repair_windows
from ton_sentinel.envelope import broker_envelope_version


_DEFAULT_MAX_ROWS = 500
_DEFAULT_MAX_WINDOWS = 24
_DEFAULT_MAX_ACTIONS = 50_000


def _strict_json(value: bytes | bytearray | memoryview | str) -> dict:
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8", errors="strict")
    if type(value) is not str:
        raise ValueError("broker envelope value must be UTF-8 JSON")

    def unique(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError("duplicate broker envelope JSON field")
            result[key] = item
        return result

    parsed = json.loads(value, object_pairs_hook=unique,
                        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("invalid JSON constant")))
    if type(parsed) is not dict:
        raise ValueError("broker envelope must be a JSON object")
    return parsed


def _key_text(value) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        try:
            value = bytes(value).decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise ValueError("broker key must be UTF-8 event_id") from exc
    if type(value) is not str or not value:
        raise ValueError("broker key must be a nonempty event_id")
    return value


def _cap(value, name: str, maximum: int) -> None:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be 1..{maximum}")


def process_canonical_microbatch(connection, lease, spark, batch_df, *,
                                 topic_incarnation: str, replay_epoch: int,
                                 batch_id: int | None = None,
                                 environment_id: str | None = None,
                                 query_id: str | None = None,
                                 pipeline_epoch: str | None = None,
                                 topic: str | None = None,
                                 max_rows: int = _DEFAULT_MAX_ROWS,
                                 max_windows: int = _DEFAULT_MAX_WINDOWS,
                                 max_actions: int = _DEFAULT_MAX_ACTIONS) -> int:
    """Verify a bounded broker batch and replace complete accepted-action windows.

    ``batch_df`` must have Kafka-compatible ``key`` and ``value`` columns. The
    caller owns an idle PostgreSQL connection and a current transition lease.
    Triggers are validated in a short fenced transaction; the touched windows are
    then repaired through ``freeze_work`` -> ``compute_replacement`` ->
    ``promote`` (Spark runs outside the transition slot). A stale promotion is
    retried from fresh inputs; a lost lease raises ``LeaseRejected``. The batch
    audit row commits only with the promotion. Caps are checked before any
    serving mutation.
    """
    _cap(max_rows, "max_rows", 10_000)
    _cap(max_windows, "max_windows", 1_000)
    _cap(max_actions, "max_actions", 1_000_000)
    if type(topic_incarnation) is not str or not topic_incarnation:
        raise ValueError("topic_incarnation must be nonempty text")
    if type(replay_epoch) is not int or replay_epoch < 0:
        raise ValueError("replay_epoch must be a nonnegative integer")
    audit_values = (batch_id, environment_id, query_id, pipeline_epoch, topic)
    audited = any(value is not None for value in audit_values)
    if audited and (type(batch_id) is not int or batch_id < 0 or any(
            type(value) is not str or not value.strip()
            for value in (environment_id, query_id, pipeline_epoch, topic))):
        raise ValueError("batch audit requires explicit batch and query identity")
    required = {"key", "value", "topic", "partition", "offset"} if audited else {"key", "value"}
    if not required.issubset(batch_df.columns):
        raise ValueError("batch_df requires key and value columns")
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("canonical microbatch requires an idle PostgreSQL connection")

    # Trigger collection is deliberately bounded before decoding or database work.
    columns = ["key", "value"]
    if audited:
        columns = ["topic", "partition", "offset", "key", "value"]
    records = batch_df.select(*columns).limit(max_rows + 1).collect()
    if len(records) > max_rows:
        raise ValueError("broker batch exceeds max_rows")

    envelopes = []
    for row in records:
        event_id = _key_text(row["key"])
        envelope = _strict_json(row["value"])
        # Version 1 (pre-generation) or version 2 (accepted generation); the
        # exact persisted envelope is compared again inside the transaction.
        broker_envelope_version(envelope)
        if (envelope["event_id"] != event_id
                or envelope["topic_incarnation"] != topic_incarnation
                or type(envelope["replay_epoch"]) is not int
                or envelope["replay_epoch"] != replay_epoch
                or type(envelope["action"]) is not dict):
            raise ValueError("broker key, delivery incarnation/epoch, or envelope action is invalid")
        try:
            Action.from_json(json.dumps(envelope["action"], separators=(",", ":")))
        except SyntheticInputRefused:
            raise                       # Explicit, not "malformed"
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("broker action payload is malformed") from exc
        envelopes.append((event_id, envelope["action"], envelope))

    if not envelopes and not audited:
        return 0

    offsets = None
    record_digest = None
    if audited:
        encoded_records = []
        offsets = []
        for row in records:
            values = (row["topic"], row["partition"], row["offset"], row["key"], row["value"])
            if (type(values[0]) is not str or values[0] != topic or type(values[1]) is not int
                    or type(values[2]) is not int or values[1] < 0 or values[2] < 0):
                raise ValueError("Kafka batch offset metadata is invalid")
            normalized = [values[0], values[1], values[2],
                          bytes(values[3]).hex(), bytes(values[4]).hex()]
            encoded_records.append(normalized)
            offsets.append({"topic": values[0], "partition": values[1], "offset": values[2]})
        encoded_records.sort(key=lambda item: (item[0], item[1], item[2]))
        record_digest = hashlib.sha256(json.dumps(
            encoded_records, separators=(",", ":"), ensure_ascii=True,
        ).encode("ascii")).hexdigest()
        offsets.sort(key=lambda item: (item["topic"], item["partition"], item["offset"]))

    identity = (environment_id, query_id, pipeline_epoch, batch_id)
    expected_audit = (topic, topic_incarnation, replay_epoch, offsets, record_digest, len(records))

    def record_audit(conn, *, write: bool = True) -> None:
        """Persist or verify the batch audit row; conflicting identity reuse is refused."""
        audit = conn.execute(
            "SELECT topic,topic_incarnation,replay_epoch,offset_metadata,record_digest,record_count "
            "FROM stream_batches WHERE environment_id=%s AND query_id=%s AND pipeline_epoch=%s "
            "AND batch_id=%s FOR UPDATE", identity,
        ).fetchone()
        if audit is None:
            if write:
                conn.execute(
                    "INSERT INTO stream_batches (environment_id,query_id,pipeline_epoch,batch_id,topic,"
                    "topic_incarnation,replay_epoch,offset_metadata,record_digest,record_count) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",
                    (*identity, topic, topic_incarnation, replay_epoch,
                     json.dumps(offsets, separators=(",", ":")), record_digest, len(records)),
                )
        else:
            stored = (audit[0], audit[1], audit[2], audit[3], audit[4], audit[5])
            if stored != expected_audit:
                raise ValueError("conflicting stream batch audit identity reuse")

    # Short fenced transaction: validate the lease, delivery identity, audit identity and
    # every trigger against the committed canonical/outbox rows. No Spark work runs here.
    with connection.transaction():
        validate_transition(connection, lease)
        config = connection.execute(
            "SELECT topic_incarnation,replay_epoch FROM outbox_delivery_config WHERE singleton=true"
        ).fetchone()
        if config is None or tuple(config) != (topic_incarnation, replay_epoch):
            raise ValueError("broker delivery incarnation/epoch is not configured")

        # Audited empty batches validate the lease and delivery configuration,
        # and persist their identity, but have no serving window to recompute.
        if not envelopes:
            record_audit(connection)
            validate_transition(connection, lease)
            return 0
        if audited:
            record_audit(connection, write=False)     # refuse conflicting reuse before Spark

        touched: set[WindowKey] = set()
        for event_id, action, envelope in envelopes:
            stored = connection.execute(
                "SELECT c.action,o.payload,o.delivery_envelope FROM canonical_actions c "
                "JOIN outbox o USING(event_id) "
                "WHERE c.event_id=%s AND EXISTS ("
                "SELECT 1 FROM scan_membership m JOIN source_ranges r USING(range_id) "
                "WHERE m.event_id=c.event_id AND r.status='accepted')",
                (event_id,),
            ).fetchone()
            if stored is None:
                raise ValueError("broker trigger is not an accepted canonical ledger action")
            canonical, outbox_action, persisted_envelope = stored
            if canonical != outbox_action or action != canonical:
                raise ValueError("broker action does not match committed canonical/outbox payload")
            # Exact persisted envelope: version, accepted generation, incarnation
            # and epoch must all be the acceptance-bound values (no downgrade).
            if json.dumps(envelope, sort_keys=True) != json.dumps(persisted_envelope, sort_keys=True):
                raise ValueError(
                    "broker envelope or accepted generation does not match persisted intent")
            try:
                parsed_action = Action.from_json(json.dumps(canonical, separators=(",", ":")))
            except SyntheticInputRefused:
                raise                       # Explicit, not "malformed"
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError("canonical action payload is malformed") from exc
            if parsed_action.identity.event_id != event_id:
                raise ValueError("broker event_id does not match canonical action identity")
            start = (parsed_action.action_end // 300) * 300
            touched.add(WindowKey(parsed_action.identity.network, start, start + 300))

        if len(touched) > max_windows:
            raise ValueError("broker batch exceeds max_windows")

    # Path: freeze the current eligible canonical inputs of every touched window
    # under the lease, compute the complete replacement outside the slot, and promote it
    # only if lease, dependency epoch and every pin are unchanged. The batch audit row is
    # staged inside that promotion transaction, so audit and serving commit together.
    # Module globals are looked up at call time so tests can inject real failures.
    result = repair_windows(
        connection, lease, spark, sorted(touched), max_actions=max_actions,
        stage=record_audit if audited else None,
        writer=lambda *args, **kwargs: replace_features(*args, **kwargs),
        compute=lambda *args: compute_replacement(*args),
    )
    return len(result.rows)
