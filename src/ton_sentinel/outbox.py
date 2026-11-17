"""SQL-fenced publication of committed ledger outbox rows to Kafka.

The ledger's canonical mutation and outbox insert are already one PostgreSQL
transaction. This module only publishes those committed intents; Kafka delivery
and the PostgreSQL acknowledgement marker cannot be atomic, so crash recovery
can replay a message and consumers must deduplicate by event_id. Lease fencing
protects SQL markers only; it cannot stop an old producer from sending to Kafka.
"""

import json
from collections.abc import Mapping

from ton_sentinel.db.fencing import validate_transition
from ton_sentinel.envelope import (ENVELOPE_VERSION, build_delivery_envelope,  # noqa: F401
                                   validate_delivery_envelope)


class PublicationError(RuntimeError):
    """A Kafka send did not receive a successful broker acknowledgement."""


def _require_idle(connection) -> None:
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("an idle PostgreSQL connection is required")


def _require_all_acks(producer) -> None:
    """Require kafka-python's normalized producer setting for all replicas."""
    config = getattr(producer, "config", None)
    if not isinstance(config, Mapping) or type(config.get("acks")) is not int or config["acks"] != -1:
        raise ValueError("producer must be configured with acks='all'")


def configure_delivery(connection, *, topic: str, topic_incarnation: str,
                       replay_epoch: int = 0) -> None:
    """Bind this outbox to one explicitly named Kafka topic incarnation."""
    if (type(topic) is not str or not topic or len(topic) > 249
            or type(topic_incarnation) is not str or not topic_incarnation
            or len(topic_incarnation) > 256 or type(replay_epoch) is not int
            or replay_epoch < 0):
        raise ValueError("invalid topic, topic incarnation, or replay epoch")
    desired = (topic, topic_incarnation, replay_epoch)
    _require_idle(connection)
    with connection.transaction():
        connection.execute(
            "INSERT INTO outbox_delivery_authority(singleton,topic,topic_incarnation,replay_epoch) "
            "VALUES (true,%s,%s,%s) ON CONFLICT (singleton) DO NOTHING", desired,
        )
        authority = connection.execute(
            "SELECT topic,topic_incarnation,replay_epoch FROM outbox_delivery_authority "
            "WHERE singleton=true FOR UPDATE"
        ).fetchone()
        if authority is None or tuple(authority) != desired:
            raise ValueError(
                "delivery configuration is immutable; explicit replay tooling required"
            )
        row = connection.execute(
            "SELECT topic,topic_incarnation,replay_epoch FROM outbox_delivery_config "
            "WHERE singleton=true FOR UPDATE"
        ).fetchone()
        if row is None:
            connection.execute(
                "INSERT INTO outbox_delivery_config(singleton,topic,topic_incarnation,replay_epoch) "
                "VALUES (true,%s,%s,%s)", desired,
            )
        elif tuple(row) != desired:
            raise ValueError(
                "delivery configuration is immutable; explicit replay tooling required"
            )


def initialize_publication_origin(connection, lease, origin: int) -> None:
    """Persist an explicit origin, reconciling legacy frontier state safely.

    For an existing frontier without an origin, the caller supplies the
    historical origin. It is recorded only when acknowledged accepted ranges
    prove uninterrupted coverage from that origin through the stored frontier.
    """
    if type(origin) is not int or origin < 0:
        raise ValueError("publication origin must be a nonnegative integer")
    _require_idle(connection)
    with connection.transaction():
        validate_transition(connection, lease)
        frontier_row = connection.execute(
            "SELECT end_exclusive FROM outbox_publication_frontier "
            "WHERE singleton=true FOR UPDATE"
        ).fetchone()
        row = connection.execute(
            "SELECT origin_inclusive FROM outbox_publication_origin "
            "WHERE singleton=true FOR UPDATE"
        ).fetchone()
        if row is not None:
            if row[0] != origin:
                raise ValueError("publication origin is immutable")
        elif frontier_row is None:
            connection.execute(
                "INSERT INTO outbox_publication_origin(singleton,origin_inclusive) "
                "VALUES (true,%s)", (origin,),
            )
        else:
            frontier = frontier_row[0]
            if origin > frontier:
                raise ValueError("publication origin is beyond the existing frontier")
            cursor = origin
            while cursor < frontier:
                coverage = connection.execute(
                    "SELECT r.end_exclusive FROM source_ranges r "
                    "WHERE r.status='accepted' AND r.start_inclusive<=%s "
                    "AND r.end_exclusive>%s AND NOT EXISTS ("
                    "SELECT 1 FROM scan_membership m JOIN outbox o USING(event_id) "
                    "WHERE m.range_id=r.range_id AND (o.publication_state<>'acknowledged' "
                    "OR o.published_at IS NULL OR o.acknowledged_at IS NULL)) "
                    "ORDER BY r.end_exclusive DESC, r.start_inclusive, r.range_id "
                    "LIMIT 1 FOR UPDATE OF r", (cursor, cursor),
                ).fetchone()
                if coverage is None:
                    raise ValueError(
                        "cannot reconcile publication origin: acknowledged accepted "
                        f"coverage has a gap at {cursor} before frontier {frontier}"
                    )
                cursor = coverage[0]
            connection.execute(
                "INSERT INTO outbox_publication_origin "
                "(singleton,origin_inclusive,reconciled_frontier_end) "
                "VALUES (true,%s,%s)", (origin, frontier),
            )
        validate_transition(connection, lease)


def publish_pending(connection, producer, lease, *, limit: int = 100,
                    ack_timeout: float = 30.0,
                    max_retries: int = 2) -> tuple[str, ...]:
    """Send committed intents in ledger order; mark only broker-acked records.

    SQL checks fence acknowledgement markers, not broker sends. A producer
    losing its lease during a send may still deliver, so takeover duplicates are
    permitted by this at-least-once protocol. Producer must use acks='all'.
    max_retries is an additional-send budget per event per invocation (0..10);
    later invocations may retry durable attempted/ambiguous intents.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("limit must be 1..1000")
    if (type(ack_timeout) not in (int, float) or ack_timeout <= 0
            or ack_timeout != ack_timeout or ack_timeout == float("inf")):
        raise ValueError("ack_timeout must be positive")
    if type(max_retries) is not int or not 0 <= max_retries <= 10:
        raise ValueError("max_retries must be 0..10")
    _require_all_acks(producer)
    _require_idle(connection)
    config = connection.execute(
        "SELECT topic, topic_incarnation, replay_epoch "
        "FROM outbox_delivery_config WHERE singleton=true"
    ).fetchone()
    if config is None:
        raise ValueError("configure_delivery must be called before publication")
    topic, incarnation, replay_epoch = config
    authority = connection.execute(
        "SELECT topic,topic_incarnation,replay_epoch "
        "FROM outbox_delivery_authority WHERE singleton=true"
    ).fetchone()
    if authority is None or tuple(authority) != tuple(config):
        raise ValueError("configured delivery differs from immutable delivery authority")
    rows = connection.execute(
        "SELECT event_id,payload,delivery_topic,delivery_topic_incarnation,"
        "replay_epoch,delivery_envelope,accepted_generation FROM outbox "
        "WHERE publication_state<>'acknowledged' "
        "ORDER BY created_at, event_id COLLATE \"C\" LIMIT %s", (limit,),
    ).fetchall()
    connection.commit()
    sent = []
    # Validate the entire selected batch before any broker side effect. Legacy
    # intents cannot acquire a fabricated acceptance contract at send time; a
    # pre-generation intent keeps its version-1 envelope, never a new generation.
    for (event_id, payload, bound_topic, bound_incarnation, bound_epoch, envelope,
         generation) in rows:
        if envelope is None or bound_topic is None:
            raise ValueError("outbox intent has no acceptance-bound delivery contract")
        if (bound_topic, bound_incarnation, bound_epoch) != (topic, incarnation, replay_epoch):
            raise ValueError("accepted delivery contract differs from configured delivery")
        validate_delivery_envelope(
            envelope, event_id=event_id, topic_incarnation=bound_incarnation,
            replay_epoch=bound_epoch, action=payload, accepted_generation=generation)
    for event_id, payload, bound_topic, bound_incarnation, bound_epoch, envelope, _ in rows:
        value = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
        for attempt in range(max_retries + 1):
            _require_idle(connection)
            with connection.transaction():
                validate_transition(connection, lease)
                connection.execute(
                    "UPDATE outbox SET publication_state='attempted', "
                    "publication_attempts=publication_attempts+1, "
                    "last_attempt_at=clock_timestamp() "
                    "WHERE event_id=%s AND publication_state<>'acknowledged'",
                    (event_id,),
                )
                validate_transition(connection, lease)
            try:
                producer.send(topic, key=event_id.encode("utf-8"), value=value).get(
                    timeout=ack_timeout)
            except Exception as exc:
                # Keep the intent durable and retry only a bounded number of
                # times per call. On process death this remains `attempted`;
                # the next holder treats it as unresolved and retries it.
                _require_idle(connection)
                with connection.transaction():
                    validate_transition(connection, lease)
                    connection.execute(
                        "UPDATE outbox SET publication_state='ambiguous' "
                        "WHERE event_id=%s AND publication_state='attempted'",
                        (event_id,),
                    )
                    validate_transition(connection, lease)
                if attempt == max_retries:
                    raise PublicationError(
                        f"Kafka did not acknowledge {event_id} after "
                        f"{attempt + 1} attempt(s)"
                    ) from exc
                continue
            _require_idle(connection)
            with connection.transaction():
                validate_transition(connection, lease)
                connection.execute(
                    "UPDATE outbox SET publication_state='acknowledged', "
                    "published_at=clock_timestamp(), acknowledged_at=clock_timestamp() "
                    "WHERE event_id=%s AND publication_state<>'acknowledged'",
                    (event_id,),
                )
                validate_transition(connection, lease)
            sent.append(event_id)
            break
    return tuple(sent)


def advance_publication_frontier(connection, lease, *, limit: int = 100) -> int | None:
    """Advance through acknowledged accepted ranges covering the frontier.

    Empty accepted ranges advance as well. An explicit immutable origin is
    required so later accepted ranges cannot leap over an unresolved gap. The
    returned value is the last complete exclusive range end.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("limit must be 1..1000")
    _require_idle(connection)
    with connection.transaction():
        validate_transition(connection, lease)
        origin = connection.execute(
            "SELECT origin_inclusive FROM outbox_publication_origin WHERE singleton=true"
        ).fetchone()
        if origin is None:
            raise ValueError("initialize_publication_origin must be called before advancing")
        current = connection.execute(
            "SELECT end_exclusive FROM outbox_publication_frontier "
            "WHERE singleton=true FOR UPDATE"
        ).fetchone()
        frontier = origin[0] if current is None else current[0]
        advanced = 0
        while advanced < limit:
            row = connection.execute(
                "SELECT r.range_id, r.end_exclusive FROM source_ranges r "
                "WHERE r.status='accepted' AND r.start_inclusive<=%s "
                "AND r.end_exclusive>%s AND NOT EXISTS ("
                "SELECT 1 FROM scan_membership m JOIN outbox o USING(event_id) "
                "WHERE m.range_id=r.range_id AND (o.publication_state<>'acknowledged' "
                "OR o.published_at IS NULL OR o.acknowledged_at IS NULL)) "
                "ORDER BY r.end_exclusive DESC, r.start_inclusive, r.range_id "
                "LIMIT 1 FOR UPDATE OF r", (frontier, frontier),
            ).fetchone()
            if row is None:
                break
            frontier = row[1]
            advanced += 1
        if current is None and advanced == 0:
            return None
        connection.execute(
            "INSERT INTO outbox_publication_frontier(singleton,end_exclusive) "
            "VALUES (true,%s) ON CONFLICT(singleton) DO UPDATE "
            "SET end_exclusive=EXCLUDED.end_exclusive,updated_at=clock_timestamp()",
            (frontier,),
        )
        validate_transition(connection, lease)
        return frontier
