"""Transactional canonical promotion and durable outbox writes."""
import json
from collections.abc import Callable
from typing import TYPE_CHECKING

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.checkpoint import validate_checkpoint
from ton_sentinel.db.fencing import LeaseRejected, acquire_transition, validate_transition
from ton_sentinel.db.migrate import migrate
from ton_sentinel.envelope import build_delivery_envelope
from ton_sentinel.normalize import normalize, semantic_digest
if TYPE_CHECKING:
    from ton_sentinel.source.scan import WalkEvidence


class IngestionPaused(RuntimeError):
    """Maintenance (storage low water) or a recovery cut paused ingestion; nothing was accepted."""
    pass


def refuse_if_paused(connection) -> None:
    recovery = connection.execute(
        "SELECT recovery_epoch, lease_owner FROM recovery_pauses WHERE released_at IS NULL"
    ).fetchone()
    if recovery is not None:
        raise IngestionPaused(
            f"ingestion paused for a recovery cut (recovery epoch {recovery[0]}, "
            f"owner {recovery[1]})")
    paused = connection.execute(
        "SELECT reason, used_bytes, budget_bytes, min_free_percent FROM ingestion_pauses "
        "WHERE released_at IS NULL").fetchone()
    if paused is not None:
        raise IngestionPaused(
            f"ingestion paused by maintenance: {paused[0]} ({paused[1]} of {paused[2]} "
            f"budget bytes used; at least {paused[3]}% must stay free)")


def accept_scan(connection, lease, evidence: "WalkEvidence", *,
                acceptance_check: Callable | None = None) -> tuple[str, ...]:
    from ton_sentinel.source.scan import WalkEvidence

    if type(evidence) is not WalkEvidence:
        raise ValueError("persisted WalkEvidence required")
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("accept_scan requires an idle connection")
    promoted = []
    with connection.transaction():
        validate_transition(connection, lease)
        if not evidence.complete:
            return ()
        refuse_if_paused(connection)
        scan = connection.execute(
            "SELECT status,start_inclusive,end_exclusive,epoch,corroborated FROM source_ranges "
            "WHERE range_id=%s FOR UPDATE",
            (evidence.range_id,),
        ).fetchone()
        if scan is None:
            raise ValueError("unknown scan range")
        if scan[0] == "accepted":
            return ()
        if scan[0] != "complete":
            return ()
        validate_checkpoint(connection, evidence.range_id, require_complete=True)
        selected = connection.execute(
            "SELECT group_id FROM corroboration_groups WHERE accepted_range_id=%s "
            "AND status IN ('corroborated','accepted') FOR UPDATE",
            (evidence.range_id,),
        ).fetchone()
        if selected is None or not scan[4]:
            return ()
        from ton_sentinel.source.scan import validate_corroboration_proof
        if not validate_corroboration_proof(
                connection, str(selected[0]), selected_range_id=evidence.range_id):
            return ()
        epoch_json = json.dumps(scan[3], ensure_ascii=True, separators=(",", ":"))
        epoch = SourceEpoch.from_json(epoch_json)
        active = connection.execute(
            "SELECT epoch FROM active_source_epoch WHERE singleton=true FOR UPDATE"
        ).fetchone()
        if active is None:
            raise RuntimeError("active source epoch state is not initialized")
        epoch_value = json.loads(epoch.to_json())
        if active[0] is None:
            connection.execute(
                "UPDATE active_source_epoch SET epoch=%s::jsonb WHERE singleton=true",
                (epoch.to_json(),),
            )
        elif active[0] != epoch_value:
            raise ValueError("explicit audited source epoch transition required")
        observations = connection.execute(
            "SELECT payload FROM action_observations WHERE range_id=%s ORDER BY ordinal",
            (evidence.range_id,),
        ).fetchall()
        # Match configure_delivery's lock order to avoid a cross-table deadlock.
        authority = connection.execute(
            "SELECT topic,topic_incarnation,replay_epoch FROM outbox_delivery_authority "
            "WHERE singleton=true FOR SHARE"
        ).fetchone()
        delivery = connection.execute(
            "SELECT topic,topic_incarnation,replay_epoch FROM outbox_delivery_config "
            "WHERE singleton=true FOR SHARE"
        ).fetchone()
        if delivery is not None and (
                authority is None or tuple(delivery) != tuple(authority)):
            raise ValueError(
                "configured delivery differs from immutable delivery authority"
            )
        if observations and authority is not None and delivery is None:
            raise ValueError(
                "delivery must be configured; missing from durable accepted delivery authority"
            )
        if observations and delivery is None:
            raise ValueError("delivery must be configured before canonical acceptance")
        if observations:
            assert delivery is not None
        for (payload,) in observations:
            action = normalize(payload, epoch)
            if not scan[1] <= action.action_end < scan[2]:
                raise ValueError("action_end outside immutable scan range")
            event_id = action.identity.event_id
            digest = semantic_digest(action)
            # Look up before inserting: the database allocates the canonical
            # generation in a BEFORE INSERT trigger, so an ON CONFLICT no-op
            # would still consume one. The fenced lease serializes acceptance.
            prior = connection.execute(
                "SELECT semantic_digest FROM canonical_actions WHERE event_id=%s",
                (event_id,),
            ).fetchone()
            if prior is None:
                # An expired or guarded identity is never new; it is
                # quarantined (backfill only) instead of failing the whole acceptance.
                from ton_sentinel.maintenance import late_delivery_reasons

                window = action.action_end // 300 * 300
                reasons, payload_from, guard_before = late_delivery_reasons(
                    connection, event_id, action.identity.network, window)
                if reasons:
                    connection.execute(
                        "INSERT INTO delivery_quarantine(range_id,event_id,network,"
                        "window_start,reasons,semantic_digest,proposed_action,payload_from,"
                        "guard_retired_before,lease_owner,lease_fence) VALUES "
                        "(%s,%s,%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s,%s,%s) "
                        "ON CONFLICT (range_id,event_id) DO NOTHING",
                        (evidence.range_id, event_id, action.identity.network, window,
                         json.dumps(list(reasons)), digest, action.to_json(), payload_from,
                         guard_before, lease.owner, lease.fence),
                    )
                    continue
                generation = connection.execute(
                    "INSERT INTO canonical_actions(event_id,semantic_digest,action) "
                    "VALUES (%s,%s,%s::jsonb) RETURNING accepted_generation",
                    (event_id, digest, action.to_json()),
                ).fetchone()[0]
                envelope = json.dumps(build_delivery_envelope(
                    event_id=event_id, topic_incarnation=delivery[1],
                    replay_epoch=delivery[2], action=json.loads(action.to_json()),
                    accepted_generation=generation,
                ), sort_keys=True, separators=(",", ":"))
                connection.execute(
                    "INSERT INTO outbox(event_id,payload,delivery_topic,"
                    "delivery_topic_incarnation,replay_epoch,accepted_generation,"
                    "delivery_envelope) VALUES (%s,%s::jsonb,%s,%s,%s,%s,%s::jsonb)",
                    (event_id, action.to_json(),
                     delivery[0], delivery[1], delivery[2], generation, envelope),
                )
                promoted.append(event_id)
            elif prior[0] != digest:
                raise ValueError("canonical identity replay has conflicting semantics")
            connection.execute(
                "INSERT INTO scan_membership(range_id,event_id) VALUES (%s,%s) "
                "ON CONFLICT DO NOTHING", (evidence.range_id, event_id),
            )
        if acceptance_check is not None:
            acceptance_check(connection)
        connection.execute(
            "UPDATE ingest_state SET cursor=GREATEST(cursor,%s) WHERE singleton=true",
            (scan[2],),
        )
        connection.execute("UPDATE source_ranges SET status='accepted' WHERE range_id=%s",
                           (evidence.range_id,))
        connection.execute(
            "UPDATE corroboration_groups SET status='accepted' WHERE group_id=%s "
            "AND status='corroborated' AND accepted_range_id=%s",
            (selected[0], evidence.range_id),
        )
        validate_transition(connection, lease)
    return tuple(promoted)


def accept_corroborated_range(connection, lease, group_id: str, *,
                              acceptance_check: Callable | None = None) -> bool:
    """Promote only the persisted walk selected by consecutive full-walk evidence."""
    if type(group_id) is not str or connection.info.transaction_status.name != "IDLE":
        raise ValueError("corroboration ID and idle connection required")
    with connection.transaction():
        row = connection.execute(
            "SELECT g.status,g.accepted_range_id,r.status FROM corroboration_groups g "
            "LEFT JOIN source_ranges r ON r.range_id=g.accepted_range_id "
            "WHERE g.group_id=%s", (group_id,),
        ).fetchone()
    if row is None or row[0] not in ("corroborated", "accepted") or row[1] is None:
        return False
    if row[0] == "accepted" and row[2] == "accepted":
        return True
    from ton_sentinel.source.scan import walk_evidence

    evidence = walk_evidence(connection, str(row[1]))
    if not evidence.complete:
        return False
    accept_scan(connection, lease, evidence, acceptance_check=acceptance_check)
    with connection.transaction():
        return connection.execute(
            "SELECT status='accepted' FROM corroboration_groups WHERE group_id=%s",
            (group_id,),
        ).fetchone()[0]
