"""Bounded source corroboration and canonical acceptance orchestration."""
from dataclasses import dataclass

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.ledger import accept_corroborated_range, refuse_if_paused
from ton_sentinel.source.scan import ScanPolicy, corroborate_range


@dataclass(frozen=True)
class IngestionResult:
    """Durable outcome of one bounded half-open source interval."""
    group_id: str
    start: int
    end: int
    status: str
    accepted: bool
    promoted_event_ids: tuple[str, ...]
    publication_intents: tuple[str, ...]


@dataclass(frozen=True)
class OutboxBudget:
    """Required backlog admission limits; not a global disk-capacity guarantee."""
    max_pending_bytes: int
    max_oldest_age_seconds: int

    def __post_init__(self):
        for name in ("max_pending_bytes", "max_oldest_age_seconds"):
            value = getattr(self, name)
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")


class IngestionBackpressure(RuntimeError):
    """Raised before scanning or acceptance when pending outbox exceeds budget."""
    pass


def _check_outbox_budget(connection, budget: OutboxBudget) -> None:
    """Check committed/uncommitted durable rows without committing caller work."""
    idle_at_entry = connection.info.transaction_status.name == "IDLE"
    row = connection.execute(
        "SELECT COALESCE(sum(pg_column_size(o)), 0)::bigint, "
        "COALESCE(bool_or(o.created_at <= clock_timestamp() - "
        "(%s * interval '1 second')), false) "
        "FROM outbox o WHERE o.publication_state <> 'acknowledged'",
        (budget.max_oldest_age_seconds,),
    ).fetchone()
    # For an idle caller this read-only transaction must not leak into the
    # subsequent operation, especially when admission is blocked. Never commit
    # an already-active caller transaction just to perform this check.
    if idle_at_entry:
        connection.rollback()
    if row[0] >= budget.max_pending_bytes or row[1]:
        raise IngestionBackpressure(
            "unacknowledged outbox backlog reached the configured budget"
        )


def ingest_range(connection, client, epoch, policy: ScanPolicy, *, owner: str,
                 budget: OutboxBudget | None = None) -> IngestionResult:
    """Walk/corroborate a bounded range, then atomically accept its proof if valid.

    Corroboration persists observations only. Incidental SQL reads are committed
    before acquiring the transition lease, which acceptance requires on idle.
    A validated ``budget`` is mandatory: admission fails closed before source
    scanning when the caller has no approved limits. It does not guarantee
    global disk capacity or concurrent hard-cap admission.
    """
    if not isinstance(policy, ScanPolicy):
        raise ValueError("ScanPolicy required")
    if not isinstance(budget, OutboxBudget):
        raise ValueError("OutboxBudget required")
    _check_outbox_budget(connection, budget)
    # A maintenance low-water pause stops ingestion before any source request.
    idle_at_entry = connection.info.transaction_status.name == "IDLE"
    try:
        refuse_if_paused(connection)
    finally:
        if idle_at_entry:
            connection.rollback()
    group_id = corroborate_range(connection, client, epoch, policy)
    connection.commit()
    group = connection.execute(
        "SELECT status,start_inclusive,end_exclusive FROM corroboration_groups "
        "WHERE group_id=%s", (group_id,),
    ).fetchone()
    connection.commit()
    if group is None:
        raise RuntimeError("corroboration group disappeared")
    if group[0] != "corroborated":
        return IngestionResult(group_id, group[1], group[2], group[0], False, (), ())

    lease = acquire_transition(connection, owner)
    acceptance_check = lambda tx: _check_outbox_budget(tx, budget)
    accepted = accept_corroborated_range(
        connection, lease, group_id, acceptance_check=acceptance_check,
    )
    if not accepted:
        return IngestionResult(group_id, group[1], group[2], "pending", False, (), ())
    promoted = connection.execute(
        "SELECT event_id FROM scan_membership WHERE range_id=(SELECT accepted_range_id "
        "FROM corroboration_groups WHERE group_id=%s) ORDER BY event_id", (group_id,),
    ).fetchall()
    intents = connection.execute(
        "SELECT o.event_id FROM outbox o JOIN scan_membership m USING(event_id) "
        "WHERE m.range_id=(SELECT accepted_range_id FROM corroboration_groups "
        "WHERE group_id=%s) AND o.published_at IS NULL ORDER BY o.event_id", (group_id,),
    ).fetchall()
    connection.commit()
    return IngestionResult(group_id, group[1], group[2], "accepted", True,
                           tuple(row[0] for row in promoted),
                           tuple(row[0] for row in intents))

def decide_corrections(connection, *, owner: str, range_id: str,
                       authorization: dict | None = None, policy=None):
    """Decide revision candidates of one corroborated walk under a fresh fenced lease.

    Revisions are admitted only with an explicit authorization; without one every change,
    disappearance or key-set replacement is quarantined and nothing canonical is retracted.
    """
    from ton_sentinel.db.revisions import decide_revision
    from ton_sentinel.source.scan import walk_evidence

    evidence = walk_evidence(connection, range_id)
    connection.commit()
    lease = acquire_transition(connection, owner)
    return decide_revision(connection, lease, evidence, authorization, policy=policy)
