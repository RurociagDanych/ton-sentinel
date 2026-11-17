"""Fail-closed action-window coverage; scan bounds are not trace-time bounds."""
from dataclasses import dataclass
import hashlib
import json
import uuid

from ton_sentinel.contracts import SourceEpoch


def observed_coverage_status(*, complete: bool) -> str:
    """Return the scan-range observation state; this is not action coverage."""
    return "bounded_observed" if complete else "pending"


@dataclass(frozen=True)
class ActionWindowCoverage:
    coverage_id: str
    network: str
    epoch: SourceEpoch
    window_start: int
    window_end: int
    status: str
    eligible_zero: bool
    reason: str
    lineage: tuple[dict, ...]


def _validate_window(network, epoch, start, end):
    if type(network) is not str or network not in ("mainnet", "testnet"):
        raise ValueError("network must be exactly mainnet or testnet")
    if type(epoch) is not SourceEpoch:
        raise ValueError("typed SourceEpoch required")
    if network != epoch.network:
        raise ValueError("network and source epoch differ")
    if (type(start) is not int or type(end) is not int
            or not 0 <= start < end <= 2**63 - 1
            or start % 300 != 0 or end - start != 300):
        raise ValueError("action window must be an integer UTC 300-second half-open interval")


def _coverage_id(network, epoch_json, start, end):
    key = json.dumps([network, json.loads(epoch_json), start, end],
                     ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return str(uuid.UUID(bytes=hashlib.sha256(key.encode("ascii")).digest()[:16], version=5))


def action_window_coverage(connection, network: str, epoch: SourceEpoch,
                           start: int, end: int) -> ActionWindowCoverage:
    """Persist and return missing coverage with exact overlapping scan lineage.

    Source scan time bounds do not map to action times. Consequently even
    complete, empty, or corroborated scans remain missing; only external,
    source-supported trace-completion mapping could make this window bounded.
    """
    _validate_window(network, epoch, start, end)
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("action_window_coverage requires an idle connection")
    epoch_json = epoch.to_json()
    with connection.transaction():
        rows = connection.execute(
            "SELECT r.range_id::text,r.start_inclusive,r.end_exclusive,r.status,"
            "r.page_count,r.action_count,r.corroborated,c.status "
            "FROM source_ranges r LEFT JOIN coverage_ranges c USING(range_id) "
            "WHERE r.epoch=%s::jsonb AND r.start_inclusive < %s "
            "AND r.end_exclusive > %s ORDER BY r.start_inclusive,r.end_exclusive,r.range_id",
            (epoch_json, end, start),
        ).fetchall()
        lineage = [
            {"range_id": row[0], "start": row[1], "end": row[2],
             "status": row[3], "page_count": row[4], "action_count": row[5],
             "corroborated": row[6], "coverage_status": row[7]}
            for row in rows
        ]
        reason = ("trace_completion_mapping_unavailable" if rows
                  else "no_action_time_scan_evidence")
        coverage_id = _coverage_id(network, epoch_json, start, end)
        encoded_lineage = json.dumps(lineage, ensure_ascii=True, sort_keys=True,
                                     separators=(",", ":"))
        connection.execute(
            "INSERT INTO action_window_coverage "
            "(coverage_id,network,epoch,window_start,window_end,status,eligible_zero,reason,lineage) "
            "VALUES (%s,%s,%s::jsonb,%s,%s,'missing',false,%s,%s::jsonb) "
            "ON CONFLICT (network,epoch,window_start,window_end) DO UPDATE SET "
            "status='missing',eligible_zero=false,reason=EXCLUDED.reason,"
            "lineage=EXCLUDED.lineage,updated_at=clock_timestamp()",
            (coverage_id, network, epoch_json, start, end, reason, encoded_lineage),
        )
        stored = connection.execute(
            "SELECT coverage_id::text,network,epoch,window_start,window_end,status,"
            "eligible_zero,reason,lineage FROM action_window_coverage "
            "WHERE network=%s AND epoch=%s::jsonb AND window_start=%s AND window_end=%s",
            (network, epoch_json, start, end),
        ).fetchone()
    stored_epoch = stored[2]
    if isinstance(stored_epoch, str):
        stored_epoch = json.loads(stored_epoch)
    stored_lineage = stored[8]
    if isinstance(stored_lineage, str):
        stored_lineage = json.loads(stored_lineage)
    return ActionWindowCoverage(
        coverage_id=stored[0], network=stored[1],
        epoch=SourceEpoch.from_json(json.dumps(stored_epoch, separators=(",", ":"))),
        window_start=stored[3], window_end=stored[4], status=stored[5],
        eligible_zero=stored[6], reason=stored[7],
        lineage=tuple(stored_lineage),
    )
