"""Finite range scanner over an injected, page-oriented source transport."""
from dataclasses import asdict, dataclass
import hashlib
import json
import time
import uuid
import math


from ton_sentinel.contracts import SourceEpoch, SyntheticInputRefused
from ton_sentinel.db.checkpoint import validate_checkpoint
from ton_sentinel.normalize import normalize
from ton_sentinel.source.coverage import observed_coverage_status


@dataclass(frozen=True)
class ScanPolicy:
    start: int
    end: int
    page_size: int
    max_pages: int
    max_actions: int
    max_bytes: int = 4_000_000
    max_seconds: int = 30
    max_requests: int = 4000

    def __post_init__(self):
        for name in ("start", "end", "page_size", "max_pages", "max_actions",
                     "max_bytes", "max_seconds", "max_requests"):
            value = getattr(self, name)
            if type(value) is not int:
                raise ValueError(f"{name} must be an integer")
        if not 0 <= self.start < self.end <= 2**63 - 1:
            raise ValueError("scan interval must be a positive half-open int64 range")
        if not 1 <= self.page_size <= 1000 or not 1 <= self.max_pages <= 1000:
            raise ValueError("page size and page budget must be 1..1000")
        if not 1 <= self.max_actions <= self.page_size * self.max_pages:
            raise ValueError("action budget must fit within the page budget")
        if not 1 <= self.max_bytes <= 64_000_000 or not 1 <= self.max_seconds <= 300:
            raise ValueError("byte and time budgets are outside bounded limits")
        if not 1 <= self.max_requests <= 10_000:
            raise ValueError("request budget must be 1..10000")


@dataclass(frozen=True)
class WalkEvidence:
    range_id: str
    complete: bool
    pages: int
    actions: int
    reason: str | None


def _json_payload(payload):
    if type(payload) is not dict:
        raise ValueError("source page entries must be JSON objects")
    return json.dumps(payload, ensure_ascii=True, allow_nan=False,
                      separators=(",", ":"), sort_keys=True)


def _acquire_walker(connection, range_id: str) -> None:
    acquired = connection.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (range_id,)
    ).fetchone()[0]
    connection.commit()
    if not acquired:
        raise ValueError("scan range already has an active walker")


def _release_walker(connection, range_id: str) -> None:
    connection.execute(
        "SELECT pg_advisory_unlock(hashtextextended(%s,0))", (range_id,)
    )
    connection.commit()


def _walk_range(connection, transport, epoch, policy, range_id):
    """Walk from the persisted checkpoint; page evidence commits atomically."""
    started = time.monotonic()
    elapsed_base = 0
    try:
        checkpoint = connection.execute(
            "UPDATE source_ranges SET "
            "elapsed_ms=elapsed_ms+GREATEST(0,floor(extract(epoch FROM "
            "(clock_timestamp()-checkpoint_at))*1000)::bigint), "
            "checkpoint_at=clock_timestamp(),status='walking',reason=NULL "
            "WHERE range_id=%s RETURNING next_offset,page_count,action_count,bytes_used,elapsed_ms,request_count",
            (range_id,),
        ).fetchone()
        if checkpoint is None:
            raise ValueError("unknown scan range")
        connection.commit()
        offset, pages, actions, total_bytes, elapsed_base, request_count = checkpoint
        ordinal = actions
        quarantined = connection.execute(
            "SELECT 1 FROM scan_quarantine WHERE range_id=%s LIMIT 1", (range_id,)
        ).fetchone()
        connection.commit()
        if quarantined:
            connection.execute(
                "UPDATE source_ranges SET status='incomplete',reason='action_quarantined' "
                "WHERE range_id=%s", (range_id,),
            )
            connection.commit()
            return range_id
        while True:
            elapsed = elapsed_base + int((time.monotonic() - started) * 1000)
            if pages >= policy.max_pages:
                raise ValueError("page budget exhausted before an empty terminating page")
            if elapsed >= policy.max_seconds * 1000:
                raise ValueError("scan time budget exhausted")
            if total_bytes >= policy.max_bytes:
                raise ValueError("byte budget exhausted")
            pages += 1
            connection.execute(
                "UPDATE source_ranges SET page_count=%s,elapsed_ms=%s,"
                "checkpoint_at=clock_timestamp() WHERE range_id=%s",
                (pages, elapsed, range_id),
            )
            connection.execute(
                "INSERT INTO scan_page_attempts(range_id,attempt_no,requested_offset) "
                "VALUES (%s,%s,%s)", (range_id, pages, offset),
            )
            connection.commit()
            def debit_attempt():
                nonlocal request_count
                row = connection.execute(
                    "UPDATE source_ranges SET request_count=request_count+1,"
                    "elapsed_ms=GREATEST(elapsed_ms,%s),checkpoint_at=clock_timestamp() "
                    "WHERE range_id=%s AND request_count < %s RETURNING request_count",
                    (elapsed_base + int((time.monotonic() - started) * 1000),
                     range_id, policy.max_requests),
                ).fetchone()
                connection.commit()
                if row is None:
                    raise ValueError("cumulative request budget exhausted")
                request_count = row[0]

            def reserve_bytes(size):
                nonlocal total_bytes
                row = connection.execute(
                    "UPDATE source_ranges SET bytes_used=bytes_used+%s,"
                    "elapsed_ms=GREATEST(elapsed_ms,%s),checkpoint_at=clock_timestamp() "
                    "WHERE range_id=%s AND bytes_used+%s <= %s RETURNING bytes_used",
                    (size, elapsed_base + int((time.monotonic() - started) * 1000),
                     range_id, size, policy.max_bytes),
                ).fetchone()
                connection.commit()
                if row is None:
                    raise ValueError("byte budget exhausted")
                total_bytes = row[0]

            if getattr(transport, "supports_durable_accounting", False):
                remaining_seconds = max(0, policy.max_seconds -
                                        (elapsed_base + int((time.monotonic() - started) * 1000)) / 1000)
                page = transport.fetch_page(
                    epoch, policy.start, policy.end, offset, policy.page_size,
                    before_request=debit_attempt, reserve_bytes=reserve_bytes,
                    remaining_seconds=remaining_seconds,
                    remaining_bytes=policy.max_bytes - total_bytes,
                )
            else:
                debit_attempt()
                page = transport.fetch_page(epoch, policy.start, policy.end, offset,
                                            policy.page_size)
            if getattr(transport, "supports_durable_accounting", False):
                page_bytes = 0
            elif type(page) in (list, tuple):
                page_bytes = sum(
                    len(json.dumps(raw, ensure_ascii=True, allow_nan=False,
                                   separators=(",", ":"), sort_keys=True).encode("ascii"))
                    for raw in page
                )
            else:
                page_bytes = len(json.dumps(
                    page, ensure_ascii=True, allow_nan=False,
                    separators=(",", ":"), sort_keys=True,
                ).encode("ascii"))
            total_bytes += page_bytes
            elapsed = elapsed_base + int((time.monotonic() - started) * 1000)
            connection.execute(
                "UPDATE source_ranges SET bytes_used=%s,elapsed_ms=%s,"
                "checkpoint_at=clock_timestamp() WHERE range_id=%s",
                (total_bytes, elapsed, range_id),
            )
            connection.commit()
            if type(page) not in (list, tuple) or len(page) > policy.page_size:
                raise ValueError("source returned a non-bounded page")
            if total_bytes >= policy.max_bytes and len(page) == policy.page_size:
                raise ValueError("byte budget exhausted")
            normalized = []
            rejected = []
            for index, raw in enumerate(page):
                if actions + len(normalized) >= policy.max_actions:
                    raise ValueError("action budget exhausted")
                encoded = _json_payload(raw)
                try:
                    normalized_action = normalize(raw, epoch)
                except SyntheticInputRefused:
                    raise                   # An explicit error, never a quarantine row
                except (TypeError, ValueError) as exc:
                    rejected.append((ordinal + index, encoded, str(exc)))
                    continue
                if normalized_action.reasons:
                    rejected.append((ordinal + index, encoded, normalized_action.reasons[0]))
                    continue
                if not policy.start <= normalized_action.action_end < policy.end:
                    rejected.append((ordinal + index, encoded, "action_end_outside_scan_range"))
                    continue
                normalized.append((ordinal + index, encoded))
            if total_bytes > policy.max_bytes:
                raise ValueError("byte budget exhausted")
            if normalized:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO action_observations(range_id,ordinal,payload) "
                        "VALUES (%s,%s,%s::jsonb)",
                        [(range_id, item_ordinal, payload)
                         for item_ordinal, payload in normalized],
                    )
                    cursor.executemany(
                        "INSERT INTO scan_observation_receipts(range_id,ordinal,payload_digest) "
                        "VALUES (%s,%s,%s)",
                        [(range_id, item_ordinal,
                          hashlib.sha256(payload.encode("ascii")).hexdigest())
                         for item_ordinal, payload in normalized],
                    )
            if rejected:
                with connection.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO scan_quarantine(range_id,ordinal,proposed_payload,reason,epoch,"
                        "start_inclusive,end_exclusive) VALUES (%s,%s,%s::jsonb,%s,%s::jsonb,%s,%s) "
                        "ON CONFLICT (range_id,ordinal) DO NOTHING",
                        [(range_id, rejected_ordinal, payload, reason, epoch.to_json(),
                          policy.start, policy.end)
                         for rejected_ordinal, payload, reason in rejected],
                    )
                connection.execute(
                    "UPDATE source_ranges SET action_count=action_count+%s,"
                    "reason='action_quarantined',status='incomplete',checkpoint_at=clock_timestamp() "
                    "WHERE range_id=%s", (len(normalized), range_id),
                )
                connection.commit()
                return range_id
            offset += len(page)
            actions += len(normalized)
            ordinal += len(page)
            elapsed = elapsed_base + int((time.monotonic() - started) * 1000)
            connection.execute(
                "UPDATE source_ranges SET next_offset=%s,page_count=%s,action_count=%s,"
                "bytes_used=%s,elapsed_ms=%s,checkpoint_at=clock_timestamp() WHERE range_id=%s",
                (offset, pages, actions, total_bytes, elapsed, range_id),
            )
            connection.commit()
            if elapsed >= policy.max_seconds * 1000:
                raise ValueError("scan time budget exhausted")
            if len(page) < policy.page_size:
                break
        connection.execute("UPDATE source_ranges SET status='complete' WHERE range_id=%s",
                           (range_id,))
        connection.execute("UPDATE coverage_ranges SET status=%s WHERE range_id=%s",
                           (observed_coverage_status(complete=True), range_id))
        connection.commit()
    except Exception as exc:
        connection.rollback()
        elapsed = elapsed_base + int((time.monotonic() - started) * 1000)
        connection.execute(
            "UPDATE source_ranges SET status='incomplete',reason=%s,"
            "elapsed_ms=GREATEST(elapsed_ms,%s),checkpoint_at=clock_timestamp() WHERE range_id=%s",
            (type(exc).__name__, elapsed, range_id),
        )
        connection.commit()
        if isinstance(exc, SyntheticInputRefused):
            raise
    return range_id


def _scan_registered_walk(connection, transport, group_id, walk_no, epoch, policy):
    """Commit the fail-closed child manifest and group link before transport I/O."""
    range_id = str(uuid.uuid4())
    encoded_policy = json.dumps(asdict(policy), separators=(",", ":"), sort_keys=True)
    connection.execute(
        "INSERT INTO source_ranges(range_id,start_inclusive,end_exclusive,epoch,status,scan_policy,corroborated) "
        "VALUES (%s,%s,%s,%s::jsonb,'walking',%s::jsonb,false)",
        (range_id, policy.start, policy.end, epoch.to_json(), encoded_policy),
    )
    connection.execute(
        "INSERT INTO coverage_ranges(range_id,status) VALUES (%s,'pending')", (range_id,)
    )
    connection.execute(
        "INSERT INTO corroboration_walks(group_id,walk_no,range_id,scan_policy) "
        "VALUES (%s,%s,%s,%s::jsonb)", (group_id, walk_no, range_id, encoded_policy),
    )
    connection.execute(
        "UPDATE corroboration_groups SET walk_count=%s WHERE group_id=%s AND status='pending'",
        (walk_no, group_id),
    )
    if connection.execute("SELECT 1 FROM corroboration_groups WHERE group_id=%s AND walk_count=%s",
                          (group_id, walk_no)).fetchone() is None:
        raise ValueError("corroboration group changed while registering a child walk")
    connection.commit()
    _acquire_walker(connection, range_id)
    try:
        return _walk_range(connection, transport, epoch, policy, range_id)
    finally:
        _release_walker(connection, range_id)


def scan_range(connection, transport, epoch: SourceEpoch, policy: ScanPolicy) -> str:
    """Persist a bounded immutable manifest before fetching any source page."""
    if type(epoch) is not SourceEpoch or type(policy) is not ScanPolicy:
        raise ValueError("typed SourceEpoch and ScanPolicy required")
    range_id = str(uuid.uuid4())
    connection.execute(
        "INSERT INTO source_ranges(range_id,start_inclusive,end_exclusive,epoch,status,scan_policy,corroborated) "
        "VALUES (%s,%s,%s,%s::jsonb,'walking',%s::jsonb,false)",
        (range_id, policy.start, policy.end, epoch.to_json(),
         json.dumps(asdict(policy), separators=(",", ":"), sort_keys=True)),
    )
    connection.execute(
        "INSERT INTO coverage_ranges(range_id,status) VALUES (%s,'pending')", (range_id,)
    )
    connection.commit()
    _acquire_walker(connection, range_id)
    try:
        return _walk_range(connection, transport, epoch, policy, range_id)
    finally:
        _release_walker(connection, range_id)


def _validate_quarantined_checkpoint(connection, range_id):
    """Check terminal rejected-page evidence without advancing its cursor."""
    row = connection.execute(
        "SELECT start_inclusive,end_exclusive,epoch,scan_policy,next_offset,page_count,"
        "action_count,status,request_count FROM source_ranges WHERE range_id=%s", (range_id,),
    ).fetchone()
    start, end, raw_epoch, policy, offset, pages, actions, status, requests = row
    raw_epoch = json.loads(raw_epoch) if isinstance(raw_epoch, str) else raw_epoch
    policy = json.loads(policy) if isinstance(policy, str) else policy
    if (status != "incomplete" or not isinstance(policy, dict)
            or policy.get("start") != start or policy.get("end") != end
            or not 0 <= offset <= actions <= policy["max_actions"]
            or not 0 <= pages <= policy["max_pages"]
            or not 0 <= requests <= policy["max_requests"]):
        raise ValueError("quarantined scan checkpoint counters are inconsistent")
    source_epoch = SourceEpoch.from_json(json.dumps(raw_epoch, separators=(",", ":")))
    attempts = connection.execute(
        "SELECT attempt_no,requested_offset FROM scan_page_attempts WHERE range_id=%s ORDER BY attempt_no",
        (range_id,),
    ).fetchall()
    attempt_offsets = [requested for _, requested in attempts]
    if (pages < 1 or len(attempts) != pages or any(
        number != expected or requested < 0 or requested > offset
        for expected, (number, requested) in enumerate(attempts, 1)
    ) or attempt_offsets[0] != 0 or attempt_offsets[-1] != offset
            or any(current < previous or current - previous > policy["page_size"]
                   for previous, current in zip(attempt_offsets, attempt_offsets[1:]))):
        raise ValueError("quarantined scan page-attempt evidence is inconsistent")
    evidence = connection.execute(
        "SELECT r.ordinal,r.payload_digest,o.ordinal,o.payload FROM scan_observation_receipts r "
        "FULL OUTER JOIN action_observations o ON o.range_id=r.range_id AND o.ordinal=r.ordinal "
        "WHERE COALESCE(r.range_id,o.range_id)=%s ORDER BY COALESCE(r.ordinal,o.ordinal)",
        (range_id,),
    ).fetchall()
    quarantines = connection.execute(
        "SELECT ordinal,proposed_payload,epoch,start_inclusive,end_exclusive FROM scan_quarantine "
        "WHERE range_id=%s ORDER BY ordinal", (range_id,),
    ).fetchall()
    if len(evidence) != actions or not quarantines:
        raise ValueError("quarantined scan evidence is incomplete")
    rejected = set()
    for ordinal, payload, epoch, qstart, qend in quarantines:
        epoch = json.loads(epoch) if isinstance(epoch, str) else epoch
        if (ordinal < 0 or payload is None or epoch != raw_epoch or qstart != start or qend != end
                or ordinal in rejected):
            raise ValueError("quarantined proposal provenance is inconsistent")
        rejected.add(ordinal)
    observed = set()
    for receipt, digest, observation, payload in evidence:
        if receipt != observation or receipt in rejected or payload is None:
            raise ValueError("quarantined scan evidence ordinals are inconsistent")
        observed.add(receipt)
        action = normalize(payload, source_epoch)
        if not start <= action.action_end < end:
            raise ValueError("action_end outside immutable scan range")
        encoded = json.dumps(payload, ensure_ascii=True, allow_nan=False,
                             separators=(",", ":"), sort_keys=True)
        if hashlib.sha256(encoded.encode("ascii")).hexdigest() != digest:
            raise ValueError("scan checkpoint evidence digest mismatch")
    combined = observed | rejected
    terminal_end = offset + policy["page_size"]
    if (len(observed) != actions or combined != set(range(len(combined)))
            or any(ordinal < offset or ordinal >= terminal_end for ordinal in rejected)):
        raise ValueError("quarantined scan evidence ordinals are inconsistent")


def resume_scan(connection, transport, range_id: str,
                epoch: SourceEpoch, policy: ScanPolicy) -> str:
    """Resume only with the exact persisted epoch, bounds, and budget policy."""
    if type(epoch) is not SourceEpoch or type(policy) is not ScanPolicy:
        raise ValueError("typed SourceEpoch and ScanPolicy required")
    if type(range_id) is not str:
        raise ValueError("range ID must be text")
    _acquire_walker(connection, range_id)
    try:
        row = connection.execute(
            "SELECT start_inclusive,end_exclusive,epoch,status,scan_policy "
            "FROM source_ranges WHERE range_id=%s FOR UPDATE", (range_id,),
        ).fetchone()
        connection.commit()
        if row is None:
            raise ValueError("unknown scan range")
        stored_epoch = row[2]
        stored_epoch = json.loads(stored_epoch) if isinstance(stored_epoch, str) else stored_epoch
        if (row[0] != policy.start or row[1] != policy.end or stored_epoch != json.loads(epoch.to_json())
                or row[4] != asdict(policy)):
            raise ValueError("resume parameters differ from immutable scan manifest")
        if row[4] is None:
            raise ValueError("legacy scan has no immutable resume manifest")
        if row[3] not in ("incomplete", "walking"):
            raise ValueError("only interrupted scans can be resumed")
        quarantined = connection.execute(
            "SELECT 1 FROM scan_quarantine WHERE range_id=%s LIMIT 1", (range_id,)
        ).fetchone()
        if quarantined:
            _validate_quarantined_checkpoint(connection, range_id)
            connection.commit()
            return range_id
        validate_checkpoint(connection, range_id)
        connection.commit()
        return _walk_range(connection, transport, epoch, policy, range_id)
    finally:
        _release_walker(connection, range_id)


def walk_evidence(connection, range_id: str) -> WalkEvidence:
    if connection.info.transaction_status.name != "IDLE":
        raise ValueError("walk_evidence requires an idle connection")
    with connection.transaction():
        row = connection.execute(
            "SELECT status,page_count,action_count,reason FROM source_ranges WHERE range_id=%s",
            (range_id,),
        ).fetchone()
        if row is None:
            raise ValueError("unknown range")
    return WalkEvidence(range_id, row[0] in ("complete", "accepted"),
                        row[1], row[2], row[3])


def _group_totals(connection, group_id):
    row = connection.execute(
        "SELECT COALESCE(sum(r.request_count),0),COALESCE(sum(r.page_count),0),"
        "COALESCE(sum(r.action_count),0),COALESCE(sum(r.bytes_used),0),"
        "COALESCE(sum(r.elapsed_ms),0) FROM corroboration_walks w "
        "JOIN source_ranges r USING(range_id) WHERE w.group_id=%s", (group_id,),
    ).fetchone()
    connection.execute(
        "UPDATE corroboration_groups SET request_count=%s,page_count=%s,"
        "action_count=%s,bytes_used=%s,elapsed_ms=%s WHERE group_id=%s",
        (*row, group_id),
    )
    return tuple(int(value) for value in row)


def _remaining_policy(policy, totals):
    requests, pages, actions, byte_count, elapsed_ms = totals
    remaining_pages = policy.max_pages - pages
    remaining_actions = min(policy.max_actions - actions,
                            remaining_pages * policy.page_size)
    remaining_bytes = policy.max_bytes - byte_count
    remaining_seconds = policy.max_seconds - math.ceil(elapsed_ms / 1000)
    remaining_requests = policy.max_requests - requests
    if min(remaining_pages, remaining_actions, remaining_bytes,
           remaining_seconds, remaining_requests) <= 0:
        return None
    return ScanPolicy(start=policy.start, end=policy.end, page_size=policy.page_size,
                      max_pages=remaining_pages, max_actions=remaining_actions,
                      max_bytes=remaining_bytes, max_seconds=remaining_seconds,
                      max_requests=remaining_requests)


def _walk_keys(connection, range_id, epoch, start, end):
    rows = connection.execute(
        "SELECT payload FROM action_observations WHERE range_id=%s ORDER BY ordinal",
        (range_id,),
    ).fetchall()
    keys = []
    for (payload,) in rows:
        action = normalize(payload, epoch)
        if not start <= action.action_end < end:
            raise ValueError("action_end outside immutable scan range")
        keys.append(action.identity.event_id)
    if len(keys) != len(set(keys)):
        raise ValueError("full walk contains duplicate action keys")
    return sorted(keys)


def validate_corroboration_proof(connection, group_id, *, selected_range_id=None):
    """Rebuild the latest adjacent pair from locked persisted observations."""
    group = connection.execute(
        "SELECT start_inclusive,end_exclusive,epoch,scan_policy,walk_count,status "
        "FROM corroboration_groups WHERE group_id=%s FOR UPDATE", (group_id,),
    ).fetchone()
    if group is None or group[4] < 2 or group[4] > 3:
        return False
    start, end, epoch_json, raw_policy, count, status = group
    epoch_json = json.loads(epoch_json) if isinstance(epoch_json, str) else epoch_json
    raw_policy = json.loads(raw_policy) if isinstance(raw_policy, str) else raw_policy
    epoch = SourceEpoch.from_json(json.dumps(epoch_json, separators=(",", ":")))
    rows = connection.execute(
        "SELECT w.walk_no,w.range_id,w.scan_policy,w.complete,w.key_set,r.start_inclusive,"
        "r.end_exclusive,r.epoch,r.status,r.corroborated,r.scan_policy FROM corroboration_walks w "
        "JOIN source_ranges r USING(range_id) WHERE w.group_id=%s "
        "ORDER BY w.walk_no DESC LIMIT 2 FOR UPDATE OF w,r", (group_id,),
    ).fetchall()
    if len(rows) != 2:
        return False
    selected, previous = rows
    if (selected[0] != count or previous[0] != count - 1
            or (selected_range_id is not None and str(selected[1]) != selected_range_id)):
        return False
    policy = ScanPolicy(**raw_policy)
    rebuilt_keys = []
    for row in rows:
        number, range_id, child_policy, complete, stored_keys, rstart, rend, repoch, rstatus, _, range_policy = row
        child_policy = json.loads(child_policy) if isinstance(child_policy, str) else child_policy
        range_policy = json.loads(range_policy) if isinstance(range_policy, str) else range_policy
        repoch = json.loads(repoch) if isinstance(repoch, str) else repoch
        if (not complete or number > count or rstatus not in ("complete", "accepted")
                or rstart != start or rend != end or repoch != epoch_json
                or child_policy != range_policy or child_policy.get("start") != start
                or child_policy.get("end") != end
                or child_policy.get("page_size") != policy.page_size
                or any(child_policy.get(name, 0) > raw_policy[name] for name in (
                    "max_pages", "max_actions", "max_bytes", "max_seconds", "max_requests"))):
            return False
        # Parent row locks prevent new FK-linked evidence; lock existing rows too
        # so the proof and the subsequent promotion share the same stable data.
        connection.execute(
            "SELECT ordinal FROM action_observations WHERE range_id=%s ORDER BY ordinal FOR SHARE",
            (range_id,),
        ).fetchall()
        connection.execute(
            "SELECT ordinal FROM scan_observation_receipts WHERE range_id=%s ORDER BY ordinal FOR SHARE",
            (range_id,),
        ).fetchall()
        validate_checkpoint(connection, str(range_id), require_complete=True)
        real_keys = _walk_keys(connection, range_id, epoch, start, end)
        stored_keys = json.loads(stored_keys) if isinstance(stored_keys, str) else stored_keys
        if stored_keys != real_keys:
            return False
        rebuilt_keys.append(real_keys)
    return rebuilt_keys[0] == rebuilt_keys[1]


def corroborate_range(connection, transport, epoch: SourceEpoch,
                      policy: ScanPolicy) -> str:
    """Persist up to three full walks; only consecutive exact key-set matches corroborate."""
    if type(epoch) is not SourceEpoch or type(policy) is not ScanPolicy:
        raise ValueError("typed SourceEpoch and ScanPolicy required")
    group_id = str(uuid.uuid4())
    connection.execute(
        "INSERT INTO corroboration_groups(group_id,start_inclusive,end_exclusive,epoch,"
        "scan_policy,status) VALUES (%s,%s,%s,%s::jsonb,%s::jsonb,'pending')",
        (group_id, policy.start, policy.end, epoch.to_json(),
         json.dumps(asdict(policy), separators=(",", ":"), sort_keys=True)),
    )
    connection.commit()
    return resume_corroboration(connection, transport, group_id)


def resume_corroboration(connection, transport, group_id: str) -> str:
    """Resume a persisted corroboration group without resetting its budgets or evidence."""
    if type(group_id) is not str:
        raise ValueError("group ID must be text")
    acquired = connection.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s,0))", ("corroboration:" + group_id,)
    ).fetchone()[0]
    connection.commit()
    if not acquired:
        raise ValueError("corroboration group already has an active walker")
    try:
        group = connection.execute(
            "SELECT start_inclusive,end_exclusive,epoch,scan_policy,status "
            "FROM corroboration_groups WHERE group_id=%s", (group_id,),
        ).fetchone()
        if group is None:
            raise ValueError("unknown corroboration group")
        if group[4] != "pending":
            return group_id
        start, end, epoch_json, raw_policy, _ = group
        epoch_json = json.loads(epoch_json) if isinstance(epoch_json, str) else epoch_json
        raw_policy = json.loads(raw_policy) if isinstance(raw_policy, str) else raw_policy
        epoch = SourceEpoch.from_json(json.dumps(epoch_json, separators=(",", ":")))
        policy = ScanPolicy(**raw_policy)

        while True:
            # Re-evaluate a persisted pair before registering another child. This
            # closes the commit/crash window after key-set persistence.
            if validate_corroboration_proof(connection, group_id):
                latest = connection.execute(
                    "SELECT range_id FROM corroboration_walks WHERE group_id=%s "
                    "ORDER BY walk_no DESC LIMIT 1", (group_id,),
                ).fetchone()[0]
                connection.execute(
                    "UPDATE corroboration_groups SET status='corroborated',"
                    "accepted_range_id=%s WHERE group_id=%s AND status='pending'",
                    (latest, group_id),
                )
                connection.execute("UPDATE source_ranges SET corroborated=true WHERE range_id=%s",
                                   (latest,))
                connection.commit()
                return group_id
            walk = connection.execute(
                "SELECT w.walk_no,w.range_id,w.scan_policy,w.complete,w.key_set,r.status "
                "FROM corroboration_walks w JOIN source_ranges r USING(range_id) "
                "WHERE w.group_id=%s ORDER BY w.walk_no DESC LIMIT 1", (group_id,),
            ).fetchone()
            if walk and walk[5] in ("incomplete", "walking"):
                walk_no, range_id, child_policy, _, _, _ = walk
                child_policy = json.loads(child_policy) if isinstance(child_policy, str) else child_policy
                resume_scan(connection, transport, str(range_id), epoch, ScanPolicy(**child_policy))
                walk = connection.execute(
                    "SELECT w.walk_no,w.range_id,w.scan_policy,w.complete,w.key_set,r.status "
                    "FROM corroboration_walks w JOIN source_ranges r USING(range_id) "
                    "WHERE w.group_id=%s ORDER BY w.walk_no DESC LIMIT 1", (group_id,),
                ).fetchone()
                if walk[5] in ("incomplete", "walking"):
                    _group_totals(connection, group_id)
                    return group_id
                walk_no, range_id, child_policy, _, _, _ = walk
                process_latest = True
            elif walk and not walk[3]:
                walk_no, range_id, child_policy, _, _, _ = walk
                process_latest = True
            else:
                process_latest = False
            if not process_latest:
                totals = _group_totals(connection, group_id)
                child = _remaining_policy(policy, totals)
                if child is None:
                    return group_id
                count = connection.execute(
                    "SELECT walk_count FROM corroboration_groups WHERE group_id=%s FOR UPDATE",
                    (group_id,),
                ).fetchone()[0]
                if count >= 3:
                    return group_id
                walk_no = count + 1
                range_id = _scan_registered_walk(
                    connection, transport, group_id, walk_no, epoch, child
                )

            connection.commit()
            evidence = walk_evidence(connection, range_id)
            if not evidence.complete:
                _group_totals(connection, group_id)
                return group_id
            validate_checkpoint(connection, range_id, require_complete=True)
            keys = _walk_keys(connection, range_id, epoch, start, end)
            encoded = json.dumps(keys, separators=(",", ":"))
            previous = connection.execute(
                "SELECT walk_no,key_set FROM corroboration_walks WHERE group_id=%s "
                "AND walk_no<%s AND complete ORDER BY walk_no DESC LIMIT 1",
                (group_id, walk_no),
            ).fetchone()
            connection.execute(
                "UPDATE corroboration_walks SET complete=true,key_set=%s::jsonb "
                "WHERE group_id=%s AND walk_no=%s", (encoded, group_id, walk_no),
            )
            _group_totals(connection, group_id)
            connection.commit()
            if previous and previous[0] == walk_no - 1 and previous[1] == keys:
                if not validate_corroboration_proof(connection, group_id):
                    raise ValueError("persisted adjacent corroboration proof failed validation")
                connection.execute(
                    "UPDATE corroboration_groups SET status='corroborated',"
                    "accepted_range_id=%s WHERE group_id=%s", (range_id, group_id),
                )
                connection.execute(
                    "UPDATE source_ranges SET corroborated=true WHERE range_id=%s", (range_id,)
                )
                connection.commit()
                return group_id
            connection.commit()
            latest = connection.execute(
                "SELECT walk_count FROM corroboration_groups WHERE group_id=%s", (group_id,),
            ).fetchone()[0]
            if latest >= 3:
                return group_id
    finally:
        connection.execute(
            "SELECT pg_advisory_unlock(hashtextextended(%s,0))", ("corroboration:" + group_id,)
        )
        connection.commit()
