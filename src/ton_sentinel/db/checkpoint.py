"""Validation of persisted scan checkpoints and their immutable evidence."""
import hashlib
import json

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.normalize import normalize


def validate_checkpoint(connection, range_id, *, require_complete=False):
    row = connection.execute(
        "SELECT start_inclusive,end_exclusive,epoch,scan_policy,next_offset,"
        "page_count,action_count,status,legacy_resume,request_count FROM source_ranges WHERE range_id=%s",
        (range_id,),
    ).fetchone()
    if row is None:
        raise ValueError("unknown scan range")
    start, end, epoch, policy, offset, pages, actions, status, legacy_resume, request_count = row
    if isinstance(epoch, str):
        epoch = json.loads(epoch)
    if isinstance(policy, str):
        policy = json.loads(policy)
    if legacy_resume:
        if policy is not None or status not in ("complete", "accepted"):
            raise ValueError("legacy scan provenance is inconsistent")
        if require_complete and status != "complete":
            raise ValueError("scan checkpoint evidence is not complete")
        source_epoch = SourceEpoch.from_json(
            json.dumps(epoch, ensure_ascii=True, separators=(",", ":")))
        observations = connection.execute(
            "SELECT ordinal,payload FROM action_observations WHERE range_id=%s ORDER BY ordinal",
            (range_id,),
        ).fetchall()
        if len(observations) != actions or any(row[0] != n for n, row in enumerate(observations)):
            raise ValueError("legacy scan observation evidence is inconsistent")
        for _, payload in observations:
            action = normalize(payload, source_epoch)
            if not start <= action.action_end < end:
                raise ValueError("action_end outside immutable scan range")
        return
    if not isinstance(policy, dict) or policy.get("start") != start or policy.get("end") != end:
        raise ValueError("scan checkpoint immutable manifest is inconsistent")
    if type(policy.get("max_requests")) is not int or not 1 <= policy["max_requests"] <= 10_000:
        raise ValueError("scan checkpoint manifest predates durable request accounting")
    if type(request_count) is not int or not 0 <= request_count <= policy["max_requests"]:
        raise ValueError("scan checkpoint request counter is inconsistent")
    try:
        source_epoch = SourceEpoch.from_json(
            json.dumps(epoch, ensure_ascii=True, separators=(",", ":")))
    except (TypeError, ValueError) as exc:
        raise ValueError("scan checkpoint epoch evidence is invalid") from exc
    if offset != actions or pages < 0 or actions < 0:
        raise ValueError("scan checkpoint counters are inconsistent")
    minimum_pages = (actions + policy["page_size"] - 1) // policy["page_size"]
    if pages < minimum_pages or (require_complete and pages <= actions // policy["page_size"]):
        raise ValueError("scan checkpoint page counter is inconsistent")
    attempts = connection.execute(
        "SELECT attempt_no,requested_offset FROM scan_page_attempts "
        "WHERE range_id=%s ORDER BY attempt_no", (range_id,),
    ).fetchall()
    if len(attempts) != pages:
        raise ValueError("scan checkpoint page-attempt evidence count is inconsistent")
    prior_offset = 0
    for expected, (attempt_no, requested_offset) in enumerate(attempts, 1):
        if (attempt_no != expected or requested_offset < prior_offset
                or requested_offset > offset):
            raise ValueError("scan checkpoint page-attempt evidence is inconsistent")
        prior_offset = requested_offset

    rows = connection.execute(
        "SELECT r.ordinal,r.payload_digest,o.ordinal,o.payload "
        "FROM scan_observation_receipts r FULL OUTER JOIN action_observations o "
        "ON o.range_id=r.range_id AND o.ordinal=r.ordinal "
        "WHERE COALESCE(r.range_id,o.range_id)=%s ORDER BY COALESCE(r.ordinal,o.ordinal)",
        (range_id,),
    ).fetchall()
    if len(rows) != actions:
        raise ValueError("scan checkpoint evidence count is inconsistent")
    for expected, (receipt_ordinal, digest, observation_ordinal, payload) in enumerate(rows):
        if receipt_ordinal != expected or observation_ordinal != expected or payload is None:
            raise ValueError("scan checkpoint evidence ordinals are inconsistent")
        action = normalize(payload, source_epoch)
        if not start <= action.action_end < end:
            raise ValueError("action_end outside immutable scan range")
        encoded = json.dumps(payload, ensure_ascii=True, allow_nan=False,
                             separators=(",", ":"), sort_keys=True)
        if hashlib.sha256(encoded.encode("ascii")).hexdigest() != digest:
            raise ValueError("scan checkpoint evidence digest mismatch")
    if require_complete and status != "complete":
        raise ValueError("scan checkpoint evidence is not complete")
