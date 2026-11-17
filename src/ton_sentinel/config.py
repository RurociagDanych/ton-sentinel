"""Explicit, deterministic policy selection for normalized observations."""

from __future__ import annotations

from typing import Any


def select_policy_observations(
    observations: list[dict[str, Any]], decisions: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Select explicitly accepted observations; never infer from arrival order."""
    if type(observations) is not list or type(decisions) is not list:
        raise ValueError("observations and decisions must be lists")

    by_id: dict[str, dict[str, Any]] = {}
    for observation in observations:
        candidate_id = observation.get("candidate_id") if type(observation) is dict else None
        if type(candidate_id) is not str or not candidate_id.strip() or candidate_id in by_id:
            raise ValueError("candidate IDs must be unique nonempty strings")
        by_id[candidate_id] = observation

    by_decision: dict[str, str] = {}
    for row in decisions:
        candidate_id = row.get("candidate_id") if type(row) is dict else None
        if (
            candidate_id not in by_id
            or candidate_id in by_decision
            or row.get("decision") not in ("accepted", "rejected")
            or type(row.get("authority")) is not str
            or not row["authority"].strip()
            or type(row.get("reason")) is not str
            or not row["reason"].strip()
        ):
            raise ValueError("invalid, duplicate, or unauthoritative policy decision")
        by_decision[candidate_id] = row["decision"]

    missing = sorted(by_id.keys() - by_decision.keys())
    accepted = [by_id[key] for key in sorted(by_id) if by_decision.get(key) == "accepted"]
    status = {
        "decision_coverage": "complete" if not missing else "incomplete",
        "accepted": len(accepted),
        "rejected": sum(value == "rejected" for value in by_decision.values()),
        "missing": missing,
        "source_coverage": "unknown",
    }
    return accepted, status


def evaluate_action_window_policy(
    policy: dict[str, Any] | None, window_start: int, window_end: int
) -> dict[str, Any]:
    """Evaluate explicit synthetic action-time bounds without certifying source coverage."""
    result = {
        "coverage_status": "missing",
        "eligible_zero": False,
        "reason": "mapping_unavailable",
        "source_coverage": "unknown",
        "features": {"status": "unsupported", "computed": False, "items": None},
        "alerts": {"status": "unsupported", "computed": False, "items": None},
    }
    if (type(window_start) is not int or type(window_end) is not int
            or window_start < 0 or window_start >= window_end):
        result["reason"] = "invalid_window"
        return result
    if policy is None:
        return result
    if (type(policy) is not dict
            or set(policy) != {"schema_version", "synthetic", "mappings"}
            or type(policy.get("schema_version")) is not int
            or policy["schema_version"] != 1
            or policy.get("synthetic") is not True
            or type(policy.get("mappings")) is not list):
        result["reason"] = "invalid_mapping"
        return result

    ranges: list[tuple[int, int]] = []
    for item in policy["mappings"]:
        if (type(item) is not dict
                or set(item) != {"action_start", "action_end", "status"}
                or type(item.get("action_start")) is not int
                or type(item.get("action_end")) is not int
                or item["action_start"] < 0
                or item["action_start"] >= item["action_end"]
                or item.get("status") != "bounded_observed"):
            result["reason"] = "invalid_mapping"
            return result
        ranges.append((item["action_start"], item["action_end"]))
    ranges.sort()
    if any(current[0] < previous[1] for previous, current in zip(ranges, ranges[1:])):
        result["reason"] = "invalid_mapping"
        return result

    cursor = window_start
    for start, end in ranges:
        if end <= cursor:
            continue
        if start > cursor:
            break
        cursor = max(cursor, end)
        if cursor >= window_end:
            result["coverage_status"] = "bounded_observed"
            result["reason"] = "explicit_mapping_covers_window"
            return result
    result["reason"] = "mapping_gap"
    return result


# --------------------------------------------------------------------------------------------
# Fixed detector policy identity and action-window coverage qualification.
from ton_sentinel.contracts import (  # noqa: E402  (kept beside the policy it serves)
    DETECTOR_IDS, QUALIFIED_MODES, WINDOW_SECONDS, DetectorPolicy,
)

DETECTOR_POLICY_V1 = DetectorPolicy()
# Literal pin of the v1 identity; migration 021 registers the same digest and body.
DETECTOR_POLICY_V1_ID = "sha256:34fc97d20f4907d727db53efd921830cdbb3ae17f877d67a1654d591db6830fc"
if DETECTOR_POLICY_V1.identity != DETECTOR_POLICY_V1_ID:
    raise RuntimeError("detector policy v1 drifted from its pinned identity")

_POLICY_FIELDS = frozenset(DETECTOR_POLICY_V1.body())


def detector_policy(raw: dict[str, Any] | None = None) -> DetectorPolicy:
    """Return the fixed v1 policy, or strictly parse an explicit policy body.

    A parsed body that differs from v1 has a different identity; only identities registered
    in ``detector_policies`` can be pinned, so it cannot silently replace v1.
    """
    if raw is None:
        return DETECTOR_POLICY_V1
    if type(raw) is not dict or set(raw) != _POLICY_FIELDS:
        raise ValueError("detector policy must name exactly: " + ", ".join(sorted(_POLICY_FIELDS)))
    detectors = raw["detectors"]
    if type(detectors) is not list or any(type(item) is not str for item in detectors):
        raise ValueError("detectors must be a list of detector IDs")
    values = dict(raw, detectors=tuple(detectors))
    policy = DetectorPolicy(**values)
    if policy.detectors != DETECTOR_IDS:
        raise ValueError("detector policy names exactly the two approved detectors")
    return policy


def qualify_action_window(*, window_start: int, delta_seconds: int, trace_ranges,
                          mode: str, synthetic: bool, certification) -> dict[str, Any]:
    """Validate evidence that one action window ``[s, s+300)`` is adequately observed.

    Spec 5.3: a zero needs a source-supported bound ``0 <= trace_completion - action_end <=
    delta`` and contiguous trace-completion ranges covering ``[s, e + delta)``. Synthetic
    evidence is bounded-observed only; ``certified`` needs explicit certification evidence.
    Returns the normalized qualification; raises ``ValueError`` otherwise.
    """
    if type(window_start) is not int or window_start < 0 or window_start % WINDOW_SECONDS:
        raise ValueError("window_start must be a five-minute UTC boundary")
    if type(delta_seconds) is not int or delta_seconds < 0:
        raise ValueError("delta_seconds must be a nonnegative integer")
    if mode not in QUALIFIED_MODES:
        raise ValueError("mode must be bounded_observed or certified")
    if type(synthetic) is not bool:
        raise ValueError("synthetic must be boolean")
    if mode == "certified":
        if synthetic:
            raise ValueError("synthetic coverage is never certified")
        if type(certification) is not dict or not certification:
            raise ValueError("certified coverage needs certification evidence")
    elif certification is not None:
        raise ValueError("only certified coverage carries certification evidence")
    window_end = window_start + WINDOW_SECONDS
    if type(trace_ranges) not in (list, tuple) or not trace_ranges:
        raise ValueError("trace_ranges must list the covering trace-completion ranges")
    ranges = []
    for item in trace_ranges:
        if (type(item) not in (list, tuple) or len(item) != 2
                or any(type(v) is not int or v < 0 for v in item) or item[0] >= item[1]):
            raise ValueError("each trace range is [start, end) with integer bounds")
        ranges.append([item[0], item[1]])
    if any(nxt[0] != prev[1] for prev, nxt in zip(ranges, ranges[1:])):
        raise ValueError("trace ranges must be contiguous and ordered")
    if ranges[0][0] > window_start or ranges[-1][1] < window_end + delta_seconds:
        raise ValueError("trace ranges must cover [window_start, window_end + delta)")
    return {"window_start": window_start, "window_end": window_end,
            "delta_seconds": delta_seconds, "trace_ranges": ranges, "mode": mode,
            "synthetic": synthetic, "certification": certification}
