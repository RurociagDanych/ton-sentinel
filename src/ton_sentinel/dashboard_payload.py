"""Dashboard payload honesty: provenance block, safety filter and fixed limits."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone

HONESTY_BANNER = (
    "Local fixture data. Not live, not certified, detector effectiveness unmeasured. "
    "Row origin is not recorded; provisional rows are not final.")

# Row origin: shown only when every scoped window with served rows carries a recorded
# origin of ``live`` (or some do, for the mixed banner). Never a certification claim.
LIVE_BANNER = (
    "Live source data: read-only indexer reads recorded as live per window. Coverage is "
    "bounded observed, not certified; detector effectiveness unmeasured. Provisional rows "
    "are not final.")
MIXED_BANNER = (
    "Mixed origin: some windows are live, the others are fixture, recorded or of unrecorded "
    "origin; not certified, detector effectiveness unmeasured. Provisional rows are not final.")
ORIGINS = ("live", "recorded", "synthetic", "unrecorded")

# Served to clients by /v1/policy; the client must not hard-code these.
LIMITS = {
    "page_size_default": 100,
    "page_size_max": 1000,
    "time_range_seconds_max": 7 * 86400,
    "response_cap_bytes": 1_048_576,
}

FORBIDDEN_KEYS = frozenset({
    "wallet", "wallets", "wallet_address", "address", "addresses", "account", "source_url",
    "url", "urls", "lease_owner", "lease_fence", "authority_grant", "offset_metadata",
    "lineage", "dsn", "request_log", "request_logs", "outbox_payload", "payload_json",
    "password", "api_key", "x-api-key", "secret", "token_secret",
})
_FORBIDDEN_VALUE = re.compile(
    r"postgres(?:ql)?://|password|api[_-]?key|https?://"
    # a raw address is refused unless it is the address part of a ``jetton:`` token identifier
    # (public jetton master), which values and detector explanations name verbatim
    r"|(?<!jetton:)(?<!jetton:-)\b-?[01]:[0-9a-fA-F]{64}\b|\b[EU]Q[A-Za-z0-9_-]{46}\b",
    re.IGNORECASE)


class UnsafePayload(ValueError):
    """The payload cannot be served: forbidden content remains or it exceeds the cap."""
    pass


def _origin_counts(origins):
    """``{origin: windows}`` from (origin, windows) rows; ``None`` if absent or malformed."""
    if origins is None:
        return None
    counts = {}
    for row in origins:
        if (not isinstance(row, (list, tuple)) or len(row) != 2 or row[0] not in ORIGINS
                or isinstance(row[1], bool) or not isinstance(row[1], int) or row[1] < 0):
            return None
        if row[1]:
            counts[row[0]] = counts.get(row[0], 0) + row[1]
    return counts


def build_provenance(*, qualification, scored, now=None, origins=None) -> dict:
    """Pure builder. ``qualification`` is (mode, synthetic, count) rows or None (unavailable).

    ``origins`` is (origin, served windows) rows over the scoped windows, origin one of
    ``ORIGINS``; ``None`` or a malformed row means the origin is unverified. The source kind is
    ``live`` only when every counted window is live, ``mixed_origin`` when some are."""
    now = now or datetime.now(timezone.utc)
    stamp = now.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    block = {
        "source_kind": "unverified_origin", "certification_state": "not_certified",
        "honesty_banner": HONESTY_BANNER, "generated_at": stamp,
        "synthetic": None, "bounded_observed": None, "certified_claimed": None,
    }
    if qualification is None:
        block.update(qualification_class="unavailable", coverage_state="unavailable")
    else:
        synthetic = bounded = claimed = 0
        for mode, is_synthetic, count in qualification:
            count = int(count)
            if mode == "certified":
                claimed += count  # a claim only; never verified, never changes the state
            elif is_synthetic:
                synthetic += count
            else:
                bounded += count
        block.update(synthetic=synthetic, bounded_observed=bounded, certified_claimed=claimed)
        if synthetic:
            block.update(qualification_class="synthetic",
                         coverage_state="synthetic_bounded_observed")
        elif bounded or claimed:
            block.update(qualification_class="bounded_observed",
                         coverage_state="bounded_observed")
        else:
            block.update(qualification_class="unrecorded", coverage_state="unrecorded")
    counts = _origin_counts(origins)
    if counts is not None:
        block["origin_windows"] = counts
        if counts.get("live") and set(counts) == {"live"}:
            block.update(source_kind="live", honesty_banner=LIVE_BANNER)
        elif counts.get("live"):
            block.update(source_kind="mixed_origin", honesty_banner=MIXED_BANNER)
    block["scoring_state"] = ("unavailable" if scored is None else
                              "scored_effectiveness_unmeasured" if scored
                              else "none_recorded")
    return block


def strip_forbidden(value):
    """Return a copy without forbidden keys at any depth."""
    if isinstance(value, dict):
        return {k: strip_forbidden(v) for k, v in value.items()
                if not (isinstance(k, str) and k.lower() in FORBIDDEN_KEYS)}
    if isinstance(value, (list, tuple)):
        return [strip_forbidden(v) for v in value]
    return value


def find_violations(value, path="$") -> list:
    """Paths of forbidden keys or forbidden-looking string values (for tests and refusal)."""
    found = []
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(key, str) and key.lower() in FORBIDDEN_KEYS:
                found.append(f"{path}.{key}")
            found += find_violations(item, f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            found += find_violations(item, f"{path}[{index}]")
    elif isinstance(value, str) and _FORBIDDEN_VALUE.search(value):
        found.append(path)
    return found


def ensure_safe(payload):
    """Strip forbidden keys, then refuse (never serve) anything still unsafe or oversized."""
    cleaned = strip_forbidden(payload)
    if find_violations(cleaned):
        raise UnsafePayload("forbidden content in payload")
    size = len(json.dumps(cleaned, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    if size > LIMITS["response_cap_bytes"]:
        raise UnsafePayload("response exceeds cap")
    return cleaned
