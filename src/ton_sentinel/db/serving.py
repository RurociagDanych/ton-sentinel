"""Transactional full-window replacement for Spark-computed features."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import re

from ton_sentinel import dashboard_payload
from ton_sentinel.db.fencing import validate_transition

# Tables the dashboard/API reader role may SELECT (migration 028). A table a later reader needs
# beyond this set requires its own additive grant migration and an update here.
READER_TABLES = frozenset({
    "served_features", "action_window_qualification", "action_window_coverage",
    "window_admission_refusals", "coverage_ranges", "detector_current", "detector_results",
    "alert_revisions", "baseline_inputs", "detector_policies", "active_detector_policy",
    "outbox", "dirty_windows", "revision_quarantine", "scan_quarantine", "delivery_quarantine",
    "stream_batches", "recovery_pauses", "ingestion_pauses", "retention_runs",
})


@dataclass(frozen=True, order=True)
class WindowKey:
    network: str
    window_start: int
    window_end: int


def _field(row, name):
    if isinstance(row, dict):
        return row[name]
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)[name]
    return getattr(row, name)


def _raw(value):
    if value is None:
        return None
    # Spark Decimal and database Decimal both stringify without losing precision.
    text = str(value)
    if not text.isascii() or not text.isdigit() or len(text) > 38:
        raise ValueError("raw feature amount must be an unsigned 38-digit integer")
    return text


def _validated(row):
    item = {name: _field(row, name) for name in (
        "network", "window_start", "window_end", "token", "generation", "swaps",
        "input_raw", "output_raw", "known_traders", "unknown_count",
        "coverage_status", "status", "provisional")}
    if item["network"] not in ("mainnet", "testnet"):
        raise ValueError("feature network must be mainnet or testnet")
    if (type(item["window_start"]) is not int or type(item["window_end"]) is not int
            or item["window_start"] < 0 or item["window_end"] != item["window_start"] + 300):
        raise ValueError("features require a five-minute half-open window")
    if type(item["token"]) is not str or not item["token"]:
        raise ValueError("feature token must be nonempty text")
    if type(item["generation"]) is not str or not item["generation"]:
        raise ValueError("feature generation must be nonempty text")
    for name in ("swaps", "known_traders", "unknown_count"):
        if type(item[name]) is not int or item[name] < 0:
            raise ValueError(f"{name} must be a nonnegative integer")
    item["input_raw"] = _raw(item["input_raw"])
    item["output_raw"] = _raw(item["output_raw"])
    if type(item["status"]) is not str or not item["status"]:
        raise ValueError("feature status must be nonempty text")
    if type(item["provisional"]) is not bool:
        raise ValueError("provisional must be boolean")
    return item


def _window_key(key):
    if type(key) is not WindowKey:
        raise ValueError("replacement targets must be WindowKey values")
    if (type(key.network) is not str or key.network not in ("mainnet", "testnet")
            or type(key.window_start) is not int or type(key.window_end) is not int
            or key.window_start < 0 or key.window_end != key.window_start + 300):
        raise ValueError("replacement targets require a valid five-minute half-open window")
    return key


def replace_features(connection, lease, features, *, windows=None) -> int:
    """Replace every supplied complete window atomically, guarded by its lease.

    Nonempty callers may continue to infer targets from their rows. Empty
    recomputations must provide explicit ``windows`` so prior rows are removed.
    The singleton lease row fences all mutations, including windows with no
    remaining feature rows.
    """
    rows = [_validated(row) for row in features]
    grouped = defaultdict(list)
    seen = set()
    for row in rows:
        key = WindowKey(row["network"], row["window_start"], row["window_end"])
        token_key = (key, row["token"])
        if token_key in seen:
            raise ValueError("duplicate token in full-window replacement")
        seen.add(token_key)
        grouped[key].append(row)

    if windows is None:
        targets = set(grouped)
    else:
        targets = set()
        for target in windows:
            target = _window_key(target)
            if target in targets:
                raise ValueError("duplicate target window in full-window replacement")
            targets.add(target)
        if not set(grouped).issubset(targets):
            raise ValueError("feature rows include an undeclared target window")

    if not targets:
        return 0
    try:
        with connection.transaction():
            validate_transition(connection, lease)
            for key in sorted(targets):
                window_rows = grouped[key]
                current = connection.execute(
                    "SELECT max(write_fence) FROM served_features "
                    "WHERE network=%s AND window_start=%s AND window_end=%s",
                    (key.network, key.window_start, key.window_end),
                ).fetchone()[0]
                if current is not None and current > lease.fence:
                    raise RuntimeError("feature replacement fence regressed")
                connection.execute(
                    "DELETE FROM served_features WHERE network=%s AND window_start=%s AND window_end=%s",
                    (key.network, key.window_start, key.window_end),
                )
                connection.cursor().executemany(
                    "INSERT INTO served_features (network, window_start, window_end, token, generation, swaps, "
                    "input_raw, output_raw, known_traders, unknown_count, coverage_status, status, provisional, write_fence) "
                    "VALUES (%(network)s, %(window_start)s, %(window_end)s, %(token)s, %(generation)s, %(swaps)s, "
                    "%(input_raw)s, %(output_raw)s, %(known_traders)s, %(unknown_count)s, %(coverage_status)s, "
                    "%(status)s, %(provisional)s, %(write_fence)s)",
                    [dict(row, write_fence=lease.fence) for row in window_rows],
                )
            validate_transition(connection, lease)
    except Exception:
        # transaction() rolls back on failure, including lease-final-validation failures.
        raise
    return len(rows)


def read_features(connection, *, network=None, window_start=None, window_end=None, token=None,
                  limit=None, after=None, fetch_extra=False):
    clauses, params = [], []
    if network is not None:
        clauses.append("network=%s")
        params.append(network)
    if window_start is not None:
        clauses.append("window_start=%s")
        params.append(window_start)
    if window_end is not None:
        clauses.append("window_end=%s")
        params.append(window_end)
    if token is not None:
        clauses.append("token=%s")
        params.append(token)
    if after is not None:
        if (type(after) is not tuple or len(after) != 4
                or type(after[0]) is not str or after[0] not in ("mainnet", "testnet")
                or type(after[1]) is not int or type(after[2]) is not int
                or after[1] < 0 or after[2] > 9223372036854775807
                or after[2] != after[1] + 300
                or type(after[3]) is not str or not after[3]):
            raise ValueError("invalid feature keyset")
        clauses.append("(network, window_start, window_end, token) > (%s, %s, %s, %s)")
        params.extend(after)
    if limit is not None:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("feature read limit must be between 1 and 1000")
        if type(fetch_extra) is not bool:
            raise ValueError("fetch_extra must be boolean")
        params.append(limit + int(fetch_extra))
    elif fetch_extra:
        raise ValueError("fetch_extra requires a bounded limit")
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    limit_clause = " LIMIT %s" if limit is not None else ""
    rows = connection.execute(
        "SELECT network, window_start, window_end, token, generation, swaps, "
        "input_raw::text, output_raw::text, known_traders, unknown_count, coverage_status, status, provisional "
        "FROM served_features" + where + " ORDER BY network, window_start, window_end, token" + limit_clause,
        params,
    ).fetchall()
    names = ("network", "window_start", "window_end", "token", "generation", "swaps",
             "input_raw", "output_raw", "known_traders", "unknown_count", "coverage_status", "status", "provisional")
    return [dict(zip(names, row)) for row in rows]


def _range_clauses(window_from, window_to, clauses, params):
    """Half-open ``[window_from, window_to)`` filter on window_start (both or neither)."""
    if (window_from is None) != (window_to is None):
        raise ValueError("window range needs both bounds")
    if window_from is None:
        return
    if (type(window_from) is not int or type(window_to) is not int or window_from < 0
            or window_from % 300 or window_to % 300 or window_to <= window_from
            or window_to - window_from > dashboard_payload.LIMITS["time_range_seconds_max"]):
        raise ValueError("invalid window range")
    clauses.append("window_start >= %s AND window_start < %s")
    params.extend((window_from, window_to))


def read_windows(connection, *, network=None, window_start=None, window_end=None, token=None,
                 limit=None, after=None, fetch_extra=False, window_from=None, window_to=None):
    """Read bounded summaries of observed persisted windows, never completeness claims."""
    clauses, params = [], []
    _range_clauses(window_from, window_to, clauses, params)
    for name, value in (("network", network), ("window_start", window_start),
                        ("window_end", window_end), ("token", token)):
        if value is not None:
            clauses.append(f"{name}=%s")
            params.append(value)
    if after is not None:
        if (type(after) is not tuple or len(after) != 3
                or type(after[0]) is not str or after[0] not in ("mainnet", "testnet")
                or type(after[1]) is not int or type(after[2]) is not int
                or after[1] < 0 or after[2] > 9223372036854775807
                or after[2] != after[1] + 300):
            raise ValueError("invalid window keyset")
        clauses.append("(network, window_start, window_end) > (%s, %s, %s)")
        params.extend(after)
    if limit is not None:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("window read limit must be between 1 and 1000")
        if type(fetch_extra) is not bool:
            raise ValueError("fetch_extra must be boolean")
        params.append(limit + int(fetch_extra))
    elif fetch_extra:
        raise ValueError("fetch_extra requires a bounded limit")
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    limit_clause = " LIMIT %s" if limit is not None else ""
    rows = connection.execute(
        "SELECT network, window_start, window_end, count(*) AS feature_rows, "
        "count(*) FILTER (WHERE provisional) AS provisional_rows, "
        "count(DISTINCT generation) AS generation_count, "
        "array_agg(DISTINCT status ORDER BY status) AS statuses "
        "FROM served_features" + where + " GROUP BY network, window_start, window_end "
        "ORDER BY network, window_start, window_end" + limit_clause,
        params,
    ).fetchall()
    names = ("network", "window_start", "window_end", "feature_rows", "provisional_rows",
             "generation_count", "statuses")
    return [dict(zip(names, row)) for row in rows]


def _check_page(limit, fetch_extra):
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("read limit must be between 1 and 1000")
    if type(fetch_extra) is not bool:
        raise ValueError("fetch_extra must be boolean")
    return limit + int(fetch_extra)


def read_tokens(connection, *, network, window_from, window_to, limit, after=None,
                fetch_extra=False):
    """Tokens with observed activity in ``[window_from, window_to)``, bytewise by token.

    Counts describe persisted rows only (never completeness); bounded by range and limit.
    """
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    clauses, params = ["network=%s"], [network]
    if window_from is None or window_to is None:
        raise ValueError("window range needs both bounds")
    _range_clauses(window_from, window_to, clauses, params)
    if after is not None:
        if type(after) is not str or not after:
            raise ValueError("invalid token keyset")
        clauses.append('token COLLATE "C" > %s COLLATE "C"')
        params.append(after)
    params.append(_check_page(limit, fetch_extra))
    rows = connection.execute(
        "SELECT token, count(*) AS feature_rows, count(DISTINCT window_start) AS window_count, "
        "sum(swaps)::bigint AS swaps, min(window_start) AS first_window_start, "
        "max(window_end) AS latest_window_end FROM served_features WHERE "
        + " AND ".join(clauses) + ' GROUP BY token ORDER BY token COLLATE "C" LIMIT %s',
        params).fetchall()
    names = ("token", "feature_rows", "window_count", "swaps", "first_window_start",
             "latest_window_end")
    return [dict(zip(names, row)) for row in rows]


def read_series(connection, *, network, token, window_from, window_to, limit, after=None,
                fetch_extra=False):
    """One token's persisted windows in ``[window_from, window_to)``, ascending, no gap filling.

    ``qualification`` is None when no qualification is recorded for the window (unrecorded).
    """
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    if type(token) is not str or not token:
        raise ValueError("token must be nonempty text")
    if window_from is None or window_to is None:
        raise ValueError("window range needs both bounds")
    clauses, params = ["f.network=%s", 'f.token COLLATE "C" = %s COLLATE "C"'], [network, token]
    range_clauses = []
    _range_clauses(window_from, window_to, range_clauses, params)
    clauses.append(range_clauses[0].replace("window_start", "f.window_start"))
    if after is not None:
        if type(after) is not int or after < 0 or after % 300 or after > 9223372036854775807:
            raise ValueError("invalid series keyset")
        clauses.append("f.window_start > %s")
        params.append(after)
    params.append(_check_page(limit, fetch_extra))
    rows = connection.execute(
        "SELECT f.window_start, f.window_end, f.generation, f.swaps, f.input_raw::text, "
        "f.output_raw::text, f.known_traders, f.unknown_count, f.coverage_status, f.status, "
        "f.provisional, q.mode, q.synthetic, q.origin FROM served_features f "
        "LEFT JOIN action_window_qualification q "
        "ON q.network = f.network AND q.window_start = f.window_start WHERE "
        + " AND ".join(clauses) + " ORDER BY f.window_start LIMIT %s", params).fetchall()
    names = ("window_start", "window_end", "generation", "swaps", "input_raw", "output_raw",
             "known_traders", "unknown_count", "coverage_status", "status", "provisional")
    result = []
    for row in rows:
        item = dict(zip(names, row[:11]))
        item["qualification"] = None if row[11] is None else {"mode": row[11],
                                                               "synthetic": row[12]}
        # Row origin of the window; the API turns it into the point provenance.
        item["origin"] = (row[13] if row[11] is not None and len(row) > 13 else None) \
            or "unrecorded"
        result.append(item)
    return result


def coverage_state(*, has_row, row_provisional, pending, swaps, qualified, in_universe,
                   coverage_missing, token_seen=True):
    """Classify one window of one token into a grid state plus stable reason strings.

    Uses only facts the reader role can see. A window without a served row is never turned
    into a zero: with qualification it is ``provisional`` (zero is not provable from here),
    without it ``missing``. ``in_universe`` approximates admission by the token's first
    served row.
    """
    for name, value in (("has_row", has_row), ("row_provisional", row_provisional),
                        ("pending", pending), ("qualified", qualified),
                        ("in_universe", in_universe), ("coverage_missing", coverage_missing),
                        ("token_seen", token_seen)):
        if type(value) is not bool:
            raise ValueError(f"{name} must be boolean")
    if has_row != (swaps is not None) or (has_row and (type(swaps) is not int or swaps < 0)):
        raise ValueError("swaps must be a nonnegative integer exactly when there is a row")
    gaps = (["no_qualification"] if not qualified else []) + (
        ["coverage_missing"] if coverage_missing else [])
    reasons = (["pending_recompute"] if pending else [])
    if has_row:
        if row_provisional:
            reasons.append("row_provisional")
        if reasons:
            return "provisional", reasons
        if gaps:
            return "partial", gaps
        return ("zero" if swaps == 0 else "observed"), []
    if pending:
        return "provisional", reasons
    if not in_universe:
        return "pre_universe", ["before_first_served_row" if token_seen
                                else "token_has_no_served_rows"]
    if gaps:
        return "missing", gaps
    return "provisional", ["qualified_without_served_row"]


def read_coverage(connection, *, network, token, window_from, window_to, limit, after=None,
                  fetch_extra=False):
    """Every five-minute window of one token in ``[window_from, window_to)``, ascending.

    Gaps are listed with their state (``missing`` / ``pre_universe``), never filled. Reads
    only reader-role tables; lineage is reduced to a count and refusal/coverage reasons are
    returned verbatim.
    """
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    if type(token) is not str or not token:
        raise ValueError("token must be nonempty text")
    if window_from is None or window_to is None:
        raise ValueError("window range needs both bounds")
    _range_clauses(window_from, window_to, [], [])
    first = window_from
    if after is not None:
        if (type(after) is not int or after < window_from or after >= window_to or after % 300):
            raise ValueError("invalid coverage keyset")
        first = after + 300
    page = _check_page(limit, fetch_extra)
    if first >= window_to:
        return []
    rows = connection.execute(
        "SELECT g.ws, f.swaps, f.known_traders, f.unknown_count, f.coverage_status, f.provisional,"
        " q.mode, q.synthetic, q.mapping_version, q.delta_seconds,"
        " (SELECT coalesce(array_agg(DISTINCT c.reason), '{}') FROM action_window_coverage c"
        "  WHERE c.network = %(n)s AND c.window_start < g.ws + 300 AND c.window_end > g.ws),"
        " (SELECT coalesce(sum(jsonb_array_length(c.lineage)), 0) FROM action_window_coverage c"
        "  WHERE c.network = %(n)s AND c.window_start < g.ws + 300 AND c.window_end > g.ws),"
        " (SELECT count(*) FROM window_admission_refusals r"
        "  WHERE r.network = %(n)s AND r.window_start = g.ws),"
        " (SELECT coalesce(array_agg(DISTINCT x), '{}') FROM window_admission_refusals r,"
        "  jsonb_array_elements_text(r.reasons) x WHERE r.network = %(n)s AND r.window_start = g.ws),"
        " EXISTS (SELECT 1 FROM dirty_windows d WHERE d.network = %(n)s"
        "  AND d.token COLLATE \"C\" = %(t)s COLLATE \"C\" AND d.window_start = g.ws"
        "  AND d.role = 'target' AND d.status = 'pending'),"
        " (SELECT min(s.window_start) FROM served_features s WHERE s.network = %(n)s"
        "  AND s.token COLLATE \"C\" = %(t)s COLLATE \"C\")"
        " FROM generate_series(%(lo)s::bigint, %(hi)s::bigint, 300) AS g(ws)"
        " LEFT JOIN served_features f ON f.network = %(n)s AND f.window_start = g.ws"
        "  AND f.token COLLATE \"C\" = %(t)s COLLATE \"C\""
        " LEFT JOIN action_window_qualification q ON q.network = %(n)s AND q.window_start = g.ws"
        " ORDER BY g.ws LIMIT %(limit)s",
        {"n": network, "t": token, "lo": first, "hi": window_to - 300, "limit": page}).fetchall()
    result = []
    for (ws, swaps, known, unknown, cov_status, row_prov, mode, synthetic, mapping, delta,
         missing, lineage, refusals, refusal_reasons, pending, first_served) in rows:
        has_row = swaps is not None
        missing = sorted(missing, key=lambda r: r.encode("utf-8"))
        state, reasons = coverage_state(
            has_row=has_row, row_provisional=bool(row_prov), pending=bool(pending),
            swaps=int(swaps) if has_row else None, qualified=mode is not None,
            in_universe=first_served is not None and ws >= first_served,
            coverage_missing=bool(missing), token_seen=first_served is not None)
        result.append({
            "window_start": ws, "window_end": ws + 300, "state": state, "reasons": reasons,
            "qualification": None if mode is None else {
                "mode": mode, "synthetic": synthetic, "mapping_version": mapping,
                "delta_seconds": delta},
            "missing": missing,
            "admission_refused": int(refusals) > 0,
            "admission_reasons": sorted(refusal_reasons, key=lambda r: r.encode("utf-8")),
            "served": {"rows": int(has_row),
                       "coverage_status_counts": {cov_status: 1} if has_row and cov_status else {},
                       "known_traders": known, "unknown_count": unknown},
            "lineage_records": int(lineage)})
    return result


# --------------------------------------------------------------------------------------------
# Current detector results and alert state (read-only).
def _detector_filters(network, token, window_start, prefix="c."):
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    clauses, params = [f"{prefix}network=%s"], [network]
    if token is not None:
        if type(token) is not str or not token:
            raise ValueError("token must be nonempty text")
        clauses.append(f"{prefix}token=%s")
        params.append(token)
    if window_start is not None:
        if type(window_start) is not int or window_start < 0 or window_start % 300:
            raise ValueError("window_start must be a five-minute boundary")
        clauses.append(f"{prefix}window_start=%s")
        params.append(window_start)
    return clauses, params


# Keyset order of the current detector views. ``network`` is a mandatory equality filter, so
# the continuation key is (token, window_start, detector_id, direction); text compares bytewise.
_DETECTOR_ORDER = (" ORDER BY c.network, c.token COLLATE \"C\", c.window_start, "
                   "c.detector_id COLLATE \"C\", c.direction COLLATE \"C\"")


def _detector_keyset(after, clauses, params):
    if after is None:
        return
    if (type(after) is not tuple or len(after) != 4
            or type(after[0]) is not str or not after[0]
            or type(after[1]) is not int or after[1] < 0 or after[1] % 300
            or after[1] > 9223372036854775807
            or type(after[2]) is not str or not after[2]
            or type(after[3]) is not str or not after[3]):
        raise ValueError("invalid detector keyset")
    clauses.append("(c.token COLLATE \"C\", c.window_start, c.detector_id COLLATE \"C\", "
                   "c.direction COLLATE \"C\") > (%s, %s, %s, %s)")
    params.extend(after)


def read_detector_results(connection, *, network, token=None, window_start=None,
                          limit=1000, fetch_extra=False, after=None):
    """Current, fresh detector result documents.

    Stale results (an input or config changed and recomputation has not promoted yet) and
    ``pending`` results are never current; earlier revisions stay audit-only.
    """
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("detector result read limit must be between 1 and 1000")
    if type(fetch_extra) is not bool:
        raise ValueError("fetch_extra must be boolean")
    clauses, params = _detector_filters(network, token, window_start)
    _detector_keyset(after, clauses, params)
    rows = connection.execute(
        "SELECT r.result FROM detector_current c JOIN detector_results r "
        "USING (network,token,direction,window_start,detector_id,result_revision) "
        "WHERE NOT c.stale AND r.status <> 'pending' AND " + " AND ".join(clauses)
        + _DETECTOR_ORDER + " LIMIT %s", (*params, limit + int(fetch_extra))).fetchall()
    return [row[0] for row in rows]


def read_detection_range(connection, *, network, token, window_from, window_to, limit,
                         after=None, fetch_extra=False):
    """Current, fresh detector results of one token in ``[window_from, window_to)``.

    Same visibility as ``read_detector_results``: stale and ``pending`` results stay hidden.
    Ordered by (window_start, detector, direction); ``after`` is that triple.
    """
    if token is None:
        raise ValueError("a detection range needs a token")
    if window_from is None or window_to is None:
        raise ValueError("window range needs both bounds")
    clauses, params = _detector_filters(network, token, None)
    range_clauses = []
    _range_clauses(window_from, window_to, range_clauses, params)
    clauses.append(range_clauses[0].replace("window_start", "c.window_start"))
    if after is not None:
        if (type(after) is not tuple or len(after) != 3 or type(after[0]) is not int
                or after[0] < 0 or after[0] % 300 or after[0] > 9223372036854775807
                or type(after[1]) is not str or not after[1]
                or type(after[2]) is not str or not after[2]):
            raise ValueError("invalid detection range keyset")
        clauses.append('(c.window_start, c.detector_id COLLATE "C", c.direction COLLATE "C") '
                       '> (%s, %s, %s)')
        params.extend(after)
    params.append(_check_page(limit, fetch_extra))
    rows = connection.execute(
        "SELECT r.result FROM detector_current c JOIN detector_results r "
        "USING (network,token,direction,window_start,detector_id,result_revision) "
        "WHERE NOT c.stale AND r.status <> 'pending' AND " + " AND ".join(clauses)
        + ' ORDER BY c.window_start, c.detector_id COLLATE "C", c.direction COLLATE "C" LIMIT %s',
        params).fetchall()
    return [row[0] for row in rows]


def read_result_revisions(connection, *, network, token, window_start, detector, limit,
                          after=None, fetch_extra=False):
    """Every result revision of one key, ascending (audit; nothing is hidden, all are labelled).

    ``label`` is ``current``, ``stale`` (the current pointer is flagged stale), ``pending``
    (a pending result) or ``superseded`` (an older revision); ``alert_state`` is the latest
    alert state recorded for that revision (``retracted`` included) or None.
    """
    clauses, params = _detector_filters(network, token, window_start, prefix="r.")
    if token is None or window_start is None or type(detector) is not str or not detector:
        raise ValueError("a revision listing needs token, window_start and detector")
    if after is not None:
        if type(after) is not int or not 1 <= after <= 9223372036854775807:
            raise ValueError("invalid result revision keyset")
        clauses.append("r.result_revision > %s")
        params.append(after)
    rows = connection.execute(
        "SELECT r.result_revision, r.status, r.triggered, r.score::text, r.input_digest, "
        "r.policy_id, r.dependency_epoch, "
        "to_char(r.promoted_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), "
        "r.result->>'explanation', (c.result_revision IS NOT NULL) AS is_current, "
        "coalesce(c.stale, false) AS pointer_stale, a.state "
        "FROM detector_results r LEFT JOIN detector_current c ON (c.network,c.token,"
        "c.direction,c.window_start,c.detector_id,c.result_revision)=(r.network,r.token,"
        "r.direction,r.window_start,r.detector_id,r.result_revision) "
        "LEFT JOIN LATERAL (SELECT x.state FROM alert_revisions x WHERE x.network=r.network "
        " AND x.token=r.token AND x.direction=r.direction AND x.window_start=r.window_start "
        " AND x.detector_id=r.detector_id AND x.result_revision=r.result_revision "
        " ORDER BY x.alert_revision DESC LIMIT 1) a ON true "
        "WHERE r.direction='both' AND r.detector_id=%s AND " + " AND ".join(clauses)
        + " ORDER BY r.result_revision LIMIT %s",
        (detector, *params, _check_page(limit, fetch_extra))).fetchall()
    result = []
    for row in rows:
        is_current, pointer_stale = row[9], row[10]
        label = ("pending" if row[1] == "pending" else "stale" if is_current and pointer_stale
                 else "current" if is_current else "superseded")
        result.append({
            "result_revision": row[0], "status": row[1], "triggered": row[2], "score": row[3],
            "input_digest": row[4], "policy_id": row[5], "dependency_epoch": row[6],
            "promoted_at": row[7], "explanation": row[8], "is_current": is_current,
            "stale": label != "current", "label": label, "alert_state": row[11]})
    return result


def read_alerts(connection, *, network, token=None, window_start=None, limit=1000,
                fetch_extra=False, after=None):
    """Currently active alerts: latest revision ``active`` on the current, fresh result."""
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("alert read limit must be between 1 and 1000")
    if type(fetch_extra) is not bool:
        raise ValueError("fetch_extra must be boolean")
    clauses, params = _detector_filters(network, token, window_start)
    _detector_keyset(after, clauses, params)
    rows = connection.execute(
        "SELECT c.network, c.token, c.direction, c.window_start, c.detector_id, "
        "a.alert_revision, a.state, a.result_revision, a.input_digest, a.dependency_epoch, "
        "r.score::text, r.result->>'explanation' FROM detector_current c "
        "JOIN LATERAL (SELECT * FROM alert_revisions x WHERE x.network=c.network "
        " AND x.token=c.token AND x.direction=c.direction AND x.window_start=c.window_start "
        " AND x.detector_id=c.detector_id ORDER BY x.alert_revision DESC LIMIT 1) a ON true "
        "JOIN detector_results r ON (r.network,r.token,r.direction,r.window_start,"
        "r.detector_id,r.result_revision)=(c.network,c.token,c.direction,c.window_start,"
        "c.detector_id,c.result_revision) "
        "WHERE a.state='active' AND NOT c.stale AND a.result_revision=c.result_revision AND "
        + " AND ".join(clauses) + _DETECTOR_ORDER + " LIMIT %s",
        (*params, limit + int(fetch_extra))).fetchall()
    names = ("network", "token", "direction", "window_start", "detector", "alert_revision",
             "state", "result_revision", "input_digest", "dependency_epoch", "score",
             "explanation")
    return [dict(zip(names, row)) for row in rows]


def read_alert_history(connection, *, network, token, window_start, detector, limit=1000,
                       fetch_extra=False, after=None):
    """Alert revisions of one key, oldest first, at most ``limit`` (audit only)."""
    if type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("alert history read limit must be between 1 and 1000")
    if type(fetch_extra) is not bool:
        raise ValueError("fetch_extra must be boolean")
    clauses, params = _detector_filters(network, token, window_start, prefix="")
    if after is not None:
        if type(after) is not int or not 1 <= after <= 9223372036854775807:
            raise ValueError("invalid alert history keyset")
        clauses.append("alert_revision > %s")
        params.append(after)
    rows = connection.execute(
        "SELECT alert_revision, state, result_revision, input_digest, dependency_epoch, reason "
        "FROM alert_revisions WHERE " + " AND ".join(clauses) + " AND detector_id=%s "
        "ORDER BY alert_revision LIMIT %s",
        (*params, detector, limit + int(fetch_extra))).fetchall()
    names = ("alert_revision", "state", "result_revision", "input_digest", "dependency_epoch",
             "reason")
    return [dict(zip(names, row)) for row in rows]


_FEED_TIME = "YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\""


def read_alert_feed(connection, *, network, window_from, window_to, limit, after=None,
                    fetch_extra=False):
    """Global alert-revision feed, newest recorded first, for ``window_start`` in [from, to).

    Every revision is listed with its ``label``: ``superseded`` (a later revision of the same
    key exists), ``current`` (latest and its result is the fresh current one) or ``stale``
    (latest, but its result is no longer current or is flagged stale). Retracted, pending and
    inactive states are listed, never hidden. ``after`` is the last key
    ``(recorded_at, token, window_start, detector, alert_revision)``. No lease fields are read.
    """
    if (type(window_from) is not int or type(window_to) is not int or window_from % 300
            or window_to % 300 or window_to <= window_from):
        raise ValueError("invalid feed range")
    clauses = ["a.network = %s", "a.direction = 'both'", "a.window_start >= %s",
               "a.window_start < %s"]
    params = [network, window_from, window_to]
    if after is not None:
        if (len(after) != 5 or type(after[0]) is not str or type(after[1]) is not str
                or type(after[2]) is not int or type(after[3]) is not str
                or type(after[4]) is not int or after[4] < 1):
            raise ValueError("invalid alert feed keyset")
        clauses.append('(a.created_at, a.token COLLATE "C", a.window_start, '
                       'a.detector_id COLLATE "C", a.alert_revision) < '
                       '(%s::timestamptz, %s COLLATE "C", %s, %s COLLATE "C", %s)')
        params += list(after)
    rows = connection.execute(
        "SELECT a.token, a.window_start, a.detector_id, a.alert_revision, a.state, "
        "a.result_revision, to_char(a.created_at AT TIME ZONE 'UTC', '" + _FEED_TIME + "'), "
        "CASE WHEN EXISTS (SELECT 1 FROM alert_revisions b WHERE (b.network,b.token,b.direction,"
        "b.window_start,b.detector_id)=(a.network,a.token,a.direction,a.window_start,"
        "a.detector_id) AND b.alert_revision > a.alert_revision) THEN 'superseded' "
        "WHEN c.result_revision = a.result_revision AND NOT c.stale THEN 'current' "
        "ELSE 'stale' END "
        "FROM alert_revisions a LEFT JOIN detector_current c ON (c.network,c.token,c.direction,"
        "c.window_start,c.detector_id)=(a.network,a.token,a.direction,a.window_start,"
        "a.detector_id) WHERE " + " AND ".join(clauses)
        + ' ORDER BY a.created_at DESC, a.token COLLATE "C" DESC, a.window_start DESC, '
        'a.detector_id COLLATE "C" DESC, a.alert_revision DESC LIMIT %s',
        (*params, _check_page(limit, fetch_extra))).fetchall()
    names = ("token", "window_start", "detector", "alert_revision", "state", "result_revision",
             "recorded_at", "label")
    return [dict(zip(names, row)) for row in rows]


def _optional(connection, statement, parameters=None):
    """Extra read-only observation; a failure is unavailable (None), never a zero."""
    try:
        with connection.transaction():
            return list(connection.execute(statement, parameters).fetchall())
    except Exception:
        return None


BASELINE_MAX_LIMIT = 10
_BASELINE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_GRID_STATES = ("observed", "zero", "partial", "provisional", "missing", "pre_universe")
_ELIGIBLE_STATES = frozenset({"observed", "zero"})
# Allowlisted cell fields: counts, states and coverage only. No scope, revision or lineage.
_CELL_FIELDS = ("state", "swaps", "known_traders", "unknown_count", "coverage", "synthetic")


def _project_cells(runs, expected):
    """Expand run-length encoded cells to one allowlisted cell per five-minute window."""
    if not isinstance(runs, list) or len(runs) > expected:
        raise ValueError("stored baseline cells are malformed")
    cells = []
    for run in runs:
        start, end = run["start"], run["end"]
        if (type(start) is not int or type(end) is not int or end <= start or start % 300
                or end % 300 or run["state"] not in _GRID_STATES):
            raise ValueError("stored baseline cell is malformed")
        for window in range(start, end, 300):
            if len(cells) >= expected:
                raise ValueError("stored baseline cells exceed the lookback")
            cells.append({"window_start": window, **{k: run[k] for k in _CELL_FIELDS}})
    return cells


def read_baseline_inputs(connection, *, network, input_digest=None, token=None,
                         window_start=None, limit, fetch_extra=False):
    """Pinned baseline inputs of one detector result, newest pin first.

    Select by ``input_digest`` (one pin) or by ``token`` + ``window_start`` (every pin of that
    scored window, at most ``BASELINE_MAX_LIMIT``). Only allowlisted columns are read: never
    ``lease_owner``, ``lease_fence`` or ``active_epoch``; cells are projected to counts, states
    and coverage.
    """
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    by_digest = input_digest is not None
    if by_digest:
        if (token is not None or window_start is not None or type(input_digest) is not str
                or _BASELINE_DIGEST.fullmatch(input_digest) is None):
            raise ValueError("select by a sha256 digest alone")
        where, params = "network=%s AND input_digest=%s", [network, input_digest]
    else:
        if (type(token) is not str or not token or type(window_start) is not int
                or window_start < 0 or window_start % 300 or window_start > 9223372036854775807):
            raise ValueError("select by digest or by token and aligned window_start")
        where = 'network=%s AND token COLLATE "C"=%s COLLATE "C" AND window_start=%s'
        params = [network, token, window_start]
    if type(limit) is not int or not 1 <= limit <= BASELINE_MAX_LIMIT:
        raise ValueError(f"baseline limit must be between 1 and {BASELINE_MAX_LIMIT}")
    if type(fetch_extra) is not bool:
        raise ValueError("fetch_extra must be boolean")
    params.append(limit + int(fetch_extra))
    rows = connection.execute(
        "SELECT input_digest, network, token, window_start, policy_id, lookback_seconds, "
        "min_prior_windows, universe_version, admitted_window_start, dependency_epoch, status, "
        "eligible_count, certification, "
        "to_char(pinned_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'), "
        "current_cell, cells FROM baseline_inputs WHERE " + where
        + " ORDER BY pinned_at DESC, input_digest LIMIT %s", params).fetchall()
    result = []
    for row in rows:
        lookback = row[5]
        expected = lookback // 300
        cells = _project_cells(row[15], expected)
        (current,) = _project_cells([row[14]], 1)
        by_state = dict.fromkeys(_GRID_STATES, 0)
        for cell in cells:
            by_state[cell["state"]] += 1
        result.append({
            "input_digest": row[0], "network": row[1], "token": row[2], "window_start": row[3],
            "policy": {"policy_id": row[4], "lookback_seconds": lookback,
                       "min_prior_windows": row[6]},
            "universe_version": row[7], "admitted_window_start": row[8],
            "dependency_epoch": row[9], "status": row[10], "eligible_count": row[11],
            "certification": row[12], "pinned_at": row[13], "current_cell": current,
            "cells": cells,
            "coverage_counts": {
                "cells_total": len(cells), "expected": expected, "by_state": by_state,
                "eligible": sum(by_state[s] for s in _ELIGIBLE_STATES)}})
    return result


_ORIGIN_SQL = (
    "SELECT coalesce(q.origin, 'unrecorded'), count(*) FROM (SELECT DISTINCT network, "
    "window_start FROM served_features WHERE (%(n)s::text IS NULL OR network=%(n)s) "
    "AND (%(f)s::bigint IS NULL OR window_start >= %(f)s) "
    "AND (%(t)s::bigint IS NULL OR window_start < %(t)s)) s "
    "LEFT JOIN action_window_qualification q "
    "ON q.network = s.network AND q.window_start = s.window_start GROUP BY 1")


def read_provenance(connection, *, network=None, window_from=None, window_to=None) -> dict:
    """Provenance block from recorded qualification and result counts.

    The source kind follows the recorded row origin of the served windows in scope
    (``network`` and ``[window_from, window_to)`` when given, else every served window).
    """
    qualification = _optional(
        connection, "SELECT mode, synthetic, count(*) FROM action_window_qualification GROUP BY 1, 2")
    scored = _optional(connection, "SELECT count(*) FROM detector_results")
    origins = _optional(connection, _ORIGIN_SQL,
                        {"n": network, "f": window_from, "t": window_to})
    return dashboard_payload.build_provenance(
        qualification=qualification, scored=None if scored is None else int(scored[0][0]),
        origins=origins)


QUALITY_MAX_GROUPS = 20
QUALITY_LABEL = (
    "Counts of recorded rows for one network and range, read through the reader role. They do "
    "not show that the data is complete. A counter marked unavailable could not be read or is "
    "not scoped to a network; it is never zero.")


def _quality_unavailable(reason):
    return {"status": "unavailable", "value": None, "reason": reason}


def _quality_rows(connection, statement, parameters):
    """One savepointed sub-query; ``None`` when it fails (permission, missing table, ...)."""
    try:
        with connection.transaction():
            return list(connection.execute(statement, parameters).fetchall())
    except Exception:
        return None


def _quality_count(connection, statement, parameters):
    rows = _quality_rows(connection, statement, parameters)
    if not rows:
        return _quality_unavailable("query_failed")
    return {"status": "observed", "value": int(rows[0][0])}


def _quality_groups(connection, statement, parameters):
    rows = _quality_rows(connection, statement + f" LIMIT {QUALITY_MAX_GROUPS + 1}", parameters)
    if rows is None:
        return _quality_unavailable("query_failed")
    return {"status": "observed",
            "value": {str(key): int(count) for key, count in rows[:QUALITY_MAX_GROUPS]},
            "truncated": len(rows) > QUALITY_MAX_GROUPS}


def read_quality(connection, *, network, window_from, window_to) -> dict:
    """Bounded data-quality counters for one network and half-open ``[from, to)`` range.

    Reader-role tables only. Each counter runs in its own savepoint; one that fails is
    ``unavailable`` with a reason and never a zero. Tables that carry no network column
    (scan_quarantine, coverage_ranges) are reported unavailable instead of globally counted.
    """
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    if (type(window_from) is not int or type(window_to) is not int or window_from < 0
            or window_from % 300 or window_to % 300 or window_to <= window_from
            or window_to - window_from > 7 * 86400):
        raise ValueError("invalid time range")
    span = (network, window_from, window_to)
    in_range = "network = %s AND window_start >= %s AND window_start < %s"
    result = {
        "served_rows_by_coverage_status": _quality_groups(
            connection, "SELECT coalesce(coverage_status, 'unrecorded') AS k, count(*) "
            f"FROM served_features WHERE {in_range} GROUP BY 1 ORDER BY 1", span),
        "served_windows": _quality_count(
            connection, f"SELECT count(DISTINCT window_start) FROM served_features "
            f"WHERE {in_range}", span),
        "dirty_windows_pending_by_role": _quality_groups(
            connection, f"SELECT role, count(*) FROM dirty_windows WHERE {in_range} "
            "AND status = 'pending' GROUP BY 1 ORDER BY 1", span),
        "revision_quarantine_open": _quality_count(
            connection, "SELECT count(*) FROM revision_quarantine WHERE network = %s "
            "AND status = 'open'", (network,)),
        "delivery_quarantine": _quality_count(
            connection, f"SELECT count(*) FROM delivery_quarantine WHERE {in_range}", span),
        "scan_quarantine": _quality_unavailable("not_network_scoped"),
        "coverage_ranges_pending": _quality_unavailable("not_network_scoped"),
        "coverage_ranges_bounded_observed": _quality_unavailable("not_network_scoped"),
    }
    rows = _quality_rows(
        connection, "SELECT mode, synthetic, count(*) FROM action_window_qualification "
        f"WHERE {in_range} GROUP BY 1, 2", span)
    if rows is None:
        result["qualifications_by_class"] = _quality_unavailable("query_failed")
    else:
        classes = {"synthetic_bounded_observed": 0, "bounded_observed": 0, "certified_claimed": 0}
        for mode, synthetic, count in rows:
            key = ("certified_claimed" if mode == "certified" else
                   "synthetic_bounded_observed" if synthetic else "bounded_observed")
            classes[key] += int(count)
        result["qualifications_by_class"] = {"status": "observed", "value": classes}
    return result


PIPELINE_LABEL = (
    "Counts of recorded rows read through the reader role. They do not measure ingestion "
    "freshness (not_measured). Kafka lag is measured only by the api's broker probe when one is "
    "configured, otherwise it is not_measured. A section marked unavailable could not be read; "
    "it is never zero. Outbox, stream progress and Kafka lag are not scoped to a network.")


def _pipeline_not_measured(reason):
    return {"status": "not_measured", "value": None, "reason": reason}


def _pipeline_count(connection, statement, parameters, *, age=False):
    """One savepointed count (optionally with an oldest age); unavailable when it cannot be read."""
    rows = _quality_rows(connection, statement, parameters)
    if not rows:
        return _quality_unavailable("query_failed")
    result = {"status": "observed", "value": int(rows[0][0])}
    if age:
        result["oldest_age_seconds"] = None if rows[0][1] is None else float(rows[0][1])
    return result


def read_pipeline(connection, *, network) -> dict:
    """Read-only pipeline status for one network, reader-role tables only.

    Each section runs in its own savepoint; one that fails is ``unavailable`` with a reason and
    never zero. The scoring backlog counts both never-scored served windows and stale results
    (same predicates as ``score_pending``). Ingestion freshness and Kafka lag are not measured
    here and are reported as such, never inferred. Stream progress exposes the newest commit
    timestamp and record count only: no offsets, ids, topics, digests or leases.
    """
    if network not in ("mainnet", "testnet"):
        raise ValueError("network must be mainnet or testnet")
    scope = (network,)
    outbox = _pipeline_count(
        connection, "SELECT count(*), extract(epoch FROM clock_timestamp() - min(created_at)) "
        "FROM outbox WHERE published_at IS NULL", (), age=True)
    outbox["scope"] = "all_networks"
    stream = _quality_rows(
        connection, "SELECT to_char(committed_at AT TIME ZONE 'UTC', "
        "'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), record_count FROM stream_batches "
        "ORDER BY committed_at DESC LIMIT 1", ())
    if stream is None:
        stream_progress = _quality_unavailable("query_failed")
    elif not stream:
        stream_progress = {"status": "none_recorded", "value": None}
    else:
        stream_progress = {"status": "observed", "value": {
            "committed_at": str(stream[0][0]), "record_count": int(stream[0][1])}}
    stream_progress["scope"] = "all_networks"
    return {
        "outbox": outbox,
        "dirty_windows_pending_by_role": _quality_groups(
            connection, "SELECT role, count(*) FROM dirty_windows WHERE network = %s "
            "AND status = 'pending' GROUP BY 1 ORDER BY 1", scope),
        "scoring_backlog": {"status": "observed", "value": {
            "unscored_served_windows": _pipeline_count(
                connection, "SELECT count(*) FROM served_features s WHERE s.network = %s "
                "AND NOT EXISTS (SELECT 1 FROM detector_current c WHERE c.network = s.network "
                "AND c.token = s.token AND c.window_start = s.window_start)", scope),
            "stale_results": _pipeline_count(
                connection, "SELECT count(DISTINCT (network, window_start, token)), "
                "extract(epoch FROM clock_timestamp() - min(updated_at)) "
                "FROM detector_current WHERE stale AND network = %s", scope, age=True),
        }},
        "stream_progress": stream_progress,
        "ingestion_freshness": _pipeline_not_measured("no_ingestion_clock_recorded"),
        "kafka_lag": _pipeline_not_measured("kafka_not_queried_by_reader"),
    }


TELEMETRY_LABEL = (
    "Storage sizes come from PostgreSQL size functions; last recorded activity is the newest "
    "timestamp written to a table, not ingestion freshness. not_measured, unsupported and "
    "unavailable values are never zero.")
_ACTIVITY_MEANING = "last_recorded_activity_not_ingestion_freshness"
_ISO = "to_char({} AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')"
_LARGEST_TABLES = 5


def _telemetry_unmeasured(status, reason):
    return {"status": status, "value": None, "reason": reason, "provenance": "not queried"}


def _telemetry_activity(connection, table, column):
    base = {"meaning": _ACTIVITY_MEANING, "at": None,
            "provenance": f"max({table}.{column}) read through the reader role"}
    rows = _quality_rows(
        connection, f"SELECT {_ISO.format(f'max({column})')} FROM {table}", ())
    if rows is None:
        return dict(base, status="unavailable", reason="query_failed")
    if not rows or rows[0][0] is None:
        return dict(base, status="none_recorded")
    return dict(base, status="observed", at=str(rows[0][0]))


def read_telemetry(connection) -> dict:
    """Measured storage and last-recorded-activity telemetry for /status.

    Every section runs in its own savepoint and is ``unavailable`` with a reason when it cannot
    be read, never zero. Kafka lag, Spark state, maintenance and rates are not measured by the
    API (no broker credentials, no checkpoint access) and disk free space is not visible from a
    database session; they are reported as such. Only reader-allowlisted table names are listed.
    """
    size = _quality_rows(connection, "SELECT pg_database_size(current_database())", ())
    if not size:
        database_size = _quality_unavailable("query_failed")
    else:
        database_size = {"status": "observed", "bytes": int(size[0][0]), "scope": "whole_database",
                         "provenance": "pg_database_size(current_database())"}
    tables = _quality_rows(
        connection,
        "SELECT t.name, pg_total_relation_size(to_regclass(quote_ident(current_schema()) "
        "|| '.' || quote_ident(t.name))) AS bytes FROM unnest(%s::text[]) AS t(name) "
        "WHERE to_regclass(quote_ident(current_schema()) || '.' || quote_ident(t.name)) "
        "IS NOT NULL ORDER BY 2 DESC, 1 LIMIT %s",
        (sorted(READER_TABLES), _LARGEST_TABLES))
    if tables is None:
        largest = _quality_unavailable("query_failed")
    else:
        largest = {"status": "observed",
                   "scope": "reader_allowlisted_tables_in_current_schema",
                   "provenance": "pg_total_relation_size over the reader allowlist",
                   "value": [{"table": str(n), "total_bytes": int(b)} for n, b in tables]}
    canonical = {"meaning": _ACTIVITY_MEANING, "at": None, "status": "unavailable",
                 "reason": "table_not_in_reader_allowlist",
                 "provenance": "canonical_actions is not granted to the reader role"}
    return {
        "spark_progress": _telemetry_spark_progress(connection),
        "maintenance": _telemetry_maintenance(connection),
        "rates": _telemetry_rates(connection),
        # Spark state store / watermark live in the checkpoint directory of a consumer run; the
        # stack has no long-running consumer service with a stable checkpoint location to mount.
        "spark_state": _telemetry_unmeasured(
            "not_measured", "no_persistent_checkpoint_location_in_stack"),
        # The per-scope ingestion clock is in source_ranges/canonical_actions, which the reader
        # role cannot read; granting them would widen the reader role.
        "ingestion_freshness": _telemetry_unmeasured(
            "not_measured", "ingestion_clock_tables_not_granted_to_reader"),
        # Replaced by ton_sentinel.telemetry_probes when the api is configured with the probes.
        "kafka_lag": _telemetry_unmeasured("not_measured", "broker_probe_not_configured"),
        "disk": _telemetry_unmeasured("not_measured", "database_volume_not_mounted"),
        **_telemetry_db_sections(connection, database_size, largest, canonical),
    }


_SPARK_QUERIES = 5
_RATE_WINDOW_SECONDS = 3600


def _telemetry_spark_progress(connection):
    """Newest audited batch per Spark query from stream_batches (PostgreSQL), not the checkpoint."""
    base = {"scope": "latest_batch_per_spark_query",
            "provenance": "stream_batches audit rows (committed Spark batch offsets mirrored in "
                          "PostgreSQL); not the checkpoint directory"}
    rows = _quality_rows(
        connection,
        "SELECT t.query_id, t.batch_id, t.topic, " + _ISO.format("t.committed_at") + ", t.audited, "
        "(SELECT jsonb_object_agg(x.part, x.last_offset) FROM (SELECT (e ->> 'partition')::int "
        "AS part, max((e ->> 'offset')::bigint) AS last_offset FROM stream_batches b, "
        "jsonb_array_elements(CASE WHEN jsonb_typeof(b.offset_metadata) = 'array' "
        "THEN b.offset_metadata ELSE '[]'::jsonb END) AS e WHERE b.environment_id = "
        "t.environment_id AND b.query_id = t.query_id AND b.topic = t.topic "
        "AND e ->> 'topic' = t.topic GROUP BY 1) x) "
        "FROM (SELECT * FROM (SELECT DISTINCT ON (environment_id, query_id) environment_id, "
        "query_id, batch_id, topic, committed_at, jsonb_typeof(offset_metadata) = 'array' "
        "AS audited FROM stream_batches ORDER BY environment_id, query_id, committed_at DESC, "
        "batch_id DESC) AS latest ORDER BY committed_at DESC, query_id LIMIT %s) AS t "
        "ORDER BY t.committed_at DESC, t.query_id", (_SPARK_QUERIES,))
    if rows is None:
        return dict(base, status="unavailable", reason="query_failed", value=None)
    if not rows:
        return dict(base, status="none_recorded", value=None)
    try:
        value = []
        for query_id, batch_id, topic, committed_at, audited, offsets in rows:
            partitions = None
            if audited:
                partitions = sorted(({"partition": int(p), "next_offset": int(o) + 1}
                                     for p, o in (offsets or {}).items()),
                                    key=lambda item: item["partition"])
            value.append({"query_id": str(query_id), "batch_id": int(batch_id),
                          "topic": str(topic), "committed_at": str(committed_at),
                          "offsets_audited": bool(audited), "partitions": partitions})
    except (TypeError, ValueError, AttributeError):
        return dict(base, status="unavailable", reason="malformed_row", value=None)
    return dict(base, status="observed", value=value)


def _telemetry_maintenance(connection):
    base = {"scope": "retention_cycle_only", "not_covered": ["vacuum", "backup"],
            "provenance": "max(retention_runs.recorded_at) of completed runs "
                          "(counts_after recorded) per kind and network"}
    rows = _quality_rows(
        connection,
        "SELECT kind, network, " + _ISO.format("max(recorded_at)") + ", count(*) "
        "FROM retention_runs WHERE counts_after IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2 LIMIT 12",
        ())
    if rows is None:
        return dict(base, status="unavailable", reason="query_failed", value=None)
    if not rows:
        return dict(base, status="none_recorded", value=None)
    try:
        value = [{"kind": str(k), "network": str(n), "last_completed_at": str(at),
                  "completed_runs": int(c)} for k, n, at, c in rows]
    except (TypeError, ValueError):
        return dict(base, status="unavailable", reason="malformed_row", value=None)
    return dict(base, status="observed", value=value)


def _telemetry_rates(connection):
    base = {"window_seconds": _RATE_WINDOW_SECONDS,
            "meaning": "rows recorded in the trailing window that are still present; counts, "
                       "not a per-second rate, and not extrapolated",
            "provenance": "count/sum over stream_batches.committed_at and outbox.created_at"}
    since = "clock_timestamp() - make_interval(secs => %s)"
    rows = _quality_rows(
        connection,
        f"SELECT (SELECT coalesce(sum(record_count), 0) FROM stream_batches "
        f"WHERE committed_at >= {since}), (SELECT count(*) FROM stream_batches "
        f"WHERE committed_at >= {since}), (SELECT count(*) FROM outbox "
        f"WHERE created_at >= {since}), " + _ISO.format("clock_timestamp()"),
        (_RATE_WINDOW_SECONDS,) * 3)
    try:
        if not rows:
            raise ValueError("no row")
        records, batches, outbox, as_of = rows[0]
        value = {"stream_records": int(records), "stream_batches": int(batches),
                 "outbox_rows": int(outbox)}
        if as_of is None or min(value.values()) < 0:
            raise ValueError("incomplete")
    except (TypeError, ValueError, IndexError):
        return dict(base, status="unavailable", reason="query_failed", value=None)
    return dict(base, status="observed", as_of=str(as_of), value=value)


def _telemetry_db_sections(connection, database_size, largest, canonical):
    return {
        "label": TELEMETRY_LABEL,
        "storage": {"database_size": database_size, "largest_tables": largest},
        "last_recorded_activity": {
            "canonical_actions": canonical,
            "outbox_created": _telemetry_activity(connection, "outbox", "created_at"),
            "outbox_published": _telemetry_activity(connection, "outbox", "published_at"),
            "stream_commit": _telemetry_activity(connection, "stream_batches", "committed_at"),
        },
    }


_POLICY_FIELDS = ("version", "window_seconds", "lookback_seconds", "min_prior_windows",
                  "score_threshold", "swap_floor", "trader_floor", "attribution_floor",
                  "mad_multiplier", "scale_floor", "direction", "detectors")
_POLICY_ID = re.compile(r"^sha256:[0-9a-f]{64}$")


def read_policy(connection, *, policy_id=None):
    """Active (``None``) or pinned detector policy, allowlisted fields only; None if absent."""
    if policy_id is None:
        row = connection.execute(
            "SELECT p.policy_id, p.version, p.body FROM detector_policies p "
            "JOIN active_detector_policy a ON a.policy_id = p.policy_id").fetchone()
    else:
        if type(policy_id) is not str or not _POLICY_ID.match(policy_id):
            raise ValueError("malformed policy id")
        row = connection.execute(
            "SELECT policy_id, version, body FROM detector_policies WHERE policy_id = %s",
            (policy_id,)).fetchone()
    if row is None:
        return None
    body = row[2]
    active = connection.execute(
        "SELECT policy_id FROM active_detector_policy").fetchone()
    result = {"policy_id": row[0], "active": bool(active and active[0] == row[0])}
    result.update({name: body[name] for name in _POLICY_FIELDS if name in body})
    return result
