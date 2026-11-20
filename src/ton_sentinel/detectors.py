"""Offline robust anomaly scoring over provisional Spark feature mappings."""
from __future__ import annotations

import argparse
import json
import math
import statistics
from typing import Any, Iterable

VERSION = "robust-mad-v1"
DEFAULT_CONFIG = {
    "baseline_seconds": 7 * 24 * 60 * 60,
    "minimum_baseline_windows": 24,
    "score_threshold": 6.0,
    "swap_floor": 10,
    "trader_floor": 5,
    "attribution_floor": 0.90,
    "mad_multiplier": 1.4826,
}
_REQUIRED = {
    "network", "token", "window_start", "window_end", "generation", "swaps",
    "known_traders", "unknown_count", "coverage_status",
    "status", "provisional",
}


def _strict_int(value: Any, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an integer >= {minimum}")
    return value


def _finite_float(value: Any, label: str) -> float:
    try:
        result = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{label} is outside the representable numeric range") from exc
    if not math.isfinite(result):
        raise ValueError(f"{label} must be finite")
    return result


def _validate_config(config: dict[str, Any] | None) -> dict[str, Any]:
    values = dict(DEFAULT_CONFIG)
    if config is not None:
        if type(config) is not dict or set(config) - set(values):
            raise ValueError("config must contain only recognized detector settings")
        values.update(config)
    for name in ("baseline_seconds", "minimum_baseline_windows", "swap_floor", "trader_floor"):
        _strict_int(values[name], name, 1)
    for name in ("score_threshold", "attribution_floor", "mad_multiplier"):
        value = values[name]
        if type(value) not in (int, float):
            raise ValueError(f"{name} must be finite numeric")
        _finite_float(value, name)
    if values["score_threshold"] < 0 or values["mad_multiplier"] <= 0:
        raise ValueError("score_threshold must be >= 0 and mad_multiplier > 0")
    if not 0 < values["attribution_floor"] <= 1:
        raise ValueError("attribution_floor must be in (0, 1]")
    return values


def _validate_feature(row: Any) -> dict[str, Any]:
    if type(row) is not dict or not _REQUIRED.issubset(row):
        raise ValueError("feature must be a mapping with Spark feature fields")
    item = {key: row[key] for key in _REQUIRED}
    item["eligible_zero"] = row.get("eligible_zero", False)
    for name in ("network", "token", "generation"):
        if type(item[name]) is not str or not item[name]:
            raise ValueError(f"{name} must be nonempty text")
    start = _strict_int(item["window_start"], "window_start")
    end = _strict_int(item["window_end"], "window_end")
    if start % 300:
        raise ValueError("window_start must align to a five-minute boundary")
    if end != start + 300:
        raise ValueError("feature window must be a five-minute half-open window")
    for name in ("swaps", "known_traders", "unknown_count"):
        _strict_int(item[name], name)
    if type(item.get("eligible_zero", False)) is not bool or type(item["provisional"]) is not bool:
        raise ValueError("eligible_zero and provisional must be booleans")

    if type(item["status"]) is not str or not item["status"]:
        raise ValueError("status must be nonempty text")
    if item["coverage_status"] not in ("bounded_observed", "missing", "unsupported"):
        raise ValueError("coverage_status must be a recognized bounded status")
    if item["known_traders"] + item["unknown_count"] > item["swaps"]:
        raise ValueError("trader counts cannot exceed swaps")
    return item


def _eligible(row: dict[str, Any]) -> bool:
    return (row["provisional"] is True and row["status"] == "provisional"
            and row["coverage_status"] == "bounded_observed"
            and (row["swaps"] > 0 or row["eligible_zero"] is True))


def _attribution(row: dict[str, Any]) -> float:
    return (_finite_float((row["swaps"] - row["unknown_count"]) / row["swaps"], "attribution")
            if row["swaps"] else 1.0)


def _median_mad(values: list[int], current: int, config: dict[str, Any]) -> tuple[float, float]:
    try:
        median_value = statistics.median(values)
    except OverflowError as exc:
        raise ValueError("baseline median is outside the representable numeric range") from exc
    median = _finite_float(median_value, "baseline median")
    try:
        mad_value = statistics.median([abs(value - median) for value in values])
    except OverflowError as exc:
        raise ValueError("baseline MAD is outside the representable numeric range") from exc
    mad = _finite_float(mad_value, "baseline MAD")
    scale = _finite_float(config["mad_multiplier"] * mad, "score scale")
    try:
        score = _finite_float((current - median) / max(1.0, scale), "detector score")
    except OverflowError as exc:
        raise ValueError("detector score is outside the representable numeric range") from exc
    return median, score


def score_features(current: dict[str, Any], history: Iterable[dict[str, Any]],
                   config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Score one current feature against same-network/token strictly prior features.

    History rows are restricted to the trailing configured interval and the
    current feature's exclusive window start. Duplicate window keys are rejected
    rather than counted twice. Both returned detector records are explicitly
    provisional and include the exact selected baseline window keys.
    """
    settings = _validate_config(config)
    target = _validate_feature(current)
    rows = [_validate_feature(row) for row in history]
    all_rows = [target, *rows]
    relevant = [row for row in all_rows
                if row["network"] == target["network"] and row["token"] == target["token"]]
    seen: set[tuple[int, int]] = set()
    for row in relevant:
        key = (row["window_start"], row["window_end"])
        if key in seen:
            raise ValueError("ambiguous duplicate feature window key")
        seen.add(key)
    cutoff = target["window_start"] - settings["baseline_seconds"]
    baseline = sorted((row for row in rows
                       if row["network"] == target["network"] and row["token"] == target["token"]
                       and cutoff <= row["window_start"] < target["window_start"]
                       and _eligible(row)), key=lambda row: row["window_start"])
    lineage = [{"network": row["network"], "token": row["token"],
                "window_start": row["window_start"], "window_end": row["window_end"],
                "generation": row["generation"]} for row in baseline]
    empty_result = {"version": VERSION, "status": "missing", "score": None,
                    "observed_value": None, "baseline_count": len(baseline),
                    "baseline_keys": lineage, "triggered": False, "reasons": []}
    if not _eligible(target):
        reason = "missing_coverage" if target["coverage_status"] in (None, "missing", "unsupported") else "ineligible_feature"
        return {"version": VERSION, "config": settings, "feature_key": _key(target),
                "source_status": "provisional", "count": {**empty_result, "reasons": [reason]},
                "unique_traders": {**empty_result, "reasons": [reason]}}
    wallet_baseline = [row for row in baseline
                       if _attribution(row) >= settings["attribution_floor"]]
    if len(baseline) < settings["minimum_baseline_windows"]:
        status = "cold_start"
        wallet_lineage = [{"network": row["network"], "token": row["token"],
                           "window_start": row["window_start"], "window_end": row["window_end"],
                           "generation": row["generation"]} for row in wallet_baseline]
        wallet_status = ("insufficient_coverage" if _attribution(target) < settings["attribution_floor"]
                         else "cold_start")
        return {"version": VERSION, "config": settings, "feature_key": _key(target),
                "source_status": "provisional", "count": {**empty_result, "status": status,
                "observed_value": target["swaps"], "reasons": ["insufficient_prior_windows"]},
                "unique_traders": {**empty_result, "baseline_count": len(wallet_baseline),
                "baseline_keys": wallet_lineage, "status": wallet_status,
                "observed_value": target["known_traders"], "attribution": _attribution(target),
                "reasons": ["insufficient_prior_windows"]}}

    count_median, count_score = _median_mad([r["swaps"] for r in baseline], target["swaps"], settings)
    attribution = _attribution(target)
    count_reasons = []
    if target["swaps"] < settings["swap_floor"]:
        count_reasons.append("below_swap_floor")
    if count_score < settings["score_threshold"]:
        count_reasons.append("below_score_threshold")
    wallet_lineage = [{"network": row["network"], "token": row["token"],
                       "window_start": row["window_start"], "window_end": row["window_end"],
                       "generation": row["generation"]} for row in wallet_baseline]
    wallet_reasons = []
    if attribution < settings["attribution_floor"]:
        wallet_reasons.append("insufficient_trader_attribution")
    if target["known_traders"] < settings["trader_floor"]:
        wallet_reasons.append("below_trader_floor")
    if len(wallet_baseline) < settings["minimum_baseline_windows"]:
        wallet_status = "cold_start"
        wallet_score = None
        wallet_median = None
        wallet_reasons.append("insufficient_prior_windows")
    else:
        wallet_median, wallet_score = _median_mad(
            [r["known_traders"] for r in wallet_baseline], target["known_traders"], settings)
        if wallet_score < settings["score_threshold"]:
            wallet_reasons.append("below_score_threshold")
    if attribution < settings["attribution_floor"]:
        wallet_status = "insufficient_coverage"
    elif len(wallet_baseline) >= settings["minimum_baseline_windows"]:
        wallet_status = "scored"
    common = {"version": VERSION, "baseline_count": len(baseline), "baseline_keys": lineage}
    count = {**common, "status": "scored", "observed_value": target["swaps"],
             "baseline_median": count_median, "score": count_score,
             "triggered": not count_reasons, "reasons": count_reasons}
    wallets = {**common, "baseline_count": len(wallet_baseline), "baseline_keys": wallet_lineage,
               "status": wallet_status, "observed_value": target["known_traders"],
               "baseline_median": wallet_median, "score": wallet_score,
               "attribution": attribution, "triggered": not wallet_reasons,
               "reasons": wallet_reasons}
    return {"version": VERSION, "config": settings, "feature_key": _key(target),
            "source_status": "provisional", "count": count, "unique_traders": wallets}


def _key(row: dict[str, Any]) -> dict[str, Any]:
    return {"network": row["network"], "token": row["token"],
            "window_start": row["window_start"], "window_end": row["window_end"],
            "generation": row["generation"]}


# The two v1 detectors over one pinned ``BaselineInput`` (spec 8.1/8.2).
#
# This is the only scoring path that may be persisted: ``db.work.promote_scores`` accepts
# results only if they equal ``score_baseline`` of the input it pins in the same transaction.
# Arithmetic is exact (rationals); published decimals are rounded half-even to 9 places and
# the score threshold compares the published score, so the explanation states exactly the
# numbers that decided the outcome. Attribution floors compare the exact ratio.
_DETECTORS = (
    ("swap_count_spike.v1", "swap_count", "swaps", "swaps", "swap_floor"),
    ("active_wallets_spike.v1", "unique_traders", "wallets", "known_traders", "trader_floor"),
)
_PLACES = 10 ** 9


def _published(value):
    """Exact rational -> decimal text, half-even at 9 places, without trailing zeros."""
    if value is None:
        return None
    from fractions import Fraction

    scaled = round(Fraction(value) * _PLACES)
    whole, part = divmod(abs(scaled), _PLACES)
    text = ("-" if scaled < 0 else "") + str(whole)
    if part:
        text += "." + str(part).rjust(9, "0").rstrip("0")
    return "0" if text == "-0" else text


def _median_exact(values):
    from fractions import Fraction

    ordered = sorted(Fraction(v) for v in values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    return (ordered[middle - 1] + ordered[middle]) / 2


def _windows_ranges(starts):
    ranges = []
    for start in starts:
        if ranges and ranges[-1][1] == start:
            ranges[-1][1] = start + 300
        else:
            ranges.append([start, start + 300])
    return ranges


def score_baseline(baseline, policy) -> tuple:
    """Score both v1 detectors from one pinned baseline input; reads nothing else.

    Returns ``(swap_count_spike.v1, active_wallets_spike.v1)`` result documents. Each names
    its key, policy, input digest, the exact sample windows, median/MAD/scale/score (decimal
    text or ``None``), status, reasons and a one-line explanation derived from those fields.
    Status precedence: ``missing`` (current window not scorable) > ``pending`` (a provisional
    input) > ``insufficient_coverage`` (wallets only: current attribution below the floor) >
    ``cold_start`` (fewer eligible prior windows than required) > ``scored``.
    """
    from fractions import Fraction

    from ton_sentinel.contracts import ELIGIBLE_GRID_STATES, BaselineInput, DetectorPolicy

    if type(baseline) is not BaselineInput:
        raise ValueError("BaselineInput required")
    if type(policy) is not DetectorPolicy:
        raise ValueError("DetectorPolicy required")
    if (baseline.policy_id != policy.identity
            or baseline.lookback_seconds != policy.lookback_seconds
            or baseline.min_prior_windows != policy.min_prior_windows):
        raise ValueError("baseline input was pinned under a different detector policy")
    threshold = Fraction(policy.score_threshold)
    attribution_floor = Fraction(policy.attribution_floor)
    current = baseline.current
    usable = current.state in ELIGIBLE_GRID_STATES
    eligible = baseline.eligible_cells()

    def attributed(cell) -> Fraction:
        return (Fraction(1) if cell.swaps == 0
                else Fraction(cell.swaps - cell.unknown_count, cell.swaps))

    results = []
    for detector, metric, unit, field, floor_name in _DETECTORS:
        wallets = metric == "unique_traders"
        sample = [cell for cell in eligible
                  if not wallets or attributed(cell) >= attribution_floor]
        observed = getattr(current, field) if usable else None
        attribution = attributed(current) if wallets and usable else None
        floor = getattr(policy, floor_name)
        median = mad = scale = score = None
        if baseline.status == "missing":
            status, reasons = "missing", ["current_" + current.state]
        elif baseline.status == "pending":
            status = "pending"
            reasons = ["current_provisional" if current.state == "provisional"
                       else "baseline_provisional"]
        elif wallets and attribution < attribution_floor:
            status, reasons = "insufficient_coverage", ["insufficient_trader_attribution"]
        elif len(sample) < policy.min_prior_windows:
            status, reasons = "cold_start", ["insufficient_prior_windows"]
        else:
            status, reasons = "scored", []
            values = [getattr(cell, field) for cell in sample]
            median = _median_exact(values)
            mad = _median_exact([abs(Fraction(value) - median) for value in values])
            scale = max(Fraction(policy.scale_floor), Fraction(policy.mad_multiplier) * mad)
            score = (Fraction(observed) - median) / scale
            if observed < floor:
                reasons.append("below_trader_floor" if wallets else "below_swap_floor")
            if Fraction(_published(score)) < threshold:
                reasons.append("below_score_threshold")
        triggered = status == "scored" and not reasons
        result = {
            "detector": detector, "metric": metric, "unit": unit,
            "network": baseline.network, "token": baseline.token,
            "direction": baseline.direction, "window_start": baseline.window_start,
            "window_end": baseline.window_start + 300,
            "policy_id": baseline.policy_id, "input_digest": baseline.digest,
            "dependency_epoch": baseline.dependency_epoch,
            "universe_version": baseline.universe_version,
            "certification": baseline.certification, "baseline_status": baseline.status,
            "current_state": current.state, "current_revision": current.revision,
            "status": status, "triggered": triggered, "observed": observed,
            "attribution": _published(attribution), "sample_count": len(sample),
            "sample_windows": _windows_ranges([cell.window_start for cell in sample]),
            "median": _published(median), "mad": _published(mad),
            "scale": _published(scale), "score": _published(score),
            "threshold": policy.score_threshold, "floor": floor,
            "min_prior_windows": policy.min_prior_windows,
            "lookback_seconds": policy.lookback_seconds, "reasons": reasons,
        }
        result["explanation"] = _explain(result, policy)
        results.append(result)
    return tuple(results)


def _explain(result: dict, policy) -> str:
    head = (f"{result['detector']} {result['network']}/{result['token']} "
            f"[{result['window_start']},{result['window_end']})")
    if result["status"] == "scored":
        text = (f"{head}: observed {result['observed']} {result['unit']} vs baseline median "
                f"{result['median']}, MAD {result['mad']}, scale {result['scale']} from "
                f"{result['sample_count']} eligible prior windows (lookback "
                f"{result['lookback_seconds']}s); score {result['score']}, threshold "
                f"{result['threshold']}, floor {result['floor']}")
        if result["metric"] == "unique_traders":
            text += f", attribution {result['attribution']} (floor {policy.attribution_floor})"
        text += (": ACTIVE" if result["triggered"]
                 else f": not triggered ({', '.join(result['reasons'])})")
    else:
        text = (f"{head}: {result['status']} ({', '.join(result['reasons'])}); "
                f"{result['sample_count']} eligible prior windows, "
                f"{result['min_prior_windows']} required")
    return text + f"; input {result['input_digest']}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Score provisional TON Sentinel feature JSON offline")
    parser.add_argument("input", help="JSON object with current and history feature mappings")
    parser.add_argument("--config", help="JSON file containing detector policy settings")
    args = parser.parse_args()
    try:
        with open(args.input, encoding="utf-8") as stream:
            payload = json.load(stream)
        if type(payload) is not dict or set(payload) - {"current", "history", "config"} or "current" not in payload:
            raise ValueError("input JSON must contain current and may contain history/config")
        if args.config:
            if "config" in payload:
                raise ValueError("specify detector config in input or --config, not both")
            with open(args.config, encoding="utf-8") as stream:
                policy = json.load(stream)
        else:
            policy = payload.get("config")
        if (args.config or "config" in payload) and type(policy) is not dict:
            raise ValueError("explicit detector config must be a JSON object")
        result = score_features(payload["current"], payload.get("history", []), policy)
        output = json.dumps(result, sort_keys=True, allow_nan=False)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError, OverflowError, TypeError) as exc:
        parser.error(f"cannot score input: {exc}")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
