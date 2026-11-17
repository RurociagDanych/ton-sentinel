#!/usr/bin/env python3
"""Host-side browser smoke for the dashboard shell.

Loads /dashboard/ in headless Chrome against the real WSGI app on loopback and checks
the DOM state, CSP violations and console errors. Skips cleanly (exit 0, status
"skipped") when Chrome is missing or SENTINEL_SKIP_BROWSER=1. Installs nothing.
Usage: dashboard_browser_smoke.py [--json PATH]
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import urllib.parse
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from ton_sentinel import dashboard_payload  # noqa: E402
from ton_sentinel.api import create_app  # noqa: E402

CHROME_NAMES = ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome")


def find_chrome():
    for name in CHROME_NAMES:
        path = shutil.which(name)
        if path:
            return path
    return None


def classify_console(log):
    """Split Chrome's stderr log into console errors and CSP violations."""
    errors, csp = [], []
    for line in log.splitlines():
        if ":CONSOLE" not in line:
            continue
        # Chrome logs some CSP refusals at INFO level, so match the text as well as the level.
        if not (":ERROR:" in line or ":WARNING:" in line
                or re.search(r"Content Security Policy|Content-Security-Policy|Refused to|Uncaught|"
                             r"Failed to load resource", line)):
            continue
        errors.append(line)
        if "Content Security Policy" in line or "Content-Security-Policy" in line:
            csp.append(line)
    return {"console_errors": errors, "csp_violations": csp}


def _policy_ok():
    provenance = dashboard_payload.build_provenance(qualification=[], scored=False)
    return {"policy": {"policy_id": "fixture"}, "limits": dict(dashboard_payload.LIMITS),
            "honesty_banner": dashboard_payload.HONESTY_BANNER, "provenance": provenance}


def overview_stubs():
    """Fixed bodies for every endpoint the overview reads. /status stays as the API shapes it."""
    from ton_sentinel import api
    provenance = dashboard_payload.build_provenance(qualification=[], scored=False)
    banner = dashboard_payload.HONESTY_BANNER
    status = api._status_payload(
        database="available", feature_rows=5, windows=2, latest_window_end=1800,
        latest_serving_write_at="2026-01-01T00:00:00Z",
        pending_work={"outbox": {"status": "observed", "pending": 3, "oldest_pending_age_seconds": 1.5},
                      "repair": {"status": "observed", "pending_windows": 0}})
    pipeline = {"network": "mainnet", "pipeline": {
        "outbox": {"status": "observed", "value": 4, "oldest_age_seconds": 12.5, "scope": "all_networks"},
        "dirty_windows_pending_by_role": {"status": "observed", "value": {"target": 2}, "truncated": False},
        "scoring_backlog": {"status": "observed", "value": {
            "unscored_served_windows": {"status": "observed", "value": 7},
            "stale_results": {"status": "unavailable", "reason": "query_failed"}}},
        "stream_progress": {"status": "none_recorded", "value": None, "scope": "all_networks"},
        "ingestion_freshness": {"status": "not_measured", "value": None,
                                "reason": "no_ingestion_clock_recorded"},
        "kafka_lag": {"status": "not_measured", "value": None, "reason": "kafka_not_queried_by_reader"}},
        "label": "fixture label", "honesty_banner": banner, "provenance": provenance}
    quality = {"network": "mainnet", "from": 0, "to": 3600, "quality": {
        "served_rows_by_coverage_status": {"status": "observed", "value": {"unrecorded": 4}, "truncated": False},
        "served_windows": {"status": "observed", "value": 2},
        "dirty_windows_pending_by_role": {"status": "unavailable", "reason": "query_failed"},
        "revision_quarantine_open": {"status": "observed", "value": 0},
        "delivery_quarantine": {"status": "observed", "value": 1},
        "scan_quarantine": {"status": "unavailable", "reason": "not_network_scoped"},
        "coverage_ranges_pending": {"status": "unavailable", "reason": "not_network_scoped"},
        "coverage_ranges_bounded_observed": {"status": "unavailable", "reason": "not_network_scoped"},
        "qualifications_by_class": {"status": "observed", "value": {
            "synthetic_bounded_observed": 0, "bounded_observed": 0, "certified_claimed": 0}}},
        "label": "fixture label", "honesty_banner": banner, "provenance": provenance}
    return {"/v1/policy": _policy_ok(), "/status": status, "/v1/pipeline": pipeline,
            "/v1/quality": quality}


EXPLORER_TOKENS = ("jetton:A", "jetton:B", "jetton:<img src=x onerror=alert(1)>")
EXPLORER_GAPS = (3, 4, 8)  # window offsets with no served row for the series stub


def explorer_stubs(drop_point_provenance=False):
    """Stub bodies for the explorer: tokens, a series with gaps, per-window coverage."""
    provenance = dashboard_payload.build_provenance(qualification=[], scored=False)
    banner = dashboard_payload.HONESTY_BANNER

    def tokens(query):
        return {"network": "mainnet", "from": int(query["from"][0]), "to": int(query["to"][0]),
                "limit": 100, "honesty_banner": banner, "provenance": provenance,
                "tokens": [{"token": t, "feature_rows": 10, "window_count": 10, "swaps": 55,
                            "first_window_start": 0, "latest_window_end": 300} for t in EXPLORER_TOKENS]}

    def series(query):
        start, end = int(query["from"][0]), int(query["to"][0])
        points = []
        for i in range(12):
            if i in EXPLORER_GAPS:
                continue
            ws = start + 300 * i
            synthetic = i == 1
            point = {"window_start": ws, "window_end": ws + 300, "generation": "g", "swaps": i + 1,
                     "input_raw": str(i + 1), "output_raw": None, "known_traders": 1, "unknown_count": 0,
                     "coverage_status": "unknown", "status": "ok", "provisional": i == 2,
                     "qualification": {"mode": "bounded_observed", "synthetic": synthetic} if i < 2 else None,
                     "provenance": dashboard_payload.build_provenance(
                         qualification=[("bounded_observed", synthetic, 1)] if i < 2 else [], scored=False)}
            if drop_point_provenance and i == 5:
                del point["provenance"]
            points.append(point)
        return {"network": "mainnet", "token": query["token"][0], "from": start, "to": end, "limit": 1000,
                "series": points, "honesty_banner": banner, "provenance": provenance}

    def coverage(query):
        ws = int(query["from"][0])
        return {"network": "mainnet", "token": query["token"][0], "from": ws, "to": ws + 300, "limit": 288,
                "honesty_banner": banner, "provenance": provenance,
                "coverage": [{"window_start": ws, "window_end": ws + 300, "state": "partial",
                              "reasons": ["no_qualification", "coverage_missing"], "qualification": None,
                              "missing": ["source_gap:fixture"], "admission_refused": False,
                              "admission_reasons": [],
                              "served": {"rows": 1, "coverage_status_counts": {"unknown": 1},
                                         "known_traders": 1, "unknown_count": 0},
                              "lineage_records": 3}]}
    return {"/v1/policy": _policy_ok(), "/v1/tokens": tokens, "/v1/series": series,
            "/v1/coverage": coverage}


def explorer_problems(dom):
    """Check the rendered explorer DOM: token list, chart with hatched gaps, labels, no leaks."""
    problems = []
    if 'data-view="explorer"' not in dom:
        problems.append("explorer view not mounted")
    if len(re.findall(r'class="token-button', dom)) != len(EXPLORER_TOKENS):
        problems.append("token list not rendered")
    if "<img src=x" in dom:
        problems.append("hostile token rendered as markup")
    if "&lt;img src=x onerror=alert(1)&gt;" not in dom:
        problems.append("hostile token not shown as text")
    chart = re.search(r'<svg[^>]*data-chart="series"[^>]*>', dom)
    if not chart:
        return problems + ["series chart not rendered"]
    attrs = chart[0]
    if 'data-point-count="9"' not in attrs:
        problems.append("expected 9 windows with a row")
    # 12 stub windows: offsets 3-4 and 8 are gaps, 0..11 hold 9 rows; the 276 windows after the stub
    # (24 h = 288 windows) have no row either, so three runs cover 279 windows.
    if 'data-gap-windows="279"' not in attrs or 'data-gap-count="3"' not in attrs:
        problems.append("expected three gap runs covering 279 windows")
    gaps = re.findall(r'<rect[^>]*class="chart-gap"[^>]*data-first-window[^>]*>', dom)
    if len(gaps) != 3 or any('fill="url(#gap-hatch)"' not in g for g in gaps):
        problems.append("gaps are not hatched rects")
    if len(re.findall(r'<rect[^>]*class="chart-bar[^"]*"[^>]*data-window-start', dom)) != 9:
        problems.append("bar count differs from rows")
    for word in ('data-qualification="unrecorded"', 'data-qualification="synthetic"',
                 'data-provisional="true"'):
        if word not in dom:
            problems.append(f"missing marker {word}")
    if "synthetic (bounded_observed)" not in dom or ">unrecorded<" not in dom:
        problems.append("table lacks synthetic/unrecorded labels")
    if "data-provenance=\"present\"" not in dom:
        problems.append("no provenance strip")
    return problems


DETECTION_POLICY_ID = "sha256:" + "a" * 64
DETECTION_DIGEST = "sha256:" + "b" * 64


def detections_stubs(drop_baseline_provenance=False, tamper_score=False):
    """Stubs for the detections view: one token, two results, an alert feed, a pin that reproduces."""
    from fractions import Fraction

    from ton_sentinel.detectors import _median_exact, _published
    provenance = dashboard_payload.build_provenance(qualification=[], scored=False)
    banner = dashboard_payload.HONESTY_BANNER
    policy = {"policy_id": DETECTION_POLICY_ID, "score_threshold": "6", "swap_floor": 5, "trader_floor": 3,
              "attribution_floor": "0.90", "mad_multiplier": "1.4826", "scale_floor": "1"}
    swaps = [10, 12, 9, 15, 11, 14, 10, 13, 12, 9, 11, 12]
    base = 1000 * 300

    def cells():
        return [{"window_start": base + 300 * i, "state": "observed", "swaps": v, "known_traders": 3,
                 "unknown_count": 0, "coverage": "unknown", "synthetic": True} for i, v in enumerate(swaps)]
    current = {"window_start": base + 300 * 12, "state": "observed", "swaps": 60, "known_traders": 4,
               "unknown_count": 0, "coverage": "unknown", "synthetic": True}
    median = _median_exact(swaps)
    mad = _median_exact([abs(Fraction(v) - median) for v in swaps])
    scale = max(Fraction(1), Fraction("1.4826") * mad)
    score = (Fraction(60) - median) / scale

    def result(detector, **over):
        doc = {"detector": detector, "token": "jetton:A", "network": "mainnet", "direction": "both",
               "window_start": current["window_start"], "window_end": current["window_start"] + 300,
               "policy_id": DETECTION_POLICY_ID, "input_digest": DETECTION_DIGEST, "status": "scored",
               "triggered": True, "observed": 60, "sample_count": len(swaps), "median": _published(median),
               "mad": _published(mad), "scale": _published(scale), "score": _published(score),
               "threshold": "6", "floor": 5, "min_prior_windows": 12, "reasons": [],
               "explanation": detector + " fixture explanation"}
        doc.update(over)
        return doc

    def tokens(query):
        return {"network": "mainnet", "from": int(query["from"][0]), "to": int(query["to"][0]), "limit": 100,
                "honesty_banner": banner, "provenance": provenance,
                "tokens": [{"token": t, "feature_rows": 10, "window_count": 10, "swaps": 55,
                            "first_window_start": 0, "latest_window_end": 300} for t in ("jetton:A", "jetton:B")]}

    def detections(query):
        first = result("swap_count_spike.v1", **({"score": "99.5"} if tamper_score else {}))
        second = result("active_wallets_spike.v1", status="cold_start", triggered=False, observed=4, sample_count=4,
                        median=None, mad=None, scale=None, score=None, reasons=["insufficient_prior_windows"])
        return {"network": "mainnet", "token": query["token"][0], "from": int(query["from"][0]),
                "to": int(query["to"][0]), "limit": 1000, "detections": [first, second], "truncated": False,
                "honesty_banner": banner, "provenance": provenance}

    def feed(query):
        return {"network": "mainnet", "from": int(query["from"][0]), "to": int(query["to"][0]), "limit": 1000,
                "truncated": False, "honesty_banner": banner, "provenance": provenance,
                "alerts": [{"token": "jetton:A", "window_start": current["window_start"],
                            "detector": "swap_count_spike.v1", "alert_revision": 1, "state": "active",
                            "result_revision": 1, "recorded_at": "2026-01-01T00:00:00.000000Z",
                            "label": "current"}]}

    def baseline(query):
        pin = {"input_digest": DETECTION_DIGEST, "network": "mainnet", "token": "jetton:A",
               "window_start": current["window_start"],
               "policy": {"policy_id": DETECTION_POLICY_ID, "lookback_seconds": 3600, "min_prior_windows": 12},
               "status": "ready", "eligible_count": len(swaps), "cells": cells(), "current_cell": current,
               "coverage_counts": {"cells_total": len(swaps), "expected": 12, "eligible": len(swaps),
                                   "by_state": {"observed": len(swaps), "zero": 0, "partial": 0, "provisional": 0,
                                                "missing": 0, "pre_universe": 0}}}
        body = {"network": "mainnet", "limit": 1, "baselines": [pin], "truncated": False,
                "honesty_banner": banner, "provenance": provenance}
        if drop_baseline_provenance:
            del body["provenance"]
        return body

    def policy_by_id(query):
        if "policy_id" in query:
            return {"policy": policy, "limits": dict(dashboard_payload.LIMITS), "honesty_banner": banner,
                    "provenance": provenance}
        return _policy_ok()
    return {"/v1/policy": policy_by_id, "/v1/tokens": tokens, "/v1/detections/range": detections,
            "/v1/alerts/feed": feed, "/v1/baseline": baseline}


def detections_problems(dom, reproduced=True):
    """Check the rendered detections view: banner, list, alert state, detail that reproduces the stored result."""
    problems = []
    if 'data-view="detections"' not in dom:
        problems.append("detections view not mounted")
    banner = re.search(r'data-banner="detection-effectiveness"', dom)
    if not banner or "unmeasured" not in dom or "not trading advice" not in dom or "synthetic" not in dom:
        problems.append("effectiveness banner missing or incomplete")
    rows = re.findall(r'<tr data-window-start="\d+" data-detector="[a-z_.0-9]+"', dom)
    if len(rows) != 2:
        problems.append("expected two result rows")
    if 'data-alert-state="active"' not in dom or 'data-alert-state="no_record"' not in dom:
        problems.append("alert states not shown (active and no_record expected)")
    if 'data-provenance="present"' not in dom:
        problems.append("no provenance strip")
    if 'data-explanation="scored"' not in dom:
        problems.append("explanation not rendered")
    for name in ("median", "mad", "scale", "score", "sample_count"):
        if f'data-value="{name}"' not in dom:
            problems.append(f"explanation lacks {name}")
    want = 'data-verdict="reproduced"' if reproduced else 'data-verdict="not_reproduced"'
    if want not in dom or f'data-reproduced="{str(reproduced).lower()}"' not in dom:
        problems.append("verdict is not " + want)
    if "Threshold rule" not in dom or "score &gt;= 6" not in dom:
        problems.append("threshold rule missing")
    return problems


HISTORY_WINDOW = 1000 * 300 + 300 * 12


def history_stubs(drop_revisions_provenance=False):
    """Stubs for the alert history view: a feed with a retracted chain and a drill-down with labels."""
    provenance = dashboard_payload.build_provenance(qualification=[], scored=False)
    banner = dashboard_payload.HONESTY_BANNER

    def item(token, detector, rev, state, label, at):
        return {"token": token, "window_start": HISTORY_WINDOW, "detector": detector, "alert_revision": rev,
                "state": state, "result_revision": rev, "recorded_at": at, "label": label}

    def feed(query):
        return {"network": "mainnet", "from": int(query["from"][0]), "to": int(query["to"][0]), "limit": 1000,
                "truncated": False, "honesty_banner": banner, "provenance": provenance,
                "alerts": [item("jetton:A", "swap_count_spike.v1", 3, "retracted", "stale", "2026-01-01T00:03:00.000000Z"),
                           item("jetton:A", "swap_count_spike.v1", 2, "pending", "superseded", "2026-01-01T00:02:00.000000Z"),
                           item("jetton:A", "swap_count_spike.v1", 1, "active", "superseded", "2026-01-01T00:01:00.000000Z"),
                           item("jetton:B", "active_wallets_spike.v1", 1, "active", "current", "2026-01-01T00:00:30.000000Z")]}

    def revisions(query):
        def rev(n, status, label, alert_state, is_current):
            return {"result_revision": n, "status": status, "triggered": status == "scored", "score": "9" if status == "scored" else None,
                    "input_digest": "sha256:" + str(n) * 64, "policy_id": "sha256:" + "a" * 64, "dependency_epoch": 1,
                    "promoted_at": "2026-01-01T00:0%d:00.000000Z" % n, "explanation": "fixture revision %d" % n,
                    "is_current": is_current, "stale": label != "current", "label": label, "alert_state": alert_state}
        body = {"network": "mainnet", "token": query["token"][0], "window_start": int(query["window_start"][0]),
                "detector": query["detector"][0], "limit": 1000, "truncated": False, "honesty_banner": banner,
                "provenance": provenance,
                "revisions": [rev(1, "scored", "superseded", "active", False), rev(2, "pending", "pending", "pending", False),
                              rev(3, "scored", "stale", "retracted", True)]}
        if drop_revisions_provenance:
            del body["provenance"]
        return body
    return {"/v1/policy": _policy_ok(), "/v1/alerts/feed": feed, "/v1/detections/revisions": revisions}


def history_problems(dom):
    """Check the rendered history view: banner, feed, provenance, drill-down labels and alert states."""
    problems = []
    if 'data-view="alerts"' not in dom:
        problems.append("history view not mounted")
    if not re.search(r'data-banner="detection-effectiveness"', dom) or "unmeasured" not in dom \
            or "not trading advice" not in dom:
        problems.append("effectiveness banner missing or incomplete")
    if len(re.findall(r'<tr class="history-latest" data-window-start="\d+"', dom)) != 2:
        problems.append("expected two feed keys")
    if 'data-provenance="present"' not in dom:
        problems.append("no provenance strip")
    if 'data-table="revisions"' not in dom:
        problems.append("drill-down did not open")
    for n, label, state in ((1, "superseded", "active"), (2, "pending", "pending"), (3, "stale", "retracted")):
        row = re.search(r'<tr[^>]*data-result-revision="%d"[^>]*>.*?</tr>' % n, dom, re.S)
        if not row or f'data-label="{label}"' not in row[0] or f'data-alert-state="{state}"' not in row[0]:
            problems.append(f"revision {n} not shown as {label}/{state}")
    latest = re.findall(r'<tr[^>]*data-result-revision="(\d+)"[^>]*data-latest="true"', dom)
    if latest != ["3"]:
        problems.append("latest stored revision not highlighted alone: %r" % (latest,))
    if 'data-alert-state="retracted"' not in dom:
        problems.append("retracted state not shown")
    if 'class="rev-label rev-current"' in re.search(r'data-table="revisions".*?</table>', dom, re.S).group(0) \
            if 'data-table="revisions"' in dom else False:
        problems.append("a stale or superseded revision is shown as current")
    return problems



def quality_stubs(drop_coverage_provenance=False):
    """Stubs for the quality view: counters (two unavailable), one token, a coverage list with gaps."""
    base = overview_stubs()
    provenance = dashboard_payload.build_provenance(qualification=[], scored=False)
    banner = dashboard_payload.HONESTY_BANNER

    def tokens(query):
        return {"network": "mainnet", "from": int(query["from"][0]), "to": int(query["to"][0]), "limit": 100,
                "tokens": [{"token": "jetton:A", "window_count": 3, "swaps": 5}], "honesty_banner": banner,
                "provenance": provenance}

    def coverage(query):
        start = int(query["from"][0])
        qual = {"mode": "bounded_observed", "synthetic": True, "mapping_version": 1, "delta_seconds": None}

        def win(offset, state, reasons, q=None):
            return {"window_start": start + 300 * offset, "state": state, "reasons": reasons, "qualification": q,
                    "missing": [], "admission_refused": False, "admission_reasons": []}
        body = {"network": "mainnet", "token": query["token"][0], "from": start, "to": int(query["to"][0]),
                "limit": 1000, "honesty_banner": banner, "provenance": provenance,
                "coverage": [win(0, "pre_universe", ["before_first_served_row"]), win(1, "observed", [], qual),
                             win(2, "missing", ["no_coverage_record"]), win(3, "provisional", ["row_provisional"])]}
        if drop_coverage_provenance:
            del body["provenance"]
        return body
    return {"/v1/policy": _policy_ok(), "/v1/quality": base["/v1/quality"], "/v1/tokens": tokens,
            "/v1/coverage": coverage}


def quality_problems(dom):
    """Check the rendered quality view: banner, tiles, unavailable shown as unavailable, coverage, gaps."""
    problems = []
    if 'data-view="quality"' not in dom:
        problems.append("quality view not mounted")
    if 'data-banner="quality-completeness"' not in dom or "do not show that the data is complete" not in dom:
        problems.append("completeness banner missing")
    if 'id="honesty-banner"' not in dom:
        problems.append("permanent shell banner missing")
    if len(re.findall(r'class="quality-tile[^"]*" data-counter="', dom)) != 9:
        problems.append("expected nine counter tiles")
    for key, detail in (("dirty_windows_pending_by_role", "query_failed"), ("scan_quarantine", "not_network_scoped")):
        tile = re.search(r'<div class="quality-tile[^"]*" data-counter="%s"[^>]*>.*?</div>' % key, dom, re.S)
        if not tile or 'data-state="unavailable"' not in tile[0] or detail not in tile[0] \
                or not re.search(r'data-value="">unavailable<', tile[0]):
            problems.append(f"{key} not shown as unavailable with its reason")
    served = re.search(r'<div class="quality-tile[^"]*" data-counter="served_windows"[^>]*>.*?</div>', dom, re.S)
    if not served or 'data-state="observed"' not in served[0]:
        problems.append("observed counter not shown as observed")
    if 'data-provenance="present"' not in dom:
        problems.append("no provenance strip")
    counts = dict(re.findall(r'data-coverage-state="(\w+)" data-count="(\d+)"', dom))
    if counts != {"observed": "1", "zero": "0", "partial": "0", "provisional": "1", "missing": "1",
                  "pre_universe": "1", "unknown": "0"}:
        problems.append("coverage breakdown wrong: %r" % (counts,))
    if "canonical is approximated" not in dom or "first served row" not in dom:
        problems.append("approximations not stated")
    gaps = re.findall(r'<tr data-window-start="\d+" data-state="(\w+)"', dom)
    if sorted(gaps) != ["missing", "pre_universe"]:
        problems.append("gap list wrong: %r" % (gaps,))
    if 'href="#/explorer/jetton%3AA/' not in dom:
        problems.append("gap rows do not link to the explorer")
    return problems


def pipeline_stubs(drop_provenance=False):
    """Stubs for the pipeline view: backlog with one unavailable count, a stream commit, two not-measured."""
    body = overview_stubs()["/v1/pipeline"]
    body["pipeline"]["stream_progress"] = {"status": "observed", "scope": "all_networks",
                                           "value": {"committed_at": "2026-01-01T00:00:00Z", "record_count": 42}}
    if drop_provenance:
        del body["provenance"]
    return {"/v1/policy": _policy_ok(), "/v1/pipeline": body}


def pipeline_problems(dom):
    """Check the rendered pipeline view: banner, tiles, separate backlog counts, not-measured panel."""
    problems = []
    if 'data-view="pipeline"' not in dom:
        problems.append("pipeline view not mounted")
    if 'data-banner="pipeline-not-measured"' not in dom or "not a health verdict" not in dom:
        problems.append("pipeline banner missing")
    if 'id="honesty-banner"' not in dom:
        problems.append("permanent shell banner missing")
    if 'data-provenance="present"' not in dom:
        problems.append("no provenance strip")

    def tile(key):
        found = re.search(r'<div class="pipe-tile[^"]*" data-tile="%s"[^>]*>.*?</div>' % key, dom, re.S)
        return found[0] if found else ""
    for key, needle in (("outbox", ">4<"), ("dirty_windows_pending_by_role", "target: 2"),
                        ("stream_progress", "42 records"), ("unscored_served_windows", ">7<")):
        t = tile(key)
        if not t or 'data-state="observed"' not in t or needle not in t:
            problems.append(f"{key} tile wrong")
    stale = tile("stale_results")
    if 'data-state="unavailable"' not in stale or "query_failed" not in stale or ">unavailable<" not in stale:
        problems.append("stale_results not shown as unavailable with its reason")
    for key, reason in (("ingestion_freshness", "no_ingestion_clock_recorded"), ("kafka_lag", "kafka_not_queried_by_reader")):
        t = tile(key)
        if 'data-state="not_measured"' not in t or ">not measured<" not in t or reason not in t:
            problems.append(f"{key} not shown as not measured with its reason")
    return problems


SCENARIO_NAMES = ("ready_with_provenance", "overview_states_rendered",
                  "overview_blocked_pipeline_missing_provenance", "blocked_missing_provenance",
                  "blocked_policy_unavailable", "explorer_series_rendered",
                  "explorer_blocked_series_missing_provenance",
                  "detections_list_detail_banner", "detections_unreproducible_marked",
                  "detections_blocked_baseline_missing_provenance",
                  "history_feed_drilldown_banner", "history_blocked_revisions_missing_provenance",
                  "quality_tiles_unavailable_banner", "quality_blocked_coverage_missing_provenance",
                  "pipeline_tiles_not_measured_banner", "pipeline_blocked_missing_provenance")


def _wrap(real_app, stubs):
    """Serve the real app; paths in ``stubs`` answer with a fixed JSON body."""
    stubs = stubs or {}

    def app(environ, start_response):
        if environ.get("PATH_INFO") in stubs:
            body = stubs[environ["PATH_INFO"]]
            if callable(body):
                body = body(urllib.parse.parse_qs(environ.get("QUERY_STRING", "")))
            data = json.dumps(body).encode()
            start_response("200 OK", [("Content-Type", "application/json"),
                                      ("Content-Length", str(len(data))),
                                      ("Cache-Control", "no-store")])
            return [data]
        return real_app(environ, start_response)
    return app


class _Quiet(WSGIRequestHandler):
    def log_message(self, *args):
        pass


def _run_chrome(chrome, url):
    with tempfile.TemporaryDirectory() as profile:
        proc = subprocess.run(
            [chrome, "--headless=new", "--no-sandbox", "--disable-gpu", "--no-first-run",
             "--disable-extensions", f"--user-data-dir={profile}", "--enable-logging=stderr",
             "--v=0", "--virtual-time-budget=5000", "--dump-dom", url],
            capture_output=True, text=True, timeout=90)
    return proc.stdout, proc.stderr


def overview_problems(dom):
    """Check the rendered overview DOM: states, never-zero rule, legend, no content leaks."""
    def state_of(row_id):
        found = re.search(r'<div class="metric" data-row-id="%s" data-state="([a-z_]+)"' % row_id, dom)
        return found[1] if found else None

    def block(row_id):
        found = re.search(r'<div class="metric" data-row-id="%s".*?</div>' % row_id, dom, re.S)
        return found[0] if found else ""

    expected = {"readiness": "unknown", "source": "unknown", "coverage": "unknown",
                "backup": "not_configured", "gate_stateful_streaming": "not_qualified",
                "gate_live_source": "not_qualified", "database": "available",
                "feature_rows": "observed", "stale_results": "unavailable",
                "ingestion_freshness": "not_measured", "kafka_lag": "not_measured",
                "stream_progress": "none_recorded", "unscored_served_windows": "observed",
                "scan_quarantine": "unavailable"}
    problems = [f"{k}: {state_of(k)!r}, expected {v!r}" for k, v in expected.items()
                if state_of(k) != v]
    for row_id, word in (("stale_results", "unavailable"), ("ingestion_freshness", "not measured"),
                         ("scan_quarantine", "unavailable")):
        if f">{word}<" not in block(row_id) or ">0<" in block(row_id):
            problems.append(f"{row_id} must read {word!r} and never 0")
    if len(re.findall(r"data-legend-state=", dom)) < 19:
        problems.append("legend incomplete")
    if 'data-view="overview"' not in dom:
        problems.append("overview view not mounted")
    return problems


def _scenario(chrome, name, stubs, expect_state, allow_console_errors, check=None, hash_route=""):
    real = create_app(dsn="postgresql://unused.invalid/none")
    server = make_server("127.0.0.1", 0, _wrap(real, stubs), handler_class=_Quiet)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        dom, log = _run_chrome(chrome, f"http://127.0.0.1:{server.server_port}/dashboard/{hash_route}")
    finally:
        server.shutdown()
    found = classify_console(log)
    state = (re.search(r'<body[^>]*data-state="([a-z]+)"', dom) or [None, None])[1]
    banner = (re.search(r'id="honesty-banner-text">([^<]*)<', dom) or [None, ""])[1]
    problems = []
    if state != expect_state:
        problems.append(f"body state {state!r}, expected {expect_state!r}")
    if found["csp_violations"]:
        problems.append("CSP violations")
    if found["console_errors"] and not allow_console_errors:
        problems.append("console errors")
    if expect_state == "ready" and banner != dashboard_payload.HONESTY_BANNER:
        problems.append("honesty banner text not from server policy")
    if expect_state == "blocked" and dashboard_payload.HONESTY_BANNER in dom.replace("&amp;", "&") \
            and "viewable" in dom:
        problems.append("unexpected content while blocked")
    if check:
        problems.extend(check(dom))
    return {"name": name, "state": state, "csp_violations": len(found["csp_violations"]),
            "console_errors": len(found["console_errors"]), "problems": problems,
            "ok": not problems}


def run_scenarios(chrome):
    if os.environ.get("SENTINEL_SKIP_BROWSER") == "1":
        return {"status": "skipped", "reason": "SENTINEL_SKIP_BROWSER=1", "scenarios": []}
    if not chrome:
        return {"status": "skipped", "reason": "no headless Chrome/Chromium found", "scenarios": []}
    broken = _policy_ok()
    del broken["provenance"]
    no_pipeline_provenance = overview_stubs()
    del no_pipeline_provenance["/v1/pipeline"]["provenance"]

    def overview_blocked(dom):
        hidden = re.search(r'<main id="view-root"[^>]*\shidden', dom)
        rows = 'data-row-id="stale_results"' in dom
        return [] if hidden and not rows else ["blocked overview still shows pipeline content"]

    def explorer_blocked(dom):
        hidden = re.search(r'<main id="view-root"[^>]*\shidden', dom)
        return [] if hidden and 'data-chart="series"' not in dom else ["blocked explorer still shows the chart"]

    def detections_blocked(dom):
        hidden = re.search(r'<main id="view-root"[^>]*\shidden', dom)
        return [] if hidden and 'data-explanation=' not in dom else ["blocked detections still shows the explanation"]

    def history_blocked(dom):
        hidden = re.search(r'<main id="view-root"[^>]*\shidden', dom)
        return [] if hidden and 'data-table="revisions"' not in dom else ["blocked history still shows revisions"]

    def quality_blocked(dom):
        hidden = re.search(r'<main id="view-root"[^>]*\shidden', dom)
        return [] if hidden and 'data-table="gaps"' not in dom else ["blocked quality still shows gaps"]

    def pipeline_blocked(dom):
        hidden = re.search(r'<main id="view-root"[^>]*\shidden', dom)
        return [] if hidden and 'data-tile="outbox"' not in dom else ["blocked pipeline still shows tiles"]

    scenarios = [
        _scenario(chrome, "ready_with_provenance", overview_stubs(), "ready", False),
        _scenario(chrome, "overview_states_rendered", overview_stubs(), "ready", False,
                  overview_problems),
        _scenario(chrome, "overview_blocked_pipeline_missing_provenance", no_pipeline_provenance,
                  "blocked", False, overview_blocked),
        _scenario(chrome, "blocked_missing_provenance", {"/v1/policy": broken}, "blocked", False),
        _scenario(chrome, "blocked_policy_unavailable", None, "blocked", True),
        _scenario(chrome, "explorer_series_rendered", explorer_stubs(), "ready", False,
                  explorer_problems, "#/explorer"),
        _scenario(chrome, "explorer_blocked_series_missing_provenance",
                  explorer_stubs(drop_point_provenance=True), "blocked", False, explorer_blocked,
                  "#/explorer"),
        _scenario(chrome, "detections_list_detail_banner", detections_stubs(), "ready", False,
                  detections_problems, "#/detections"),
        _scenario(chrome, "detections_unreproducible_marked", detections_stubs(tamper_score=True), "ready", False,
                  lambda dom: detections_problems(dom, reproduced=False), "#/detections"),
        _scenario(chrome, "detections_blocked_baseline_missing_provenance",
                  detections_stubs(drop_baseline_provenance=True), "blocked", False, detections_blocked,
                  "#/detections"),
        _scenario(chrome, "history_feed_drilldown_banner", history_stubs(), "ready", False,
                  history_problems, "#/alerts"),
        _scenario(chrome, "history_blocked_revisions_missing_provenance",
                  history_stubs(drop_revisions_provenance=True), "blocked", False, history_blocked,
                  "#/alerts"),
        _scenario(chrome, "quality_tiles_unavailable_banner", quality_stubs(), "ready", False,
                  quality_problems, "#/quality"),
        _scenario(chrome, "quality_blocked_coverage_missing_provenance",
                  quality_stubs(drop_coverage_provenance=True), "blocked", False, quality_blocked,
                  "#/quality"),
        _scenario(chrome, "pipeline_tiles_not_measured_banner", pipeline_stubs(), "ready", False,
                  pipeline_problems, "#/pipeline"),
        _scenario(chrome, "pipeline_blocked_missing_provenance", pipeline_stubs(drop_provenance=True),
                  "blocked", False, pipeline_blocked, "#/pipeline"),
    ]
    assert tuple(s["name"] for s in scenarios) == SCENARIO_NAMES
    status = "passed" if all(s["ok"] for s in scenarios) else "failed"
    return {"status": status, "chrome": subprocess.run([chrome, "--version"], capture_output=True,
                                                       text=True).stdout.strip(),
            "scenarios": scenarios}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", help="write the result to this path")
    args = parser.parse_args(argv)
    result = run_scenarios(find_chrome())
    text = json.dumps(result, indent=2)
    print(text)
    if args.json:
        Path(args.json).write_text(text + "\n")
    return 1 if result["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
