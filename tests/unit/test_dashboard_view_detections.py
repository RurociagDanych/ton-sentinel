"""Detections and alerts view."""

import json
import re
import shutil
import subprocess
from fractions import Fraction
from pathlib import Path

import pytest

from ton_sentinel import api
from ton_sentinel.detectors import _median_exact, _published

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
NEW_JS = ["detections-model.js", "detections.js"]
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
POLICY = {"policy_id": "sha256:" + "a" * 64, "score_threshold": "6", "swap_floor": 5, "trader_floor": 3,
          "attribution_floor": "0.90", "mad_multiplier": "1.4826", "scale_floor": "1"}
DIGEST = "sha256:" + "b" * 64


def text(name):
    return (DASH / name).read_text(encoding="utf-8")


def node_json(tmp_path, body):
    script = tmp_path / "check.mjs"
    script.write_text(
        f'import * as D from "file://{DASH}/detections-model.js";\n'
        f'import * as C from "file://{DASH}/client.js";\n{body}\n')
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)



def fixture():
    """20 prior cells (18 eligible; one partial, one low attribution) and the current cell."""
    swaps = [10, 12, 9, 15, 11, 14, 10, 13, 12, 9, 40, 11, 12, 10, 13, 9, 14, 12, 11, 10]
    cells = []
    for i, value in enumerate(swaps):
        state = "partial" if i == 3 else "observed"
        unknown = 8 if i == 5 else 0
        cells.append({"window_start": 1000 * 300 + 300 * i, "state": state, "swaps": value,
                      "known_traders": max(1, value // 3) - 0, "unknown_count": unknown,
                      "coverage": "unknown", "synthetic": True})
    current = {"window_start": 1000 * 300 + 300 * 20, "state": "observed", "swaps": 90,
               "known_traders": 31, "unknown_count": 0, "coverage": "unknown", "synthetic": True}
    return cells, current


def stored(detector, field, cells, current, wallets):
    sample = [c for c in cells if c["state"] in ("observed", "zero")]
    if wallets:
        sample = [c for c in sample if c["swaps"] == 0
                  or Fraction(c["swaps"] - c["unknown_count"], c["swaps"]) >= Fraction("0.90")]
    values = [c[field] for c in sample]
    median = _median_exact(values)
    mad = _median_exact([abs(Fraction(v) - median) for v in values])
    scale = max(Fraction(POLICY["scale_floor"]), Fraction(POLICY["mad_multiplier"]) * mad)
    score = (Fraction(current[field]) - median) / scale
    floor = POLICY["trader_floor" if wallets else "swap_floor"]
    reasons = []
    if current[field] < floor:
        reasons.append("below_trader_floor" if wallets else "below_swap_floor")
    if Fraction(_published(score)) < Fraction(POLICY["score_threshold"]):
        reasons.append("below_score_threshold")
    return {"detector": detector, "status": "scored", "triggered": not reasons, "observed": current[field],
            "sample_count": len(sample), "median": _published(median), "mad": _published(mad),
            "scale": _published(scale), "score": _published(score), "threshold": POLICY["score_threshold"],
            "floor": floor, "reasons": reasons, "input_digest": DIGEST, "policy_id": POLICY["policy_id"],
            "min_prior_windows": 12, "token": "jetton:A", "window_start": current["window_start"]}


def scenario():
    cells, current = fixture()
    pin = {"input_digest": DIGEST, "policy": {"policy_id": POLICY["policy_id"], "lookback_seconds": 6000,
                                              "min_prior_windows": 12},
           "cells": cells, "current_cell": current, "status": "ready"}
    results = {"count": stored("swap_count_spike.v1", "swaps", cells, current, False),
               "wallets": stored("active_wallets_spike.v1", "known_traders", cells, current, True)}
    return pin, results


NODE_HEAD = "const S = %s;\nconst POLICY = %s;\n"


def head():
    pin, results = scenario()
    return NODE_HEAD % (json.dumps({"pin": pin, "results": results}), json.dumps(POLICY))



def test_detections_modules_are_allowlisted_packaged_and_served():
    for name in NEW_JS:
        assert (DASH / name).is_file(), name
        assert name in api._DASHBOARD_ASSETS, name
    package_data = (ROOT / "pyproject.toml").read_text()
    assert "dashboard/*.js" in package_data


@pytest.mark.parametrize("name", NEW_JS)
def test_detections_modules_follow_shell_rules(name):
    src = text(name)
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(",
                      "new Function", "XMLHttpRequest", "WebSocket", "localStorage",
                      "sessionStorage", "document.cookie", "fetch("):
        assert forbidden not in src, (name, forbidden)
    assert "https://" not in src and "http://" not in src
    assert not re.search(r"\.style\s*[.=\[]|setAttribute\(\s*[\"']style|\sstyle\s*=", src)
    for spec in re.findall(r"""(?:from|import)\s*\(?\s*[\"']([^\"']+)[\"']""", src):
        assert spec.startswith("./"), (name, spec)


def test_views_module_routes_detections_to_real_view_and_keeps_other_stubs():
    views = text("views.js")
    assert 'from "./detections.js"' in views and "mountDetections" in views
    assert views.count("This view is a stub") == 0


def test_detections_view_uses_only_documented_endpoints_through_the_client():
    src = text("detections.js") + text("detections-model.js")
    for path in ("/v1/tokens", "/v1/detections/range", "/v1/baseline", "/v1/alerts/feed", "/v1/policy"):
        assert f'"{path}"' in src, path
    assert "getChecked" in src
    for other in ("/v1/detections/revisions", "/v1/alerts\"", "/v1/features"):
        assert other not in src, other
    assert "fetch(" not in src


def test_effectiveness_banner_text_is_permanent_and_complete():
    model, view = text("detections-model.js"), text("detections.js")
    for phrase in ("unmeasured", "no labelled", "not trading advice", "synthetic"):
        assert phrase in model.lower(), phrase
    assert "DETECTION_BANNER" in view and 'dataset.banner = "detection-effectiveness"' in view
    first_fetch = min(i for i in (view.find("loadTokens("), view.find("getChecked(")) if i >= 0)
    assert view.find("DETECTION_BANNER") < first_fetch
    assert "hidden" not in view[view.find("DETECTION_BANNER"):view.find("DETECTION_BANNER") + 400]
    assert "dismiss" not in (view + model).lower()


def test_detections_view_blocks_the_shell_on_provenance_errors():
    view = text("detections.js")
    assert "ProvenanceError" in view and "onBlock" in view
    assert 'dataset.provenance = "present"' in view


def test_css_defines_detection_classes():
    css = text("app.css")
    for cls in (".detection-banner", ".alert-state", ".explain-table", ".explain-match", ".explain-mismatch"):
        assert cls in css, cls
    assert "url(" not in css


def test_harness_has_detections_scenarios():
    source = (ROOT / "scripts" / "dashboard_browser_smoke.py").read_text()
    for needle in ("detections_list_detail_banner", "detections_blocked_baseline_missing_provenance",
                   "detections_stubs", "detection-effectiveness"):
        assert needle in source, needle



@needs_node
def test_explanation_reproduces_stored_median_mad_scale_and_score_for_both_detectors(tmp_path):
    out = node_json(tmp_path, head() + """
const res = {};
for (const key of ["count", "wallets"]) {
  const e = D.explain(S.results[key], S.pin, POLICY);
  res[key] = {kind: e.kind, rows: e.rows, reproduced: e.reproduced, triggered: e.recomputed_triggered};
}
console.log(JSON.stringify(res));""")
    pin, results = scenario()
    for key in ("count", "wallets"):
        entry, truth = out[key], results[key]
        assert entry["kind"] == "scored" and entry["reproduced"] is True, entry
        rows = {r["name"]: r for r in entry["rows"]}
        for name in ("median", "mad", "scale", "score", "sample_count"):
            assert rows[name]["stored"] == str(truth[name]), (key, name)
            assert rows[name]["recomputed"] == str(truth[name]), (key, name)
            assert rows[name]["match"] is True
        assert entry["triggered"] is truth["triggered"] is True
    # the fixture exercises an even-sized sample (x.5 median) and a repeating-decimal score
    assert "." in results["count"]["median"] or "." in results["count"]["score"]
    assert results["wallets"]["sample_count"] < results["count"]["sample_count"]


@needs_node
def test_explanation_flags_a_stored_value_that_the_pin_does_not_reproduce(tmp_path):
    out = node_json(tmp_path, head() + """
const bad = Object.assign({}, S.results.count, {score: "99.5"});
const e = D.explain(bad, S.pin, POLICY);
console.log(JSON.stringify({kind: e.kind, reproduced: e.reproduced,
  score: e.rows.find((r) => r.name === "score"), median: e.rows.find((r) => r.name === "median")}));""")
    assert out["reproduced"] is False and out["kind"] == "scored"
    assert out["score"]["match"] is False and out["score"]["stored"] == "99.5"
    assert out["median"]["match"] is True


@needs_node
def test_explanation_refuses_a_pin_or_policy_that_is_not_the_one_the_result_names(tmp_path):
    out = node_json(tmp_path, head() + """
const wrongDigest = D.explain(S.results.count, Object.assign({}, S.pin, {input_digest: "sha256:" + "c".repeat(64)}), POLICY);
const wrongPolicy = D.explain(S.results.count, S.pin, Object.assign({}, POLICY, {policy_id: "sha256:" + "d".repeat(64)}));
const noPolicy = D.explain(S.results.count, S.pin, null);
console.log(JSON.stringify({wrongDigest, wrongPolicy, noPolicy}));""")
    for key in ("wrongDigest", "wrongPolicy", "noPolicy"):
        assert out[key]["kind"] == "input_mismatch" and out[key]["reproduced"] is False, key
        assert out[key]["rows"] == [] and out[key]["reason"], key


@needs_node
def test_explanation_for_unscored_status_shows_sample_count_and_reasons_without_arithmetic(tmp_path):
    out = node_json(tmp_path, head() + """
const cold = Object.assign({}, S.results.count, {status: "cold_start", triggered: false, median: null, mad: null,
  scale: null, score: null, sample_count: 4, reasons: ["insufficient_prior_windows"]});
const e = D.explain(cold, S.pin, POLICY);
console.log(JSON.stringify(e));""")
    assert out["kind"] == "not_scored" and out["rows"] == [] and out["reproduced"] is False
    assert "insufficient_prior_windows" in out["reasons"]
    assert out["sample_count"] == 4 and out["min_prior_windows"] == 12


@needs_node
def test_threshold_rule_names_score_threshold_and_floors_and_recomputes_the_trigger(tmp_path):
    out = node_json(tmp_path, head() + """
const low = Object.assign({}, S.results.count, {score: "5.9", reasons: ["below_score_threshold"], triggered: false});
const e = D.explain(low, S.pin, POLICY);
const ok = D.explain(S.results.wallets, S.pin, POLICY);
console.log(JSON.stringify({rule: ok.rule, recomputed: ok.recomputed_triggered, lowRule: e.rule, lowReproduced: e.reproduced}));""")
    assert "score >= 6" in out["rule"] and "observed >= 3" in out["rule"]
    assert out["recomputed"] is True
    assert out["lowReproduced"] is False


@needs_node
def test_alert_status_takes_latest_revision_per_result_and_never_invents_one(tmp_path):
    out = node_json(tmp_path, """
const W = 1700000100 - (1700000100 % 300);
const A = (rev, state, over) => Object.assign({token: "jetton:A", window_start: W, detector: "swap_count_spike.v1",
  alert_revision: rev, state, result_revision: 1, recorded_at: "2026-01-01T00:00:00.000000Z", label: "current"}, over || {});
const result = {token: "jetton:A", window_start: W, detector: "swap_count_spike.v1"};
const other = {token: "jetton:A", window_start: W, detector: "active_wallets_spike.v1"};
const latest = D.alertStatus([A(1, "active", {label: "superseded"}), A(3, "retracted"), A(2, "pending", {label: "superseded"}),
  A(1, "active", {token: "jetton:B"})], result, {truncated: false});
const none = D.alertStatus([A(1, "active")], other, {truncated: false});
const unknown = D.alertStatus([A(1, "active")], other, {truncated: true});
const inactive = D.alertStatus([A(4, "inactive")], result, {truncated: false});
const weird = D.alertStatus([A(5, "exploded")], result, {truncated: false});
console.log(JSON.stringify({latest, none, unknown, inactive, weird, states: D.ALERT_STATES}));""")
    assert out["latest"]["state"] == "retracted" and out["latest"]["alert_revision"] == 3
    assert out["none"]["state"] == "no_record" and "no alert record" in out["none"]["text"].lower()
    assert out["unknown"]["state"] == "unknown" and "truncated" in out["unknown"]["text"].lower()
    assert out["inactive"]["state"] == "inactive"
    assert out["weird"]["state"] == "unknown"
    assert out["states"] == ["active", "pending", "retracted", "inactive"]


@needs_node
def test_detection_provenance_is_required_on_every_response_and_blocks(tmp_path):
    out = node_json(tmp_path, """
const prov = {source_kind: "unverified_origin", certification_state: "not_certified", coverage_state: "unrecorded",
  scoring_state: "scored_effectiveness_unmeasured", honesty_banner: "b", generated_at: "2026-01-01T00:00:00Z"};
const tries = {};
for (const [name, payload] of [["ok", {detections: [], provenance: prov}], ["none", {detections: []}],
  ["empty", {detections: [], provenance: {}}], ["not_object", null]]) {
  try { D.requireDetectionProvenance(payload); tries[name] = "accepted"; }
  catch (e) { tries[name] = e instanceof C.ProvenanceError ? "blocked" : "other:" + e.message; }
}
console.log(JSON.stringify(tries));""")
    assert out == {"ok": "accepted", "none": "blocked", "empty": "blocked", "not_object": "blocked"}


@needs_node
def test_decimal_policy_text_parses_exactly_and_bad_text_is_refused(tmp_path):
    out = node_json(tmp_path, """
const r = {};
for (const t of ["1.4826", "0.90", "6", "1", "-2.5"]) r[t] = String(D.parseDecimal(t));
for (const t of ["", "1e3", "abc", null, 1.5]) { try { D.parseDecimal(t); r[String(t)] = "ok"; } catch (e) { r[String(t)] = "refused"; } }
console.log(JSON.stringify({r, pub: [D.publish(D.parseDecimal("2.5")), D.publish(D.frac(1n, 3n)), D.publish(D.frac(-1n, 8n)),
  D.publish(D.frac(1n, 2000000000n)), D.publish(D.frac(3n, 2000000000n))]}));""")
    assert out["r"]["1.4826"] == "7413/5000"
    assert out["r"]["0.90"] == "9/10" and out["r"]["6"] == "6" and out["r"]["-2.5"] == "-5/2"
    for bad in ("", "1e3", "abc", "null", "1.5"):
        assert out["r"][bad] == "refused", bad
    assert out["pub"] == ["2.5", "0.333333333", "-0.125", "0", "0.000000002"]
