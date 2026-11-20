"""Alert history and revisions view."""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from ton_sentinel import api

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
NEW_JS = ["history-model.js", "history.js"]
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def text(name):
    return (DASH / name).read_text(encoding="utf-8")


def node_json(tmp_path, body):
    script = tmp_path / "check.mjs"
    script.write_text(
        f'import * as H from "file://{DASH}/history-model.js";\n'
        f'import * as C from "file://{DASH}/client.js";\n{body}\n')
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


FEED_HEAD = """
const W = 1700000100 - (1700000100 % 300);
const A = (token, detector, rev, state, label, at, over) => Object.assign({token, window_start: W, detector,
  alert_revision: rev, state, result_revision: rev, recorded_at: at, label}, over || {});
"""



def test_history_modules_are_allowlisted_packaged_and_served():
    for name in NEW_JS:
        assert (DASH / name).is_file(), name
        assert name in api._DASHBOARD_ASSETS, name
    assert "dashboard/*.js" in (ROOT / "pyproject.toml").read_text()


@pytest.mark.parametrize("name", NEW_JS)
def test_history_modules_follow_shell_rules(name):
    src = text(name)
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(",
                      "new Function", "XMLHttpRequest", "WebSocket", "localStorage",
                      "sessionStorage", "document.cookie", "fetch("):
        assert forbidden not in src, (name, forbidden)
    assert "https://" not in src and "http://" not in src
    assert not re.search(r"\.style\s*[.=\[]|setAttribute\(\s*[\"']style|\sstyle\s*=", src)
    for spec in re.findall(r"""(?:from|import)\s*\(?\s*[\"']([^\"']+)[\"']""", src):
        assert spec.startswith("./"), (name, spec)


def test_views_module_routes_alerts_to_real_view_and_keeps_other_stubs():
    views = text("views.js")
    assert 'from "./history.js"' in views and "mountHistory" in views
    assert views.count("This view is a stub") == 0
    assert '"/v1/alerts/feed"' in views and '"/v1/detections/revisions"' in views
    assert "/v1/alert-feed" not in views


def test_history_view_uses_only_documented_endpoints_through_the_client():
    src = text("history.js") + text("history-model.js")
    for path in ("/v1/alerts/feed", "/v1/detections/revisions"):
        assert f'"{path}"' in src, path
    assert "getChecked" in src
    for other in ('"/v1/alerts"', "/v1/alerts/history", "/v1/features", "/v1/baseline"):
        assert other not in src, other
    assert "fetch(" not in src


def test_effectiveness_banner_is_permanent_and_built_before_any_request():
    view = text("history.js")
    assert "DETECTION_BANNER" in view and 'dataset.banner = "detection-effectiveness"' in view
    first_fetch = min(i for i in (view.find("loadFeed("), view.find("getChecked(")) if i >= 0)
    assert view.find("DETECTION_BANNER") < first_fetch
    assert "hidden" not in view[view.find("DETECTION_BANNER"):view.find("DETECTION_BANNER") + 400]
    assert "dismiss" not in (view + text("history-model.js")).lower()


def test_history_view_blocks_the_shell_on_provenance_errors_and_shows_provenance():
    view = text("history.js")
    assert "ProvenanceError" in view and "onBlock" in view
    assert 'dataset.provenance = "present"' in view
    assert "source_kind" in view


def test_history_filter_is_labelled_as_client_side_on_loaded_items():
    view = text("history.js")
    assert "filterNote" in view and "dataset.filterNote" in view


def test_css_defines_history_classes():
    css = text("app.css")
    for cls in (".history-latest", ".rev-label", ".rev-current", ".rev-stale", ".rev-superseded", ".partial-banner"):
        assert cls in css, cls
    assert "url(" not in css


def test_harness_has_history_scenarios():
    source = (ROOT / "scripts" / "dashboard_browser_smoke.py").read_text()
    for needle in ("history_feed_drilldown_banner", "history_blocked_revisions_missing_provenance",
                   "history_stubs", "history_problems"):
        assert needle in source, needle



@needs_node
def test_timeline_orders_revisions_and_labels_retracted_pending(tmp_path):
    out = node_json(tmp_path, """
const rows = [
  {result_revision: 3, status: "scored", label: "current", alert_state: "retracted", score: "7", input_digest: "sha256:c", policy_id: "p", promoted_at: "t3", is_current: true, stale: false, explanation: "x"},
  {result_revision: 1, status: "scored", label: "superseded", alert_state: "active", score: "9", input_digest: "sha256:a", policy_id: "p", promoted_at: "t1", is_current: false, stale: true, explanation: "x"},
  {result_revision: 2, status: "pending", label: "pending", alert_state: "pending", score: null, input_digest: "sha256:b", policy_id: "p", promoted_at: "t2", is_current: false, stale: true, explanation: "x"},
  {result_revision: 4, status: "scored", label: "current", alert_state: null, score: "1", input_digest: "sha256:d", policy_id: "p", promoted_at: "t4", is_current: true, stale: false, explanation: "x"},
];
console.log(JSON.stringify(H.timeline(rows)));""")
    revs = [r["result_revision"] for r in out["rows"]]
    assert revs == [1, 2, 3, 4]
    by = {r["result_revision"]: r for r in out["rows"]}
    assert by[3]["alert_state"] == "retracted" and by[3]["alert_known"] is True
    assert by[2]["alert_state"] == "pending" and by[2]["label"] == "pending"
    assert by[1]["alert_state"] == "active" and by[1]["label"] == "superseded"
    assert by[4]["alert_state"] == "no_record" and by[4]["alert_known"] is True
    assert [r["result_revision"] for r in out["rows"] if r["latest"]] == [4]
    assert out["latest_revision"] == 4


@needs_node
def test_stale_result_revision_labelled_not_current(tmp_path):
    out = node_json(tmp_path, """
const rows = [
  {result_revision: 1, status: "scored", label: "stale", alert_state: "active", score: "9", is_current: true, stale: true},
  {result_revision: 0, status: "scored", label: "weird", alert_state: "exploded", score: "1", is_current: false, stale: true},
];
const t = H.timeline(rows);
console.log(JSON.stringify({t, desc: H.LABELS}));""")
    stale = [r for r in out["t"]["rows"] if r["result_revision"] == 1][0]
    assert stale["label"] == "stale" and stale["current"] is False and stale["label_known"] is True
    assert "not current" in stale["label_text"].lower() or "stale" in stale["label_text"].lower()
    weird = [r for r in out["t"]["rows"] if r["result_revision"] == 0][0]
    assert weird["label_known"] is False and weird["current"] is False
    assert weird["alert_known"] is False and weird["alert_state"] == "unknown"
    assert set(out["desc"]) >= {"current", "stale", "superseded", "pending"}


@needs_node
def test_feed_partial_banner_on_cap(tmp_path):
    out = node_json(tmp_path, """
const capped = H.feedCap({items: new Array(4000).fill(0), truncated: true}, 4);
const full = H.feedCap({items: new Array(3).fill(0), truncated: false}, 4);
console.log(JSON.stringify({capped, full}));""")
    assert out["capped"]["partial"] is True
    assert "partial" in out["capped"]["text"].lower() and "4000" in out["capped"]["text"]
    assert out["full"]["partial"] is False and out["full"]["text"] == ""


@needs_node
def test_group_feed_collects_revisions_per_key_and_marks_latest_from_stored_revisions(tmp_path):
    out = node_json(tmp_path, FEED_HEAD + """
const S = "swap_count_spike.v1", V = "active_wallets_spike.v1";
const items = [
  A("jetton:A", S, 2, "retracted", "current", "2026-01-01T00:02:00.000000Z"),
  A("jetton:B", V, 1, "active", "current", "2026-01-01T00:05:00.000000Z"),
  A("jetton:A", S, 1, "active", "superseded", "2026-01-01T00:01:00.000000Z"),
  A("jetton:A", S, 3, "pending", "stale", "2026-01-01T00:03:00.000000Z"),
];
const groups = H.groupFeed(items);
console.log(JSON.stringify(groups));""")
    assert [g["token"] for g in out] == ["jetton:B", "jetton:A"]
    a = out[1]
    assert [r["alert_revision"] for r in a["revisions"]] == [1, 2, 3]
    assert a["latest"]["alert_revision"] == 3 and a["latest"]["state"] == "pending"
    assert [r["alert_revision"] for r in a["revisions"] if r["latest"]] == [3]
    assert a["revisions"][2]["label"] == "stale" and a["revisions"][1]["state"] == "retracted"


@needs_node
def test_filter_is_exact_on_loaded_items_and_labelled_as_client_side(tmp_path):
    out = node_json(tmp_path, FEED_HEAD + """
const S = "swap_count_spike.v1", V = "active_wallets_spike.v1";
const items = [A("jetton:A", S, 1, "active", "current", "t"), A("jetton:AB", V, 1, "active", "current", "t"),
  A("jetton:B", S, 1, "inactive", "current", "t")];
const byToken = H.filterFeed(items, {token: "jetton:A", detector: ""});
const byDet = H.filterFeed(items, {token: "", detector: S});
const both = H.filterFeed(items, {token: "jetton:A", detector: V});
const none = H.filterFeed(items, {token: "", detector: ""});
console.log(JSON.stringify({
  token: byToken.map((i) => i.token), det: byDet.map((i) => i.token), both: both.length, none: none.length,
  noteOn: H.filterNote(3, 1, {token: "jetton:A", detector: ""}, false),
  noteCut: H.filterNote(3, 1, {token: "jetton:A", detector: ""}, true),
  noteOff: H.filterNote(3, 3, {token: "", detector: ""}, false),
  detectors: H.detectorsOf(items)}));""")
    assert out["token"] == ["jetton:A"]
    assert out["det"] == ["jetton:A", "jetton:B"] and out["both"] == 0 and out["none"] == 3
    assert "client-side" in out["noteOn"].lower() and "loaded" in out["noteOn"].lower() and "1 of 3" in out["noteOn"]
    assert "not searched" in out["noteCut"].lower() or "more" in out["noteCut"].lower()
    assert out["noteOff"] == ""
    assert out["detectors"] == ["active_wallets_spike.v1", "swap_count_spike.v1"]


@needs_node
def test_history_provenance_is_required_on_every_response_and_blocks(tmp_path):
    out = node_json(tmp_path, """
const prov = {source_kind: "unverified_origin", certification_state: "not_certified", coverage_state: "unrecorded",
  scoring_state: "scored_effectiveness_unmeasured", honesty_banner: "b", generated_at: "2026-01-01T00:00:00Z"};
const tries = {};
for (const [name, payload, fn] of [["ok", {alerts: [], provenance: prov}, "requireHistoryProvenance"],
  ["none", {alerts: []}, "requireHistoryProvenance"], ["null", null, "requireHistoryProvenance"],
  ["list_ok", {revisions: [], provenance: prov}, "requireList"], ["list_bad", {revisions: {}, provenance: prov}, "requireList"]]) {
  try { fn === "requireList" ? H.requireList(payload, "revisions") : H.requireHistoryProvenance(payload); tries[name] = "accepted"; }
  catch (e) { tries[name] = e instanceof C.ProvenanceError ? "blocked" : "other:" + e.message; }
}
console.log(JSON.stringify(tries));""")
    assert out == {"ok": "accepted", "none": "blocked", "null": "blocked", "list_ok": "accepted",
                   "list_bad": "blocked"}
