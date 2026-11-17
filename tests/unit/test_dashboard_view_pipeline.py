"""Pipeline status view. Static checks of served files and node logic."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from ton_sentinel import api

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
NEW_JS = ["pipeline-model.js", "pipeline.js"]
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def text(name):
    return (DASH / name).read_text(encoding="utf-8")


def node_json(tmp_path, body):
    script = tmp_path / "check.mjs"
    script.write_text(
        f'import * as P from "file://{DASH}/pipeline-model.js";\n'
        f'import * as C from "file://{DASH}/client.js";\n{body}\n')
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


# ---- static -----------------------------------------------------------------------------------------

def test_pipeline_modules_are_allowlisted_packaged_and_served():
    for name in NEW_JS:
        assert (DASH / name).is_file(), name
        assert name in api._DASHBOARD_ASSETS, name
    assert "dashboard/*.js" in (ROOT / "pyproject.toml").read_text()


@pytest.mark.parametrize("name", NEW_JS)
def test_pipeline_modules_follow_shell_rules(name):
    src = text(name)
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(",
                      "new Function", "XMLHttpRequest", "WebSocket", "localStorage",
                      "sessionStorage", "document.cookie", "fetch("):
        assert forbidden not in src, (name, forbidden)
    assert "https://" not in src and "http://" not in src
    assert not re.search(r"\.style\s*[.=\[]|setAttribute\(\s*[\"']style|\sstyle\s*=", src)
    for spec in re.findall(r"""(?:from|import)\s*\(?\s*["']([^"']+)["']""", src):
        assert spec.startswith("./"), (name, spec)


def test_views_module_routes_pipeline_to_real_view_and_no_stub_remains():
    views = text("views.js")
    assert 'from "./pipeline.js"' in views and "mountPipeline" in views
    assert "This view is a stub" not in views
    assert '"/v1/pipeline"' in views


def test_pipeline_view_uses_only_the_pipeline_endpoint_through_the_client():
    src = text("pipeline.js") + text("pipeline-model.js")
    assert '"/v1/pipeline"' in src and "getChecked" in src and "fetch(" not in src
    for other in ('"/v1/quality"', '"/v1/coverage"', '"/v1/series"', '"/v1/alerts', '"/v1/detections'):
        assert other not in src, other


def test_pipeline_view_blocks_the_shell_on_provenance_errors_and_shows_provenance():
    view = text("pipeline.js")
    assert "ProvenanceError" in view and "onBlock" in view
    assert 'dataset.provenance = "present"' in view and "source_kind" in view


def test_pipeline_view_has_permanent_banner_built_before_any_request():
    view = text("pipeline.js") + text("pipeline-model.js")
    assert 'dataset.banner = "pipeline-not-measured"' in view
    lowered = view.lower()
    assert "dismiss" not in lowered
    assert view.find("pipeline-not-measured") < view.find("getChecked(")
    assert "not measured" in lowered and "not a health" in lowered


def test_pipeline_view_auto_refresh_is_at_most_every_30_seconds_and_pauses_when_hidden():
    view = text("pipeline.js")
    assert "createRefresher" in view and "REFRESH_MS" in view
    assert "document.hidden" in view and "visibilitychange" in view
    assert "refresher.stop()" in view
    assert re.search(r"export const REFRESH_MS = (\d+);", text("model.js")).group(1) == "30000"
    assert "setInterval" not in view.replace("setIntervalFn", "").replace("clearIntervalFn", "").replace(
        "window.setInterval", "")


def test_css_defines_pipeline_classes():
    css = text("app.css")
    for cls in (".pipe-tile", ".pipe-not-measured", ".pipe-backlog"):
        assert cls in css, cls
    assert "url(" not in css


def test_existing_view_tests_no_longer_expect_a_stub():
    for name in ("test_dashboard_view_overview.py", "test_dashboard_view_explorer.py",
                 "test_dashboard_view_detections.py", "test_dashboard_view_history.py"):
        body = (ROOT / "tests" / "unit" / name).read_text()
        assert 'views.count("This view is a stub") == 1' not in body, name


def test_harness_has_pipeline_scenarios():
    source = (ROOT / "scripts" / "dashboard_browser_smoke.py").read_text()
    for needle in ("pipeline_tiles_not_measured_banner", "pipeline_blocked_missing_provenance",
                   "pipeline_problems"):
        assert needle in source, needle


# ---- logic ------------------------------------------------------------------------------------------

PIPE = """
const prov = {source_kind: "unverified_origin", certification_state: "not_certified", coverage_state: "unrecorded",
  scoring_state: "scored_effectiveness_unmeasured", honesty_banner: "b", generated_at: "2026-01-01T00:00:00Z"};
const pipe = {
  outbox: {status: "observed", value: 0, oldest_age_seconds: null, scope: "all_networks"},
  dirty_windows_pending_by_role: {status: "observed", value: {target: 2}, truncated: false},
  scoring_backlog: {status: "observed", value: {
    unscored_served_windows: {status: "observed", value: 7},
    stale_results: {status: "unavailable", reason: "query_failed"}}},
  stream_progress: {status: "observed", value: {committed_at: "2026-01-01T00:00:00Z", record_count: 42, offset: 9, topic: "t"}, scope: "all_networks"},
  ingestion_freshness: {status: "not_measured", value: null, reason: "no_ingestion_clock_recorded"},
  kafka_lag: {status: "not_measured", value: null, reason: "kafka_not_queried_by_reader"},
};
"""


@needs_node
def test_backlog_counts_are_separate_and_unavailable_is_never_zero(tmp_path):
    out = node_json(tmp_path, PIPE + """
const panel = P.backlogPanel(pipe.scoring_backlog);
const both = P.backlogPanel({status: "unavailable", reason: "query_failed"});
const omitted = P.backlogPanel({status: "observed", value: {unscored_served_windows: {status: "observed", value: 0}}});
console.log(JSON.stringify({panel, both, omitted}));""")
    items = {i["key"]: i for i in out["panel"]["items"]}
    assert set(items) == {"unscored_served_windows", "stale_results"}
    assert items["unscored_served_windows"]["text"] == "7" and items["unscored_served_windows"]["state"] == "observed"
    stale = items["stale_results"]
    assert stale["state"] == "unavailable" and stale["text"] == "unavailable" and stale["detail"] == "query_failed"
    assert "total" not in out["panel"] and all("7" not in str(i["text"]) for i in [stale])  # never summed
    for item in out["both"]["items"]:
        assert item["state"] == "unavailable" and item["text"] != "0" and item["detail"] == "query_failed"
    om = {i["key"]: i for i in out["omitted"]["items"]}
    assert om["unscored_served_windows"]["text"] == "0" and om["unscored_served_windows"]["state"] == "observed"
    assert om["stale_results"]["state"] == "unavailable" and om["stale_results"]["text"] != "0"


@needs_node
def test_not_measured_panel_is_explicit_and_never_shows_a_value(tmp_path):
    out = node_json(tmp_path, PIPE + """
const ok = P.notMeasuredPanel(pipe);
const lying = P.notMeasuredPanel({ingestion_freshness: {status: "observed", value: 3}, kafka_lag: null});
console.log(JSON.stringify({ok, lying}));""")
    items = {i["key"]: i for i in out["ok"]["items"]}
    assert set(items) == {"ingestion_freshness", "kafka_lag"}
    assert items["ingestion_freshness"]["state"] == "not_measured"
    assert items["ingestion_freshness"]["text"] == "not measured"
    assert items["ingestion_freshness"]["detail"] == "no_ingestion_clock_recorded"
    assert items["kafka_lag"]["detail"] == "kafka_not_queried_by_reader"
    for item in out["lying"]["items"]:  # anything but not_measured fails closed to unavailable, never a number
        assert item["state"] == "unavailable" and item["text"] not in ("3", "0")


@needs_node
def test_tiles_outbox_dirty_and_stream_show_counts_only(tmp_path):
    out = node_json(tmp_path, PIPE + """
console.log(JSON.stringify({tiles: P.tiles(pipe), empty: P.tiles({})}));""")
    by = {t["key"]: t for t in out["tiles"]}
    assert set(by) == {"outbox", "dirty_windows_pending_by_role", "stream_progress"}
    assert by["outbox"]["text"] == "0" and by["outbox"]["state"] == "observed"
    assert "target: 2" in by["dirty_windows_pending_by_role"]["text"]
    stream = by["stream_progress"]
    assert "42" in stream["text"] and "2026-01-01T00:00:00Z" in stream["text"]
    assert "topic" not in stream["text"] and "offset" not in stream["text"]
    assert "9" not in stream["text"].replace("2026-01-01T00:00:00Z", "").replace("42", "")
    for tile in out["empty"]:  # omitted sections are unavailable, never zero
        assert tile["state"] == "unavailable" and tile["text"] != "0"


@needs_node
def test_stream_progress_states_and_malformed_values(tmp_path):
    out = node_json(tmp_path, """
const cases = {
  none: {stream_progress: {status: "none_recorded", value: null}},
  bad: {stream_progress: {status: "observed", value: {committed_at: "x", record_count: "42"}}},
  down: {stream_progress: {status: "unavailable", reason: "query_failed"}},
};
console.log(JSON.stringify(Object.fromEntries(Object.entries(cases).map(([k, v]) =>
  [k, P.tiles(v).find((t) => t.key === "stream_progress")]))));""")
    assert out["none"]["state"] == "none_recorded" and out["none"]["text"] == "none recorded"
    assert out["bad"]["state"] == "unavailable" and out["bad"]["text"] not in ("0", "42")
    assert out["down"]["state"] == "unavailable" and out["down"]["detail"] == "query_failed"


@needs_node
def test_pipeline_provenance_is_required_and_blocks(tmp_path):
    out = node_json(tmp_path, PIPE + """
const tries = {};
const run = (name, fn) => { try { fn(); tries[name] = "accepted"; }
  catch (e) { tries[name] = e instanceof C.ProvenanceError ? "blocked" : "other:" + e.message; } };
run("ok", () => P.requirePipelineProvenance({pipeline: pipe, provenance: prov}));
run("none", () => P.requirePipelineProvenance({pipeline: pipe}));
run("null", () => P.requirePipelineProvenance(null));
run("pipe_ok", () => P.requirePipeline({pipeline: pipe, provenance: prov}));
run("pipe_bad", () => P.requirePipeline({pipeline: [], provenance: prov}));
console.log(JSON.stringify(tries));""")
    assert out == {"ok": "accepted", "none": "blocked", "null": "blocked", "pipe_ok": "accepted",
                   "pipe_bad": "blocked"}
