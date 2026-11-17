"""Overview and health view. State rendering rules, static checks, node logic."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from ton_sentinel import api, dashboard_payload
from ton_sentinel.db.serving import PIPELINE_LABEL, QUALITY_LABEL  # noqa: F401

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
NEW_JS = ["states.js", "model.js", "overview.js"]
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def text(name):
    return (DASH / name).read_text(encoding="utf-8")


def node_json(tmp_path, body):
    script = tmp_path / "check.mjs"
    script.write_text(
        f'import * as S from "file://{DASH}/states.js";\n'
        f'import * as M from "file://{DASH}/model.js";\n{body}\n')
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


# ---- static: files are served, packaged, and obey the shell rules ----------------------------

def test_new_view_modules_are_allowlisted_packaged_and_served():
    for name in NEW_JS:
        assert (DASH / name).is_file(), name
        assert name in api._DASHBOARD_ASSETS, name


@pytest.mark.parametrize("name", NEW_JS)
def test_view_modules_follow_shell_rules(name):
    src = text(name)
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(",
                      "new Function", "XMLHttpRequest", "WebSocket", "localStorage",
                      "sessionStorage", "document.cookie", "fetch("):
        assert forbidden not in src, (name, forbidden)
    assert "https://" not in src and "http://" not in src
    assert not re.search(r"\.style\s*[.=\[]|setAttribute\(\s*[\"']style", src)
    for spec in re.findall(r"""(?:from|import)\s*\(?\s*["']([^"']+)["']""", src):
        assert spec.startswith("./"), (name, spec)


def test_views_module_routes_overview_to_real_view_and_keeps_other_stubs():
    views = text("views.js")
    assert 'from "./overview.js"' in views
    assert views.count("This view is a stub") == 0  # still used by the five other views


def test_overview_uses_only_the_documented_endpoints_through_the_client():
    src = text("overview.js") + text("model.js")
    for path in ("/status", "/v1/pipeline", "/v1/quality"):
        assert f'"{path}"' in src, path
    assert "getChecked" in src and "getJson" in src
    assert "visibilitychange" in src and "30000" in src


def test_index_still_has_no_inline_script_or_style():
    html = text("index.html")
    assert not re.search(r"<style\b|\sstyle\s*=|\son[a-z]+\s*=", html, re.I)


def test_css_defines_every_badge_tone_with_text_not_colour_only():
    css = text("app.css")
    for tone in ("ok", "info", "warn", "bad", "neutral", "unknown"):
        assert f".badge-{tone}" in css, tone
    assert "--color-ok-" in css


# ---- vocabulary: every server state value maps to a legend entry -----------------------------

def server_state_values():
    prov = dashboard_payload.build_provenance(qualification=[], scored=False)
    seen = {prov["source_kind"], prov["certification_state"], prov["coverage_state"],
            prov["scoring_state"]}
    for q in (None, [("bounded_observed", True, 1)], [("bounded_observed", False, 1)], []):
        for s in (None, True, False):
            p = dashboard_payload.build_provenance(qualification=q, scored=s)
            seen |= {p["coverage_state"], p["scoring_state"], p["source_kind"],
                     p["certification_state"]}
    status = api._status_payload(database="available", feature_rows=1, windows=1)
    down = api._status_payload(database="unavailable", query="unavailable")
    for payload in (status, down):
        seen |= {payload["status"], payload["availability"]["process"],
                 payload["availability"]["database"], payload["status_query"],
                 payload["readiness"], payload["source"]["status"],
                 payload["coverage"]["status"], payload["backup"],
                 payload["serving_freshness"]["status"],
                 payload["serving_freshness"]["lag"]["status"]}
        seen |= {g["status"] for g in payload["gates"].values()}
        seen |= set(payload["operational_domains"].values())
        seen |= {v["status"] for v in payload["pending_work"].values()}
    seen |= {"observed", "unavailable", "not_measured", "none_recorded", "empty"}
    return seen


def test_legend_covers_every_state_the_server_can_emit():
    legend = text("states.js")
    missing = [s for s in sorted(server_state_values()) if f'"{s}"' not in legend]
    assert missing == []


@needs_node
def test_legend_entries_have_label_tone_and_meaning(tmp_path):
    out = node_json(tmp_path, """
const bad = Object.entries(S.STATE_LEGEND).filter(([k, v]) =>
  !v.label || !v.meaning || !["ok","info","warn","bad","neutral"].includes(v.tone));
console.log(JSON.stringify({n: Object.keys(S.STATE_LEGEND).length, bad: bad.map(b => b[0])}));""")
    assert out["bad"] == [] and out["n"] >= 19


@needs_node
def test_unknown_state_fails_closed_never_ok(tmp_path):
    out = node_json(tmp_path, """
const b = S.badge("totally_new_state");
const c = S.badge(undefined);
console.log(JSON.stringify([b, c, S.badge("available")]));""")
    new, missing, ok = out
    assert new["tone"] == "unknown" and new["known"] is False and "totally_new_state" in new["label"]
    assert missing["tone"] == "unknown" and missing["known"] is False
    assert ok["tone"] == "ok" and ok["known"] is True


# ---- the plan's RED tests --------------------------------------------------------------------

STATUS_OK = api._status_payload(database="available", feature_rows=5, windows=2,
                                latest_window_end=1800, latest_serving_write_at="2026-01-01T00:00:00Z",
                                pending_work={"outbox": {"status": "observed", "pending": 3,
                                                         "oldest_pending_age_seconds": 1.5},
                                              "repair": {"status": "observed", "pending_windows": 0}})
STATUS_DOWN = api._status_payload(database="unavailable", query="unavailable")


@needs_node
def test_overview_shows_unknown_readiness_and_not_qualified_gates_verbatim(tmp_path):
    out = node_json(tmp_path, f"""
const tiles = M.statusTiles({json.dumps(STATUS_OK)});
console.log(JSON.stringify(tiles));""")
    by_id = {t["id"]: t for t in out}
    assert by_id["readiness"]["state"] == "unknown" and by_id["readiness"]["tone"] == "unknown" or \
        by_id["readiness"]["state"] == "unknown"
    assert by_id["readiness"]["tone"] != "ok"
    assert by_id["source"]["state"] == "unknown"
    assert by_id["coverage"]["state"] == "unknown"
    assert by_id["backup"]["state"] == "not_configured"
    assert by_id["gate_stateful_streaming"]["state"] == "not_qualified"
    assert by_id["gate_live_source"]["state"] == "not_qualified"
    for tile in out:
        assert tile["tone"] != "ok" or tile["state"] in ("available", "ok", "observed")
    assert by_id["gate_live_source"]["tone"] in ("warn", "bad")


@needs_node
def test_overview_db_unavailable_state(tmp_path):
    out = node_json(tmp_path, f"""
console.log(JSON.stringify([M.statusTiles({json.dumps(STATUS_DOWN)}), M.statusTiles(null),
  M.statusTiles({{}}), M.statusTiles({{availability: "x"}})]));""")
    down, none, empty, odd = out
    by_id = {t["id"]: t for t in down}
    assert by_id["database"]["state"] == "unavailable"
    assert by_id["status_query"]["state"] == "unavailable"
    assert by_id["feature_rows"]["state"] == "unavailable"
    assert by_id["feature_rows"]["text"] != "0" and by_id["windows"]["text"] != "0"
    for fallback in (none, empty, odd):  # no/garbled /status: every tile unavailable, never ok
        assert fallback and all(t["state"] == "unavailable" for t in fallback)
        assert all(t["tone"] != "ok" and t["text"] != "0" for t in fallback)


@needs_node
def test_overview_scoring_backlog_visible(tmp_path):
    pipeline = {"network": "mainnet", "pipeline": {
        "outbox": {"status": "observed", "value": 4, "oldest_age_seconds": 12.5, "scope": "all_networks"},
        "dirty_windows_pending_by_role": {"status": "observed", "value": {"target": 2}, "truncated": False},
        "scoring_backlog": {"status": "observed", "value": {
            "unscored_served_windows": {"status": "observed", "value": 7},
            "stale_results": {"status": "observed", "value": 3, "oldest_age_seconds": 90.0}}},
        "stream_progress": {"status": "none_recorded", "value": None, "scope": "all_networks"},
        "ingestion_freshness": {"status": "not_measured", "value": None, "reason": "no_ingestion_clock_recorded"},
        "kafka_lag": {"status": "not_measured", "value": None, "reason": "kafka_not_queried_by_reader"}}}
    out = node_json(tmp_path, f"console.log(JSON.stringify(M.pipelineRows({json.dumps(pipeline)})));")
    by_id = {r["id"]: r for r in out}
    assert by_id["unscored_served_windows"]["text"] == "7"
    assert by_id["stale_results"]["text"] == "3"
    assert by_id["stale_results"]["state"] == "observed"
    assert "oldest" in by_id["stale_results"]["detail"]
    assert by_id["outbox"]["text"] == "4" and "all networks" in by_id["outbox"]["detail"]
    assert by_id["stream_progress"]["state"] == "none_recorded"
    assert by_id["stream_progress"]["text"] not in ("0", "")
    assert by_id["ingestion_freshness"]["state"] == "not_measured"
    assert by_id["ingestion_freshness"]["text"] == "not measured"
    assert "no_ingestion_clock_recorded" in by_id["ingestion_freshness"]["detail"]
    assert by_id["kafka_lag"]["text"] == "not measured"


@needs_node
def test_overview_refresh_paused_when_hidden(tmp_path):
    out = node_json(tmp_path, """
let hidden = false, runs = 0, fn = null, cleared = 0, listeners = {};
const r = M.createRefresher({
  intervalMs: 30000, run: () => { runs += 1; },
  isHidden: () => hidden,
  setIntervalFn: (f, ms) => { fn = f; r_ms = ms; return 7; },
  clearIntervalFn: () => { cleared += 1; },
  onVisibility: (handler) => { listeners.v = handler; return () => { listeners.v = null; }; },
});
var r_ms;
r.start();
fn(); hidden = true; fn(); fn();            // two ticks while hidden do nothing
const whileHidden = runs;
hidden = false; listeners.v();             // becoming visible refreshes once
const afterVisible = runs;
r.stop();
console.log(JSON.stringify({whileHidden, afterVisible, ms: r_ms, cleared, removed: listeners.v === null}));""")
    assert out == {"whileHidden": 1, "afterVisible": 2, "ms": 30000, "cleared": 1, "removed": True}


# ---- remaining state rendering rules ---------------------------------------------------------

@needs_node
def test_unavailable_and_not_measured_are_never_rendered_as_zero(tmp_path):
    out = node_json(tmp_path, """
const cells = [
  S.counterCell({status: "unavailable", reason: "query_failed"}),
  S.counterCell({status: "unavailable", value: 0}),
  S.counterCell({status: "not_measured", value: 0, reason: "r"}),
  S.counterCell({status: "observed", value: 0}),
  S.counterCell({status: "observed"}),
  S.counterCell({status: "observed", value: "5"}),
  S.counterCell({status: "bogus", value: 3}),
  S.counterCell(null), S.counterCell(undefined), S.counterCell(7),
];
console.log(JSON.stringify(cells));""")
    unavailable, unavail_zero, not_measured, observed_zero, no_value, string_value, bogus, *garbage = out
    assert unavailable["text"] == "unavailable" and unavailable["detail"] == "query_failed"
    assert unavail_zero["text"] == "unavailable"
    assert not_measured["text"] == "not measured" and not_measured["state"] == "not_measured"
    assert observed_zero["text"] == "0" and observed_zero["state"] == "observed"  # a real zero
    assert no_value["state"] == "unavailable" and no_value["text"] == "unavailable"
    assert string_value["state"] == "unavailable"
    assert bogus["tone"] == "unknown" and bogus["text"] != "3"
    for cell in garbage:
        assert cell["state"] == "unavailable" and cell["text"] == "unavailable"


@needs_node
def test_quality_headline_for_selected_network_and_range(tmp_path):
    quality = {"network": "testnet", "from": 0, "to": 3600, "quality": {
        "served_rows_by_coverage_status": {"status": "observed", "value": {"unrecorded": 4}, "truncated": False},
        "served_windows": {"status": "observed", "value": 2},
        "dirty_windows_pending_by_role": {"status": "unavailable", "reason": "query_failed"},
        "revision_quarantine_open": {"status": "observed", "value": 0},
        "delivery_quarantine": {"status": "observed", "value": 1},
        "scan_quarantine": {"status": "unavailable", "reason": "not_network_scoped"},
        "coverage_ranges_pending": {"status": "unavailable", "reason": "not_network_scoped"},
        "coverage_ranges_bounded_observed": {"status": "unavailable", "reason": "not_network_scoped"},
        "qualifications_by_class": {"status": "observed", "value": {
            "synthetic_bounded_observed": 2, "bounded_observed": 0, "certified_claimed": 0}}}}
    out = node_json(tmp_path, f"console.log(JSON.stringify(M.qualityRows({json.dumps(quality)})));")
    by_id = {r["id"]: r for r in out}
    assert by_id["served_windows"]["text"] == "2"
    assert "unrecorded: 4" in by_id["served_rows_by_coverage_status"]["text"]
    assert by_id["dirty_windows_pending_by_role"]["text"] == "unavailable"
    assert by_id["scan_quarantine"]["detail"] == "not_network_scoped"
    assert by_id["revision_quarantine_open"]["text"] == "0"
    assert "synthetic_bounded_observed: 2" in by_id["qualifications_by_class"]["text"]
    assert by_id["qualifications_by_class"]["state"] == "observed"


@needs_node
def test_quality_range_is_aligned_half_open_and_within_seven_days(tmp_path):
    out = node_json(tmp_path, """
const now = 1_700_000_123_000;
console.log(JSON.stringify(["1h","6h","24h","7d","bogus"].map(k => M.qualityRange(k, now))));""")
    for entry in out[:4]:
        assert entry["from"] % 300 == 0 and entry["to"] % 300 == 0
        assert 0 < entry["to"] - entry["from"] <= 7 * 86400
    assert [e["to"] - e["from"] for e in out[:4]] == [3600, 6 * 3600, 86400, 7 * 86400]
    assert out[4]["to"] - out[4]["from"] == 86400  # unknown key falls back to the default 24h


@needs_node
def test_missing_provenance_blocks_overview_inputs(tmp_path):
    prov = dashboard_payload.build_provenance(qualification=[], scored=False)
    good = {"network": "mainnet", "pipeline": {}, "provenance": prov}
    out = node_json(tmp_path, f"""
const good = {json.dumps(good)};
const results = [];
for (const payload of [good, {{...good, provenance: undefined}}, {{network: "mainnet"}}, null]) {{
  try {{ M.requireProvenance(payload); results.push("ok"); }}
  catch (e) {{ results.push(e.name); }}
}}
console.log(JSON.stringify(results));""")
    assert out == ["ok", "ProvenanceError", "ProvenanceError", "ProvenanceError"]


@needs_node
def test_pipeline_label_and_policy_banner_text_pass_through_verbatim(tmp_path):
    out = node_json(tmp_path, f"""
const p = {{label: {json.dumps(PIPELINE_LABEL)}, pipeline: {{}}}};
console.log(JSON.stringify(M.pipelineLabel(p)));""")
    assert out == PIPELINE_LABEL
    out = node_json(tmp_path, 'console.log(JSON.stringify(M.pipelineLabel({pipeline: {}})));')
    assert out == ""  # no label in the payload: nothing invented


@needs_node
def test_status_tiles_use_server_values_only(tmp_path):
    out = node_json(tmp_path, f"console.log(JSON.stringify(M.statusTiles({json.dumps(STATUS_OK)})));")
    by_id = {t["id"]: t for t in out}
    assert by_id["feature_rows"]["text"] == "5" and by_id["windows"]["text"] == "2"
    assert by_id["outbox_pending"]["text"] == "3"
    assert by_id["repair_pending"]["text"] == "0"
    assert by_id["database"]["state"] == "available"
    assert by_id["latest_window_end"]["text"] == "1800"
    assert by_id["latest_write"]["text"] == "2026-01-01T00:00:00Z"


def test_legend_is_rendered_for_every_state_badge_in_overview():
    src = text("overview.js")
    assert "STATE_LEGEND" in src and "legend" in src
    css = text("app.css")
    assert ".legend" in css


# ---- browser harness: overview scenarios -----------------------------------------------------

def load_harness():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "dashboard_browser_smoke", ROOT / "scripts" / "dashboard_browser_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_harness_has_overview_scenarios_and_stub_support():
    h = load_harness()
    names = h.SCENARIO_NAMES
    for name in ("ready_with_provenance", "overview_states_rendered",
                 "overview_blocked_pipeline_missing_provenance",
                 "blocked_missing_provenance", "blocked_policy_unavailable"):
        assert name in names
    fixtures = h.overview_stubs()
    assert set(fixtures) >= {"/v1/policy", "/status", "/v1/pipeline", "/v1/quality"}
    for path in ("/v1/pipeline", "/v1/quality", "/v1/policy"):
        assert "provenance" in fixtures[path]
    assert "provenance" not in fixtures["/status"]  # /status is unchanged and carries none


def test_harness_stub_wrapper_serves_stub_and_passes_other_paths():
    import io
    h = load_harness()
    seen = {}
    real = lambda environ, start: (start("204 No Content", []), [b"real"])[1]  # noqa: E731
    app = h._wrap(real, {"/status": {"x": 1}})

    def start(status, headers, *exc):
        seen["status"] = status

    body = b"".join(app({"PATH_INFO": "/status", "QUERY_STRING": "", "REQUEST_METHOD": "GET",
                         "wsgi.input": io.BytesIO()}, start))
    assert json.loads(body) == {"x": 1} and seen["status"].startswith("200")
    assert b"".join(app({"PATH_INFO": "/other"}, start)) == b"real"
