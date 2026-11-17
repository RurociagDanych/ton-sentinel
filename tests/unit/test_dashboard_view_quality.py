"""Data quality and coverage view. Static checks of served files and node logic."""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from ton_sentinel import api

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
NEW_JS = ["quality-model.js", "quality.js"]
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")


def text(name):
    return (DASH / name).read_text(encoding="utf-8")


def node_json(tmp_path, body):
    script = tmp_path / "check.mjs"
    script.write_text(
        f'import * as Q from "file://{DASH}/quality-model.js";\n'
        f'import * as C from "file://{DASH}/client.js";\n{body}\n')
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


# ---- static: files are served, packaged, and obey the shell rules ----------------------------------

def test_quality_modules_are_allowlisted_packaged_and_served():
    for name in NEW_JS:
        assert (DASH / name).is_file(), name
        assert name in api._DASHBOARD_ASSETS, name
    assert "dashboard/*.js" in (ROOT / "pyproject.toml").read_text()


@pytest.mark.parametrize("name", NEW_JS)
def test_quality_modules_follow_shell_rules(name):
    src = text(name)
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(",
                      "new Function", "XMLHttpRequest", "WebSocket", "localStorage",
                      "sessionStorage", "document.cookie", "fetch("):
        assert forbidden not in src, (name, forbidden)
    assert "https://" not in src and "http://" not in src
    assert not re.search(r"\.style\s*[.=\[]|setAttribute\(\s*[\"']style|\sstyle\s*=", src)
    for spec in re.findall(r"""(?:from|import)\s*\(?\s*["']([^"']+)["']""", src):
        assert spec.startswith("./"), (name, spec)


def test_views_module_routes_quality_to_real_view():
    views = text("views.js")
    assert 'from "./quality.js"' in views and "mountQuality" in views
    assert views.count("This view is a stub") == 0
    assert '"/v1/quality"' in views and '"/v1/coverage"' in views


def test_quality_view_uses_only_documented_endpoints_through_the_client():
    src = text("quality.js") + text("quality-model.js")
    for path in ("/v1/quality", "/v1/coverage", "/v1/tokens"):
        assert f'"{path}"' in src, path
    assert "getChecked" in src and "fetch(" not in src
    for other in ('"/v1/series"', '"/v1/features"', '"/v1/alerts', '"/v1/pipeline"'):
        assert other not in src, other


def test_completeness_disclaimer_present():
    view = text("quality.js") + text("quality-model.js")
    assert 'dataset.banner = "quality-completeness"' in view
    lowered = view.lower()
    assert "do not show that the data is complete" in lowered or "not a completeness" in lowered
    first_fetch = min(i for i in (view.find("loadQuality("), view.find("getChecked(")) if i >= 0)
    assert view.find("quality-completeness") < first_fetch
    assert "dismiss" not in lowered
    # the approximations of the coverage view are stated on the view
    for needle in ("canonical", "first served row", "pending"):
        assert needle in lowered, needle


def test_quality_view_blocks_the_shell_on_provenance_errors_and_shows_provenance():
    view = text("quality.js")
    assert "ProvenanceError" in view and "onBlock" in view
    assert 'dataset.provenance = "present"' in view and "source_kind" in view


def test_gap_rows_link_to_the_explorer_and_explorer_reads_the_link():
    view = text("quality.js") + text("quality-model.js")
    assert "#/explorer/" in view and "encodeURIComponent" in view and 'setAttribute("href"' in view
    explorer = text("explorer.js")
    assert "context.route" in explorer
    assert "onBlock: setBlocked, route" in text("app.js")


def test_css_defines_quality_classes():
    css = text("app.css")
    for cls in (".quality-tile", ".tile-unavailable", ".cov-strip", ".cov-cell", ".cov-missing", ".gap-list"):
        assert cls in css, cls
    assert "url(" not in css


def test_harness_has_quality_scenarios():
    source = (ROOT / "scripts" / "dashboard_browser_smoke.py").read_text()
    for needle in ("quality_tiles_unavailable_banner", "quality_blocked_coverage_missing_provenance",
                   "quality_stubs", "quality_problems"):
        assert needle in source, needle


# ---- logic: tiles, breakdown, strip, gaps ---------------------------------------------------------

QUALITY_BODY = """
const q = {
  served_rows_by_coverage_status: {status: "observed", value: {unrecorded: 4}, truncated: false},
  served_windows: {status: "observed", value: 0},
  dirty_windows_pending_by_role: {status: "unavailable", reason: "query_failed"},
  revision_quarantine_open: {status: "observed", value: 0},
  delivery_quarantine: {status: "observed", value: 1},
  scan_quarantine: {status: "unavailable", reason: "not_network_scoped"},
  coverage_ranges_pending: {status: "unavailable", reason: "not_network_scoped"},
  qualifications_by_class: {status: "observed", value: {synthetic_bounded_observed: 2, bounded_observed: 0, certified_claimed: 0}},
};
"""


@needs_node
def test_unavailable_counter_not_zero(tmp_path):
    out = node_json(tmp_path, QUALITY_BODY + """
const tiles = Q.tiles(q);
console.log(JSON.stringify(tiles));""")
    by = {t["key"]: t for t in out}
    assert len(out) == 9  # every documented counter has a tile, also those the response omitted
    for key in ("dirty_windows_pending_by_role", "scan_quarantine", "coverage_ranges_pending"):
        tile = by[key]
        assert tile["state"] == "unavailable" and tile["text"] == "unavailable" and tile["text"] != "0"
    assert by["scan_quarantine"]["detail"] == "not_network_scoped"
    assert by["dirty_windows_pending_by_role"]["detail"] == "query_failed"
    # an omitted counter is unavailable with a reason, never zero
    omitted = by["coverage_ranges_bounded_observed"]
    assert omitted["state"] == "unavailable" and omitted["text"] != "0" and omitted["detail"]
    # a real observed zero stays a zero and is shown as observed
    assert by["served_windows"]["state"] == "observed" and by["served_windows"]["text"] == "0"
    assert by["delivery_quarantine"]["text"] == "1"
    assert "synthetic_bounded_observed: 2" in by["qualifications_by_class"]["text"]
    assert all(t["title"] for t in out)


@needs_node
def test_malformed_quality_fails_closed_to_unavailable(tmp_path):
    out = node_json(tmp_path, """
const tiles = Q.tiles({served_windows: {status: "observed", value: "7"}, delivery_quarantine: {status: "weird", value: 3},
  revision_quarantine_open: null});
console.log(JSON.stringify(tiles.filter((t) => ["served_windows", "delivery_quarantine", "revision_quarantine_open"].includes(t.key))));
""")
    for tile in out:
        assert tile["text"] not in ("7", "3", "0"), tile
        assert tile["state"] != "observed"


@needs_node
def test_coverage_strip_distinguishes_unrecorded_missing_synthetic_bounded(tmp_path):
    out = node_json(tmp_path, """
const W = (i, state, q, reasons) => ({window_start: 3000 + 300 * i, state, reasons: reasons || [], qualification: q});
const wins = [
  W(0, "observed", {mode: "bounded_observed", synthetic: false}),
  W(1, "observed", {mode: "bounded_observed", synthetic: true}),
  W(2, "provisional", null, ["qualified_without_served_row"]),
  W(3, "missing", null, ["no_coverage_record"]),
  W(4, "pre_universe", null, ["before_first_served_row"]),
  W(5, "zero", {mode: "bounded_observed", synthetic: false}),
  W(6, "bogus", null),
];
console.log(JSON.stringify({strip: Q.strip(wins), br: Q.breakdown(wins, 3000, 3000 + 300 * 10)}));""")
    cls = [c["qualification"] for c in out["strip"]]
    assert cls == ["bounded", "synthetic", "unrecorded", "unrecorded", "unrecorded", "bounded", "unrecorded"]
    assert [c["state"] for c in out["strip"]][3] == "missing"
    assert out["strip"][6]["state"] == "unknown"  # unrecognised state fails closed
    br = out["br"]
    assert br["by_state"] == {"observed": 2, "zero": 1, "partial": 0, "provisional": 1, "missing": 1,
                              "pre_universe": 1, "unknown": 1}
    assert br["by_qualification"] == {"bounded": 2, "synthetic": 1, "unrecorded": 4}
    assert br["listed"] == 7 and br["slots"] == 10
    assert br["not_listed"] == 3  # unlisted slots are not called missing and not zero
    assert "missing" not in br["not_listed_text"].lower() or "not" in br["not_listed_text"].lower()


@needs_node
def test_breakdown_counts_ignore_windows_outside_range_and_duplicates(tmp_path):
    out = node_json(tmp_path, """
const wins = [{window_start: 0, state: "observed"}, {window_start: 0, state: "missing"},
  {window_start: 99999, state: "observed"}, {window_start: 301, state: "observed"}, {window_start: 300, state: "partial"}];
console.log(JSON.stringify(Q.breakdown(wins, 0, 900)));""")
    assert out["by_state"]["observed"] == 1 and out["by_state"]["partial"] == 1
    assert out["by_state"]["missing"] == 0
    assert out["ignored"] == 3 and out["listed"] == 2 and out["slots"] == 3


@needs_node
def test_gap_list_holds_missing_and_pre_universe_only_in_order(tmp_path):
    out = node_json(tmp_path, """
const wins = [
  {window_start: 900, state: "missing", reasons: ["no_coverage_record"], missing: ["gap_a"], admission_refused: true, admission_reasons: ["r1"]},
  {window_start: 300, state: "pre_universe", reasons: ["before_first_served_row"]},
  {window_start: 600, state: "observed", reasons: []},
  {window_start: 1200, state: "provisional", reasons: ["row_provisional"]},
  {window_start: 0, state: "missing", reasons: []},
];
console.log(JSON.stringify(Q.gaps(wins, "jetton:A")));""")
    assert [g["window_start"] for g in out] == [0, 300, 900]
    assert [g["state"] for g in out] == ["missing", "pre_universe", "missing"]
    assert out[2]["reasons"] == ["no_coverage_record", "gap_a", "r1"] or "no_coverage_record" in out[2]["reasons"]
    assert all(g["href"].startswith("#/explorer/jetton%3AA/") for g in out)
    assert out[0]["href"].endswith("/0")


@needs_node
def test_explorer_link_encodes_hostile_tokens_and_parses_back(tmp_path):
    out = node_json(tmp_path, """
const tok = "jetton:<img src=x>/a b";
const href = Q.explorerHref(tok, 600);
const parts = href.replace(/^#\\//, "").split("/");
console.log(JSON.stringify({href, back: decodeURIComponent(parts[1]), start: parts[2]}));""")
    assert "<" not in out["href"] and " " not in out["href"]
    assert out["back"] == "jetton:<img src=x>/a b" and out["start"] == "600"


@needs_node
def test_quality_and_coverage_provenance_are_required_and_block(tmp_path):
    out = node_json(tmp_path, """
const prov = {source_kind: "unverified_origin", certification_state: "not_certified", coverage_state: "unrecorded",
  scoring_state: "scored_effectiveness_unmeasured", honesty_banner: "b", generated_at: "2026-01-01T00:00:00Z"};
const tries = {};
const run = (name, fn) => { try { fn(); tries[name] = "accepted"; }
  catch (e) { tries[name] = e instanceof C.ProvenanceError ? "blocked" : "other:" + e.message; } };
run("ok", () => Q.requireQualityProvenance({quality: {}, provenance: prov}));
run("none", () => Q.requireQualityProvenance({quality: {}}));
run("null", () => Q.requireQualityProvenance(null));
run("quality_ok", () => Q.requireQuality({quality: {}, provenance: prov}));
run("quality_bad", () => Q.requireQuality({quality: [], provenance: prov}));
run("list_ok", () => Q.requireList({coverage: []}, "coverage"));
run("list_bad", () => Q.requireList({coverage: {}}, "coverage"));
console.log(JSON.stringify(tries));""")
    assert out == {"ok": "accepted", "none": "blocked", "null": "blocked", "quality_ok": "accepted",
                   "quality_bad": "blocked", "list_ok": "accepted", "list_bad": "blocked"}


@needs_node
def test_partial_notice_when_coverage_is_cut(tmp_path):
    out = node_json(tmp_path, """
console.log(JSON.stringify({cut: Q.coverageCap({count: 4000, truncated: true}, 4), full: Q.coverageCap({count: 3, truncated: false}, 4)}));""")
    assert out["cut"]["partial"] is True and "4000" in out["cut"]["text"]
    assert out["full"] == {"partial": False, "text": ""}
