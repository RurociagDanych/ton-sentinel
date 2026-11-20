"""Token and window explorer."""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from ton_sentinel import api

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
NEW_JS = ["explorer-model.js", "explorer.js"]
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
COVERAGE_STATES = ("zero", "partial", "provisional", "missing", "pre_universe")


def text(name):
    return (DASH / name).read_text(encoding="utf-8")


def node_json(tmp_path, body):
    script = tmp_path / "check.mjs"
    script.write_text(
        f'import * as X from "file://{DASH}/explorer-model.js";\n'
        f'import * as S from "file://{DASH}/states.js";\n'
        f'import * as C from "file://{DASH}/client.js";\n{body}\n')
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


FIXTURE_JS = """
const T0 = 1700000100 - (1700000100 % 300);
const prov = (cov, src) => ({source_kind: src || "unverified_origin", certification_state: "not_certified",
  coverage_state: cov || "unrecorded", scoring_state: "scored_effectiveness_unmeasured",
  honesty_banner: "b", generated_at: "2026-01-01T00:00:00Z"});
const point = (i, extra) => Object.assign({window_start: T0 + 300 * i, window_end: T0 + 300 * i + 300,
  generation: "g", swaps: i + 1, known_traders: 1, unknown_count: 0, coverage_status: "unknown",
  status: "ok", provisional: false, qualification: null, provenance: prov()}, extra || {});
"""



def test_explorer_modules_are_allowlisted_packaged_and_served():
    for name in NEW_JS:
        assert (DASH / name).is_file(), name
        assert name in api._DASHBOARD_ASSETS, name


@pytest.mark.parametrize("name", NEW_JS)
def test_explorer_modules_follow_shell_rules(name):
    src = text(name)
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(",
                      "new Function", "XMLHttpRequest", "WebSocket", "localStorage",
                      "sessionStorage", "document.cookie", "fetch("):
        assert forbidden not in src, (name, forbidden)
    assert "https://" not in src and "http://" not in src
    assert not re.search(r"\.style\s*[.=\[]|setAttribute\(\s*[\"']style|\sstyle\s*=", src)
    for spec in re.findall(r"""(?:from|import)\s*\(?\s*[\"']([^\"']+)[\"']""", src):
        assert spec.startswith("./"), (name, spec)


def test_views_module_routes_explorer_to_real_view_and_keeps_other_stubs():
    views = text("views.js")
    assert 'from "./explorer.js"' in views and "mountExplorer" in views
    assert views.count("This view is a stub") == 0


def test_explorer_uses_only_documented_endpoints_through_the_client():
    src = text("explorer.js") + text("explorer-model.js")
    for path in ("/v1/tokens", "/v1/series", "/v1/coverage"):
        assert f'"{path}"' in src, path
    assert "getChecked" in src
    assert "wallet" not in src.lower()


def test_chart_is_svg_built_in_code_without_inline_style_or_remote_origin():
    src = text("explorer.js")
    assert "createElementNS" in src and "<svg" not in src
    assert "pattern" in src and 'class' in src.replace("className", "class")
    assert not re.search(r"<style|style=", src)


def test_css_defines_explorer_chart_classes():
    css = text("app.css")
    for cls in (".chart-gap", ".chart-bar", ".chart-bar-synthetic", ".chart-bar-unrecorded",
                ".chart-bar-provisional", ".chart-hatch-line"):
        assert cls in css, cls


def test_legend_knows_every_coverage_state_the_detail_panel_can_show():
    legend = text("states.js")
    missing = [s for s in COVERAGE_STATES if f'"{s}"' not in legend]
    assert missing == []


def test_index_still_has_no_inline_script_or_style():
    html = text("index.html")
    assert not re.search(r"<style\b|\sstyle\s*=|\son[a-z]+\s*=", html, re.I)


def test_harness_has_explorer_scenarios():
    source = (ROOT / "scripts" / "dashboard_browser_smoke.py").read_text()
    for needle in ("explorer_series_rendered", "explorer_blocked_series_missing_provenance",
                   "explorer_stubs", "chart-gap"):
        assert needle in source, needle



@needs_node
def test_range_over_7_days_disabled_client_side_and_400_handled(tmp_path):
    out = node_json(tmp_path, FIXTURE_JS + """
const seven = 7 * 86400;
const checks = {
  exactly_seven: X.checkRange(T0, T0 + seven),
  over_by_one_window: X.checkRange(T0, T0 + seven + 300),
  reversed: X.checkRange(T0 + 300, T0),
  empty: X.checkRange(T0, T0),
  unaligned: X.checkRange(T0 + 1, T0 + 301),
  not_integer: X.checkRange(1.5, 301),
  negative: X.checkRange(-300, 300),
  nan: X.checkRange(NaN, 300),
};
const message = X.failureMessage(new C.ApiError("request failed", 400));
const other = X.failureMessage(new C.ApiError("request failed", 503));
console.log(JSON.stringify({checks, message, other, max: X.MAX_RANGE_SECONDS}));""")
    assert out["max"] == 7 * 86400
    ok = {k: v["ok"] for k, v in out["checks"].items()}
    assert ok == {"exactly_seven": True, "over_by_one_window": False, "reversed": False,
                  "empty": False, "unaligned": False, "not_integer": False, "negative": False,
                  "nan": False}
    assert "7 days" in out["checks"]["over_by_one_window"]["reason"]
    assert all(v["ok"] or v["reason"] for v in out["checks"].values())
    assert "refused" in out["message"].lower() and "range" in out["message"].lower()
    assert "400" not in out["other"] or "refused" not in out["other"].lower()


@needs_node
def test_preset_and_custom_ranges_are_aligned_utc_and_within_limit(tmp_path):
    out = node_json(tmp_path, """
const now = Date.UTC(2026, 10, 15, 12, 3, 41);
const presets = Object.fromEntries(Object.keys(X.PRESETS).map(k => [k, X.presetRange(k, now)]));
const custom = X.customRange("2026-11-14T10:01", "2026-11-14T11:00");
const bad = X.customRange("", "2026-11-14T11:00");
const wide = X.customRange("2026-11-01T00:00", "2026-11-14T00:00");
console.log(JSON.stringify({presets, custom, bad, wide, keys: Object.keys(X.PRESETS)}));""")
    for key, r in out["presets"].items():
        assert r["from"] % 300 == 0 and r["to"] % 300 == 0 and 0 < r["to"] - r["from"] <= 7 * 86400
    assert out["keys"][-1] == "7d" and out["presets"]["7d"]["to"] - out["presets"]["7d"]["from"] == 7 * 86400
    # 2026-11-14T10:01Z (UTC) rounds down to 10:00Z; 11:00Z is already aligned
    from datetime import datetime, timezone
    ten = int(datetime(2026, 11, 14, 10, 0, tzinfo=timezone.utc).timestamp())
    assert out["custom"]["ok"] is True
    assert out["custom"]["from"] == ten and out["custom"]["to"] == ten + 3600
    assert out["bad"]["ok"] is False
    assert out["wide"]["ok"] is False and "7 days" in out["wide"]["reason"]



@needs_node
def test_token_paging_is_keyset_deduplicated_and_ends_without_cursor(tmp_path):
    out = node_json(tmp_path, """
const range = {from: 600, to: 3600};
let pager = X.newPager();
const first = X.tokensParams("mainnet", range, pager);
pager = X.pagerAdd(pager, [{token: "a"}, {token: "b"}], "CUR1");
const second = X.tokensParams("mainnet", range, pager);
pager = X.pagerAdd(pager, [{token: "b"}, {token: "c"}], null);
console.log(JSON.stringify({first, second, items: pager.items.map(i => i.token),
  cursor: pager.cursor, done: pager.done, limit: X.TOKEN_PAGE_LIMIT}));""")
    assert "cursor" not in out["first"] and out["first"]["network"] == "mainnet"
    assert out["first"]["from"] == "600" and out["first"]["to"] == "3600"
    assert out["first"]["limit"] == str(out["limit"])
    assert out["second"]["cursor"] == "CUR1"
    assert out["items"] == ["a", "b", "c"] and out["cursor"] is None and out["done"] is True


@needs_node
def test_token_filter_labelled_as_loaded_page_only(tmp_path):
    out = node_json(tmp_path, """
const items = [{token: "jetton:Alpha"}, {token: "jetton:beta"}, {token: "other"}];
console.log(JSON.stringify({
  hit: X.filterTokens(items, "ALPHA").map(i => i.token),
  empty: X.filterTokens(items, "").length,
  none: X.filterTokens(items, "zzz").length,
  label_more: X.filterLabel(3, false),
  label_done: X.filterLabel(3, true),
}));""")
    assert out["hit"] == ["jetton:Alpha"] and out["empty"] == 3 and out["none"] == 0
    assert "loaded" in out["label_more"] and "3" in out["label_more"]
    assert "not" in out["label_more"].lower() or "only" in out["label_more"].lower()
    assert "more" in out["label_more"].lower()
    assert "loaded" in out["label_done"]
    assert "filter" in text("explorer.js").lower() and "filterLabel" in text("explorer.js")



@needs_node
def test_series_gap_rendered_as_no_row_hatch(tmp_path):
    out = node_json(tmp_path, FIXTURE_JS + """
const rows = [0, 1, 4, 5, 9].map(i => point(i));
const grid = X.buildGrid(T0, T0 + 300 * 12, rows);
const chart = X.chartModel(grid, {width: 1200, height: 200});
console.log(JSON.stringify({slots: grid.slots.length, present: grid.slots.filter(s => s.point).length,
  bars: chart.bars.map(b => [b.window_start - T0, b.h > 0]), gaps: chart.gaps.map(g => [g.first - T0, g.count]),
  gap_total: chart.gap_windows, ignored: grid.ignored}));""")
    assert out["slots"] == 12 and out["present"] == 5
    assert [b[0] for b in out["bars"]] == [0, 300, 1200, 1500, 2700]
    assert all(b[1] for b in out["bars"])
    assert out["gaps"] == [[600, 2], [1800, 3], [3000, 2]]
    assert out["gap_total"] == 7 and out["ignored"] == 0


@needs_node
def test_gap_is_never_filled_with_zero_and_foreign_points_are_ignored(tmp_path):
    out = node_json(tmp_path, FIXTURE_JS + """
const rows = [point(0), point(0, {swaps: 99}), point(50), point(1, {window_start: T0 + 301})];
const grid = X.buildGrid(T0, T0 + 300 * 4, rows);
const chart = X.chartModel(grid, {width: 800, height: 100});
console.log(JSON.stringify({present: grid.slots.filter(s => s.point).length, first: grid.slots[0].point.swaps,
  ignored: grid.ignored, bars: chart.bars.length, gaps: chart.gaps.map(g => g.count), empty: X.chartModel(X.buildGrid(T0, T0 + 900, []), {width: 800, height: 100}).bars.length}));""")
    assert out["present"] == 1 and out["first"] == 1 and out["ignored"] == 3
    assert out["bars"] == 1 and out["gaps"] == [3] and out["empty"] == 0


@needs_node
def test_unrecorded_and_synthetic_windows_labelled_on_chart_and_table(tmp_path):
    out = node_json(tmp_path, FIXTURE_JS + """
const synth = {mode: "bounded_observed", synthetic: true};
const real = {mode: "bounded_observed", synthetic: false};
const rows = [point(0), point(1, {qualification: synth, provenance: prov("synthetic_bounded_observed")}),
  point(2, {qualification: real, provenance: prov("bounded_observed")})];
const labels = rows.map(p => X.pointLabels(p));
const chart = X.chartModel(X.buildGrid(T0, T0 + 900, rows), {width: 900, height: 100});
console.log(JSON.stringify({labels, kinds: chart.bars.map(b => b.kind), classes: chart.bars.map(b => b.className),
  legend: chart.legend}));""")
    kinds = [lab["qualification"] for lab in out["labels"]]
    assert kinds == ["unrecorded", "synthetic", "bounded_observed"]
    assert out["kinds"] == ["unrecorded", "synthetic", "bounded_observed"]
    assert "chart-bar-unrecorded" in out["classes"][0] and "chart-bar-synthetic" in out["classes"][1]
    assert "chart-bar-unrecorded" not in out["classes"][2] and "chart-bar-synthetic" not in out["classes"][2]
    assert out["labels"][0]["text"] == "unrecorded" and out["labels"][1]["text"].startswith("synthetic")
    assert out["legend"] and "unrecorded" in json.dumps(out["legend"]) and "synthetic" in json.dumps(out["legend"])
    src = text("explorer.js")
    assert "pointLabels" in src and "dataset.qualification" in src


@needs_node
def test_provisional_rows_marked(tmp_path):
    out = node_json(tmp_path, FIXTURE_JS + """
const rows = [point(0, {provisional: true}), point(1)];
const chart = X.chartModel(X.buildGrid(T0, T0 + 600, rows), {width: 600, height: 100});
console.log(JSON.stringify({labels: rows.map(p => X.pointLabels(p)), classes: chart.bars.map(b => b.className)}));""")
    assert out["labels"][0]["provisional"] is True and out["labels"][1]["provisional"] is False
    assert "chart-bar-provisional" in out["classes"][0]
    assert "chart-bar-provisional" not in out["classes"][1]
    assert "provisional" in text("explorer.js")


@needs_node
def test_series_collection_follows_cursor_and_flags_truncation(tmp_path):
    out = node_json(tmp_path, """
const mk = (n, cursor) => ({series: Array.from({length: n}, (_, i) => ({i})), next_cursor: cursor});
const pages = [mk(2, "c1"), mk(2, "c2"), mk(1, null)];
let calls = [];
const done = await X.collectSeries(async (cursor) => { calls.push(cursor); return pages[calls.length - 1]; }, 4);
calls = [];
const endless = await X.collectSeries(async (cursor) => { calls.push(cursor); return mk(1, "more"); }, 3);
console.log(JSON.stringify({done: [done.points.length, done.truncated, done.pages], first_calls: calls,
  endless: [endless.points.length, endless.truncated, endless.pages], max: X.MAX_SERIES_PAGES}));""")
    assert out["done"] == [5, False, 3]
    assert out["endless"] == [3, True, 3]
    assert out["first_calls"] == [None, "more", "more"] and out["max"] >= 3



@needs_node
def test_missing_provenance_blocks_explorer_inputs(tmp_path):
    out = node_json(tmp_path, FIXTURE_JS + """
const result = {};
const attempt = (name, fn) => { try { fn(); result[name] = "accepted"; } catch (e) { result[name] = e.name; } };
const good = {tokens: [], provenance: prov()};
attempt("tokens_ok", () => X.requireListProvenance(good));
attempt("tokens_missing", () => X.requireListProvenance({tokens: []}));
attempt("tokens_partial", () => X.requireListProvenance({tokens: [], provenance: {source_kind: "x"}}));
attempt("series_ok", () => X.requireSeriesProvenance({series: [point(0)], provenance: prov()}));
attempt("series_point_missing", () => X.requireSeriesProvenance({series: [Object.assign(point(0), {provenance: undefined})], provenance: prov()}));
attempt("series_top_missing", () => X.requireSeriesProvenance({series: [point(0)]}));
attempt("series_not_list", () => X.requireSeriesProvenance({series: "x", provenance: prov()}));
console.log(JSON.stringify(result));""")
    assert out == {"tokens_ok": "accepted", "tokens_missing": "ProvenanceError",
                   "tokens_partial": "ProvenanceError", "series_ok": "accepted",
                   "series_point_missing": "ProvenanceError", "series_top_missing": "ProvenanceError",
                   "series_not_list": "ProvenanceError"}


def test_explorer_blocks_the_shell_on_provenance_errors():
    src = text("explorer.js")
    assert "ProvenanceError" in src and "onBlock" in src
    assert src.count("getChecked") >= 3



@needs_node
def test_window_detail_shows_coverage_state_and_reasons_verbatim(tmp_path):
    out = node_json(tmp_path, """
const row = (ws, state, reasons, extra) => Object.assign({window_start: ws, window_end: ws + 300, state, reasons,
  qualification: null, missing: [], admission_refused: false, admission_reasons: [],
  served: {rows: 1, coverage_status_counts: {unknown: 1}, known_traders: 1, unknown_count: 0}, lineage_records: 4}, extra || {});
const payload = {coverage: [row(900, "partial", ["no_qualification", "coverage_missing"], {
  missing: ["source_gap:range_a"], admission_refused: true, admission_reasons: ["epoch_mismatch"]})]};
const d = X.coverageDetail(payload, 900);
const absent = X.coverageDetail(payload, 1200);
const unknown = X.coverageDetail({coverage: [row(900, "mystery", [])]}, 900);
const malformed = X.coverageDetail({coverage: "nope"}, 900);
console.log(JSON.stringify({d, absent, unknown_known: S.badge(unknown.state).known, malformed}));""")
    d = out["d"]
    assert d["state"] == "partial" and d["reasons"] == ["no_qualification", "coverage_missing"]
    assert d["missing"] == ["source_gap:range_a"] and d["admission_reasons"] == ["epoch_mismatch"]
    assert d["admission_refused"] is True and d["lineage_records"] == 4
    assert out["absent"]["state"] == "unavailable" and out["malformed"]["state"] == "unavailable"
    assert out["unknown_known"] is False
    assert "lineage" not in json.dumps({k: v for k, v in d.items() if k != "lineage_records"})


def test_detail_panel_reads_coverage_for_exactly_one_window():
    src = text("explorer.js")
    assert '"/v1/coverage"' in src and "WINDOW_SECONDS" in src
    assert "reasons" in src
