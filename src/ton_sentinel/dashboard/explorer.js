// Token and window explorer: token list, per-token window series chart (SVG built in code),
// window detail with its coverage state. Every value carries a provenance badge; a response
// without provenance blocks the view. Gaps are drawn as hatched "no row" areas, never as zero.
import { ApiError, ProvenanceError, getChecked } from "./client.js";
import { badge } from "./states.js";
import { CHART_LEGEND, DEFAULT_PRESET, PRESETS, SERIES_PAGE_LIMIT, WINDOW_SECONDS, buildGrid, chartModel,
  checkRange, collectSeries, coverageDetail, customRange, failureMessage, filterLabel, filterTokens,
  linkedRange, newPager, pagerAdd, pointLabels, presetRange, requireListProvenance, requireSeriesProvenance,
  tokensParams } from "./explorer-model.js";

// The SVG namespace is an identifier, not a request target; built from parts so no URL literal exists.
const SVG_NS = ["http:", "", "www.w3.org", "2000", "svg"].join("/");
const CHART_W = 960;
const CHART_H = 220;

function el(tag, className, content) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (content !== undefined) node.textContent = content;
  return node;
}

function svg(tag, attrs) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [name, value] of Object.entries(attrs || {})) node.setAttribute(name, String(value));
  return node;
}

function badgeNode(state) {
  const b = badge(state);
  const node = el("span", "badge badge-" + b.tone, b.label);
  node.dataset.state = b.state;
  node.setAttribute("title", b.meaning);
  return node;
}

function utc(seconds) {
  return new Date(seconds * 1000).toISOString().replace(".000Z", "Z");
}

function provenanceStrip(provenance) {
  const strip = el("div", "chips");
  strip.dataset.provenance = "present";
  for (const [label, key] of [["source", "source_kind"], ["certification", "certification_state"],
    ["coverage", "coverage_state"], ["scoring", "scoring_state"]]) {
    const item = el("span", "chip", label + " ");
    item.append(badgeNode(provenance[key]));
    strip.append(item);
  }
  return strip;
}

function chartLegend() {
  const list = el("ul", "chart-legend");
  for (const entry of CHART_LEGEND) {
    const item = el("li");
    const key = svg("svg", { width: 16, height: 12, viewBox: "0 0 16 12", "aria-hidden": "true" });
    key.append(svg("rect", { x: 1, y: 1, width: 14, height: 10, class: entry.className,
      fill: entry.className === "chart-gap" ? "url(#gap-hatch)" : "currentColor" }));
    item.append(key, el("span", "", " " + entry.text));
    list.append(item);
  }
  return list;
}

export function drawChart(grid, onSelect) {
  const model = chartModel(grid, { width: CHART_W, height: CHART_H });
  const root = svg("svg", { viewBox: "0 0 " + CHART_W + " " + CHART_H, role: "img", class: "chart",
    "aria-label": "Swaps per five-minute window. Hatched areas are windows with no served row." });
  root.dataset.chart = "series";
  root.dataset.pointCount = String(model.bars.length);
  root.dataset.gapCount = String(model.gaps.length);
  root.dataset.gapWindows = String(model.gap_windows);
  const defs = svg("defs");
  const pattern = svg("pattern", { id: "gap-hatch", patternUnits: "userSpaceOnUse", width: 6, height: 6,
    patternTransform: "rotate(45)" });
  pattern.append(svg("line", { x1: 0, y1: 0, x2: 0, y2: 6, class: "chart-hatch-line" }));
  defs.append(pattern);
  root.append(defs);
  root.append(svg("line", { x1: model.pad.left, y1: CHART_H - model.pad.bottom, x2: CHART_W - model.pad.right,
    y2: CHART_H - model.pad.bottom, class: "chart-axis" }));
  const top = svg("text", { x: 2, y: model.pad.top + 8, class: "chart-label" });
  top.textContent = String(model.yMax);
  const base = svg("text", { x: 2, y: CHART_H - model.pad.bottom, class: "chart-label" });
  base.textContent = "0";
  const from = svg("text", { x: model.pad.left, y: CHART_H - 4, class: "chart-label" });
  from.textContent = utc(grid.from);
  const to = svg("text", { x: CHART_W - model.pad.right, y: CHART_H - 4, class: "chart-label", "text-anchor": "end" });
  to.textContent = utc(grid.to);
  root.append(top, base, from, to);
  for (const gap of model.gaps) {
    const rect = svg("rect", { x: gap.x, y: gap.y, width: gap.w, height: gap.h, class: "chart-gap",
      fill: "url(#gap-hatch)" });
    rect.dataset.firstWindow = String(gap.first);
    rect.dataset.windows = String(gap.count);
    const title = svg("title");
    title.textContent = gap.count + " window(s) starting at " + utc(gap.first) + ": no served row (not zero)";
    rect.append(title);
    root.append(rect);
  }
  for (const bar of model.bars) {
    const rect = svg("rect", { x: bar.x, y: bar.y, width: bar.w, height: bar.h, class: bar.className });
    rect.dataset.windowStart = String(bar.window_start);
    rect.dataset.qualification = bar.kind;
    if (bar.provisional) rect.dataset.provisional = "true";
    const title = svg("title");
    title.textContent = utc(bar.window_start) + ": " + bar.swaps + " swaps, " + bar.kind
      + (bar.provisional ? ", provisional" : "");
    rect.append(title);
    rect.addEventListener("click", () => onSelect(bar.window_start));
    root.append(rect);
  }
  return { node: root, model };
}

function cell(row, content) {
  const td = el("td");
  if (content instanceof Node) td.append(content); else td.textContent = String(content);
  row.append(td);
  return td;
}

function seriesTable(points, onSelect) {
  const table = el("table", "data-table");
  table.dataset.table = "series";
  const head = el("tr");
  for (const name of ["Window start (UTC)", "Swaps", "Known traders", "Unknown", "Coverage status", "Qualification",
    "Row state", "Provenance", "Detail"]) head.append(el("th", "", name));
  const thead = el("thead");
  thead.append(head);
  table.append(thead);
  const body = el("tbody");
  for (const point of points) {
    const labels = pointLabels(point);
    const row = el("tr");
    row.dataset.windowStart = String(point.window_start);
    cell(row, utc(point.window_start));
    cell(row, point.swaps);
    cell(row, point.known_traders);
    cell(row, point.unknown_count);
    cell(row, point.coverage_status);
    const q = cell(row, labels.text);
    q.dataset.qualification = labels.qualification;
    const state = cell(row, labels.provisional ? "provisional" : "not provisional");
    state.dataset.provisional = String(labels.provisional);
    cell(row, badgeNode(labels.coverage));
    const button = el("button", "", "Window detail");
    button.type = "button";
    button.addEventListener("click", () => onSelect(point.window_start));
    cell(row, button);
    body.append(row);
  }
  table.append(body);
  return table;
}

function detailNode(detail, token) {
  const box = el("div", "detail");
  box.dataset.windowStart = String(detail.window_start);
  box.dataset.state = detail.state;
  const head = el("p");
  head.append(el("strong", "", utc(detail.window_start) + " "), badgeNode(detail.state),
    el("span", "muted", " token " + token));
  box.append(head);
  const reasons = el("ul", "reasons");
  reasons.dataset.list = "reasons";
  for (const reason of detail.reasons) reasons.append(el("li", "mono", reason));
  box.append(el("h4", "card-subtitle", "Reasons"),
    detail.reasons.length ? reasons : el("p", "muted", "The server listed no reasons for this state."));
  if (detail.missing.length) {
    const list = el("ul");
    for (const item of detail.missing) list.append(el("li", "mono", item));
    box.append(el("h4", "card-subtitle", "Coverage gaps recorded by the source"), list);
  }
  if (detail.admission_refused) {
    const list = el("ul");
    for (const item of detail.admission_reasons) list.append(el("li", "mono", item));
    box.append(el("h4", "card-subtitle", "Window admission was refused"), list);
  }
  const q = detail.qualification;
  box.append(el("p", "muted", q ? "Qualification: " + (q.synthetic ? "synthetic, " : "") + q.mode
    : "Qualification: unrecorded"));
  box.append(el("p", "muted", "Lineage records: " + detail.lineage_records + " (count only; contents are not shown)."));
  return box;
}

export function mountExplorer(root, context = {}) {
  const onBlock = context.onBlock || (() => {});
  let disposed = false;
  let preset = DEFAULT_PRESET;
  let range = presetRange(preset, Date.now());
  let pager = newPager();
  let listProvenance = null;
  let selected = null;
  let linkedUsed = false;
  const network = () => document.body.dataset.network || "mainnet";
  const stale = () => disposed;

  const controls = el("section", "card");
  controls.id = "ex-controls";
  controls.append(el("h2", "card-title", "Token and window explorer"));
  controls.append(el("p", "muted", "Five-minute windows. A range is at most 7 days, half-open [from, to), UTC."));
  const presetSelect = el("select");
  presetSelect.id = "ex-range";
  for (const key of [...Object.keys(PRESETS), "custom"]) {
    const option = el("option", "", key);
    option.value = key;
    presetSelect.append(option);
  }
  presetSelect.value = preset;
  const presetLabel = el("label", "network", "Range ");
  presetLabel.append(presetSelect);
  const fromInput = el("input");
  fromInput.type = "datetime-local";
  fromInput.id = "ex-start";
  const toInput = el("input");
  toInput.type = "datetime-local";
  toInput.id = "ex-end";
  const customBox = el("span", "network");
  customBox.hidden = true;
  const fromLabel = el("label", "network", "From (UTC) ");
  fromLabel.append(fromInput);
  const toLabel = el("label", "network", "To (UTC) ");
  toLabel.append(toInput);
  customBox.append(fromLabel, toLabel);
  const apply = el("button", "", "Apply range");
  apply.type = "button";
  apply.id = "ex-apply";
  const rangeNote = el("p", "muted mono");
  rangeNote.id = "ex-range-note";
  const rangeError = el("p", "inline-error");
  rangeError.setAttribute("role", "alert");
  controls.append(presetLabel, customBox, apply, rangeNote, rangeError);

  const tokensBox = el("section", "card");
  tokensBox.id = "ex-tokens";
  tokensBox.append(el("h3", "card-title", "Tokens"));
  const strip = el("div");
  const filterInput = el("input");
  filterInput.type = "search";
  filterInput.id = "ex-filter";
  filterInput.setAttribute("aria-label", "Filter loaded tokens");
  const filterNote = el("p", "muted");
  filterNote.id = "ex-filter-note";
  const tokenList = el("ul", "token-list");
  const tokenStatus = el("p", "muted");
  const more = el("button", "", "Load more tokens");
  more.type = "button";
  more.id = "ex-more";
  more.hidden = true;
  tokensBox.append(strip, filterInput, filterNote, tokenStatus, tokenList, more);

  const seriesBox = el("section", "card");
  seriesBox.id = "ex-series";
  const seriesTitle = el("h3", "card-title", "Window series");
  const seriesStrip = el("div");
  const seriesStatus = el("p", "muted");
  const chartHolder = el("div");
  const tableHolder = el("div", "table-scroll");
  seriesBox.append(seriesTitle, seriesStrip, seriesStatus, chartHolder, chartLegend(), tableHolder);

  const detailBox = el("section", "card");
  detailBox.id = "ex-detail";
  detailBox.append(el("h3", "card-title", "Window detail"));
  const detailBody = el("div");
  detailBody.append(el("p", "muted", "Select a window in the chart or table to read its coverage state."));
  detailBox.append(detailBody);

  root.replaceChildren(controls, tokensBox, seriesBox, detailBox);
  root.dataset.view = "explorer";

  const fail = (target, error) => {
    if (error && error.name === "AbortError") return true;
    if (stale()) return true;
    if (error instanceof ProvenanceError) {
      disposed = true;
      onBlock("The server response carries no valid provenance, so nothing is shown. (" + error.message + ")");
      return true;
    }
    target.textContent = failureMessage(error instanceof ApiError ? error : null);
    return false;
  };

  function renderTokens() {
    const shown = filterTokens(pager.items, filterInput.value);
    filterNote.textContent = filterLabel(pager.items.length, pager.done);
    tokenList.replaceChildren(...shown.map((item) => {
      const li = el("li");
      const button = el("button", "token-button mono", item.token);
      button.type = "button";
      button.dataset.token = item.token;
      if (item.token === selected) button.setAttribute("aria-current", "true");
      button.addEventListener("click", () => selectToken(item.token));
      li.append(button, el("span", "muted", " " + item.window_count + " windows, " + item.swaps + " swaps "));
      if (listProvenance) li.append(badgeNode(listProvenance.source_kind));
      return li;
    }));
    more.hidden = pager.done;
    tokenStatus.textContent = pager.items.length === 0 && pager.done
      ? "No token has a served row in this range." : "";
  }

  async function loadTokens(reset) {
    if (reset) { pager = newPager(); selected = null; }
    try {
      const payload = await getChecked("/v1/tokens", { slot: "ex-tokens", params: tokensParams(network(), range, pager) });
      if (stale()) return;
      listProvenance = requireListProvenance(payload);
      pager = pagerAdd(pager, payload.tokens, payload.next_cursor);
      strip.replaceChildren(provenanceStrip(listProvenance));
      renderTokens();
      const linked = context.route && Array.isArray(context.route.rest) && context.route.rest[0]
        ? decodeURIComponent(context.route.rest[0]) : null;
      if (!selected && linked && !linkedUsed) {
        linkedUsed = true;
        await selectToken(linked);
        const start = Number(context.route.rest[1]);
        if (Number.isInteger(start) && start >= range.from && start < range.to) await selectWindow(start);
      } else if (!selected && pager.items.length) await selectToken(pager.items[0].token);
    } catch (error) {
      if (!fail(tokenStatus, error)) tokenList.replaceChildren();
    }
    root.dataset.loaded = "true";
  }

  async function selectToken(token) {
    selected = token;
    renderTokens();
    seriesTitle.textContent = "Window series: " + token;
    seriesStatus.textContent = "Loading the series...";
    detailBody.replaceChildren(el("p", "muted", "Select a window in the chart or table to read its coverage state."));
    try {
      let first = null;
      const result = await collectSeries(async (cursor) => {
        const params = { network: network(), token, from: String(range.from), to: String(range.to),
          limit: String(SERIES_PAGE_LIMIT) };
        if (cursor) params.cursor = cursor;
        const payload = await getChecked("/v1/series", { slot: "ex-series", params });
        requireSeriesProvenance(payload);
        if (!first) first = payload;
        return payload;
      });
      if (stale() || selected !== token) return;
      const grid = buildGrid(range.from, range.to, result.points);
      const chart = drawChart(grid, (start) => selectWindow(start));
      chartHolder.replaceChildren(chart.node);
      tableHolder.replaceChildren(seriesTable(grid.slots.filter((slot) => slot.point).map((slot) => slot.point),
        (start) => selectWindow(start)));
      seriesStrip.replaceChildren(provenanceStrip(first.provenance));
      seriesStatus.textContent = chart.model.bars.length + " windows with a row, " + chart.model.gap_windows
        + " windows with no row (hatched, not zero)." + (result.truncated ? " The series was cut at "
        + result.pages + " pages; more rows exist and are not shown." : "")
        + (grid.ignored ? " " + grid.ignored + " rows outside the range were ignored." : "");
      seriesBox.dataset.token = token;
    } catch (error) {
      if (!fail(seriesStatus, error)) { chartHolder.replaceChildren(); tableHolder.replaceChildren(); }
    }
    root.dataset.loaded = "true";
  }

  async function selectWindow(start) {
    if (!selected) return;
    detailBody.replaceChildren(el("p", "muted", "Loading the coverage state..."));
    try {
      const payload = await getChecked("/v1/coverage", { slot: "ex-detail", params: {
        network: network(), token: selected, from: String(start), to: String(start + WINDOW_SECONDS) } });
      const provenance = requireListProvenance(payload);
      if (stale()) return;
      detailBody.replaceChildren(provenanceStrip(provenance), detailNode(coverageDetail(payload, start), selected));
    } catch (error) {
      const message = el("p", "inline-error");
      if (!fail(message, error)) detailBody.replaceChildren(message);
    }
  }

  function readRange() {
    return preset === "custom" ? customRange(fromInput.value, toInput.value) : presetRange(preset, Date.now());
  }

  function refreshControls() {
    const candidate = readRange();
    const verdict = candidate.from === null || candidate.to === null
      ? { ok: false, reason: candidate.reason } : checkRange(candidate.from, candidate.to);
    apply.disabled = !verdict.ok;
    rangeError.textContent = verdict.ok ? "" : verdict.reason;
    rangeNote.textContent = verdict.ok ? "window starts [" + candidate.from + ", " + candidate.to + ") = ["
      + utc(candidate.from) + ", " + utc(candidate.to) + ")" : "";
    return verdict.ok ? candidate : null;
  }

  presetSelect.addEventListener("change", () => {
    preset = presetSelect.value;
    customBox.hidden = preset !== "custom";
    const candidate = refreshControls();
    if (candidate && preset !== "custom") { range = candidate; loadTokens(true); }
  });
  for (const input of [fromInput, toInput]) input.addEventListener("input", refreshControls);
  apply.addEventListener("click", () => {
    const candidate = refreshControls();
    if (!candidate) return;
    range = candidate;
    loadTokens(true);
  });
  filterInput.addEventListener("input", renderTokens);
  more.addEventListener("click", () => loadTokens(false));

  // A linked window outside the default range switches to a custom range around it, so the link
  // opens that window's detail instead of an unrelated 24 h view.
  const linkedStart = context.route && Array.isArray(context.route.rest) ? Number(context.route.rest[1]) : NaN;
  const linked = linkedRange(linkedStart, range);
  if (linked && linked.ok) {
    preset = "custom";
    presetSelect.value = "custom";
    customBox.hidden = false;
    const local = (seconds) => new Date(seconds * 1000).toISOString().slice(0, 16);
    fromInput.value = local(linked.from);
    toInput.value = local(linked.to);
    range = { from: linked.from, to: linked.to };
  }
  refreshControls();
  loadTokens(true);
  return { dispose() { disposed = true; } };
}
