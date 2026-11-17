// Data quality and coverage: the quality counters as tiles (observed, or unavailable with its
// reason, never zero), a coverage breakdown per state for one token and range, and a gap list linking
// to the explorer. The completeness disclaimer is built before any request and is never hidden. A
// response without provenance blocks the view.
import { ApiError, ProvenanceError, getChecked } from "./client.js";
import { badge } from "./states.js";
import { DEFAULT_PRESET, PRESETS, failureMessage, presetRange, tokensParams, newPager, pagerAdd } from "./explorer-model.js";
import { COMPLETENESS_TEXT, COVERAGE_APPROXIMATIONS, COVERAGE_STATES, QUALIFICATION_CLASSES, breakdown, coverageCap,
  gaps, requireList, requireQuality, requireQualityProvenance, strip, tiles } from "./quality-model.js";

const PAGE_LIMIT = 1000;
const MAX_PAGES = 4;

function el(tag, className, content) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (content !== undefined) node.textContent = content;
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
  const bar = el("div", "chips");
  bar.dataset.provenance = "present";
  for (const [label, key] of [["source", "source_kind"], ["certification", "certification_state"],
    ["coverage", "coverage_state"], ["scoring", "scoring_state"]]) {
    const item = el("span", "chip", label + " ");
    item.append(badgeNode(provenance[key]));
    bar.append(item);
  }
  return bar;
}

function cell(tr, content) {
  const td = el("td");
  if (content instanceof Node) td.append(content); else td.textContent = String(content);
  tr.append(td);
  return td;
}

function headRow(names) {
  const head = el("tr");
  for (const name of names) head.append(el("th", "", name));
  const thead = el("thead");
  thead.append(head);
  return thead;
}

export function mountQuality(root, context = {}) {
  const onBlock = context.onBlock || (() => {});
  const wanted = context.route && Array.isArray(context.route.rest) && context.route.rest[0]
    ? decodeURIComponent(context.route.rest[0]) : null;
  let disposed = false;
  let preset = DEFAULT_PRESET;
  let range = presetRange(preset, Date.now());
  let selected = wanted;
  let pager = newPager();
  const network = () => document.body.dataset.network || "mainnet";
  const stale = () => disposed;

  const banner = el("section", "card detection-banner");
  banner.dataset.banner = "quality-completeness";
  banner.setAttribute("role", "note");
  banner.append(el("h2", "card-title", "Counts are not a completeness claim"), el("p", "", COMPLETENESS_TEXT));

  const controls = el("section", "card");
  controls.append(el("h2", "card-title", "Data quality and coverage"));
  const rangeSelect = el("select");
  rangeSelect.id = "dq-range";
  for (const key of Object.keys(PRESETS)) {
    const option = el("option", "", key);
    option.value = key;
    rangeSelect.append(option);
  }
  rangeSelect.value = preset;
  const rangeLabel = el("label", "network", "Range ");
  rangeLabel.append(rangeSelect);
  const tokenSelect = el("select");
  tokenSelect.id = "dq-token";
  const tokenLabel = el("label", "network", "Token ");
  tokenLabel.append(tokenSelect);
  const rangeNote = el("p", "muted mono");
  controls.append(rangeLabel, tokenLabel, rangeNote);

  const tilesBox = el("section", "card");
  tilesBox.id = "dq-tiles";
  tilesBox.append(el("h3", "card-title", "Quality counters"));
  const tilesStrip = el("div");
  const tilesStatus = el("p", "muted");
  const tileGrid = el("div", "tile-grid");
  tilesBox.append(tilesStrip, tilesStatus, tileGrid);

  const covBox = el("section", "card");
  covBox.id = "dq-coverage";
  covBox.append(el("h3", "card-title", "Coverage per window"));
  const covStrip = el("div");
  const partial = el("p", "partial-banner");
  partial.setAttribute("role", "status");
  partial.hidden = true;
  partial.dataset.partial = "false";
  const covStatus = el("p", "muted");
  const covBody = el("div");
  const approx = el("ul", "reasons");
  approx.dataset.approximations = "";
  for (const line of COVERAGE_APPROXIMATIONS) approx.append(el("li", "muted", line));
  covBox.append(el("p", "muted", "Approximations used by the coverage states:"), approx, covStrip, partial, covStatus, covBody);

  const gapBox = el("section", "card");
  gapBox.id = "dq-gaps";
  gapBox.append(el("h3", "card-title", "Gap list: missing and before first row"));
  const gapBody = el("div");
  gapBox.append(gapBody);

  root.replaceChildren(banner, controls, tilesBox, covBox, gapBox);
  root.dataset.view = "quality";

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

  function renderTiles(list) {
    tileGrid.replaceChildren(...list.map((tile) => {
      const box = el("div", "quality-tile" + (tile.state === "observed" ? "" : " tile-unavailable"));
      box.dataset.counter = tile.key;
      box.dataset.state = tile.state;
      box.append(el("h4", "tile-title", tile.title));
      const value = el("p", "tile-value", tile.text);
      value.dataset.value = "";
      box.append(value);
      const state = el("p", "tile-state");
      state.append(badgeNode(tile.state));
      if (tile.detail) state.append(" ", el("span", "muted mono", tile.detail));
      box.append(state, el("p", "muted", "scope: " + tile.scope));
      return box;
    }));
  }

  async function loadTiles() {
    tilesStatus.textContent = "Loading the quality counters...";
    try {
      const payload = await getChecked("/v1/quality", { slot: "dq-tiles", params: {
        network: network(), from: String(range.from), to: String(range.to) } });
      const provenance = requireQualityProvenance(payload);
      const quality = requireQuality(payload);
      if (stale()) return;
      tilesStrip.replaceChildren(provenanceStrip(provenance));
      tilesStatus.textContent = typeof payload.label === "string" ? payload.label : "";
      renderTiles(tiles(quality));
    } catch (error) {
      if (!fail(tilesStatus, error)) tileGrid.replaceChildren();
    }
  }

  async function loadTokens() {
    try {
      const payload = await getChecked("/v1/tokens", { slot: "dq-tokens", params: tokensParams(network(), range, pager) });
      requireQualityProvenance(payload);
      pager = pagerAdd(pager, requireList(payload, "tokens"), payload.next_cursor);
      if (stale()) return;
      const options = pager.items.map((item) => {
        const option = el("option", "", item.token);
        option.value = item.token;
        return option;
      });
      if (selected && !pager.items.some((item) => item.token === selected)) {
        const extra = el("option", "", selected);
        extra.value = selected;
        options.unshift(extra);
      }
      tokenSelect.replaceChildren(...options);
      if (!selected && pager.items.length) selected = pager.items[0].token;
      if (selected) tokenSelect.value = selected;
    } catch (error) {
      fail(covStatus, error);
    }
  }

  async function loadCoverage() {
    gapBody.replaceChildren();
    covBody.replaceChildren();
    if (!selected) {
      covStatus.textContent = "No token has a served row in this range, so there is no coverage to break down.";
      return;
    }
    covStatus.textContent = "Loading the coverage of " + selected + "...";
    try {
      const windows = [];
      let first = null;
      let cursor = null;
      let pages = 0;
      while (pages < MAX_PAGES) {
        const params = { network: network(), token: selected, from: String(range.from), to: String(range.to), limit: String(PAGE_LIMIT) };
        if (cursor) params.cursor = cursor;
        const payload = await getChecked("/v1/coverage", { slot: "dq-coverage", params });
        const provenance = requireQualityProvenance(payload);
        const list = requireList(payload, "coverage");
        if (!first) first = provenance;
        windows.push(...list);
        pages += 1;
        cursor = typeof payload.next_cursor === "string" && payload.next_cursor !== "" ? payload.next_cursor : null;
        if (cursor === null) break;
      }
      if (stale()) return;
      const cap = coverageCap({ count: windows.length, truncated: cursor !== null }, MAX_PAGES);
      partial.hidden = !cap.partial;
      partial.dataset.partial = String(cap.partial);
      partial.textContent = cap.text;
      covStrip.replaceChildren(provenanceStrip(first));
      renderCoverage(windows);
    } catch (error) {
      if (!fail(covStatus, error)) { covBody.replaceChildren(); gapBody.replaceChildren(); }
    }
    root.dataset.loaded = "true";
  }

  function renderCoverage(windows) {
    const counts = breakdown(windows, range.from, range.to);
    covStatus.textContent = counts.listed + " of " + counts.slots + " windows listed for " + selected + "."
      + (counts.not_listed_text ? " " + counts.not_listed_text : "")
      + (counts.ignored ? " " + counts.ignored + " returned rows outside the range or repeated were ignored." : "");
    const table = el("table", "data-table");
    table.dataset.table = "coverage-breakdown";
    table.append(headRow(["State", "Windows"]));
    const body = el("tbody");
    for (const state of [...COVERAGE_STATES, "unknown"]) {
      const tr = el("tr");
      tr.dataset.coverageState = state;
      tr.dataset.count = String(counts.by_state[state]);
      cell(tr, badgeNode(state));
      cell(tr, counts.by_state[state]);
      body.append(tr);
    }
    table.append(body);
    const qual = el("p", "muted");
    qual.dataset.qualification = "";
    qual.textContent = "Qualification of listed windows: " + QUALIFICATION_CLASSES.map((c) => c + " " + counts.by_qualification[c]).join(", ")
      + ". Unrecorded means no qualification record, not zero.";
    const cells = el("div", "cov-strip");
    cells.dataset.strip = "";
    for (const item of strip(windows)) {
      const node = el("span", "cov-cell cov-" + item.state + " cov-q-" + item.qualification);
      node.dataset.windowStart = String(item.window_start);
      node.dataset.state = item.state;
      node.dataset.qualification = item.qualification;
      node.setAttribute("title", utc(item.window_start) + " " + item.state + ", qualification " + item.qualification);
      cells.append(node);
    }
    covBody.replaceChildren(table, qual, cells);

    const list = gaps(windows, selected);
    if (list.length === 0) {
      gapBody.replaceChildren(el("p", "muted", "No listed window is missing or before the first row."));
      return;
    }
    const gapTable = el("table", "data-table gap-list");
    gapTable.dataset.table = "gaps";
    gapTable.append(headRow(["Window start (UTC)", "State", "Reasons", "Explorer"]));
    const gapRows = el("tbody");
    for (const gap of list) {
      const tr = el("tr");
      tr.dataset.windowStart = String(gap.window_start);
      tr.dataset.state = gap.state;
      cell(tr, utc(gap.window_start));
      cell(tr, badgeNode(gap.state));
      cell(tr, gap.reasons.length ? gap.reasons.join(", ") : "none recorded");
      const link = el("a", "", "Open in explorer");
      link.setAttribute("href", gap.href);
      cell(tr, link);
      gapRows.append(tr);
    }
    gapTable.append(gapRows);
    const holder = el("div", "table-scroll");
    holder.append(gapTable);
    gapBody.replaceChildren(holder);
  }

  async function loadQuality() {
    rangeNote.textContent = "window starts [" + utc(range.from) + ", " + utc(range.to) + ")";
    pager = newPager();
    await Promise.all([loadTiles(), loadTokens()]);
    await loadCoverage();
  }

  rangeSelect.addEventListener("change", () => {
    preset = rangeSelect.value;
    range = presetRange(preset, Date.now());
    loadQuality();
  });
  tokenSelect.addEventListener("change", () => {
    selected = tokenSelect.value;
    loadCoverage();
  });
  loadQuality();
  return { dispose() { disposed = true; } };
}
