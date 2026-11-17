// Detections and alerts: current detector results of one token over a range, an alert status per
// result, and a detail that recomputes median, MAD, scale and score from the pinned baseline cells.
// The effectiveness banner is built before any request and is never hidden. A response without
// provenance blocks the view.
import { ApiError, ProvenanceError, getChecked } from "./client.js";
import { badge } from "./states.js";
import { DEFAULT_PRESET, PRESETS, failureMessage, newPager, pagerAdd, presetRange, tokensParams } from "./explorer-model.js";
import { DETECTION_BANNER, alertStatus, explain, requireDetectionProvenance } from "./detections-model.js";

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

function alertNode(status) {
  const node = el("span", "alert-state alert-" + status.state, status.state.replace("_", " "));
  node.dataset.alertState = status.state;
  node.setAttribute("title", status.text);
  return node;
}

function cell(tr, content) {
  const td = el("td");
  if (content instanceof Node) td.append(content); else td.textContent = String(content);
  tr.append(td);
  return td;
}

export function explanationNode(explained, pin, result) {
  const box = el("div", "detail");
  box.dataset.explanation = explained.kind;
  box.dataset.reproduced = String(explained.reproduced);
  box.append(el("p", "mono", result.explanation || ""));
  if (explained.kind === "input_mismatch") {
    const warn = el("p", "inline-error", "Not reproducible: " + explained.reason);
    warn.setAttribute("role", "alert");
    box.append(warn);
    return box;
  }
  box.append(el("p", "muted", "Threshold rule: " + explained.rule));
  if (explained.kind === "not_scored") {
    box.append(el("p", "", "Status " + explained.status + ": no score was computed. Eligible prior windows "
      + explained.sample_count + " of " + explained.min_prior_windows + " required. Reasons: "
      + (explained.reasons.join(", ") || "none") + "."));
    return box;
  }
  box.append(el("p", "muted mono", explained.formula));
  const table = el("table", "data-table explain-table");
  table.dataset.table = "explanation";
  const head = el("tr");
  for (const name of ["Value", "Stored result", "Recomputed from the pin", "Same"]) head.append(el("th", "", name));
  const thead = el("thead");
  thead.append(head);
  table.append(thead);
  const body = el("tbody");
  for (const item of explained.rows) {
    const tr = el("tr");
    tr.dataset.value = item.name;
    cell(tr, item.name);
    cell(tr, item.stored);
    cell(tr, item.recomputed);
    const verdict = cell(tr, item.match ? "yes" : "NO");
    verdict.className = item.match ? "explain-match" : "explain-mismatch";
    body.append(tr);
  }
  table.append(body);
  box.append(table);
  const verdict = el("p", explained.reproduced ? "explain-match" : "explain-mismatch",
    explained.reproduced
      ? "Reproduced: the pinned cells give exactly the stored median, MAD, scale, score and trigger ("
        + (explained.recomputed_triggered ? "triggered" : "not triggered") + ")."
      : "NOT reproduced: the pinned cells do not give the stored result. Treat the stored result as unexplained.");
  verdict.dataset.verdict = explained.reproduced ? "reproduced" : "not_reproduced";
  box.append(verdict);
  const counts = pin.coverage_counts || {};
  const by = counts.by_state || {};
  box.append(el("p", "muted", "Pinned input " + pin.input_digest + ": " + (counts.cells_total ?? pin.cells.length) + " cells, "
    + Object.entries(by).map(([k, v]) => k + " " + v).join(", ") + "; eligible for the median: observed and zero cells"
    + (explained.detector.startsWith("active_wallets") ? " with attribution at or above the policy floor" : "") + "."));
  return box;
}

export function mountDetections(root, context = {}) {
  const onBlock = context.onBlock || (() => {});
  let disposed = false;
  let preset = DEFAULT_PRESET;
  let range = presetRange(preset, Date.now());
  let pager = newPager();
  let selected = null;
  let rowsByKey = new Map();
  const network = () => document.body.dataset.network || "mainnet";
  const stale = () => disposed;

  const banner = el("section", "card detection-banner");
  banner.dataset.banner = "detection-effectiveness";
  banner.setAttribute("role", "note");
  banner.append(el("h2", "card-title", "Detection effectiveness is unmeasured"), el("p", "", DETECTION_BANNER));

  const controls = el("section", "card");
  controls.append(el("h2", "card-title", "Detections and alerts"));
  controls.append(el("p", "muted", "Current detector results per token for five-minute windows, half-open [from, to), UTC, at most 7 days."));
  const rangeSelect = el("select");
  rangeSelect.id = "dt-range";
  for (const key of Object.keys(PRESETS)) {
    const option = el("option", "", key);
    option.value = key;
    rangeSelect.append(option);
  }
  rangeSelect.value = preset;
  const rangeLabel = el("label", "network", "Range ");
  rangeLabel.append(rangeSelect);
  const rangeNote = el("p", "muted mono");
  controls.append(rangeLabel, rangeNote);

  const tokensBox = el("section", "card");
  tokensBox.id = "dt-tokens";
  tokensBox.append(el("h3", "card-title", "Tokens"));
  const tokenStrip = el("div");
  const tokenStatus = el("p", "muted");
  const tokenList = el("ul", "token-list");
  const more = el("button", "", "Load more tokens");
  more.type = "button";
  more.id = "dt-more";
  more.hidden = true;
  tokensBox.append(tokenStrip, tokenStatus, tokenList, more);

  const listBox = el("section", "card");
  listBox.id = "dt-list";
  const listTitle = el("h3", "card-title", "Results");
  const listStrip = el("div");
  const listStatus = el("p", "muted");
  const listHolder = el("div", "table-scroll");
  listBox.append(listTitle, listStrip, listStatus, listHolder);

  const detailBox = el("section", "card");
  detailBox.id = "dt-detail";
  detailBox.append(el("h3", "card-title", "Result detail"));
  const detailBody = el("div");
  detailBody.append(el("p", "muted", "Select a result to see how its score follows from the pinned baseline."));
  detailBox.append(detailBody);

  root.replaceChildren(banner, controls, tokensBox, listBox, detailBox);
  root.dataset.view = "detections";

  const fail = (target, error) => {
    if (error && error.name === "AbortError") return true;
    if (stale()) return true;
    if (error instanceof ProvenanceError) {
      disposed = true;
      onBlock("The server response carries no valid provenance, so nothing is shown. (" + error.message + ")");
      return true;
    }
    target.textContent = error instanceof ApiError && error.status === 404
      ? "The server holds no record for this request (404). Nothing is shown for it."
      : failureMessage(error instanceof ApiError ? error : null);
    return false;
  };

  function renderTokens() {
    tokenList.replaceChildren(...pager.items.map((item) => {
      const li = el("li");
      const button = el("button", "token-button mono", item.token);
      button.type = "button";
      button.dataset.token = item.token;
      if (item.token === selected) button.setAttribute("aria-current", "true");
      button.addEventListener("click", () => selectToken(item.token));
      li.append(button);
      return li;
    }));
    more.hidden = pager.done;
    tokenStatus.textContent = pager.items.length === 0 && pager.done ? "No token has a served row in this range." : "";
  }

  async function loadTokens(reset) {
    if (reset) { pager = newPager(); selected = null; }
    try {
      const payload = await getChecked("/v1/tokens", { slot: "dt-tokens", params: tokensParams(network(), range, pager) });
      if (stale()) return;
      const provenance = requireDetectionProvenance(payload);
      pager = pagerAdd(pager, payload.tokens, payload.next_cursor);
      tokenStrip.replaceChildren(provenanceStrip(provenance));
      renderTokens();
      if (!selected && pager.items.length) await selectToken(pager.items[0].token);
    } catch (error) {
      if (!fail(tokenStatus, error)) tokenList.replaceChildren();
    }
    root.dataset.loaded = "true";
  }

  async function collect(path, slot, params, listKey) {
    const items = [];
    let first = null;
    let cursor = null;
    let pages = 0;
    while (pages < MAX_PAGES) {
      const query = { ...params, limit: String(PAGE_LIMIT) };
      if (cursor) query.cursor = cursor;
      const payload = await getChecked(path, { slot, params: query });
      requireDetectionProvenance(payload);
      if (!Array.isArray(payload[listKey])) throw new ProvenanceError("response is not a list");
      if (!first) first = payload;
      items.push(...payload[listKey]);
      pages += 1;
      cursor = typeof payload.next_cursor === "string" && payload.next_cursor !== "" ? payload.next_cursor : null;
      if (cursor === null) break;
    }
    return { items, first, truncated: cursor !== null };
  }

  async function selectToken(token) {
    selected = token;
    renderTokens();
    listTitle.textContent = "Results: " + token;
    listStatus.textContent = "Loading the results...";
    detailBody.replaceChildren(el("p", "muted", "Select a result to see how its score follows from the pinned baseline."));
    try {
      const base = { network: network(), from: String(range.from), to: String(range.to) };
      const detections = await collect("/v1/detections/range", "dt-list", { ...base, token }, "detections");
      const feed = await collect("/v1/alerts/feed", "dt-feed", base, "alerts");
      if (stale() || selected !== token) return;
      rowsByKey = new Map();
      const table = el("table", "data-table");
      table.dataset.table = "detections";
      const head = el("tr");
      for (const name of ["Window start (UTC)", "Detector", "Status", "Observed", "Score", "Triggered", "Alert",
        "Provenance", "Detail"]) head.append(el("th", "", name));
      const thead = el("thead");
      thead.append(head);
      table.append(thead);
      const body = el("tbody");
      for (const result of detections.items) {
        if (!result || typeof result !== "object") continue;
        const key = result.window_start + "|" + result.detector;
        rowsByKey.set(key, result);
        const status = alertStatus(feed.items, { ...result, token }, { truncated: feed.truncated });
        const tr = el("tr");
        tr.dataset.windowStart = String(result.window_start);
        tr.dataset.detector = result.detector;
        cell(tr, utc(result.window_start));
        cell(tr, result.detector);
        cell(tr, result.status);
        cell(tr, result.observed ?? "none");
        cell(tr, result.score ?? "none");
        cell(tr, result.triggered ? "yes" : "no");
        cell(tr, alertNode(status));
        cell(tr, badgeNode(detections.first.provenance.source_kind));
        const button = el("button", "", "Explain");
        button.type = "button";
        button.addEventListener("click", () => selectResult(key));
        cell(tr, button);
        body.append(tr);
      }
      table.append(body);
      listHolder.replaceChildren(table);
      listStrip.replaceChildren(provenanceStrip(detections.first.provenance));
      listStatus.textContent = detections.items.length === 0
        ? "No current detector result for this token in the range."
        : detections.items.length + " current results." + (detections.truncated ? " The list was cut at " + MAX_PAGES
          + " pages; more results exist and are not shown." : "") + (feed.truncated
          ? " The alert feed was cut at " + MAX_PAGES + " pages; missing alert records read as unknown." : "");
      listBox.dataset.token = token;
      const firstKey = [...rowsByKey.keys()][0];
      if (firstKey) await selectResult(firstKey);
    } catch (error) {
      if (!fail(listStatus, error)) listHolder.replaceChildren();
    }
    root.dataset.loaded = "true";
  }

  async function selectResult(key) {
    const result = rowsByKey.get(key);
    if (!result || !selected) return;
    detailBody.replaceChildren(el("p", "muted", "Loading the pinned baseline..."));
    try {
      const baselinePayload = await getChecked("/v1/baseline", { slot: "dt-detail", params: {
        network: network(), input_digest: String(result.input_digest) } });
      const provenance = requireDetectionProvenance(baselinePayload);
      const pin = Array.isArray(baselinePayload.baselines) ? baselinePayload.baselines[0] : null;
      const policyPayload = await getChecked("/v1/policy", { slot: "dt-policy", params: { policy_id: String(result.policy_id) } });
      requireDetectionProvenance(policyPayload);
      if (stale()) return;
      const explained = explain(result, pin, policyPayload.policy);
      const head = el("p");
      head.append(el("strong", "", utc(result.window_start) + " " + result.detector + " "), el("span", "muted", "token " + selected));
      detailBody.replaceChildren(provenanceStrip(provenance), head, explanationNode(explained, pin || { cells: [], input_digest: "" }, result));
    } catch (error) {
      const message = el("p", "inline-error");
      if (!fail(message, error)) detailBody.replaceChildren(message);
    }
  }

  rangeSelect.addEventListener("change", () => {
    preset = rangeSelect.value;
    range = presetRange(preset, Date.now());
    rangeNote.textContent = "window starts [" + utc(range.from) + ", " + utc(range.to) + ")";
    loadTokens(true);
  });
  more.addEventListener("click", () => loadTokens(false));
  rangeNote.textContent = "window starts [" + utc(range.from) + ", " + utc(range.to) + ")";
  loadTokens(true);
  return { dispose() { disposed = true; } };
}
