// Alert history and revisions: a range feed of alert revisions per network, a client-side
// token/detector filter over the loaded items (labelled as such), and a drill-down from a feed
// item to its result revisions. The effectiveness banner is built before any request and is never
// hidden. A response without provenance blocks the view.
import { ApiError, ProvenanceError, getChecked } from "./client.js";
import { badge } from "./states.js";
import { DEFAULT_PRESET, PRESETS, failureMessage, presetRange } from "./explorer-model.js";
import { DETECTION_BANNER, detectorsOf, feedCap, filterFeed, filterNote, groupFeed, requireHistoryProvenance,
  requireList, timeline } from "./history-model.js";

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

function alertNode(state, known = true, title = "") {
  const node = el("span", "alert-state alert-" + state, state.replace("_", " "));
  node.dataset.alertState = state;
  node.dataset.known = String(known);
  if (title) node.setAttribute("title", title);
  return node;
}

function labelNode(label, text) {
  const node = el("span", "rev-label rev-" + label, label);
  node.dataset.label = label;
  node.setAttribute("title", text);
  return node;
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

export function mountHistory(root, context = {}) {
  const onBlock = context.onBlock || (() => {});
  let disposed = false;
  let preset = DEFAULT_PRESET;
  let range = presetRange(preset, Date.now());
  let loaded = [];
  let truncated = false;
  let openKey = null;
  const network = () => document.body.dataset.network || "mainnet";
  const stale = () => disposed;

  const banner = el("section", "card detection-banner");
  banner.dataset.banner = "detection-effectiveness";
  banner.setAttribute("role", "note");
  banner.append(el("h2", "card-title", "Detection effectiveness is unmeasured"), el("p", "", DETECTION_BANNER));

  const controls = el("section", "card");
  controls.append(el("h2", "card-title", "Alert history and revisions"));
  controls.append(el("p", "muted", "Every alert revision recorded for five-minute windows, half-open [from, to), UTC, at most 7 days. "
    + "Retracted and pending are states of their own. The highlighted record of a key is the highest stored revision."));
  const rangeSelect = el("select");
  rangeSelect.id = "ah-range";
  for (const key of Object.keys(PRESETS)) {
    const option = el("option", "", key);
    option.value = key;
    rangeSelect.append(option);
  }
  rangeSelect.value = preset;
  const rangeLabel = el("label", "network", "Range ");
  rangeLabel.append(rangeSelect);
  const tokenInput = el("input");
  tokenInput.id = "ah-token";
  tokenInput.type = "text";
  tokenInput.setAttribute("placeholder", "exact token, e.g. jetton:...");
  tokenInput.setAttribute("autocomplete", "off");
  const tokenLabel = el("label", "network", "Token ");
  tokenLabel.append(tokenInput);
  const detectorSelect = el("select");
  detectorSelect.id = "ah-detector";
  const detectorLabel = el("label", "network", "Detector ");
  detectorLabel.append(detectorSelect);
  const rangeNote = el("p", "muted mono");
  controls.append(rangeLabel, tokenLabel, detectorLabel, rangeNote);

  const feedBox = el("section", "card");
  feedBox.id = "ah-feed";
  feedBox.append(el("h3", "card-title", "Alert feed"));
  const feedStrip = el("div");
  const partial = el("p", "partial-banner");
  partial.setAttribute("role", "status");
  partial.hidden = true;
  partial.dataset.partial = "false";
  const feedStatus = el("p", "muted");
  const filterNoteNode = el("p", "muted");
  filterNoteNode.dataset.filterNote = "";
  const feedHolder = el("div", "table-scroll");
  feedBox.append(feedStrip, partial, feedStatus, filterNoteNode, feedHolder);

  const detailBox = el("section", "card");
  detailBox.id = "ah-detail";
  detailBox.append(el("h3", "card-title", "Result revisions"));
  const detailBody = el("div");
  detailBody.append(el("p", "muted", "Select a feed item to see the result revisions behind it."));
  detailBox.append(detailBody);

  root.replaceChildren(banner, controls, feedBox, detailBox);
  root.dataset.view = "alerts";

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

  function refreshDetectors() {
    const current = detectorSelect.value;
    const all = el("option", "", "all detectors");
    all.value = "";
    detectorSelect.replaceChildren(all, ...detectorsOf(loaded).map((id) => {
      const option = el("option", "", id);
      option.value = id;
      return option;
    }));
    detectorSelect.value = [...detectorSelect.options].some((o) => o.value === current) ? current : "";
  }

  function currentGroups() {
    const filter = { token: tokenInput.value.trim(), detector: detectorSelect.value };
    const shown = filterFeed(loaded, filter);
    return { filter, shown, groups: groupFeed(shown) };
  }

  function renderFeed() {
    const { filter, shown, groups } = currentGroups();
    filterNoteNode.textContent = filterNote(loaded.length, shown.length, filter, truncated);
    const table = el("table", "data-table");
    table.dataset.table = "alert-feed";
    table.append(headRow(["Window start (UTC)", "Token", "Detector", "Latest stored alert", "Revisions", "Recorded", "Detail"]));
    const body = el("tbody");
    for (const group of groups) {
      const tr = el("tr", "history-latest");
      tr.dataset.windowStart = String(group.window_start);
      tr.dataset.detector = group.detector;
      tr.dataset.token = group.token;
      tr.dataset.latest = "true";
      cell(tr, utc(group.window_start));
      cell(tr, group.token).className = "mono";
      cell(tr, group.detector);
      const latest = group.latest;
      const latestCell = cell(tr, alertNode(latest.state, latest.state !== "unknown",
        "Highest stored alert revision " + latest.alert_revision + " of this key."));
      latestCell.append(" ", labelNode(latest.label, "Label of the result revision this alert refers to."));
      const history = el("ol", "rev-list");
      for (const rev of group.revisions) {
        const item = el("li", rev.latest ? "history-latest" : "");
        item.dataset.alertRevision = String(rev.alert_revision);
        item.dataset.latest = String(rev.latest);
        item.append("#" + rev.alert_revision + " ", alertNode(rev.state, rev.state !== "unknown"), " ",
          labelNode(rev.label, "Label of result revision " + rev.result_revision + "."));
        history.append(item);
      }
      cell(tr, history);
      cell(tr, latest.recorded_at);
      const button = el("button", "", "Revisions");
      button.type = "button";
      button.addEventListener("click", () => openDetail(group));
      cell(tr, button);
      body.append(tr);
    }
    table.append(body);
    feedHolder.replaceChildren(table);
    feedStatus.textContent = loaded.length === 0
      ? "No alert record exists in this range."
      : groups.length + " keys with " + shown.length + " alert records shown.";
  }

  async function loadFeed() {
    feedStatus.textContent = "Loading the alert feed...";
    detailBody.replaceChildren(el("p", "muted", "Select a feed item to see the result revisions behind it."));
    openKey = null;
    try {
      const items = [];
      let first = null;
      let cursor = null;
      let pages = 0;
      while (pages < MAX_PAGES) {
        const params = { network: network(), from: String(range.from), to: String(range.to), limit: String(PAGE_LIMIT) };
        if (cursor) params.cursor = cursor;
        const payload = await getChecked("/v1/alerts/feed", { slot: "ah-feed", params });
        const provenance = requireHistoryProvenance(payload);
        const list = requireList(payload, "alerts");
        if (!first) first = provenance;
        items.push(...list);
        pages += 1;
        cursor = typeof payload.next_cursor === "string" && payload.next_cursor !== "" ? payload.next_cursor : null;
        if (cursor === null) break;
      }
      if (stale()) return;
      loaded = items;
      truncated = cursor !== null;
      const cap = feedCap({ items, truncated }, MAX_PAGES);
      partial.hidden = !cap.partial;
      partial.dataset.partial = String(cap.partial);
      partial.textContent = cap.text;
      feedStrip.replaceChildren(provenanceStrip(first));
      refreshDetectors();
      renderFeed();
      const firstGroup = currentGroups().groups[0];
      if (firstGroup) await openDetail(firstGroup);
    } catch (error) {
      if (!fail(feedStatus, error)) feedHolder.replaceChildren();
    }
    root.dataset.loaded = "true";
  }

  async function openDetail(group) {
    const mine = group.token + "|" + group.window_start + "|" + group.detector;
    openKey = mine;
    detailBody.replaceChildren(el("p", "muted", "Loading the result revisions..."));
    try {
      const params = { network: network(), token: group.token, window_start: String(group.window_start),
        detector: group.detector, limit: String(PAGE_LIMIT) };
      const payload = await getChecked("/v1/detections/revisions", { slot: "ah-detail", params });
      const provenance = requireHistoryProvenance(payload);
      const list = requireList(payload, "revisions");
      if (stale() || openKey !== mine) return;
      const line = timeline(list);
      const head = el("p");
      head.append(el("strong", "", utc(group.window_start) + " " + group.detector + " "), el("span", "muted mono", "token " + group.token));
      const table = el("table", "data-table");
      table.dataset.table = "revisions";
      table.append(headRow(["Result revision", "Label", "Alert state", "Status", "Score", "Input digest", "Promoted", "Explanation"]));
      const body = el("tbody");
      for (const row of line.rows) {
        const tr = el("tr", row.latest ? "history-latest" : "");
        tr.dataset.resultRevision = String(row.result_revision);
        tr.dataset.latest = String(row.latest);
        tr.dataset.label = row.label;
        cell(tr, "#" + row.result_revision + (row.latest ? " (latest stored)" : ""));
        cell(tr, labelNode(row.label, row.label_text));
        cell(tr, alertNode(row.alert_state, row.alert_known, row.alert_state === "no_record" ? "No alert record for this result revision." : ""));
        cell(tr, row.status);
        cell(tr, row.score ?? "none");
        cell(tr, row.input_digest ?? "none").className = "mono";
        cell(tr, row.promoted_at ?? "none");
        cell(tr, row.explanation ?? "none");
        body.append(tr);
      }
      table.append(body);
      const holder = el("div", "table-scroll");
      holder.append(table);
      const children = [provenanceStrip(provenance), head];
      if (payload.truncated === true) {
        const cut = el("p", "partial-banner", "Partial list: more result revisions exist and were not loaded.");
        cut.dataset.partial = "true";
        children.push(cut);
      }
      children.push(line.rows.length === 0 ? el("p", "muted", "No result revision is stored for this key.") : holder);
      detailBody.replaceChildren(...children);
      detailBox.dataset.revisions = String(line.rows.length);
    } catch (error) {
      const message = el("p", "inline-error");
      if (!fail(message, error)) detailBody.replaceChildren(message);
    }
  }

  rangeSelect.addEventListener("change", () => {
    preset = rangeSelect.value;
    range = presetRange(preset, Date.now());
    rangeNote.textContent = "window starts [" + utc(range.from) + ", " + utc(range.to) + ")";
    loadFeed();
  });
  tokenInput.addEventListener("input", renderFeed);
  detectorSelect.addEventListener("change", renderFeed);
  rangeNote.textContent = "window starts [" + utc(range.from) + ", " + utc(range.to) + ")";
  loadFeed();
  return { dispose() { disposed = true; } };
}
