// Overview and health: service status, pipeline summary, data-quality headline, legend.
// Every value carries its state badge; a response without provenance blocks the whole view.
import { ApiError, ProvenanceError, getChecked, getJson } from "./client.js";
import { STATE_LEGEND, badge } from "./states.js";
import { DEFAULT_RANGE, RANGES, REFRESH_MS, createRefresher, pipelineLabel, pipelineRows,
  provenanceRows, qualityRange, qualityRows, requireProvenance, statusTiles } from "./model.js";

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

function metric(item) {
  const box = el("div", "metric");
  box.dataset.rowId = item.id;
  box.dataset.state = item.state;
  box.append(el("span", "metric-title", item.title), badgeNode(item.state),
    el("span", "metric-value mono", item.text));
  if (item.detail) box.append(el("span", "metric-detail muted", item.detail));
  return box;
}

function section(id, title) {
  const node = el("section", "card");
  node.id = id;
  node.append(el("h2", "card-title", title));
  return node;
}

function failure(label, error) {
  const status = error instanceof ApiError ? "http " + error.status : "no usable response";
  return [{ id: "section_unavailable", title: label, state: "unavailable", text: "unavailable", detail: status }];
}

function blockReason(error) {
  return "The server response carries no valid provenance, so nothing is shown. (" + error.message + ")";
}

function legend() {
  const node = section("ov-legend", "State legend");
  node.classList.add("legend");
  node.append(el("p", "muted", "Every badge on this page is one of these states. A state this client does not recognise is shown as unrecognised, never as healthy."));
  const list = el("dl", "legend-list");
  for (const state of Object.keys(STATE_LEGEND)) {
    const entry = el("div", "legend-entry");
    entry.dataset.legendState = state;
    const term = el("dt");
    term.append(badgeNode(state));
    entry.append(term, el("dd", "", STATE_LEGEND[state].meaning));
    list.append(entry);
  }
  node.append(list);
  return node;
}

export function mountOverview(root, context = {}) {
  const onBlock = context.onBlock || (() => {});
  let disposed = false;
  let range = DEFAULT_RANGE;

  const statusBox = section("ov-status", "Service status");
  const pipelineBox = section("ov-pipeline", "Pipeline summary");
  const qualityBox = section("ov-quality", "Data-quality headline");
  const policyBox = section("ov-policy", "Policy and provenance");
  const statusBody = el("div", "metrics");
  const pipelineBody = el("div", "metrics");
  const pipelineNote = el("p", "muted");
  const qualityBody = el("div", "metrics");
  const qualityNote = el("p", "muted");
  const policyBody = el("div", "metrics");
  const policyBanner = el("p", "policy-banner");
  statusBox.append(statusBody);
  pipelineBox.append(pipelineNote, pipelineBody);

  const selector = el("select");
  selector.id = "quality-range";
  for (const key of Object.keys(RANGES)) {
    const option = el("option", "", key);
    option.value = key;
    selector.append(option);
  }
  selector.value = range;
  const rangeLabel = el("label", "network", "Range ");
  rangeLabel.append(selector);
  const scope = el("p", "muted mono");
  qualityBox.append(rangeLabel, scope, qualityNote, qualityBody);
  policyBox.append(policyBanner, policyBody);

  root.replaceChildren(statusBox, pipelineBox, qualityBox, policyBox, legend());
  root.dataset.view = "overview";

  const fill = (body, rows) => body.replaceChildren(...rows.map(metric));
  const network = () => document.body.dataset.network || "mainnet";
  const stale = () => disposed;
  const block = (error) => { disposed = true; refresher.stop(); onBlock(blockReason(error)); };

  async function loadStatus() {
    try {
      const status = await getJson("/status", { slot: "ov-status" });
      if (!stale()) fill(statusBody, statusTiles(status));
    } catch (error) {
      if (error.name === "AbortError" || stale()) return;
      fill(statusBody, statusTiles(null));
      statusBody.prepend(metric(failure("Service status", error)[0]));
    }
  }

  async function loadPipeline() {
    try {
      const payload = await getChecked("/v1/pipeline", { slot: "ov-pipeline", params: { network: network() } });
      if (stale()) return;
      fill(pipelineBody, pipelineRows(payload));
      pipelineNote.textContent = pipelineLabel(payload);
      const provenance = requireProvenance(payload);
      policyBanner.textContent = provenance.honesty_banner;
      fill(policyBody, provenanceRows(provenance));
      policyBox.dataset.generatedAt = provenance.generated_at;
    } catch (error) {
      if (error.name === "AbortError" || stale()) return;
      if (error instanceof ProvenanceError) { block(error); return; }
      fill(pipelineBody, failure("Pipeline summary", error));
    }
  }

  async function loadQuality() {
    const { from, to } = qualityRange(range, Date.now());
    scope.textContent = "network " + network() + ", window starts [" + from + ", " + to + ")";
    try {
      const payload = await getChecked("/v1/quality", {
        slot: "ov-quality", params: { network: network(), from: String(from), to: String(to) } });
      if (stale()) return;
      fill(qualityBody, qualityRows(payload));
      qualityNote.textContent = typeof payload.label === "string" ? payload.label : "";
    } catch (error) {
      if (error.name === "AbortError" || stale()) return;
      if (error instanceof ProvenanceError) { block(error); return; }
      fill(qualityBody, failure("Data-quality headline", error));
    }
  }

  const loadAll = () => Promise.all([loadStatus(), loadPipeline(), loadQuality()]);
  selector.addEventListener("change", () => { range = selector.value; loadQuality(); });

  const refresher = createRefresher({
    intervalMs: REFRESH_MS,
    run: loadAll,
    isHidden: () => document.hidden,
    setIntervalFn: (fn, ms) => window.setInterval(fn, ms),
    clearIntervalFn: (id) => window.clearInterval(id),
    onVisibility: (handler) => {
      document.addEventListener("visibilitychange", handler);
      return () => document.removeEventListener("visibilitychange", handler);
    },
  });
  loadAll().then(() => { root.dataset.loaded = "true"; });
  refresher.start();

  return { dispose() { disposed = true; refresher.stop(); } };
}
