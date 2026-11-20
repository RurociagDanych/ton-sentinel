// Pipeline status view. The banner is built before any request and never hidden. Refreshes at most every 30 s
// and not while the tab is hidden; a response without provenance blocks.
import { ApiError, ProvenanceError, getChecked } from "./client.js";
import { badge } from "./states.js";
import { REFRESH_MS, createRefresher } from "./model.js";
import { failureMessage } from "./explorer-model.js";
import { BANNER_TEXT, backlogPanel, notMeasuredPanel, requirePipeline, requirePipelineProvenance, tiles } from "./pipeline-model.js";

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

function tileNode(tile, extra) {
  const box = el("div", "pipe-tile" + (tile.state === "observed" ? "" : " tile-unavailable") + (extra || ""));
  box.dataset.tile = tile.key;
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
}

export function mountPipeline(root, context = {}) {
  const onBlock = context.onBlock || (() => {});
  let disposed = false;
  const network = () => document.body.dataset.network || "mainnet";

  const banner = el("section", "card detection-banner");
  banner.dataset.banner = "pipeline-not-measured";
  banner.setAttribute("role", "note");
  banner.append(el("h2", "card-title", "Pipeline status: counts, not a health verdict"), el("p", "", BANNER_TEXT));

  const status = el("p", "muted");
  const strip = el("div");
  const tileBox = el("section", "card");
  tileBox.id = "pipe-tiles";
  tileBox.append(el("h3", "card-title", "Outbox, dirty windows and stream"), strip, status);
  const tileGrid = el("div", "tile-grid");
  tileBox.append(tileGrid);

  const backlogBox = el("section", "card pipe-backlog");
  backlogBox.id = "pipe-backlog";
  backlogBox.append(el("h3", "card-title", "Scoring backlog"),
    el("p", "muted", "Two separate counts: served windows with no result at all, and results marked stale. They are not added together."));
  const backlogGrid = el("div", "tile-grid");
  backlogBox.append(backlogGrid);

  const notBox = el("section", "card pipe-freshness");
  notBox.id = "pipe-not-measured";
  notBox.append(el("h3", "card-title", "Freshness and Kafka lag"));
  const notText = el("p", "muted");
  const notGrid = el("div", "tile-grid");
  notBox.append(notText, notGrid);

  root.replaceChildren(banner, tileBox, backlogBox, notBox);
  root.dataset.view = "pipeline";

  function render(pipeline, payload, provenance) {
    strip.replaceChildren(provenanceStrip(provenance));
    status.textContent = typeof payload.label === "string" ? payload.label : "";
    tileGrid.replaceChildren(...tiles(pipeline).map((t) => tileNode(t)));
    backlogGrid.replaceChildren(...backlogPanel(pipeline.scoring_backlog).items.map((t) => tileNode(t)));
    const panel = notMeasuredPanel(pipeline);
    notText.textContent = panel.text;
    notGrid.replaceChildren(...panel.items.map((t) => tileNode(t, t.state === "observed" ? "" : " pipe-not-measured")));
  }

  async function load() {
    try {
      const payload = await getChecked("/v1/pipeline", { slot: "pipe-all", params: { network: network() } });
      const provenance = requirePipelineProvenance(payload);
      const pipeline = requirePipeline(payload);
      if (disposed) return;
      render(pipeline, payload, provenance);
    } catch (error) {
      if ((error && error.name === "AbortError") || disposed) return;
      if (error instanceof ProvenanceError) {
        disposed = true;
        refresher.stop();
        onBlock("The server response carries no valid provenance, so nothing is shown. (" + error.message + ")");
        return;
      }
      status.textContent = failureMessage(error instanceof ApiError ? error : null);
      tileGrid.replaceChildren();
      backlogGrid.replaceChildren();
      notGrid.replaceChildren();
    }
  }

  const refresher = createRefresher({
    intervalMs: REFRESH_MS,
    run: load,
    isHidden: () => document.hidden,
    setIntervalFn: (fn, ms) => window.setInterval(fn, ms),
    clearIntervalFn: (id) => window.clearInterval(id),
    onVisibility: (handler) => {
      document.addEventListener("visibilitychange", handler);
      return () => document.removeEventListener("visibilitychange", handler);
    },
  });
  status.textContent = "Loading the pipeline status...";
  load().then(() => { root.dataset.loaded = "true"; });
  refresher.start();

  return { dispose() { disposed = true; refresher.stop(); } };
}
