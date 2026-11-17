// Shell: loads the active policy and honesty banner, then routes. Fails closed on any problem.
import { getChecked, ProvenanceError } from "./client.js";
import { startRouter, ROUTES } from "./router.js";
import { renderView, VIEWS } from "./views.js";

const byId = (id) => document.getElementById(id);

function setBlocked(reason) {
  const blocking = byId("blocking-state");
  byId("blocking-reason").textContent = reason;
  blocking.hidden = false;
  byId("view-root").hidden = true;
  byId("provenance-bar").hidden = true;
  document.body.dataset.state = "blocked";
}

function showProvenance(provenance) {
  const bar = byId("provenance-bar");
  const rows = [["source", provenance.source_kind], ["certification", provenance.certification_state],
    ["coverage", provenance.coverage_state], ["scoring", provenance.scoring_state],
    ["generated", provenance.generated_at]];
  bar.replaceChildren(...rows.map(([label, value]) => {
    const chip = document.createElement("span");
    chip.className = "chip";
    chip.textContent = label + ": " + value;
    return chip;
  }));
  bar.hidden = false;
}

function buildNav() {
  const nav = byId("main-nav");
  nav.replaceChildren(...VIEWS.map((view) => {
    const link = document.createElement("a");
    link.setAttribute("href", "#/" + view.id);
    link.dataset.route = view.id;
    link.textContent = view.title;
    return link;
  }));
}

let lastRoute = null;

function onRoute(route) {
  lastRoute = route;
  for (const link of byId("main-nav").children) {
    if (link.dataset.route === route.name) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
  renderView(byId("view-root"), route, { onBlock: setBlocked, route });
  document.body.dataset.route = route.name;
}

async function boot() {
  buildNav();
  const select = byId("network-select");
  select.addEventListener("change", () => {
    document.body.dataset.network = select.value;
    if (lastRoute && document.body.dataset.state === "ready") onRoute(lastRoute);
  });
  document.body.dataset.network = select.value;
  try {
    const policy = await getChecked("/v1/policy", { slot: "policy" });
    const banner = policy.honesty_banner || policy.provenance.honesty_banner;
    if (typeof banner !== "string" || banner === "") throw new ProvenanceError("no honesty banner");
    byId("honesty-banner-text").textContent = banner;
    showProvenance(policy.provenance);
    byId("blocking-state").hidden = true;
    byId("view-root").hidden = false;
    document.body.dataset.state = "ready";
    startRouter(onRoute);
  } catch (error) {
    const reason = error instanceof ProvenanceError
      ? "The server response carries no valid provenance, so nothing is shown. (" + error.message + ")"
      : "The dashboard could not load the active policy, so nothing is shown.";
    setBlocked(reason);
  }
}

boot();
export { ROUTES };
