// Hash routing to the six views: #/overview, #/explorer, ...
export const ROUTES = ["overview", "explorer", "detections", "alerts", "quality", "pipeline"];

export function parseHash(hash) {
  const parts = String(hash || "").replace(/^#\/?/, "").split("/");
  const name = ROUTES.includes(parts[0]) ? parts[0] : ROUTES[0];
  return { name, rest: parts.slice(1).filter(Boolean) };
}

export function startRouter(onRoute) {
  const run = () => onRoute(parseHash(window.location.hash));
  window.addEventListener("hashchange", run);
  if (!window.location.hash) window.location.hash = "#/overview";
  run();
}
