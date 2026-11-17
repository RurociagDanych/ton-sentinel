// Views. All six are real.
import { mountOverview } from "./overview.js";
import { mountExplorer } from "./explorer.js";
import { mountDetections } from "./detections.js";
import { mountHistory } from "./history.js";
import { mountQuality } from "./quality.js";
import { mountPipeline } from "./pipeline.js";

export const VIEWS = [
  { id: "overview", title: "Overview and health",
    endpoints: ["/health", "/status", "/v1/pipeline", "/v1/policy"] },
  { id: "explorer", title: "Token and window explorer",
    endpoints: ["/v1/tokens", "/v1/series", "/v1/features"] },
  { id: "detections", title: "Detections and alerts",
    endpoints: ["/v1/detections", "/v1/baseline", "/v1/alerts"] },
  { id: "alerts", title: "Alert history and revisions",
    endpoints: ["/v1/alerts/feed", "/v1/detections/revisions"] },
  { id: "quality", title: "Data quality and coverage",
    endpoints: ["/v1/quality", "/v1/coverage", "/v1/tokens"] },
  { id: "pipeline", title: "Pipeline status",
    endpoints: ["/v1/pipeline"] },
];

let active = null;

export function renderView(root, route, context = {}) {
  if (active) { active.dispose(); active = null; }
  const view = VIEWS.find((item) => item.id === route.name) || VIEWS[0];
  if (view.id === "overview") {
    active = mountOverview(root, context);
    return view;
  }
  if (view.id === "explorer") {
    active = mountExplorer(root, context);
    return view;
  }
  if (view.id === "detections") {
    active = mountDetections(root, context);
    return view;
  }
  if (view.id === "alerts") {
    active = mountHistory(root, context);
    return view;
  }
  if (view.id === "quality") {
    active = mountQuality(root, context);
    return view;
  }
  active = mountPipeline(root, context);
  return view;
}
