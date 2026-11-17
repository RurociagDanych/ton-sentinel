// Pure logic of the pipeline status view. An unavailable or omitted value is never a zero; the two
// scoring backlog counts stay separate and are never summed; ingestion freshness is only ever shown as
// "not measured" with the server's reason; Kafka lag is shown as a record count only when the server
// measured it (broker probe), otherwise with its state. Stream progress shows a commit time and a
// record count and nothing else. No DOM here.
import { ProvenanceError } from "./client.js";
import { cell, counterCell, groupCell, kafkaLagCell } from "./states.js";
import { requireDetectionProvenance } from "./detections-model.js";

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);

export const requirePipelineProvenance = requireDetectionProvenance;
export { kafkaLagCell };

export function requirePipeline(payload) {
  if (!isObject(payload) || !isObject(payload.pipeline)) throw new ProvenanceError("response has no pipeline object");
  return payload.pipeline;
}

export const BANNER_TEXT = "Everything here is a count of recorded work or an explicit gap. Ingestion freshness is "
  + "not measured, and Kafka lag is a record count only where the server's broker probe measured it, so no tile on "
  + "this page is a health or freshness verdict. A value marked unavailable could not be read; it is never zero.";

export const NOT_MEASURED_TEXT = "Ingestion freshness is not measured: it is not zero and not healthy. Kafka lag is "
  + "shown as a record count only when the server measured it; otherwise it shows its state and reason.";

const view = (key, title, scope, c) => ({ key, title, scope, state: c.state, tone: c.tone, label: c.label,
  known: c.known, text: c.text, detail: c.detail });

function streamCell(section) {
  if (isObject(section) && section.status === "observed" && isObject(section.value)) {
    const { committed_at: at, record_count: count } = section.value;
    if (typeof at === "string" && at !== "" && Number.isSafeInteger(count) && count >= 0) {
      return cell("observed", count + " records, committed " + at, "");
    }
    return cell("unavailable", "unavailable", "observed with a malformed value");
  }
  return counterCell(section);
}

// Outbox, dirty windows by role and the latest stream commit. A missing section is unavailable.
export function tiles(pipeline) {
  const p = isObject(pipeline) ? pipeline : {};
  const scope = (section, fallback) => (isObject(section) && typeof section.scope === "string" ? section.scope : fallback);
  return [
    view("outbox", "Outbox pending", scope(p.outbox, "all_networks"), counterCell(p.outbox)),
    view("dirty_windows_pending_by_role", "Pending dirty windows by role", "network", groupCell(p.dirty_windows_pending_by_role)),
    view("stream_progress", "Latest stream commit", scope(p.stream_progress, "all_networks"), streamCell(p.stream_progress)),
  ];
}

export const BACKLOG_ITEMS = [
  { key: "unscored_served_windows", title: "Never-scored served windows" },
  { key: "stale_results", title: "Stale results" },
];

// The two backlog counts, each from its own sub-counter. If the whole section is not observed, both carry
// its state and reason. Never a total, never a zero for an unreadable count.
export function backlogPanel(section) {
  const whole = isObject(section) && section.status === "observed" && isObject(section.value);
  const items = BACKLOG_ITEMS.map((def) => {
    const c = whole ? counterCell(section.value[def.key]) : counterCell(section);
    return view(def.key, def.title, "network", c);
  });
  return { items };
}

export const NOT_MEASURED_ITEMS = [
  { key: "ingestion_freshness", title: "Ingestion freshness" },
  { key: "kafka_lag", title: "Kafka lag" },
];

// Ingestion freshness: only an explicit not_measured is shown as such; anything else (a value, a missing
// section) fails closed to unavailable. Kafka lag: a fully measured value is a record count,
// not_measured/unavailable keep their reason, and anything malformed is unavailable, never a number.
export function notMeasuredPanel(pipeline) {
  const p = isObject(pipeline) ? pipeline : {};
  const items = NOT_MEASURED_ITEMS.map((def) => {
    const section = p[def.key];
    if (def.key === "kafka_lag") {
      const c = kafkaLagCell(section);
      return view(def.key, def.title, c.state === "observed" ? "spark_queries" : "none", c);
    }
    if (isObject(section) && section.status === "not_measured") {
      return view(def.key, def.title, "none", counterCell({ status: "not_measured", reason: section.reason }));
    }
    return view(def.key, def.title, "none", cell("unavailable", "unavailable", "expected not_measured from the server"));
  });
  return { items, text: NOT_MEASURED_TEXT };
}
