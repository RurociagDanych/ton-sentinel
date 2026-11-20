// Pure view models for the overview: server payload in, rows of {id, title, state, tone, text, detail} out.
import { ProvenanceError, validateProvenance } from "./client.js";
import { cell, counterCell, groupCell, kafkaLagCell, stateCell } from "./states.js";

export const REFRESH_MS = 30000;
export const RANGES = { "1h": 3600, "6h": 6 * 3600, "24h": 86400, "7d": 7 * 86400 };
export const DEFAULT_RANGE = "24h";

function row(id, title, c) {
  return { id, title, state: c.state, tone: c.tone, label: c.label, known: c.known, text: c.text, detail: c.detail };
}

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);
const valueOr = (value, fallback) => (isObject(value) ? value : fallback);

export function requireProvenance(payload) {
  if (!isObject(payload)) throw new ProvenanceError("response has no provenance block");
  return validateProvenance(payload);
}

function numberCell(status, value, detail) {
  if (typeof value === "number" && Number.isFinite(value)) return cell("observed", String(value), detail);
  return cell("unavailable", "unavailable", "no value in response");
}

function textCell(value) {
  if (typeof value === "string" && value !== "") return cell("observed", value, "");
  return cell("unavailable", "unavailable", "no value in response");
}

export function statusTiles(status) {
  const s = valueOr(status, {});
  const availability = valueOr(s.availability, {});
  const dbOk = availability.database === "available" && s.status_query === "available";
  const gates = valueOr(s.gates, {});
  const pending = valueOr(s.pending_work, {});
  const freshness = valueOr(s.serving_freshness, {});
  const rowsOrUnavailable = (value) => (dbOk ? numberCell("observed", value) : cell("unavailable", "unavailable", "database query unavailable"));
  // /status counts and timestamps are global: say so, whatever network the page shows.
  const global = (c) => ({ ...c, detail: (c.detail ? c.detail + "; " : "") + "all networks" });
  const lagWord = isObject(freshness.lag) && freshness.lag.meaning === "not_measured" ? "lag not measured" : "";
  const tiles = [
    row("process", "Process", stateCell(availability.process)),
    row("database", "Database", stateCell(availability.database)),
    row("status_query", "Status query", stateCell(s.status_query)),
    row("readiness", "Readiness", stateCell(s.readiness)),
    row("source", "Source", stateCell(valueOr(s.source, {}).status)),
    row("coverage", "Coverage", stateCell(valueOr(s.coverage, {}).status)),
    row("backup", "Backup", stateCell(s.backup)),
    row("gate_stateful_streaming", "Gate: stateful streaming", stateCell(valueOr(gates.stateful_streaming, {}).status)),
    row("gate_live_source", "Gate: live source", stateCell(valueOr(gates.live_source, {}).status)),
    row("feature_rows", "Served rows", global(rowsOrUnavailable(s.feature_rows))),
    row("windows", "Served windows", global(rowsOrUnavailable(s.windows))),
    row("latest_window_end", "Latest window end", global(dbOk && freshness.latest_window_end !== null && freshness.latest_window_end !== undefined
      ? cell("observed", String(freshness.latest_window_end), "") : cell(dbOk ? "none_recorded" : "unavailable",
        dbOk ? "none recorded" : "unavailable", ""))),
    row("latest_write", "Latest serving write", global(dbOk && typeof freshness.latest_serving_write_at === "string"
      ? cell("observed", freshness.latest_serving_write_at, "") : cell(dbOk ? "none_recorded" : "unavailable",
        dbOk ? "none recorded" : "unavailable", ""))),
    row("serving_freshness", "Serving freshness", global({ ...stateCell(freshness.status), detail: lagWord })),
    row("outbox_pending", "Outbox pending", global(pendingCell(pending.outbox, "pending"))),
    row("repair_pending", "Repair pending windows", global(pendingCell(pending.repair, "pending_windows"))),
  ];
  return tiles;
}

function pendingCell(section, key) {
  const s = valueOr(section, {});
  if (s.status === "observed" && typeof s[key] === "number" && Number.isFinite(s[key])) {
    return cell("observed", String(s[key]), "");
  }
  return cell("unavailable", "unavailable", "");
}

export function pipelineLabel(payload) {
  return isObject(payload) && typeof payload.label === "string" ? payload.label : "";
}

function scoped(c, section) {
  if (isObject(section) && section.scope === "all_networks") {
    return { ...c, detail: (c.detail ? c.detail + "; " : "") + "all networks" };
  }
  return c;
}

export function pipelineRows(payload) {
  const p = valueOr(isObject(payload) ? payload.pipeline : null, {});
  const backlog = valueOr(p.scoring_backlog, {});
  const inner = backlog.status === "observed" ? valueOr(backlog.value, {}) : {};
  const scoringUnavailable = backlog.status === "observed" ? null : counterCell(backlog);
  const stream = valueOr(p.stream_progress, {});
  let streamCell;
  if (stream.status === "observed" && isObject(stream.value)) {
    streamCell = cell("observed", String(stream.value.committed_at || "unavailable"),
      "records " + String(stream.value.record_count));
  } else {
    streamCell = counterCell(stream);
  }
  return [
    row("outbox", "Outbox pending", scoped(counterCell(p.outbox), p.outbox)),
    row("dirty_windows_pending_by_role", "Dirty windows pending by role", groupCell(p.dirty_windows_pending_by_role)),
    row("unscored_served_windows", "Never-scored served windows", scoringUnavailable || counterCell(inner.unscored_served_windows)),
    row("stale_results", "Stale detector results", scoringUnavailable || counterCell(inner.stale_results)),
    row("stream_progress", "Latest stream commit", scoped(streamCell, stream)),
    row("ingestion_freshness", "Ingestion freshness", counterCell(p.ingestion_freshness)),
    row("kafka_lag", "Kafka lag", kafkaLagCell(p.kafka_lag)),
  ];
}

export function qualityRows(payload) {
  const q = valueOr(isObject(payload) ? payload.quality : null, {});
  return [
    row("served_windows", "Served windows in range", counterCell(q.served_windows)),
    row("served_rows_by_coverage_status", "Served rows by coverage status", groupCell(q.served_rows_by_coverage_status)),
    row("qualifications_by_class", "Window qualifications by class", groupCell(q.qualifications_by_class)),
    row("dirty_windows_pending_by_role", "Dirty windows pending in range", groupCell(q.dirty_windows_pending_by_role)),
    row("revision_quarantine_open", "Open revision quarantine", counterCell(q.revision_quarantine_open)),
    row("delivery_quarantine", "Delivery quarantine in range", counterCell(q.delivery_quarantine)),
    row("scan_quarantine", "Scan quarantine", counterCell(q.scan_quarantine)),
    row("coverage_ranges_pending", "Coverage ranges pending", counterCell(q.coverage_ranges_pending)),
    row("coverage_ranges_bounded_observed", "Coverage ranges bounded observed", counterCell(q.coverage_ranges_bounded_observed)),
  ];
}

/** Half-open [from, to) range aligned to 300 s; the server caps it at 7 days. */
export function qualityRange(key, nowMs) {
  const seconds = Object.prototype.hasOwnProperty.call(RANGES, key) ? RANGES[key] : RANGES[DEFAULT_RANGE];
  const to = (Math.floor(nowMs / 1000 / 300) + 1) * 300;
  return { from: to - seconds, to };
}

export function provenanceRows(provenance) {
  return [
    row("source_kind", "Source", stateCell(provenance.source_kind)),
    row("certification_state", "Certification", stateCell(provenance.certification_state)),
    row("coverage_state", "Coverage", stateCell(provenance.coverage_state)),
    row("scoring_state", "Scoring", stateCell(provenance.scoring_state)),
  ];
}

/** Auto-refresh on a fixed interval, paused while the tab is hidden, one refresh on becoming visible. */
export function createRefresher({ intervalMs = REFRESH_MS, run, isHidden, setIntervalFn, clearIntervalFn, onVisibility }) {
  let timer = null;
  let unsubscribe = null;
  const tick = () => { if (!isHidden()) run(); };
  return {
    start() {
      if (timer !== null) return;
      timer = setIntervalFn(tick, intervalMs);
      unsubscribe = onVisibility(tick);
    },
    stop() {
      if (timer !== null) clearIntervalFn(timer);
      timer = null;
      if (unsubscribe) unsubscribe();
      unsubscribe = null;
    },
  };
}
