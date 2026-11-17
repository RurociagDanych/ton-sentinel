// Pure logic of the data quality and coverage view: counter tiles (an unavailable counter is never a
// zero), the per-state coverage breakdown over a grid of five-minute windows, the coverage strip and
// the gap list. Nothing here infers a state: every value comes from the server, and an unrecognised
// value fails closed to "unknown". No DOM here.
import { ProvenanceError } from "./client.js";
import { counterCell, groupCell } from "./states.js";
import { WINDOW_SECONDS } from "./explorer-model.js";
import { requireDetectionProvenance } from "./detections-model.js";

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);

export const requireQualityProvenance = requireDetectionProvenance;

export function requireList(payload, key) {
  if (!isObject(payload) || !Array.isArray(payload[key])) throw new ProvenanceError("response is not a list");
  return payload[key];
}

export function requireQuality(payload) {
  if (!isObject(payload) || !isObject(payload.quality)) throw new ProvenanceError("response has no quality object");
  return payload.quality;
}

export const COMPLETENESS_TEXT = "These counts describe recorded rows for one network and range. They do not show that "
  + "the data is complete: a window or row that was never recorded is not counted anywhere. A tile marked unavailable "
  + "could not be read or is not scoped to a network; it is never zero.";

export const COVERAGE_APPROXIMATIONS = [
  "canonical is approximated by a served row that is not provisional: the canonical tables are not readable by this service.",
  "A window counts as in the universe once it reaches the token's first served row; earlier windows are shown as before first row.",
  "pending is read from a pending target row in the dirty-window queue. A window with a qualification but no served row is provisional, never zero.",
  "Qualification is shown as recorded; its source-epoch validity is not checked.",
];

export const COVERAGE_STATES = ["observed", "zero", "partial", "provisional", "missing", "pre_universe"];
export const QUALIFICATION_CLASSES = ["bounded", "synthetic", "unrecorded"];

// ---- counter tiles -------------------------------------------------------------------------------

export const COUNTERS = [
  { key: "served_rows_by_coverage_status", title: "Served rows by coverage status", group: true, scope: "network, range" },
  { key: "served_windows", title: "Served windows", group: false, scope: "network, range" },
  { key: "dirty_windows_pending_by_role", title: "Pending dirty windows by role", group: true, scope: "network, range" },
  { key: "revision_quarantine_open", title: "Open revision quarantine", group: false, scope: "network, not range-scoped" },
  { key: "delivery_quarantine", title: "Delivery quarantine", group: false, scope: "network, range" },
  { key: "qualifications_by_class", title: "Qualifications by class", group: true, scope: "network, range" },
  { key: "scan_quarantine", title: "Scan quarantine", group: false, scope: "not network scoped" },
  { key: "coverage_ranges_pending", title: "Coverage ranges pending", group: false, scope: "not network scoped" },
  { key: "coverage_ranges_bounded_observed", title: "Coverage ranges bounded observed", group: false, scope: "not network scoped" },
];

// One tile per documented counter. A counter the response omits or malforms is unavailable, never 0.
export function tiles(quality) {
  const source = isObject(quality) ? quality : {};
  return COUNTERS.map((def) => {
    const cell = def.group ? groupCell(source[def.key]) : counterCell(source[def.key]);
    return { key: def.key, title: def.title, scope: def.scope, state: cell.state, tone: cell.tone,
      label: cell.label, known: cell.known, text: cell.text, detail: cell.detail };
  });
}

// ---- coverage: strip, breakdown, gaps --------------------------------------------------------------

export function qualificationClass(window) {
  const q = isObject(window) ? window.qualification : null;
  if (!isObject(q)) return "unrecorded";
  if (q.synthetic === true) return "synthetic";
  if (typeof q.mode === "string" && q.mode.includes("bounded_observed")) return "bounded";
  return "unrecorded";
}

const stateOf = (window) => (isObject(window) && COVERAGE_STATES.includes(window.state) ? window.state : "unknown");

export function strip(windows) {
  return (Array.isArray(windows) ? windows : []).filter((w) => isObject(w) && Number.isInteger(w.window_start))
    .map((w) => ({ window_start: w.window_start, state: stateOf(w), raw_state: w.state, qualification: qualificationClass(w) }));
}

// Counts over the listed windows of [from, to). Windows outside the range, off the grid or repeated are
// ignored and counted. Slots with no listed window are "not listed": never missing and never zero.
export function breakdown(windows, from, to) {
  const slots = Math.max(0, Math.floor((to - from) / WINDOW_SECONDS));
  const byState = { observed: 0, zero: 0, partial: 0, provisional: 0, missing: 0, pre_universe: 0, unknown: 0 };
  const byQualification = { bounded: 0, synthetic: 0, unrecorded: 0 };
  const seen = new Set();
  let ignored = 0;
  for (const w of Array.isArray(windows) ? windows : []) {
    const start = isObject(w) ? w.window_start : null;
    const ok = Number.isInteger(start) && start >= from && start < to && (start - from) % WINDOW_SECONDS === 0 && !seen.has(start);
    if (!ok) { ignored += 1; continue; }
    seen.add(start);
    byState[stateOf(w)] += 1;
    byQualification[qualificationClass(w)] += 1;
  }
  const notListed = slots - seen.size;
  return { by_state: byState, by_qualification: byQualification, listed: seen.size, slots, ignored, not_listed: notListed,
    not_listed_text: notListed === 0 ? "" : notListed + " of " + slots + " windows were not returned by the server. "
      + "They are not counted as missing and not as zero." };
}

export function explorerHref(token, windowStart) {
  return "#/explorer/" + encodeURIComponent(token) + "/" + String(windowStart);
}

// Windows whose state is missing or pre_universe, ascending, each with its reasons verbatim.
export function gaps(windows, token) {
  const out = [];
  for (const w of Array.isArray(windows) ? windows : []) {
    if (!isObject(w) || !Number.isInteger(w.window_start) || !["missing", "pre_universe"].includes(w.state)) continue;
    const reasons = [];
    for (const list of [w.reasons, w.missing, w.admission_reasons]) {
      for (const r of Array.isArray(list) ? list : []) if (typeof r === "string" && !reasons.includes(r)) reasons.push(r);
    }
    out.push({ window_start: w.window_start, state: w.state, reasons, admission_refused: w.admission_refused === true,
      href: explorerHref(token, w.window_start) });
  }
  return out.sort((a, b) => a.window_start - b.window_start);
}

export function coverageCap(collected, maxPages) {
  if (!collected || !collected.truncated) return { partial: false, text: "" };
  return { partial: true, text: "Partial coverage: only " + collected.count + " windows were loaded (cap " + maxPages
    + " pages); the range holds more. Counts and gaps below cover the loaded windows only." };
}
