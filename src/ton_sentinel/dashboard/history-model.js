// Pure logic of the alert history view: grouping of alert-feed items per key, the client-side
// filter and its label, the result-revision timeline with labels, and the partial-feed notice.
// Nothing here infers a state: every value shown comes from a stored revision, and an unrecognised
// value fails closed to "unknown". No DOM here.
import { ProvenanceError } from "./client.js";
import { ALERT_STATES, requireDetectionProvenance } from "./detections-model.js";

export { DETECTION_BANNER } from "./detections-model.js";

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);

export const LABELS = {
  current: "The detector's current result for this key and it is fresh.",
  stale: "This was the current result but its pointer is flagged stale: not current, do not rely on it.",
  superseded: "A later result revision replaced this one.",
  pending: "The result is pending and not final.",
};

export const requireHistoryProvenance = requireDetectionProvenance;

export function requireList(payload, key) {
  if (!isObject(payload) || !Array.isArray(payload[key])) throw new ProvenanceError("response is not a list");
  return payload[key];
}

const known = (label) => Object.prototype.hasOwnProperty.call(LABELS, label);

// ---- feed: grouping and filter ------------------------------------------------------------------

const keyOf = (item) => item.token + "\u0000" + item.window_start + "\u0000" + item.detector;

function usable(item) {
  return isObject(item) && typeof item.token === "string" && typeof item.detector === "string"
    && Number.isInteger(item.window_start) && Number.isInteger(item.alert_revision);
}

// One group per token/window/detector; revisions ascending; the latest is the highest stored
// alert_revision. Groups are ordered by the recorded time of their latest revision, newest first.
export function groupFeed(items) {
  const map = new Map();
  for (const item of Array.isArray(items) ? items : []) {
    if (!usable(item)) continue;
    const key = keyOf(item);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(item);
  }
  const groups = [];
  for (const list of map.values()) {
    const revisions = [...list].sort((a, b) => a.alert_revision - b.alert_revision);
    const latest = revisions[revisions.length - 1];
    groups.push({
      token: latest.token, window_start: latest.window_start, detector: latest.detector, latest,
      revisions: revisions.map((r) => ({ alert_revision: r.alert_revision, state: ALERT_STATES.includes(r.state) ? r.state : "unknown",
        raw_state: r.state, result_revision: r.result_revision, recorded_at: r.recorded_at,
        label: known(r.label) ? r.label : "unknown", latest: r === latest })),
    });
  }
  groups.sort((a, b) => String(b.latest.recorded_at).localeCompare(String(a.latest.recorded_at))
    || (a.token < b.token ? -1 : a.token > b.token ? 1 : 0));
  for (const group of groups) {
    group.latest = group.revisions[group.revisions.length - 1];
  }
  return groups;
}

// Exact match on token and on detector, applied to the loaded items only.
export function filterFeed(items, { token = "", detector = "" } = {}) {
  return (Array.isArray(items) ? items : []).filter((item) => isObject(item)
    && (token === "" || item.token === token) && (detector === "" || item.detector === detector));
}

export function detectorsOf(items) {
  return [...new Set((Array.isArray(items) ? items : []).filter(isObject).map((i) => i.detector)
    .filter((d) => typeof d === "string"))].sort();
}

export function filterNote(loaded, shown, filter, truncated) {
  const active = Boolean(filter && (filter.token || filter.detector));
  if (!active) return "";
  return "Client-side filter on the " + loaded + " loaded alert records: " + shown + " of " + loaded + " match."
    + (truncated ? " The feed was cut before the end of the range, so more matching records may exist on the server and were not searched." : "");
}

export function feedCap(collected, maxPages) {
  const count = Array.isArray(collected && collected.items) ? collected.items.length : 0;
  if (!collected || !collected.truncated) return { partial: false, text: "" };
  return { partial: true, text: "Partial feed: only " + count + " alert records were loaded (cap " + maxPages
    + " pages); the range holds more. Latest states shown here may not be the latest stored revisions." };
}

// ---- result-revision timeline ----------------------------------------------------------------------

// Rows ascending by result_revision. The highlighted row is the highest stored result_revision.
export function timeline(revisions) {
  const list = (Array.isArray(revisions) ? revisions : []).filter((r) => isObject(r) && Number.isInteger(r.result_revision));
  const rows = [...list].sort((a, b) => a.result_revision - b.result_revision).map((r) => {
    const labelKnown = known(r.label);
    const alertState = r.alert_state === null || r.alert_state === undefined ? "no_record"
      : ALERT_STATES.includes(r.alert_state) ? r.alert_state : "unknown";
    return {
      result_revision: r.result_revision, status: r.status, score: r.score ?? null, triggered: r.triggered ?? null,
      input_digest: r.input_digest ?? null, policy_id: r.policy_id ?? null, promoted_at: r.promoted_at ?? null,
      explanation: r.explanation ?? null,
      label: labelKnown ? r.label : "unknown", label_known: labelKnown,
      label_text: labelKnown ? LABELS[r.label] : "The view does not recognise this label, so it is not treated as current.",
      current: r.label === "current",
      alert_state: alertState, alert_known: alertState !== "unknown",
      latest: false,
    };
  });
  const last = rows.length ? rows[rows.length - 1] : null;
  if (last) last.latest = true;
  return { rows, latest_revision: last ? last.result_revision : null };
}
