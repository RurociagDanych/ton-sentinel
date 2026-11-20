// Pure logic of the detections and alerts view: effectiveness banner text, exact rational arithmetic that
// recomputes median, MAD, scale and score from the pinned baseline cells, and alert status. No DOM here.
import { ProvenanceError, validateProvenance } from "./client.js";

export const DETECTION_BANNER = "Detection effectiveness is unmeasured: no labelled anomaly data exists, so "
  + "precision and recall are unknown. Both detectors were verified only on injected synthetic spikes. "
  + "This output is not trading advice.";

export const ALERT_STATES = ["active", "pending", "retracted", "inactive"];
export const SCORE_PLACES = 1000000000n;
export const DETECTORS = {
  "swap_count_spike.v1": { field: "swaps", wallets: false, floorKey: "swap_floor", unit: "swaps" },
  "active_wallets_spike.v1": { field: "known_traders", wallets: true, floorKey: "trader_floor", unit: "wallets" },
};
const ELIGIBLE = ["observed", "zero"];
const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);


const gcd = (a, b) => { a = a < 0n ? -a : a; b = b < 0n ? -b : b; while (b) [a, b] = [b, a % b]; return a; };

class Frac {
  constructor(n, d) {
    if (d === 0n) throw new RangeError("zero denominator");
    if (d < 0n) { n = -n; d = -d; }
    const g = gcd(n, d) || 1n;
    this.n = n / g;
    this.d = d / g;
  }
  toString() { return this.d === 1n ? this.n.toString() : this.n + "/" + this.d; }
}

export const frac = (n, d = 1n) => new Frac(BigInt(n), BigInt(d));
const add = (a, b) => new Frac(a.n * b.d + b.n * a.d, a.d * b.d);
const sub = (a, b) => new Frac(a.n * b.d - b.n * a.d, a.d * b.d);
const mul = (a, b) => new Frac(a.n * b.n, a.d * b.d);
const div = (a, b) => new Frac(a.n * b.d, a.d * b.n);
const cmp = (a, b) => { const x = a.n * b.d; const y = b.n * a.d; return x < y ? -1 : x > y ? 1 : 0; };
const abs = (a) => new Frac(a.n < 0n ? -a.n : a.n, a.d);

/** Parse decimal text ("1.4826", "6", "-2.5") to an exact fraction. Anything else is refused. */
export function parseDecimal(textValue) {
  if (typeof textValue !== "string" || !/^-?\d+(\.\d+)?$/.test(textValue)) {
    throw new RangeError("not a plain decimal text");
  }
  const [whole, part = ""] = textValue.replace("-", "").split(".");
  const n = BigInt(whole + part) * (textValue.startsWith("-") ? -1n : 1n);
  return new Frac(n, 10n ** BigInt(part.length));
}

function roundHalfEven(value) {
  const scaledN = value.n * SCORE_PLACES;
  let q = scaledN / value.d;
  const r = scaledN % value.d;
  if (r !== 0n) {
    const twice = (r < 0n ? -r : r) * 2n;
    const away = q + (scaledN < 0n ? -1n : 1n);
    if (twice > value.d || (twice === value.d && (q % 2n !== 0n))) q = away;
  }
  return q;
}

/** Format a fraction as decimal text, half-even at 9 places, no trailing zeros: the same text the scorer stores. */
export function publish(value) {
  const scaled = roundHalfEven(value);
  const negative = scaled < 0n;
  const magnitude = negative ? -scaled : scaled;
  const whole = magnitude / SCORE_PLACES;
  const part = magnitude % SCORE_PLACES;
  let out = (negative ? "-" : "") + whole;
  if (part) out += "." + part.toString().padStart(9, "0").replace(/0+$/, "");
  return out === "-0" ? "0" : out;
}

function median(values) {
  const ordered = values.slice().sort(cmp);
  const middle = Math.floor(ordered.length / 2);
  if (ordered.length % 2) return ordered[middle];
  return div(add(ordered[middle - 1], ordered[middle]), frac(2));
}


export function requireDetectionProvenance(payload) {
  if (!isObject(payload)) throw new ProvenanceError("response has no provenance block");
  return validateProvenance(payload);
}


const row = (name, stored, recomputed) => ({ name, stored: stored === null || stored === undefined ? "none" : String(stored),
  recomputed: String(recomputed), match: String(stored) === String(recomputed) });

function mismatch(reason) {
  return { kind: "input_mismatch", reproduced: false, rows: [], reason };
}

/** Recompute what the scorer computed from the cells the pin returns and compare with the stored result. */
export function explain(result, pin, policy) {
  const spec = isObject(result) ? DETECTORS[result.detector] : null;
  if (!spec) return mismatch("The detector of this result is not one this view knows how to explain.");
  if (!isObject(pin) || pin.input_digest !== result.input_digest || !Array.isArray(pin.cells)
      || !isObject(pin.current_cell)) {
    return mismatch("The baseline pin does not carry the input digest named by this result.");
  }
  if (!isObject(policy) || policy.policy_id !== result.policy_id
      || !isObject(pin.policy) || pin.policy.policy_id !== result.policy_id) {
    return mismatch("The policy read for this explanation is not the policy the result was scored under.");
  }
  const base = { detector: result.detector, status: result.status, reasons: Array.isArray(result.reasons) ? result.reasons : [],
    sample_count: result.sample_count, min_prior_windows: result.min_prior_windows,
    threshold: result.threshold, floor: result.floor, unit: spec.unit };
  if (result.status !== "scored") return { ...base, kind: "not_scored", reproduced: false, rows: [], recomputed_triggered: null,
    rule: ruleText(policy, result, spec) };
  let multiplier, scaleFloor, attributionFloor, threshold;
  try {
    multiplier = parseDecimal(policy.mad_multiplier);
    scaleFloor = parseDecimal(policy.scale_floor);
    attributionFloor = parseDecimal(policy.attribution_floor);
    threshold = parseDecimal(String(result.threshold));
  } catch (error) {
    return mismatch("The policy values are not plain decimal text, so nothing is recomputed.");
  }
  let sample = pin.cells.filter((cell) => isObject(cell) && ELIGIBLE.includes(cell.state));
  if (spec.wallets) {
    sample = sample.filter((cell) => cell.swaps === 0
      || cmp(frac(cell.swaps - cell.unknown_count, cell.swaps), attributionFloor) >= 0);
  }
  const values = sample.map((cell) => frac(cell[spec.field]));
  if (values.length === 0) return mismatch("The pin holds no eligible cell, so nothing is recomputed.");
  const med = median(values);
  const mad = median(values.map((v) => abs(sub(v, med))));
  const madScaled = mul(multiplier, mad);
  const scale = cmp(scaleFloor, madScaled) >= 0 ? scaleFloor : madScaled;
  const observed = pin.current_cell[spec.field];
  const score = div(sub(frac(observed), med), scale);
  const reasons = [];
  if (observed < result.floor) reasons.push(spec.wallets ? "below_trader_floor" : "below_swap_floor");
  if (cmp(parseDecimal(publish(score)), threshold) < 0) reasons.push("below_score_threshold");
  const rows = [row("observed", result.observed, observed), row("sample_count", result.sample_count, sample.length),
    row("median", result.median, publish(med)), row("mad", result.mad, publish(mad)),
    row("scale", result.scale, publish(scale)), row("score", result.score, publish(score))];
  const triggered = reasons.length === 0;
  const reasonsMatch = JSON.stringify(reasons) === JSON.stringify(base.reasons);
  const reproduced = rows.every((item) => item.match) && triggered === (result.triggered === true) && reasonsMatch;
  return { ...base, kind: "scored", rows, reproduced, recomputed_triggered: triggered, recomputed_reasons: reasons,
    stored_triggered: result.triggered === true, rule: ruleText(policy, result, spec),
    formula: "score = (observed - median) / max(" + policy.scale_floor + ", " + policy.mad_multiplier + " x MAD)" };
}

export function ruleText(policy, result, spec) {
  return "Triggered when status is scored and score >= " + result.threshold + " and observed >= " + result.floor
    + " (" + spec.unit + ")" + (spec.wallets ? " and attribution >= " + policy.attribution_floor + " in the current window" : "")
    + "; any failed condition is listed as a reason.";
}


const ALERT_TEXT = {
  active: "An alert is active for this result.",
  pending: "An alert is pending: the result is not final.",
  retracted: "The alert was retracted.",
  inactive: "The alert is inactive.",
};

/**
 * Latest alert revision of this result's key.
 * No record is not "inactive"; a truncated feed that does not show one is "unknown"; an unrecognised state fails closed to "unknown".
 */
export function alertStatus(alerts, result, { truncated = false } = {}) {
  const list = (Array.isArray(alerts) ? alerts : []).filter((alert) => isObject(alert)
    && alert.token === result.token && alert.window_start === result.window_start && alert.detector === result.detector);
  if (list.length === 0) {
    return truncated
      ? { state: "unknown", text: "The alert feed was truncated and shows no record for this result; its status is unknown." }
      : { state: "no_record", text: "No alert record exists for this result in the loaded feed." };
  }
  const latest = list.reduce((best, item) => (item.alert_revision > best.alert_revision ? item : best));
  if (!ALERT_STATES.includes(latest.state)) {
    return { state: "unknown", alert_revision: latest.alert_revision, text: "The feed reports a state this view does not know; it is not shown as active." };
  }
  return { state: latest.state, alert_revision: latest.alert_revision, label: latest.label || null,
    recorded_at: latest.recorded_at || null, text: ALERT_TEXT[latest.state] };
}
