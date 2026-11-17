// Pure logic of the token and window explorer: range rules, keyset paging, the window grid
// (gaps stay absent, never filled), chart geometry, labels and provenance checks. No DOM here.
import { ApiError, ProvenanceError, validateProvenance } from "./client.js";

export const WINDOW_SECONDS = 300;
export const MAX_RANGE_SECONDS = 7 * 86400;
export const TOKEN_PAGE_LIMIT = 100;
export const SERIES_PAGE_LIMIT = 1000;
export const MAX_SERIES_PAGES = 4;
export const PRESETS = { "1h": 3600, "6h": 6 * 3600, "24h": 86400, "7d": MAX_RANGE_SECONDS };
export const DEFAULT_PRESET = "24h";

const isObject = (value) => value !== null && typeof value === "object" && !Array.isArray(value);

// ---- ranges: at most 7 days, five-minute aligned, half-open [from, to) ---------------------------

export function checkRange(from, to) {
  const whole = (n) => typeof n === "number" && Number.isInteger(n) && n >= 0;
  if (!whole(from) || !whole(to)) return { ok: false, reason: "The range needs two whole, non-negative times." };
  if (from % WINDOW_SECONDS || to % WINDOW_SECONDS) {
    return { ok: false, reason: "Range bounds must sit on five-minute window boundaries." };
  }
  if (to <= from) return { ok: false, reason: "The end of the range must be after its start." };
  if (to - from > MAX_RANGE_SECONDS) {
    return { ok: false, reason: "The range is longer than 7 days. The server refuses it, so it is not requested." };
  }
  return { ok: true, reason: "" };
}

export function presetRange(key, nowMs) {
  const seconds = Object.prototype.hasOwnProperty.call(PRESETS, key) ? PRESETS[key] : PRESETS[DEFAULT_PRESET];
  const to = (Math.floor(nowMs / 1000 / WINDOW_SECONDS) + 1) * WINDOW_SECONDS;
  return { from: to - seconds, to, ...checkRange(to - seconds, to) };
}

// "YYYY-MM-DDTHH:MM" (datetime-local) read as UTC; start rounds down, end rounds up.
export function customRange(fromText, toText) {
  const parse = (value) => (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$/.test(String(value)) ? Date.parse(value + ":00Z") / 1000 : NaN);
  const a = parse(fromText);
  const b = parse(toText);
  if (!Number.isFinite(a) || !Number.isFinite(b)) {
    return { from: null, to: null, ok: false, reason: "Enter both a start and an end time (UTC)." };
  }
  const from = Math.floor(a / WINDOW_SECONDS) * WINDOW_SECONDS;
  const to = Math.ceil(b / WINDOW_SECONDS) * WINDOW_SECONDS;
  return { from, to, ...checkRange(from, to) };
}

// A deep link (#/explorer/<token>/<window_start>) must open a range that holds its window.
// Returns null when the start is not a window start or already lies in the current range; otherwise
// a 24 h custom range centred on the window, clamped at 0.
export const LINK_SPAN_SECONDS = 86400;
export function linkedRange(start, range) {
  if (!Number.isSafeInteger(start) || start < 0 || start % WINDOW_SECONDS) return null;
  if (range && Number.isInteger(range.from) && Number.isInteger(range.to) && start >= range.from && start < range.to) {
    return null;
  }
  const from = Math.max(0, start - LINK_SPAN_SECONDS / 2);
  const to = from + LINK_SPAN_SECONDS;
  return { from, to, ...checkRange(from, to) };
}

export function failureMessage(error) {
  if (error instanceof ApiError && error.status === 400) {
    return "The server refused this request (400). The range or token was not accepted; nothing is shown for it.";
  }
  if (error instanceof ApiError) return "The server could not answer (http " + error.status + "). Nothing is shown for this request.";
  return "No usable response arrived. Nothing is shown for this request.";
}

// ---- token list: keyset paging, client-side filter over the loaded page only ----------------------

export function newPager() {
  return { items: [], cursor: null, done: false };
}

export function pagerAdd(pager, rows, nextCursor) {
  const known = new Set(pager.items.map((item) => item.token));
  const fresh = (Array.isArray(rows) ? rows : []).filter((row) => isObject(row) && typeof row.token === "string"
    && row.token !== "" && !known.has(row.token) && known.add(row.token));
  const cursor = typeof nextCursor === "string" && nextCursor !== "" ? nextCursor : null;
  return { items: pager.items.concat(fresh), cursor, done: cursor === null };
}

export function tokensParams(network, range, pager) {
  const params = { network, from: String(range.from), to: String(range.to), limit: String(TOKEN_PAGE_LIMIT) };
  if (pager && pager.cursor) params.cursor = pager.cursor;
  return params;
}

export function filterTokens(items, textValue) {
  const needle = String(textValue || "").trim().toLowerCase();
  if (needle === "") return items.slice();
  return items.filter((item) => item.token.toLowerCase().includes(needle));
}

export function filterLabel(loaded, done) {
  const more = done ? "All matching tokens of this range are loaded." : "More tokens exist; the filter does not search them.";
  return "Filter applies to the " + loaded + " tokens loaded so far only. " + more;
}

// ---- series: one slot per five-minute window; a window without a row is a gap --------------------

export function buildGrid(from, to, points) {
  const slots = [];
  for (let start = from; start < to; start += WINDOW_SECONDS) slots.push({ window_start: start, point: null });
  let ignored = 0;
  for (const point of Array.isArray(points) ? points : []) {
    const start = isObject(point) ? point.window_start : null;
    const ok = Number.isInteger(start) && start >= from && start < to && (start - from) % WINDOW_SECONDS === 0;
    const slot = ok ? slots[(start - from) / WINDOW_SECONDS] : null;
    if (!slot || slot.point) { ignored += 1; continue; }
    slot.point = point;
  }
  return { from, to, slots, ignored };
}

export function pointLabels(point) {
  const q = isObject(point) ? point.qualification : null;
  let qualification;
  let text;
  if (!isObject(q)) { qualification = "unrecorded"; text = "unrecorded"; }
  else if (q.synthetic === true) { qualification = "synthetic"; text = "synthetic (" + String(q.mode) + ")"; }
  else { qualification = String(q.mode); text = String(q.mode); }
  const coverage = isObject(point) && isObject(point.provenance) ? point.provenance.coverage_state : undefined;
  return { qualification, text, provisional: isObject(point) && point.provisional === true, coverage };
}

const KIND_CLASS = { unrecorded: " chart-bar-unrecorded", synthetic: " chart-bar-synthetic" };

export const CHART_LEGEND = [
  { className: "chart-bar", text: "window with a served row" },
  { className: "chart-bar chart-bar-unrecorded", text: "unrecorded: no qualification record for the window" },
  { className: "chart-bar chart-bar-synthetic", text: "synthetic: the window is qualified as synthetic" },
  { className: "chart-bar chart-bar-provisional", text: "provisional: the served row may still change" },
  { className: "chart-gap", text: "no row: the window has no served row; it is not a zero" },
];

export function chartModel(grid, { width, height }) {
  const pad = { left: 36, right: 8, top: 8, bottom: 18 };
  const innerW = Math.max(1, width - pad.left - pad.right);
  const innerH = Math.max(1, height - pad.top - pad.bottom);
  const n = Math.max(1, grid.slots.length);
  const slotW = innerW / n;
  const present = grid.slots.filter((slot) => slot.point);
  const swapsOf = (point) => (typeof point.swaps === "number" && Number.isFinite(point.swaps) && point.swaps > 0 ? point.swaps : 0);
  const yMax = Math.max(1, ...present.map((slot) => swapsOf(slot.point)));
  const bars = [];
  const gaps = [];
  let run = null;
  grid.slots.forEach((slot, index) => {
    const x = pad.left + index * slotW;
    if (slot.point) {
      run = null;
      const labels = pointLabels(slot.point);
      const swaps = swapsOf(slot.point);
      const h = Math.max(1, (swaps / yMax) * innerH); // a present window with 0 swaps keeps a visible 1 px base
      const className = "chart-bar" + (KIND_CLASS[labels.qualification] || "") + (labels.provisional ? " chart-bar-provisional" : "");
      bars.push({ window_start: slot.window_start, x, y: pad.top + innerH - h, w: Math.max(0.5, slotW - 0.2), h,
        swaps, kind: labels.qualification, provisional: labels.provisional, className });
    } else if (run) {
      run.count += 1;
      run.w = x + slotW - run.x;
    } else {
      run = { first: slot.window_start, count: 1, x, y: pad.top, w: slotW, h: innerH };
      gaps.push(run);
    }
  });
  const gapWindows = gaps.reduce((total, gap) => total + gap.count, 0);
  return { width, height, pad, yMax, bars, gaps, gap_windows: gapWindows, legend: CHART_LEGEND };
}

// Follows next_cursor until it ends or the page cap is hit. A cap hit is reported, never hidden.
export async function collectSeries(fetchPage, maxPages = MAX_SERIES_PAGES) {
  const points = [];
  let cursor = null;
  let pages = 0;
  let truncated = false;
  while (pages < maxPages) {
    const payload = await fetchPage(cursor);
    pages += 1;
    points.push(...(Array.isArray(payload.series) ? payload.series : []));
    cursor = typeof payload.next_cursor === "string" && payload.next_cursor !== "" ? payload.next_cursor : null;
    if (cursor === null) break;
  }
  if (cursor !== null) truncated = true;
  return { points, truncated, pages };
}

// ---- provenance: a response or a series point without it blocks the view --------------------------

export function requireListProvenance(payload) {
  if (!isObject(payload)) throw new ProvenanceError("response has no provenance block");
  return validateProvenance(payload);
}

export function requireSeriesProvenance(payload) {
  const block = requireListProvenance(payload);
  if (!Array.isArray(payload.series)) throw new ProvenanceError("series response is not a list");
  for (const point of payload.series) {
    if (!isObject(point)) throw new ProvenanceError("series point is not an object");
    validateProvenance({ provenance: point.provenance });
  }
  return block;
}

// ---- window detail from /v1/coverage --------------------------------------------------------------

const UNAVAILABLE = { state: "unavailable", reasons: [], missing: [], admission_refused: false,
  admission_reasons: [], lineage_records: 0, qualification: null, served: null };

export function coverageDetail(payload, windowStart) {
  const rows = isObject(payload) && Array.isArray(payload.coverage) ? payload.coverage : null;
  const row = rows ? rows.find((item) => isObject(item) && item.window_start === windowStart) : null;
  if (!row || typeof row.state !== "string") return { ...UNAVAILABLE, window_start: windowStart };
  const list = (value) => (Array.isArray(value) ? value.filter((item) => typeof item === "string") : []);
  return { window_start: windowStart, state: row.state, reasons: list(row.reasons), missing: list(row.missing),
    admission_refused: row.admission_refused === true, admission_reasons: list(row.admission_reasons),
    lineage_records: Number.isInteger(row.lineage_records) ? row.lineage_records : 0,
    qualification: isObject(row.qualification) ? row.qualification : null,
    served: isObject(row.served) ? row.served : null };
}
