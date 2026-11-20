// State vocabulary: maps explicit server state values to a badge. The client never infers a
// state; a value it does not know fails closed to the "unknown" tone, never to "ok".

export const STATE_LEGEND = {
  "observed": { label: "observed", tone: "info", meaning: "A value was read from recorded rows. It is a count, not a quality judgement." },
  "unavailable": { label: "unavailable", tone: "bad", meaning: "The value could not be read. It is not zero and not healthy." },
  "not_measured": { label: "not measured", tone: "warn", meaning: "Nothing measures this yet. It is not zero and not healthy." },
  "none_recorded": { label: "none recorded", tone: "neutral", meaning: "The source holds no record. This is not a measured zero." },
  "empty": { label: "empty", tone: "neutral", meaning: "The table was read and holds no rows." },
  "ok": { label: "ok", tone: "ok", meaning: "The database query succeeded. It says nothing about data quality." },
  "available": { label: "available", tone: "ok", meaning: "The component answered. It does not mean data is fresh or certified." },
  "unknown": { label: "unknown", tone: "warn", meaning: "The service does not know and does not claim a state." },
  "unsupported": { label: "unsupported", tone: "neutral", meaning: "This domain is not instrumented by the read-only service." },
  "not_configured": { label: "not configured", tone: "warn", meaning: "The capability is not set up." },
  "not_qualified": { label: "not qualified", tone: "warn", meaning: "No qualification evidence exists, so the gate is not passed." },
  "not_granted": { label: "not granted", tone: "warn", meaning: "No approval was recorded." },
  "unverified_origin": { label: "unverified origin", tone: "warn", meaning: "Row origin is not recorded; rows may be fixture or synthetic." },
  "live": { label: "live", tone: "info", meaning: "Every window in scope is recorded as read live from the source. It is not certified." },
  "mixed_origin": { label: "mixed origin", tone: "warn", meaning: "Some windows in scope are live; others are fixture, recorded or of unrecorded origin." },
  "not_certified": { label: "not certified", tone: "warn", meaning: "Nothing is certified by this service." },
  "unrecorded": { label: "unrecorded", tone: "neutral", meaning: "No coverage record exists for these rows." },
  "bounded_observed": { label: "bounded observed", tone: "info", meaning: "Coverage was observed within a bounded range. It is not full coverage." },
  "synthetic_bounded_observed": { label: "synthetic, bounded observed", tone: "warn", meaning: "Rows include synthetic fixture data observed within a bounded range." },
  "certified_claimed": { label: "certified (claimed)", tone: "warn", meaning: "A record claims certification. This service does not verify it." },
  "zero": { label: "zero", tone: "info", meaning: "The window has a served row with 0 swaps and a qualification record." },
  "partial": { label: "partial", tone: "warn", meaning: "The window has a served row but coverage gaps or a missing qualification are recorded." },
  "provisional": { label: "provisional", tone: "warn", meaning: "The window is not final: provisional row, pending work, or qualified without a served row." },
  "missing": { label: "missing", tone: "bad", meaning: "No served row exists for this window. It is a gap, not a zero." },
  "pre_universe": { label: "before first row", tone: "neutral", meaning: "The window lies before the token's first served row." },
  "scored_effectiveness_unmeasured": { label: "scored, effectiveness unmeasured", tone: "warn", meaning: "Detectors ran. Their effectiveness is unmeasured." },
};

const UNKNOWN_MEANING = "The client does not recognise this state value, so it is shown as unknown.";

export function badge(state) {
  if (typeof state === "string" && Object.prototype.hasOwnProperty.call(STATE_LEGEND, state)) {
    const entry = STATE_LEGEND[state];
    return { state, label: entry.label, tone: entry.tone, meaning: entry.meaning, known: true };
  }
  const shown = typeof state === "string" && state !== "" ? state.slice(0, 60) : "missing";
  return { state: typeof state === "string" ? state : "missing", label: "unrecognised: " + shown,
    tone: "unknown", meaning: UNKNOWN_MEANING, known: false };
}

function cell(state, text, detail) {
  const b = badge(state);
  return { state: b.state, tone: b.tone, label: b.label, known: b.known, text, detail: detail || "" };
}

/**
 * A counter is {status, value?, reason?}. Only "observed" with a finite number shows a number;
 * every other state shows its own words and never a zero.
 */
export function counterCell(counter) {
  if (!counter || typeof counter !== "object" || Array.isArray(counter) || typeof counter.status !== "string") {
    return cell("unavailable", "unavailable", "no state in response");
  }
  const reason = typeof counter.reason === "string" ? counter.reason : "";
  switch (counter.status) {
    case "observed":
      if (typeof counter.value === "number" && Number.isFinite(counter.value)) {
        const age = typeof counter.oldest_age_seconds === "number"
          ? "oldest " + Math.round(counter.oldest_age_seconds) + " s" : "";
        return cell("observed", String(counter.value), age);
      }
      return cell("unavailable", "unavailable", "observed without a numeric value");
    case "unavailable":
      return cell("unavailable", "unavailable", reason);
    case "not_measured":
      return cell("not_measured", "not measured", reason);
    case "none_recorded":
      return cell("none_recorded", "none recorded", reason);
    default:
      return cell(counter.status, "unrecognised state", reason);
  }
}

/** A group counter is {status, value: {key: count}, truncated?}: shown as "key: n" pairs. */
export function groupCell(counter) {
  if (counter && counter.status === "observed" && counter.value && typeof counter.value === "object"
      && !Array.isArray(counter.value)) {
    const entries = Object.entries(counter.value);
    if (entries.every(([, n]) => typeof n === "number" && Number.isFinite(n))) {
      const text = entries.length === 0 ? "no groups" : entries.map(([k, n]) => k + ": " + n).join(", ");
      return cell("observed", text, counter.truncated === true ? "list truncated" : "");
    }
    return cell("unavailable", "unavailable", "observed with a malformed value");
  }
  return counterCell(counter);
}

/**
 * Kafka lag cell: an observed value is a list of per-Spark-query entries {query_id, status, total_lag_records}.
 * Shown as a record count only when every entry was measured; one unreadable or malformed entry makes
 * the whole value unavailable, never a number. Per-query counts are listed, not added together.
 */
export function kafkaLagCell(section) {
  if (section && typeof section === "object" && !Array.isArray(section) && section.status === "observed") {
    const entries = section.value;
    const measured = (e) => e && typeof e === "object" && e.status === "observed"
      && Number.isSafeInteger(e.total_lag_records) && e.total_lag_records >= 0;
    if (!Array.isArray(entries) || entries.length === 0) {
      return cell("unavailable", "unavailable", "observed without a per-query lag");
    }
    if (!entries.every(measured)) {
      return cell("unavailable", "unavailable", "the lag of at least one query could not be read");
    }
    const text = entries.length === 1 ? entries[0].total_lag_records + " records"
      : entries.map((e) => String(e.query_id) + ": " + e.total_lag_records + " records").join(", ");
    return cell("observed", text, "log end offset minus the next offset after the newest audited record");
  }
  return counterCell(section);
}

export function stateCell(state) {
  if (typeof state !== "string" || state === "") return cell("unavailable", "unavailable", "no state in response");
  const b = badge(state);
  return { state: b.state, tone: b.tone, label: b.label, known: b.known, text: b.label, detail: "" };
}

export { cell };
