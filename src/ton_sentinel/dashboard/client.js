// Same-origin, read-only fetch client. Every data response must carry a provenance block;
// without it the client fails closed (DD3) and callers show the blocking state.

export const REQUEST_TIMEOUT_MS = 10000;
const PROVENANCE_KEYS = ["source_kind", "certification_state", "coverage_state",
  "scoring_state", "honesty_banner", "generated_at"];

export class ProvenanceError extends Error {
  constructor(message) {
    super(message);
    this.name = "ProvenanceError";
  }
}

export class ApiError extends Error {
  constructor(message, status) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

export function checkedPath(path) {
  if (typeof path !== "string" || !path.startsWith("/") || path.startsWith("//") || path.includes("\\")) {
    throw new ApiError("path must be a same-origin absolute path", 0);
  }
  return path;
}

export function validateProvenance(payload) {
  const block = payload && typeof payload === "object" && !Array.isArray(payload) ? payload.provenance : null;
  if (!block || typeof block !== "object" || Array.isArray(block)) {
    throw new ProvenanceError("response has no provenance block");
  }
  for (const key of PROVENANCE_KEYS) {
    if (typeof block[key] !== "string" || block[key] === "") {
      throw new ProvenanceError("provenance is missing " + key);
    }
  }
  return block;
}

const inflight = new Map();

// One in-flight request per slot: a new call aborts the previous call in the same slot.
export async function getJson(path, { slot = "default", params = null } = {}) {
  checkedPath(path);
  const query = params ? new URLSearchParams(params).toString() : "";
  const url = query ? path + "?" + query : path;
  if (inflight.has(slot)) inflight.get(slot).abort();
  const controller = new AbortController();
  inflight.set(slot, controller);
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetch(url, {
      method: "GET", credentials: "omit", cache: "no-store", redirect: "error",
      headers: { Accept: "application/json" }, signal: controller.signal,
    });
    if (!response.ok) throw new ApiError("request failed", response.status);
    return await response.json();
  } finally {
    clearTimeout(timer);
    if (inflight.get(slot) === controller) inflight.delete(slot);
  }
}

// Data fetch that refuses any payload without a valid provenance block.
export async function getChecked(path, options) {
  const payload = await getJson(path, options);
  validateProvenance(payload);
  return payload;
}
