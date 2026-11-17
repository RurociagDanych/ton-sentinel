"""Small read-only WSGI API for durable feature rows."""
from __future__ import annotations

import base64
import hashlib
from datetime import datetime, timezone
from html import escape
from importlib.resources import files
import json
import logging
import math
import re
from string import Template
import threading
import time
from urllib.parse import parse_qsl, urlencode

import psycopg

from ton_sentinel import dashboard_payload
from ton_sentinel.db.serving import (
    BASELINE_MAX_LIMIT, PIPELINE_LABEL, QUALITY_LABEL, read_pipeline, read_alert_feed, read_alert_history, read_alerts, read_baseline_inputs, read_detection_range, read_detector_results, read_features,
    read_coverage, read_provenance, read_quality, read_result_revisions, read_series, read_tokens, read_windows,
)
from ton_sentinel.db.serving import READER_TABLES
from ton_sentinel.db.serving import read_policy as serving_read_policy
from ton_sentinel.db.serving import read_telemetry as serving_read_telemetry
from ton_sentinel.redaction import redact

DEFAULT_FEATURE_LIMIT = 100
MAX_FEATURE_LIMIT = 1000
MAX_QUERY_LENGTH = 4096
MAX_QUERY_FIELDS = 5
MAX_CURSOR_LENGTH = 4096
STATEMENT_TIMEOUT = "3000ms"
SERIES_MAX_SPAN = dashboard_payload.LIMITS["time_range_seconds_max"]
SERIES_DEFAULT_LIMIT = 288
WINDOWS_MAX_FIELDS = 7  # network, window_start, window_end, token, limit, from, to
MAX_STATE_GROUPS = 50
_LOG = logging.getLogger("ton_sentinel.api")
_CSP_BASE = ("default-src 'none'; script-src 'self'; style-src {style}; img-src 'self' data:; "
             "connect-src 'self'; font-src 'none'; base-uri 'none'; form-action 'none'; "
             "frame-ancestors 'none'")
_SECURITY_HEADERS = (
    ("X-Content-Type-Options", "nosniff"),
    ("X-Frame-Options", "DENY"),
    ("Referrer-Policy", "no-referrer"),
)
# Fixed allowlist: request path component -> (packaged file, content type). Nothing else is served.
_DASHBOARD_ASSETS = {
    "app.css": ("app.css", "text/css; charset=utf-8"),
    **{name: (name, "text/javascript; charset=utf-8")
       for name in ("app.js", "client.js", "router.js", "views.js",
                    "states.js", "model.js", "overview.js", "explorer-model.js",
                    "explorer.js", "detections-model.js",
                    "detections.js", "history-model.js", "history.js", "quality-model.js", "quality.js",
                    "pipeline-model.js", "pipeline.js")},
}
_DASHBOARD_ALLOW = "GET, HEAD"
_LOG_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})


def create_app(*, dsn: str, telemetry_probes=None):
    """Build an API with explicit database settings; no implicit environment load.

    ``telemetry_probes`` (``ton_sentinel.telemetry_probes.TelemetryProbes``) adds the optional
    credential-free Kafka end-offset and database-volume probes to the telemetry block.
    """
    if type(dsn) is not str or not dsn.strip():
        raise ValueError("an explicit PostgreSQL DSN is required")

    registry = _MetricsRegistry()
    collector = _MetricsCollector(dsn, telemetry_probes)

    def app(environ, start_response):
        """Serve one request and emit one structured, redacted log record for it."""
        started = time.perf_counter()
        seen = {}

        def recording_start_response(status, headers, *exc_info):
            seen["status"] = status
            present = {name.lower() for name, _ in headers}
            extra = [(n, v) for n, v in _SECURITY_HEADERS if n.lower() not in present]
            if "content-security-policy" not in present:
                extra.append(("Content-Security-Policy", _CSP_BASE.format(style="'self'")))
            return start_response(status, [*headers, *extra], *exc_info)

        result = None
        try:
            result = dispatch(environ, recording_start_response)
            return result
        finally:
            _log_request(environ, seen.get("status"), started)
            registry.observe_request(environ, seen.get("status"), result,
                                     time.perf_counter() - started)

    def dispatch(environ, start_response):
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "")
        if path == "/dashboard/" or path.startswith("/dashboard/"):
            return _serve_dashboard(environ, start_response, method, path)
        if path == "/favicon.ico" and method in ("GET", "HEAD"):
            # Browsers request this unasked; answer 204 so the dashboard console stays free of 404s.
            start_response("204 No Content", [("Cache-Control", "max-age=86400")])
            return []
        if method != "GET":
            return _respond(start_response, "405 Method Not Allowed", {"error": "read_only"},
                            extra=[("Allow", "GET")])
        try:
            if path == "/":
                status, payload = _read_status(dsn, telemetry_probes)
                return _respond_html(start_response, status, _render_demo(payload))
            if path == "/health":
                with psycopg.connect(dsn, connect_timeout=3) as connection:
                    with connection.transaction():
                        _setup_read_transaction(connection)
                        connection.execute("SELECT 1")
                return _respond(start_response, "200 OK", {"status": "ok"})
            if path == "/status":
                status, payload = _read_status(dsn, telemetry_probes)
                return _respond(start_response, status, payload)
            if path == "/metrics":
                return _serve_metrics(start_response, registry, collector)
            if path == "/v1/features":
                raw_query = environ.get("QUERY_STRING", "")
                if (type(raw_query) is not str
                        or len(raw_query) > MAX_QUERY_LENGTH + MAX_CURSOR_LENGTH + len("&cursor=")):
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                if raw_query and raw_query.count("&") + 1 > MAX_QUERY_FIELDS + 1:
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                if re.search(r"%(?![0-9A-Fa-f]{2})", raw_query):
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                pairs = parse_qsl(
                    raw_query, keep_blank_values=True, strict_parsing=True, errors="strict"
                ) if raw_query else []
                # Reserve a separate bounded budget for the server-issued cursor.
                ordinary_query = "&".join(
                    field for (name, _), field in zip(pairs, raw_query.split("&"))
                    if name != "cursor"
                )
                if len(ordinary_query) > MAX_QUERY_LENGTH:
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                query = {}
                for key, value in pairs:
                    if not key or key in query:
                        return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                    query[key] = value
                allowed = {"network", "window_start", "window_end", "token", "limit", "cursor"}
                if set(query) - allowed:
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                if "cursor" not in query and len(pairs) > MAX_QUERY_FIELDS:
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                network = query.get("network")
                if network is not None and network not in ("mainnet", "testnet"):
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_network"})
                start = _query_int(query, "window_start")
                end = _query_int(query, "window_end")
                token = query.get("token")
                if token == "":
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                limit = _query_int(query, "limit")
                if limit is None:
                    limit = DEFAULT_FEATURE_LIMIT
                if not 1 <= limit <= MAX_FEATURE_LIMIT:
                    return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
                filters = {"network": network, "window_start": start,
                           "window_end": end, "token": token}
                after = _decode_cursor(query["cursor"], filters) if "cursor" in query else None
                with psycopg.connect(dsn, connect_timeout=3) as connection:
                    with connection.transaction():
                        _setup_read_transaction(connection)
                        rows = read_features(connection, network=network, window_start=start, window_end=end,
                                             token=token, limit=limit, after=after, fetch_extra=True)
                has_more = len(rows) > limit
                rows = rows[:limit]
                payload: dict[str, object] = {"features": rows}
                if has_more:
                    last = rows[-1]
                    key = (last["network"], last["window_start"], last["window_end"], last["token"])
                    try:
                        payload["next_cursor"] = _encode_cursor(filters, key)
                    except ValueError:
                        # Do not return an unusable continuation or silently truncate.
                        return _respond(start_response, "422 Unprocessable Entity",
                                        {"error": "pagination_key_too_large"})
                return _respond(start_response, "200 OK", payload)
            if path == "/v1/windows":
                status, payload = _read_windows_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/tokens":
                status, payload = _read_tokens_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/series":
                status, payload = _read_series_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/coverage":
                status, payload = _read_coverage_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/quality":
                status, payload = _read_quality_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/pipeline":
                status, payload = _read_pipeline_api(dsn, environ.get("QUERY_STRING", ""),
                                                      telemetry_probes)
                return _respond(start_response, status, payload)
            if path == "/v1/policy":
                status, payload = _read_policy_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/baseline":
                status, payload = _read_baseline_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/detections/range":
                status, payload = _read_detection_range_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/alerts/feed":
                status, payload = _read_alert_feed_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path == "/v1/detections/revisions":
                status, payload = _read_result_revisions_api(dsn, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            if path in _DETECTOR_VIEWS:
                status, payload = _read_detector_view(dsn, path, environ.get("QUERY_STRING", ""))
                return _respond(start_response, status, payload)
            return _respond(start_response, "404 Not Found", {"error": "not_found"})
        except (ValueError, TypeError):
            return _respond(start_response, "400 Bad Request", {"error": "invalid_query"})
        except Exception:
            # Do not disclose DSNs, database errors, or other configuration details.
            return _respond(start_response, "503 Service Unavailable", {"error": "unavailable"})

    app.metrics_registry = registry
    return app


METRICS_MAX_BYTES = 256 * 1024
METRICS_SERIES_BUDGET = 200
METRICS_TTL_SECONDS = 15.0
METRICS_DEADLINE_SECONDS = 8.0
METRICS_QUALITY_WINDOWS = 288  # fixed trailing 24 h, inside the 2016-window bound
# Closed label enums. Anything unmatched is folded into ``other``; nothing is taken from raw input.
METRICS_ROUTES = ("/", "/health", "/status", "/metrics", "/dashboard", "/v1/features",
                  "/v1/windows", "/v1/pipeline", "/v1/quality", "other")
METRICS_HISTOGRAM_ROUTES = ("/health", "/status", "/metrics", "/v1/features", "other")
METRICS_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 3.5, 5)
METRICS_ERRORS = ("invalid_query", "invalid_network", "read_only", "not_found", "unavailable",
                  "response_too_large", "pagination_key_too_large", "other")
METRICS_QUALITY_KINDS = ("revision_quarantine_open", "delivery_quarantine",
                         "dirty_windows_pending")
_METRICS_NETWORKS = ("mainnet", "testnet")
_METRICS_SOURCES = ("database", "status_query", "telemetry", "pipeline")
_METRICS_SERVED_STATUS = ("provisional", "final", "other")
_METRICS_SERVED_COVERAGE = ("bounded_observed", "missing", "unsupported", "unrecorded", "other")
_METRICS_ACTIVITIES = ("outbox_created", "outbox_published", "stream_commit")
METRICS_RUN_KINDS = ("payload", "guards", "derived")
METRICS_ROW_SERIES = ("stream_records", "stream_batches", "outbox_rows")
_METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
_PROCESS_START = time.time()
_P = "ton_sentinel_"
# name -> (type, help). Order is the catalogue order of the monitoring plan (section 3).
_METRIC_FAMILIES = {
    "http_requests_total": ("counter", "HTTP requests by known route, method class and status class (2xx includes 3xx)."),
    "http_request_duration_seconds": ("histogram", "Wall time spent serving a request, by known route."),
    "http_errors_total": ("counter", "Error responses by error code from the response body."),
    "process_start_time_seconds": ("gauge", "Start time of the api process, unix seconds."),
    "build_info": ("gauge", "Static build information; value is always 1."),
    "source_up": ("gauge", "1 if the last collector refresh read the source; 0 means its gauges are omitted."),
    "db_probe_duration_seconds": ("gauge", "Duration of the last collector refresh."),
    "collector_age_seconds": ("gauge", "Age of the cached database snapshot."),
    "serving_feature_rows": ("gauge", "Rows in served_features (as /status feature_rows)."),
    "serving_windows": ("gauge", "Distinct served windows (as /status windows)."),
    "serving_latest_window_end_timestamp_seconds": ("gauge", "Latest served window end, all scopes, unix seconds."),
    "serving_latest_write_timestamp_seconds": ("gauge", "Latest write to served_features, unix seconds."),
    "outbox_pending": ("gauge", "Unpublished outbox rows."),
    "outbox_oldest_pending_age_seconds": ("gauge", "Age of the oldest unpublished outbox row."),
    "repair_pending": ("gauge", "dirty_windows rows with status pending."),
    "database_size_bytes": ("gauge", "Whole-database size from pg_database_size."),
    "table_size_bytes": ("gauge", "Total size of the largest reader-allowlisted tables."),
    "last_activity_timestamp_seconds": ("gauge", "Newest recorded row timestamp per activity; not ingestion freshness."),
    "served_rows": ("gauge", "Served rows by closed status, coverage status and provisional flag."),
    "pipeline_backlog": ("gauge", "Never-scored served windows plus stale detector results, per network."),
    "quality_issues": ("gauge", "Recorded quality counters over the trailing 24 h, per network and kind."),
    "kafka_lag_records": ("gauge", "Kafka log end minus the newest audited Spark position, most recently committed query; omitted unless every partition is measured."),
    "database_volume_size_bytes": ("gauge", "Size of the filesystem backing the database volume (statvfs, read-only mount)."),
    "database_volume_available_bytes": ("gauge", "Bytes available to unprivileged writers on the database volume filesystem."),
    "last_maintenance_timestamp_seconds": ("gauge", "Newest completed retention run per run kind, unix seconds; vacuum and backup not covered."),
    "recorded_rows_last_hour": ("gauge", "Rows recorded in the trailing hour that are still present; counts, not a rate."),
}


def _monotonic():
    return time.monotonic()


def _status_class(code):
    return "4xx" if 400 <= code < 500 else "5xx" if code >= 500 else "2xx"


def _metric_route(path):
    if path == "/dashboard/" or path.startswith("/dashboard/"):
        return "/dashboard"
    return path if path in METRICS_ROUTES and path != "/dashboard" else "other"


class _MetricsRegistry:
    """In-process request counters; bounded because every label is a closed enum."""

    def __init__(self):
        self._lock = threading.Lock()
        self.requests = {}
        self.errors = {}
        self.durations = {}

    def observe_request(self, environ, status, result, seconds):
        code = int(status[:3]) if type(status) is str and status[:3].isdigit() else 500
        error = None
        if code >= 400 and isinstance(result, list):
            try:
                error = json.loads(b"".join(result)).get("error")
            except Exception:
                error = None
        self.observe(_metric_route(environ.get("PATH_INFO", "")),
                     environ.get("REQUEST_METHOD", "GET"), code, error, seconds)

    def observe(self, route, method, code, error, seconds):
        route = route if route in METRICS_ROUTES else "other"
        key = (route, "GET" if method == "GET" else "other", _status_class(code))
        hist_route = route if route in METRICS_HISTOGRAM_ROUTES else "other"
        slot = next((i for i, b in enumerate(METRICS_BUCKETS) if seconds <= b),
                    len(METRICS_BUCKETS))
        with self._lock:
            self.requests[key] = self.requests.get(key, 0) + 1
            if code >= 400:
                label = error if error in METRICS_ERRORS else "other"
                self.errors[label] = self.errors.get(label, 0) + 1
            entry = self.durations.setdefault(
                hist_route, [[0] * (len(METRICS_BUCKETS) + 1), 0.0, 0])
            entry[0][slot] += 1
            entry[1] += seconds
            entry[2] += 1

    def copy(self):
        with self._lock:
            return (dict(self.requests), dict(self.errors),
                    {k: [list(v[0]), v[1], v[2]] for k, v in self.durations.items()})


def _num(value):
    """A real finite number, or None (bool and NaN are not measurements)."""
    if type(value) is bool or not isinstance(value, (int, float)) or not math.isfinite(value):
        return None
    return value


def _iso_seconds(text):
    try:
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc).timestamp()
    except (TypeError, ValueError):
        return None


class _MetricsCollector:
    """Cached, single-flight DB snapshot: one connection, one read-only transaction."""

    def __init__(self, dsn, probes=None):
        self._dsn = dsn
        self._probes = probes
        self._lock = threading.Lock()
        self._snapshot = None

    def snapshot(self):
        snap = self._snapshot
        if snap is not None and _monotonic() - snap["at"] < METRICS_TTL_SECONDS:
            return snap
        with self._lock:  # concurrent scrapes wait for the one refresh
            snap = self._snapshot
            if snap is not None and _monotonic() - snap["at"] < METRICS_TTL_SECONDS:
                return snap
            snap = self._collect()
            self._snapshot = snap
            return snap

    def _collect(self):
        started = time.perf_counter()
        sources = dict.fromkeys(_METRICS_SOURCES, 0)
        samples = []
        try:
            with psycopg.connect(self._dsn, connect_timeout=3) as connection:
                with connection.transaction():
                    _setup_read_transaction(connection)
                    connection.execute("SELECT 1")
                    sources["database"] = 1
                    deadline = started + METRICS_DEADLINE_SECONDS
                    for source, section in (("status_query", _metrics_status),
                                            ("telemetry", lambda c, a: _metrics_telemetry(
                                                c, a, self._probes)),
                                            ("pipeline", _metrics_pipeline)):
                        try:
                            sources[source] = int(time.perf_counter() < deadline
                                                  and section(connection, samples.append))
                        except Exception:
                            sources[source] = 0
        except Exception:
            samples, sources = [], dict.fromkeys(_METRICS_SOURCES, 0)
        return {"at": _monotonic(), "duration": time.perf_counter() - started,
                "sources": sources, "samples": samples}


def _metrics_status(connection, add):
    """Serving rows and pending work, the same queries /status runs. False if any part failed."""
    ok = True
    rows = _optional_rows(
        connection,
        "SELECT count(*), count(DISTINCT (network, window_start, window_end)) "
        ", max(window_end), max(updated_at) FROM served_features")
    if rows:
        feature_rows, windows, window_end, written = rows[0]
        add(("serving_feature_rows", {}, int(feature_rows)))
        add(("serving_windows", {}, int(windows)))
        if window_end is not None:
            add(("serving_latest_window_end_timestamp_seconds", {}, int(window_end)))
        if written is not None:
            add(("serving_latest_write_timestamp_seconds", {}, written.timestamp()))
    else:
        ok = False
    groups = _optional_rows(
        connection,
        "SELECT status, provisional, coverage_status, count(*) FROM served_features "
        "GROUP BY 1, 2, 3 ORDER BY 1, 2, 3 NULLS FIRST LIMIT %s", (MAX_STATE_GROUPS + 1,))
    if groups is None or len(groups) > MAX_STATE_GROUPS:
        ok = False  # unreadable or truncated: a partial total would be a wrong number
    else:
        totals = {}
        for status, provisional, coverage, count in groups:
            key = (status if status in _METRICS_SERVED_STATUS else "other",
                   "unrecorded" if coverage is None else
                   coverage if coverage in _METRICS_SERVED_COVERAGE else "other",
                   "true" if provisional else "false")
            totals[key] = totals.get(key, 0) + int(count)
        for (status, coverage, provisional), count in sorted(totals.items()):
            add(("served_rows", {"status": status, "coverage_status": coverage,
                                 "provisional": provisional}, count))
    outbox = _optional_rows(
        connection, "SELECT count(*), extract(epoch FROM clock_timestamp() - min(created_at)) "
        "FROM outbox WHERE published_at IS NULL")
    if outbox:
        add(("outbox_pending", {}, int(outbox[0][0])))
        if outbox[0][1] is not None:
            add(("outbox_oldest_pending_age_seconds", {}, round(float(outbox[0][1]), 3)))
    else:
        ok = False
    repair = _optional_rows(
        connection, "SELECT count(*) FROM dirty_windows WHERE status = 'pending'")
    if repair:
        add(("repair_pending", {}, int(repair[0][0])))
    else:
        ok = False
    return ok


def _metrics_telemetry(connection, add, probes=None):
    telemetry = _safe_telemetry(connection, probes)
    if not isinstance(telemetry, dict):
        return False
    ok = True
    storage = telemetry.get("storage") or {}
    size = storage.get("database_size") or {}
    if size.get("status") == "observed" and _num(size.get("bytes")) is not None:
        add(("database_size_bytes", {}, int(size["bytes"])))
    else:
        ok = False
    largest = storage.get("largest_tables") or {}
    if largest.get("status") == "observed" and isinstance(largest.get("value"), list):
        for item in largest["value"][:len(READER_TABLES)]:
            table, total = item.get("table"), _num(item.get("total_bytes"))
            if table in READER_TABLES and total is not None:
                add(("table_size_bytes", {"table": table}, int(total)))
    else:
        ok = False
    activity = telemetry.get("last_recorded_activity") or {}
    for name in _METRICS_ACTIVITIES:
        entry = activity.get(name) or {}
        if entry.get("status") == "none_recorded":
            continue  # nothing recorded yet: no sample, and not an error
        moment = _iso_seconds(entry.get("at")) if entry.get("status") == "observed" else None
        if moment is None:
            ok = False
        else:
            add(("last_activity_timestamp_seconds", {"activity": name}, moment))
    _metrics_completion(telemetry, add)
    return ok


def _metrics_completion(telemetry, add):
    """Completion gauges. Anything not observed is omitted (never zero); these sections do
    not mark the telemetry source down because not_measured is their configured state."""
    lag = telemetry.get("kafka_lag") or {}
    if lag.get("status") == "observed" and isinstance(lag.get("value"), list) and lag["value"]:
        newest = lag["value"][0]  # spark_progress order: most recently committed query first
        total = _num(newest.get("total_lag_records")) if newest.get("status") == "observed" else None
        if total is not None and total >= 0:
            add(("kafka_lag_records", {}, int(total)))
    disk = telemetry.get("disk") or {}
    if disk.get("status") == "observed":
        size, available = _num(disk.get("total_bytes")), _num(disk.get("available_bytes"))
        if size is not None and available is not None:
            add(("database_volume_size_bytes", {}, int(size)))
            add(("database_volume_available_bytes", {}, int(available)))
    maintenance = telemetry.get("maintenance") or {}
    if maintenance.get("status") == "observed" and isinstance(maintenance.get("value"), list):
        newest = {}
        for run in maintenance["value"]:
            moment = _iso_seconds(run.get("last_completed_at"))
            if run.get("kind") in METRICS_RUN_KINDS and moment is not None:
                newest[run["kind"]] = max(moment, newest.get(run["kind"], moment))
        for kind in METRICS_RUN_KINDS:
            if kind in newest:
                add(("last_maintenance_timestamp_seconds", {"run_kind": kind}, newest[kind]))
    rates = telemetry.get("rates") or {}
    if rates.get("status") == "observed" and isinstance(rates.get("value"), dict):
        for series in METRICS_ROW_SERIES:
            count = _num(rates["value"].get(series))
            if count is not None and count >= 0:
                add(("recorded_rows_last_hour", {"series": series}, int(count)))


def _metrics_pipeline(connection, add):
    ok = True
    window_to = int(time.time()) // 300 * 300
    window_from = window_to - METRICS_QUALITY_WINDOWS * 300
    for network in _METRICS_NETWORKS:
        try:
            backlog = read_pipeline(connection, network=network)["scoring_backlog"]
            parts = backlog["value"] if backlog.get("status") == "observed" else {}
            unscored, stale = parts["unscored_served_windows"], parts["stale_results"]
            if (unscored.get("status") == "observed" and stale.get("status") == "observed"
                    and _num(unscored.get("value")) is not None
                    and _num(stale.get("value")) is not None):
                add(("pipeline_backlog", {"network": network},
                     int(unscored["value"]) + int(stale["value"])))
            else:
                ok = False
        except Exception:
            ok = False
        try:
            quality = read_quality(connection, network=network, window_from=window_from,
                                   window_to=window_to)
        except Exception:
            ok = False
            continue
        for kind in METRICS_QUALITY_KINDS:
            counter = quality.get(
                "dirty_windows_pending_by_role" if kind == "dirty_windows_pending" else kind) or {}
            value = counter.get("value")
            if kind == "dirty_windows_pending" and counter.get("status") == "observed":
                value = (None if counter.get("truncated") or not isinstance(value, dict)
                         else sum(value.values()))
            if counter.get("status") == "observed" and _num(value) is not None:
                add(("quality_issues", {"network": network, "kind": kind}, int(value)))
            else:
                ok = False
    return ok


def _build_info():
    try:
        from importlib.metadata import version
        release = version("ton-sentinel")
    except Exception:
        release = "unknown"
    migration = "unknown"
    try:
        numbers = [m.group(1) for entry in files("ton_sentinel").joinpath("db", "migrations").iterdir()
                   if (m := re.match(r"^([0-9]{3})_", entry.name))]
        migration = max(numbers) if numbers else migration
    except Exception:
        pass
    safe = lambda text: text if re.fullmatch(r"[0-9A-Za-z._+-]{1,32}", text) else "unknown"
    return safe(release), safe(migration)


def _format_value(value):
    value = float(value)
    return str(int(value)) if value.is_integer() and abs(value) < 1e15 else repr(value)


def _format_labels(labels):
    if not labels:
        return ""
    escaped = ((k, str(v).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"'))
               for k, v in labels.items())
    return "{" + ",".join(f'{k}="{v}"' for k, v in escaped) + "}"


def _render_metrics(registry, snapshot):
    """Prometheus text format 0.0.4, rendered by hand; omitted gauges have no HELP/TYPE/sample."""
    requests, errors, durations = registry.copy()
    samples = {name: [] for name in _METRIC_FAMILIES}
    for (route, method, klass), count in sorted(requests.items()):
        samples["http_requests_total"].append(
            ({"route": route, "method": method, "status_class": klass}, count))
    for error, count in sorted(errors.items()):
        samples["http_errors_total"].append(({"error": error}, count))
    histogram = []
    for route, (slots, total, count) in sorted(durations.items()):
        running = 0
        for bound, slot in zip(METRICS_BUCKETS, slots):
            running += slot
            histogram.append(("_bucket", {"route": route, "le": _format_value(bound)}, running))
        histogram.append(("_bucket", {"route": route, "le": "+Inf"}, count))
        histogram.append(("_sum", {"route": route}, total))
        histogram.append(("_count", {"route": route}, count))
    samples["process_start_time_seconds"].append(({}, _PROCESS_START))
    release, migration = _build_info()
    samples["build_info"].append(({"version": release, "schema_migration": migration}, 1))
    for source in _METRICS_SOURCES:
        samples["source_up"].append(({"source": source}, snapshot["sources"][source]))
    samples["db_probe_duration_seconds"].append(({}, round(snapshot["duration"], 6)))
    samples["collector_age_seconds"].append(
        ({}, max(0.0, round(_monotonic() - snapshot["at"], 3))))
    for name, labels, value in snapshot["samples"]:
        samples[name].append((labels, value))
    always = {"http_requests_total", "http_request_duration_seconds", "http_errors_total"}
    lines, total_series = [], 0
    for name, (kind, doc) in _METRIC_FAMILIES.items():
        rows = histogram if name == "http_request_duration_seconds" else samples[name]
        if not rows and name not in always:
            continue
        full = _P + name
        lines.append(f"# HELP {full} {doc}")
        lines.append(f"# TYPE {full} {kind}")
        for row in rows:
            if name == "http_request_duration_seconds":
                suffix, labels, value = row
            else:
                suffix, (labels, value) = "", row
            lines.append(f"{full}{suffix}{_format_labels(labels)} {_format_value(value)}")
            total_series += 1
    body = redact("\n".join(lines) + "\n")
    return body, total_series


def _serve_metrics(start_response, registry, collector):
    body, series = _render_metrics(registry, collector.snapshot())
    raw = body.encode("utf-8")
    if len(raw) > METRICS_MAX_BYTES or series > METRICS_SERIES_BUDGET:
        return _respond(start_response, "422 Unprocessable Entity",
                        {"error": "response_too_large"})
    start_response("200 OK", [("Content-Type", _METRICS_CONTENT_TYPE),
                              ("Content-Length", str(len(raw))), ("Cache-Control", "no-store")])
    return [raw]


def _serve_dashboard(environ, start_response, method, path):
    """Serve packaged dashboard files from a fixed allowlist; GET and HEAD only."""
    if method not in ("GET", "HEAD"):
        return _respond(start_response, "405 Method Not Allowed", {"error": "read_only"},
                        extra=[("Allow", _DASHBOARD_ALLOW)])
    if path == "/dashboard/":
        name, content_type = "index.html", "text/html; charset=utf-8"
    elif path.startswith("/dashboard/assets/") and path[len("/dashboard/assets/"):] in _DASHBOARD_ASSETS:
        name, content_type = _DASHBOARD_ASSETS[path[len("/dashboard/assets/"):]]
    else:
        return _respond(start_response, "404 Not Found", {"error": "not_found"})
    try:
        body = files("ton_sentinel").joinpath("dashboard", name).read_bytes()
    except OSError:
        return _respond(start_response, "404 Not Found", {"error": "not_found"})
    etag = '"' + hashlib.sha256(body).hexdigest()[:32] + '"'
    headers = [("Content-Type", content_type), ("ETag", etag), ("Cache-Control", "no-cache")]
    if environ.get("HTTP_IF_NONE_MATCH") == etag:
        start_response("304 Not Modified", headers)
        return []
    start_response("200 OK", [*headers, ("Content-Length", str(len(body)))])
    return [] if method == "HEAD" else [body]


def _demo_csp(template_text):
    """CSP for the existing demo page: its inline <style> is allowed by hash, never unsafe-inline."""
    hashes = []
    for match in re.finditer(r"<style>(.*?)</style>", template_text, re.S):
        digest = base64.b64encode(hashlib.sha256(match.group(1).encode("utf-8")).digest()).decode()
        hashes.append(f"'sha256-{digest}'")
    return _CSP_BASE.format(style=" ".join(["'self'", *hashes]))


def _log_request(environ, status, started):
    """One single-line JSON record: known route/method names only, never query, DSN or errors."""
    method = environ.get("REQUEST_METHOD", "GET")
    path = environ.get("PATH_INFO", "")
    code = None
    if type(status) is str and status[:3].isdigit():
        code = int(status[:3])
    record = {
        "event": "http_request",
        "method": method if method in _LOG_METHODS else "other",
        "route": path if path in _LOG_ROUTES else "unmatched",
        "status": code,
        "duration_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }
    _LOG.info(json.dumps(record, separators=(",", ":"), sort_keys=True))


_DETECTOR_VIEWS = {
    "/v1/detections": ({"network", "token", "window_start", "limit", "cursor"}, "detections"),
    "/v1/alerts": ({"network", "token", "window_start", "limit", "cursor"}, "alerts"),
    "/v1/alerts/history": ({"network", "token", "window_start", "detector", "limit", "cursor"},
                           "history"),
}
_LOG_ROUTES = frozenset({"/", "/health", "/status", "/v1/features", "/v1/windows",
                         *_DETECTOR_VIEWS})
_DETECTOR_ID = re.compile(r"[a-z][a-z0-9_]{0,63}\.v[0-9]{1,4}")
_BAD = ("400 Bad Request", {"error": "invalid_query"})


def _read_detector_view(dsn, path, raw_query):
    """Current detector results / active alerts / one key's alert history; bounded, GET-only.

    Validation, including the cursor, is complete before any connection is opened.
    ``truncated`` reports that more rows matched than ``limit``; ``next_cursor`` then
    continues strictly after the last returned key with the same view and filters.
    """
    allowed, name = _DETECTOR_VIEWS[path]
    if (type(raw_query) is not str
            or len(raw_query) > MAX_QUERY_LENGTH + MAX_CURSOR_LENGTH + len("&cursor=")):
        return _BAD
    if raw_query and (raw_query.count("&") + 1 > len(allowed)
                      or re.search(r"%(?![0-9A-Fa-f]{2})", raw_query)):
        return _BAD
    pairs = parse_qsl(raw_query, keep_blank_values=True, strict_parsing=True,
                      errors="strict") if raw_query else []
    # The server-issued cursor has its own bounded budget, as for /v1/features.
    if len("&".join(field for (key, _), field in zip(pairs, raw_query.split("&"))
                    if key != "cursor")) > MAX_QUERY_LENGTH:
        return _BAD
    query = {}
    for key, value in pairs:
        if not key or key in query or key not in allowed:
            return _BAD
        query[key] = value
    network = query.get("network")
    if network not in ("mainnet", "testnet"):
        return "400 Bad Request", {"error": "invalid_network"}
    token = query.get("token")
    start = _query_int(query, "window_start")
    limit = _query_int(query, "limit")
    limit = DEFAULT_FEATURE_LIMIT if limit is None else limit
    if token == "" or not 1 <= limit <= MAX_FEATURE_LIMIT or (start is not None and start % 300):
        return _BAD
    filters = {"network": network, "token": token, "window_start": start}
    if name == "history":
        detector = query.get("detector")
        if (token is None or start is None or detector is None
                or _DETECTOR_ID.fullmatch(detector) is None):
            return _BAD
        filters["detector"] = detector
        reader = read_alert_history
    else:
        reader = read_detector_results if name == "detections" else read_alerts
    after = _decode_detector_cursor(query["cursor"], name, filters) if "cursor" in query else None
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = reader(connection, **filters, limit=limit, fetch_extra=True, after=after)
    truncated = len(rows) > limit
    rows = rows[:limit]
    payload = {name: rows, "limit": limit, "truncated": truncated}
    if truncated:
        last = rows[-1]
        key = last["alert_revision"] if name == "history" else [
            last["token"], last["window_start"], last["detector"], last["direction"]]
        try:
            payload["next_cursor"] = _encode_detector_cursor(name, filters, key)
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return "200 OK", payload


def _encode_detector_cursor(view, filters, key):
    raw = json.dumps({"v": 1, "e": view, "f": filters, "k": key},
                     separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    cursor = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    if len(cursor) > MAX_CURSOR_LENGTH:
        raise ValueError("cursor is too large")
    return cursor


def _decode_detector_cursor(cursor, view, filters):
    """Strict inverse of ``_encode_detector_cursor`` bound to one view and its exact filters."""
    if (type(cursor) is not str or not cursor or len(cursor) > MAX_CURSOR_LENGTH
            or re.fullmatch(r"[A-Za-z0-9_-]+", cursor) is None):
        raise ValueError("invalid cursor")
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != cursor:
            raise ValueError("invalid cursor")
        payload = json.loads(raw.decode("utf-8"))
        if (json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8") != raw
                or type(payload) is not dict or set(payload) != {"v", "e", "f", "k"}
                or type(payload["v"]) is not int or payload["v"] != 1 or payload["e"] != view
                or type(payload["f"]) is not dict or set(payload["f"]) != set(filters)
                or any(type(payload["f"][n]) is not type(v) for n, v in filters.items())
                or payload["f"] != filters):
            raise ValueError("invalid cursor")
        key = payload["k"]
        if view == "history":
            if type(key) is not int or not 1 <= key <= 9223372036854775807:
                raise ValueError("invalid cursor")
            return key
        if type(key) is not list or len(key) != 4:
            raise ValueError("invalid cursor")
        token, start, detector, direction = key
        if (type(token) is not str or not token or type(start) is not int or start < 0
                or start % 300 or start > 9223372036854775807
                or type(detector) is not str or _DETECTOR_ID.fullmatch(detector) is None
                or direction != "both"):
            raise ValueError("invalid cursor")
        if ((filters["token"] is not None and filters["token"] != token)
                or (filters["window_start"] is not None and filters["window_start"] != start)):
            raise ValueError("cursor key conflicts with filters")
        return token, start, detector, direction
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid cursor") from error


def _read_windows_api(dsn, raw_query):
    if (type(raw_query) is not str
            or len(raw_query) > MAX_QUERY_LENGTH + MAX_CURSOR_LENGTH + len("&cursor=")):
        return "400 Bad Request", {"error": "invalid_query"}
    if raw_query and raw_query.count("&") + 1 > MAX_QUERY_FIELDS + 1:
        return "400 Bad Request", {"error": "invalid_query"}
    if re.search(r"%(?![0-9A-Fa-f]{2})", raw_query):
        return "400 Bad Request", {"error": "invalid_query"}
    pairs = parse_qsl(raw_query, keep_blank_values=True, strict_parsing=True, errors="strict") if raw_query else []
    ordinary_query = "&".join(field for (name, _), field in zip(pairs, raw_query.split("&"))
                               if name != "cursor")
    if len(ordinary_query) > MAX_QUERY_LENGTH:
        return "400 Bad Request", {"error": "invalid_query"}
    query = {}
    for key, value in pairs:
        if not key or key in query:
            return "400 Bad Request", {"error": "invalid_query"}
        query[key] = value
    if set(query) - {"network", "window_start", "window_end", "token", "limit", "cursor",
                     "from", "to"}:
        return "400 Bad Request", {"error": "invalid_query"}
    if "cursor" not in query and len(pairs) > WINDOWS_MAX_FIELDS:
        return "400 Bad Request", {"error": "invalid_query"}
    network = query.get("network")
    if network is not None and network not in ("mainnet", "testnet"):
        return "400 Bad Request", {"error": "invalid_network"}
    start, end, limit = (_query_int(query, name) for name in ("window_start", "window_end", "limit"))
    window_from, window_to = _query_range(query)
    token = query.get("token")
    if token == "":
        return "400 Bad Request", {"error": "invalid_query"}
    limit = DEFAULT_FEATURE_LIMIT if limit is None else limit
    if not 1 <= limit <= MAX_FEATURE_LIMIT:
        return "400 Bad Request", {"error": "invalid_query"}
    filters = {"network": network, "window_start": start, "window_end": end, "token": token}
    if window_from is not None:  # absent keys keep pre-range cursors valid and unreusable here
        filters.update(window_from=window_from, window_to=window_to)
    after = _decode_window_cursor(query["cursor"], filters) if "cursor" in query else None
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_windows(connection, network=network, window_start=start, window_end=end,
                                token=token, window_from=window_from, window_to=window_to,
                                limit=limit, after=after, fetch_extra=True)
    has_more = len(rows) > limit
    rows = rows[:limit]
    payload: dict[str, object] = {"windows": rows}
    if has_more:
        last = rows[-1]
        key = (last["network"], last["window_start"], last["window_end"])
        try:
            cursor = _encode_window_cursor(filters, key)
            continuation = urlencode([
                (name, value) for name, value in (
                    ("network", network), ("window_start", start), ("window_end", end),
                    ("token", token), ("from", window_from), ("to", window_to), ("limit", limit)
                ) if value is not None
            ])
            full_query = f"{continuation}&cursor={cursor}"
            if len(full_query.encode("ascii")) > MAX_QUERY_LENGTH + MAX_CURSOR_LENGTH + len("&cursor="):
                raise ValueError("continuation is too large")
            payload["next_cursor"] = cursor
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return "200 OK", payload


def _encode_window_cursor(filters, key):
    raw = json.dumps({"v": 1, "e": "windows", "f": filters, "k": key},
                     separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    cursor = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    if len(cursor) > MAX_CURSOR_LENGTH:
        raise ValueError("cursor is too large")
    return cursor


def _decode_window_cursor(cursor, filters):
    if (type(cursor) is not str or not cursor or len(cursor) > MAX_CURSOR_LENGTH
            or re.fullmatch(r"[A-Za-z0-9_-]+", cursor) is None):
        raise ValueError("invalid cursor")
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != cursor:
            raise ValueError("invalid cursor")
        payload = json.loads(raw.decode("utf-8"))
        if (json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8") != raw
                or type(payload) is not dict or set(payload) != {"v", "e", "f", "k"}
                or type(payload["v"]) is not int or payload["v"] != 1
                or payload["e"] != "windows" or type(payload["f"]) is not dict
                or set(payload["f"]) != set(filters) or payload["f"] != filters
                or type(payload["k"]) is not list or len(payload["k"]) != 3):
            raise ValueError("invalid cursor")
        network, start, end = payload["k"]
        if (type(network) is not str or network not in ("mainnet", "testnet")
                or type(start) is not int or type(end) is not int or start < 0
                or end > 9223372036854775807 or end != start + 300):
            raise ValueError("invalid cursor")
        for name, value in (("network", network), ("window_start", start), ("window_end", end)):
            if filters[name] is not None and filters[name] != value:
                raise ValueError("cursor key conflicts with filters")
        return network, start, end
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid cursor") from error


def _parse_query(raw_query, allowed):
    """Strictly parse a bounded query string; None means invalid (never partially trusted)."""
    if (type(raw_query) is not str
            or len(raw_query) > MAX_QUERY_LENGTH + MAX_CURSOR_LENGTH + len("&cursor=")
            or re.search(r"%(?![0-9A-Fa-f]{2})", raw_query)):
        return None
    try:
        pairs = parse_qsl(raw_query, keep_blank_values=True, strict_parsing=True,
                          errors="strict") if raw_query else []
    except ValueError:
        return None
    ordinary = "&".join(f for (name, _), f in zip(pairs, raw_query.split("&")) if name != "cursor")
    query = {}
    for key, value in pairs:
        if not key or key in query or key not in allowed:
            return None
        query[key] = value
    if len(ordinary) > MAX_QUERY_LENGTH or len(query) > len(allowed):
        return None
    return query


def _query_range(query):
    """Validated ``(from, to)`` window_start range, or ``(None, None)`` when neither is given."""
    window_from, window_to = _query_int(query, "from"), _query_int(query, "to")
    if window_from is None and window_to is None:
        return None, None
    if (window_from is None or window_to is None or window_from % 300 or window_to % 300
            or window_to <= window_from or window_to - window_from > SERIES_MAX_SPAN):
        raise ValueError("invalid time range")
    return window_from, window_to


def _encode_endpoint_cursor(endpoint, filters, key):
    raw = json.dumps({"v": 1, "e": endpoint, "f": filters, "k": key},
                     separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    cursor = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    if len(cursor) > MAX_CURSOR_LENGTH:
        raise ValueError("cursor is too large")
    return cursor


def _decode_endpoint_cursor(cursor, endpoint, filters):
    """Return the key list of a cursor issued by ``endpoint`` for exactly these filters."""
    if (type(cursor) is not str or not cursor or len(cursor) > MAX_CURSOR_LENGTH
            or re.fullmatch(r"[A-Za-z0-9_-]+", cursor) is None):
        raise ValueError("invalid cursor")
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != cursor:
            raise ValueError("invalid cursor")
        payload = json.loads(raw.decode("utf-8"))
        if (type(payload) is not dict or set(payload) != {"v", "e", "f", "k"}
                or json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8") != raw
                or payload["v"] != 1 or type(payload["v"]) is not int
                or payload["e"] != endpoint or payload["f"] != filters
                or type(payload["k"]) is not list):
            raise ValueError("invalid cursor")
        return payload["k"]
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid cursor") from error


def _page_limit(query, default):
    limit = _query_int(query, "limit")
    limit = default if limit is None else limit
    if not 1 <= limit <= MAX_FEATURE_LIMIT:
        raise ValueError("invalid limit")
    return limit


def _read_tokens_api(dsn, raw_query):
    """GET /v1/tokens: token index with activity counts for a network and bounded range."""
    query = _parse_query(raw_query, {"network", "from", "to", "limit", "cursor"})
    if query is None or query.get("network") not in ("mainnet", "testnet"):
        return "400 Bad Request", {"error": "invalid_query"}
    window_from, window_to = _query_range(query)
    if window_from is None:
        return "400 Bad Request", {"error": "invalid_query"}
    limit = _page_limit(query, DEFAULT_FEATURE_LIMIT)
    filters = {"network": query["network"], "from": window_from, "to": window_to, "limit": limit}
    after = None
    if "cursor" in query:
        key = _decode_endpoint_cursor(query["cursor"], "tokens", filters)
        if len(key) != 1 or type(key[0]) is not str or not key[0]:
            raise ValueError("invalid cursor")
        after = key[0]
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_tokens(connection, network=query["network"], window_from=window_from,
                               window_to=window_to, limit=limit, after=after, fetch_extra=True)
            provenance = read_provenance(
                connection, network=query["network"], window_from=window_from, window_to=window_to)
    has_more = len(rows) > limit
    rows = rows[:limit]
    payload = {"network": query["network"], "from": window_from, "to": window_to,
               "limit": limit, "tokens": rows, "honesty_banner": provenance["honesty_banner"],
               "provenance": provenance}
    if has_more:
        try:
            payload["next_cursor"] = _encode_endpoint_cursor("tokens", filters, [rows[-1]["token"]])
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return _safe(payload)


def _read_series_api(dsn, raw_query):
    """GET /v1/series: one token's window series over at most seven days, ascending."""
    query = _parse_query(raw_query, {"network", "token", "from", "to", "limit", "cursor"})
    if (query is None or query.get("network") not in ("mainnet", "testnet")
            or not query.get("token") or _echo_refused(query.get("token"))):
        return "400 Bad Request", {"error": "invalid_query"}
    window_from, window_to = _query_range(query)
    if window_from is None:
        return "400 Bad Request", {"error": "invalid_query"}
    limit = _page_limit(query, SERIES_DEFAULT_LIMIT)
    network, token = query["network"], query["token"]
    filters = {"network": network, "token": token, "from": window_from, "to": window_to,
               "limit": limit}
    after = None
    if "cursor" in query:
        key = _decode_endpoint_cursor(query["cursor"], "series", filters)
        if (len(key) != 1 or type(key[0]) is not int or key[0] < window_from
                or key[0] >= window_to or key[0] % 300):
            raise ValueError("invalid cursor")
        after = key[0]
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_series(connection, network=network, token=token, window_from=window_from,
                               window_to=window_to, limit=limit, after=after, fetch_extra=True)
            provenance = read_provenance(
                connection, network=network, window_from=window_from, window_to=window_to)
    has_more = len(rows) > limit
    rows = rows[:limit]
    now = datetime.now(timezone.utc)
    scored = provenance["scoring_state"] != "none_recorded" if provenance[
        "scoring_state"] != "unavailable" else None
    points = []
    for row in rows:
        qualification = row["qualification"]
        point = dict(row)
        origin = point.pop("origin", "unrecorded")
        point["provenance"] = dashboard_payload.build_provenance(
            qualification=[] if qualification is None
            else [(qualification["mode"], qualification["synthetic"], 1)],
            scored=scored, now=now,
            origins=[(origin, 1)])
        points.append(point)
    payload = {"network": network, "token": token, "from": window_from, "to": window_to,
               "limit": limit, "series": points, "honesty_banner": provenance["honesty_banner"],
               "provenance": provenance}
    if has_more:
        try:
            payload["next_cursor"] = _encode_endpoint_cursor(
                "series", filters, [rows[-1]["window_start"]])
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return _safe(payload)


def _read_coverage_api(dsn, raw_query):
    """GET /v1/coverage: per-window coverage state of one token over a bounded range."""
    query = _parse_query(raw_query, {"network", "token", "from", "to", "limit", "cursor"})
    if (query is None or query.get("network") not in ("mainnet", "testnet")
            or not query.get("token") or _echo_refused(query.get("token"))):
        return "400 Bad Request", {"error": "invalid_query"}
    window_from, window_to = _query_range(query)
    if window_from is None:
        return "400 Bad Request", {"error": "invalid_query"}
    limit = _page_limit(query, SERIES_DEFAULT_LIMIT)
    network, token = query["network"], query["token"]
    filters = {"network": network, "token": token, "from": window_from, "to": window_to,
               "limit": limit}
    after = None
    if "cursor" in query:
        key = _decode_endpoint_cursor(query["cursor"], "coverage", filters)
        if (len(key) != 1 or type(key[0]) is not int or key[0] < window_from
                or key[0] >= window_to or key[0] % 300):
            raise ValueError("invalid cursor")
        after = key[0]
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_coverage(connection, network=network, token=token,
                                 window_from=window_from, window_to=window_to, limit=limit,
                                 after=after, fetch_extra=True)
            provenance = read_provenance(
                connection, network=network, window_from=window_from, window_to=window_to)
    has_more = len(rows) > limit
    rows = rows[:limit]
    payload = {"network": network, "token": token, "from": window_from, "to": window_to,
               "limit": limit, "coverage": rows,
               "honesty_banner": provenance["honesty_banner"], "provenance": provenance}
    if has_more:
        try:
            payload["next_cursor"] = _encode_endpoint_cursor(
                "coverage", filters, [rows[-1]["window_start"]])
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return _safe(payload)


def _read_quality_api(dsn, raw_query):
    """GET /v1/quality: bounded data-quality counters for one network over a range."""
    query = _parse_query(raw_query, {"network", "from", "to"})
    if query is None or query.get("network") not in ("mainnet", "testnet"):
        return _BAD
    window_from, window_to = _query_range(query)
    if window_from is None:
        return _BAD
    network = query["network"]
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            quality = read_quality(connection, network=network, window_from=window_from,
                                   window_to=window_to)
            provenance = read_provenance(
                connection, network=network, window_from=window_from, window_to=window_to)
    return _safe({"network": network, "from": window_from, "to": window_to,
                  "quality": quality, "label": QUALITY_LABEL,
                  "honesty_banner": provenance["honesty_banner"], "provenance": provenance})


def _read_pipeline_api(dsn, raw_query, probes=None):
    """GET /v1/pipeline: read-only pipeline status for one network (outbox, backlog, stream)."""
    query = _parse_query(raw_query, {"network"})
    if query is None or query.get("network") not in ("mainnet", "testnet"):
        return _BAD
    network = query["network"]
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            pipeline = read_pipeline(connection, network=network)
            if probes is not None and probes.kafka_bootstrap is not None:
                # Same measured value and probe as /status and /metrics (safety filter applied).
                telemetry = _safe_telemetry(connection, probes)
                lag = (telemetry or {}).get("kafka_lag")
                pipeline["kafka_lag"] = lag if isinstance(lag, dict) else {
                    "status": "unavailable", "value": None, "reason": "telemetry_unavailable",
                    "provenance": "telemetry_probes kafka lag; telemetry block could not be read"}
            provenance = read_provenance(
                connection, network=network)
    return _safe({"network": network, "pipeline": pipeline, "label": PIPELINE_LABEL,
                  "honesty_banner": provenance["honesty_banner"], "provenance": provenance})


def _read_detection_range_api(dsn, raw_query):
    """GET /v1/detections/range: current fresh results of one token over a bounded range."""
    query = _parse_query(raw_query, {"network", "token", "from", "to", "limit", "cursor"})
    if (query is None or query.get("network") not in ("mainnet", "testnet")
            or not query.get("token") or _echo_refused(query.get("token"))):
        return _BAD
    window_from, window_to = _query_range(query)
    if window_from is None:
        return _BAD
    limit = _page_limit(query, DEFAULT_FEATURE_LIMIT)
    network, token = query["network"], query["token"]
    filters = {"network": network, "token": token, "from": window_from, "to": window_to,
               "limit": limit}
    after = None
    if "cursor" in query:
        key = _decode_endpoint_cursor(query["cursor"], "detection_range", filters)
        if (len(key) != 3 or type(key[0]) is not int or key[0] < window_from
                or key[0] >= window_to or key[0] % 300 or type(key[1]) is not str
                or _DETECTOR_ID.fullmatch(key[1]) is None or key[2] != "both"):
            raise ValueError("invalid cursor")
        after = tuple(key)
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_detection_range(
                connection, network=network, token=token, window_from=window_from,
                window_to=window_to, limit=limit, after=after, fetch_extra=True)
            provenance = read_provenance(
                connection, network=network, window_from=window_from, window_to=window_to)
    truncated = len(rows) > limit
    rows = rows[:limit]
    payload = {"network": network, "token": token, "from": window_from, "to": window_to,
               "limit": limit, "detections": rows, "truncated": truncated,
               "honesty_banner": provenance["honesty_banner"], "provenance": provenance}
    if truncated:
        last = rows[-1]
        try:
            payload["next_cursor"] = _encode_endpoint_cursor(
                "detection_range", filters,
                [last["window_start"], last["detector"], last["direction"]])
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return _safe(payload)


def _read_result_revisions_api(dsn, raw_query):
    """GET /v1/detections/revisions: all result revisions of one key, ascending and labelled."""
    query = _parse_query(raw_query, {"network", "token", "window_start", "detector", "limit",
                                     "cursor"})
    if query is None or query.get("network") not in ("mainnet", "testnet") \
            or not query.get("token") or _echo_refused(query.get("token")):
        return _BAD
    start = _query_int(query, "window_start")
    detector = query.get("detector")
    if (start is None or start < 0 or start % 300 or detector is None
            or _DETECTOR_ID.fullmatch(detector) is None):
        return _BAD
    limit = _page_limit(query, DEFAULT_FEATURE_LIMIT)
    network, token = query["network"], query["token"]
    filters = {"network": network, "token": token, "window_start": start, "detector": detector,
               "limit": limit}
    after = None
    if "cursor" in query:
        key = _decode_endpoint_cursor(query["cursor"], "detection_revisions", filters)
        if len(key) != 1 or type(key[0]) is not int or not 1 <= key[0] <= 9223372036854775807:
            raise ValueError("invalid cursor")
        after = key[0]
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_result_revisions(
                connection, network=network, token=token, window_start=start,
                detector=detector, limit=limit, after=after, fetch_extra=True)
            provenance = read_provenance(
                connection, network=network, window_from=start, window_to=start + 300)
    truncated = len(rows) > limit
    rows = rows[:limit]
    payload = {"network": network, "token": token, "window_start": start, "detector": detector,
               "limit": limit, "revisions": rows, "truncated": truncated,
               "honesty_banner": provenance["honesty_banner"], "provenance": provenance}
    if truncated:
        try:
            payload["next_cursor"] = _encode_endpoint_cursor(
                "detection_revisions", filters, [rows[-1]["result_revision"]])
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return _safe(payload)


_FEED_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z")


def _read_alert_feed_api(dsn, raw_query):
    """GET /v1/alerts/feed: global alert revisions of one network over a bounded range."""
    query = _parse_query(raw_query, {"network", "from", "to", "limit", "cursor"})
    if query is None or query.get("network") not in ("mainnet", "testnet"):
        return _BAD
    window_from, window_to = _query_range(query)
    if window_from is None:
        return _BAD
    limit = _page_limit(query, DEFAULT_FEATURE_LIMIT)
    network = query["network"]
    filters = {"network": network, "from": window_from, "to": window_to, "limit": limit}
    after = None
    if "cursor" in query:
        key = _decode_endpoint_cursor(query["cursor"], "alert_feed", filters)
        if (len(key) != 5 or type(key[0]) is not str or _FEED_TIME.fullmatch(key[0]) is None
                or type(key[1]) is not str or not key[1] or type(key[2]) is not int
                or key[2] < window_from or key[2] >= window_to or key[2] % 300
                or type(key[3]) is not str or _DETECTOR_ID.fullmatch(key[3]) is None
                or type(key[4]) is not int or not 1 <= key[4] <= 9223372036854775807):
            raise ValueError("invalid cursor")
        after = tuple(key)
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_alert_feed(connection, network=network, window_from=window_from,
                                   window_to=window_to, limit=limit, after=after,
                                   fetch_extra=True)
            provenance = read_provenance(
                connection, network=network, window_from=window_from, window_to=window_to)
    truncated = len(rows) > limit
    rows = rows[:limit]
    payload = {"network": network, "from": window_from, "to": window_to, "limit": limit,
               "alerts": rows, "truncated": truncated,
               "honesty_banner": provenance["honesty_banner"], "provenance": provenance}
    if truncated:
        last = rows[-1]
        try:
            payload["next_cursor"] = _encode_endpoint_cursor(
                "alert_feed", filters, [last["recorded_at"], last["token"],
                                        last["window_start"], last["detector"],
                                        last["alert_revision"]])
        except ValueError:
            return "422 Unprocessable Entity", {"error": "pagination_key_too_large"}
    return _safe(payload)


_BASELINE_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
BASELINE_DEFAULT_LIMIT = 5


def _read_baseline_api(dsn, raw_query):
    """GET /v1/baseline: the pinned baseline input behind a detector result.

    Select with ``input_digest`` (one pin) or ``token`` + ``window_start`` (that window's pins,
    newest first, at most ten). Absent pins are an explicit 404, never an empty success.
    """
    query = _parse_query(raw_query, {"network", "input_digest", "token", "window_start", "limit"})
    if query is None or query.get("network") not in ("mainnet", "testnet"):
        return _BAD
    network = query["network"]
    digest = query.get("input_digest")
    if digest is not None:
        if (_BASELINE_DIGEST.fullmatch(digest) is None
                or {"token", "window_start", "limit"} & set(query)):
            return _BAD
        selector, limit = {"input_digest": digest}, 1
    else:
        start, token = _query_int(query, "window_start"), query.get("token")
        if start is None or start % 300 or not token:
            return _BAD
        limit = _query_int(query, "limit")
        limit = BASELINE_DEFAULT_LIMIT if limit is None else limit
        if not 1 <= limit <= BASELINE_MAX_LIMIT:
            return _BAD
        selector = {"token": token, "window_start": start}
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            rows = read_baseline_inputs(connection, network=network, limit=limit,
                                        fetch_extra=True, **selector)
            provenance = read_provenance(
                connection, network=network)
    if not rows:
        return "404 Not Found", {"error": "baseline_not_found"}
    truncated = len(rows) > limit
    payload = {"network": network, "limit": limit, "baselines": rows[:limit],
               "truncated": truncated, "honesty_banner": provenance["honesty_banner"],
               "provenance": provenance}
    cleaned = dashboard_payload.strip_forbidden(payload)
    if len(json.dumps(cleaned, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
           ) > dashboard_payload.LIMITS["response_cap_bytes"]:
        return "422 Unprocessable Entity", {"error": "response_too_large"}
    return _safe(payload)


def _echo_refused(*values):
    """True when an echoed query value would make the safety filter refuse the whole response.

    Such a value (a wallet or raw address used as a token id) is bad input: answer 400 before
    any connection instead of a 503 that reads as a server failure.
    """
    return any(isinstance(v, str) and dashboard_payload.find_violations(v) for v in values)


def _safe(payload):
    try:
        return "200 OK", dashboard_payload.ensure_safe(payload)
    except dashboard_payload.UnsafePayload:
        return "503 Service Unavailable", {"error": "unavailable"}


def _read_policy_api(dsn, raw_query):
    """GET /v1/policy[?policy_id=]: policy identity, banner and fixed limits, from the server."""
    pairs = parse_qsl(raw_query, keep_blank_values=True, strict_parsing=True,
                      errors="strict") if raw_query else []
    if len(raw_query) > MAX_QUERY_LENGTH or len(pairs) > 1 or any(k != "policy_id" for k, _ in pairs):
        return "400 Bad Request", {"error": "invalid_query"}
    policy_id = pairs[0][1] if pairs else None
    with psycopg.connect(dsn, connect_timeout=3) as connection:
        with connection.transaction():
            _setup_read_transaction(connection)
            policy = serving_read_policy(connection, policy_id=policy_id)
            provenance = read_provenance(connection)
    if policy is None:
        return "404 Not Found", {"error": "policy_not_found"}
    try:
        payload = dashboard_payload.ensure_safe({
            "policy": policy, "limits": dict(dashboard_payload.LIMITS),
            "honesty_banner": provenance["honesty_banner"], "provenance": provenance})
    except dashboard_payload.UnsafePayload:
        return "503 Service Unavailable", {"error": "unavailable"}
    return "200 OK", payload


def _read_status(dsn, probes=None):
    """Read the single authoritative status view used by both renderings."""
    try:
        with psycopg.connect(dsn, connect_timeout=3) as connection:
            with connection.transaction():
                try:
                    _setup_read_transaction(connection)
                    row = connection.execute(
                        "SELECT count(*), count(DISTINCT (network, window_start, window_end)) "
                        ", max(window_end), max(updated_at) "
                        "FROM served_features").fetchone()
                except Exception:
                    return "503 Service Unavailable", _status_payload(
                        database="available", query="unavailable")
                groups = _optional_rows(
                    connection,
                    "SELECT status, provisional, coverage_status, count(*) FROM served_features "
                    "GROUP BY 1, 2, 3 ORDER BY 1, 2, 3 NULLS FIRST LIMIT %s", (MAX_STATE_GROUPS + 1,))
                qualification = _optional_rows(
                    connection,
                    "SELECT mode, synthetic, count(*) FROM action_window_qualification "
                    "GROUP BY 1, 2")
                outbox = _optional_rows(
                    connection,
                    "SELECT count(*), extract(epoch FROM clock_timestamp() - min(created_at)) "
                    "FROM outbox WHERE published_at IS NULL")
                repair = _optional_rows(
                    connection,
                    "SELECT count(*) FROM dirty_windows WHERE status = 'pending'")
                telemetry = _safe_telemetry(connection, probes)
    except Exception:
        return "503 Service Unavailable", _status_payload(
            database="unavailable", query="unavailable")
    latest_write = row[3]
    if latest_write is not None:
        latest_write = latest_write.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return "200 OK", _status_payload(database="available", feature_rows=row[0], windows=row[1],
                                     latest_window_end=row[2], latest_serving_write_at=latest_write,
                                     groups=groups, qualification=qualification,
                                     pending_work=_pending_work(outbox, repair),
                                     telemetry=telemetry)


def _safe_telemetry(connection, probes=None):
    """Telemetry through the dashboard safety filter; refused content becomes unavailable."""
    try:
        telemetry = serving_read_telemetry(connection)
        if probes is not None:
            telemetry = probes.apply(telemetry)
        return dashboard_payload.ensure_safe(telemetry)
    except Exception:
        return None


def _pending_work(outbox, repair):
    """Sanitized aggregates only; an unreadable source is unavailable, never zero."""
    result = {"outbox": {"status": "unavailable"}, "repair": {"status": "unavailable"}}
    if outbox:
        pending, age = outbox[0]
        result["outbox"] = {"status": "observed", "pending": int(pending),
                            "oldest_pending_age_seconds":
                            None if age is None else round(float(age), 3)}
    if repair:
        result["repair"] = {"status": "observed", "pending_windows": int(repair[0][0])}
    return result


def _optional_rows(connection, statement, parameters=None):
    """Extra read-only observation; a failure becomes ``None`` (unavailable), never a zero."""
    try:
        with connection.transaction():
            return list(connection.execute(statement, parameters).fetchall())
    except Exception:
        return None


def _qualification_view(rows):
    label = ("Counts of recorded whole-window qualification records. Records flagged synthetic are "
             "fixture evidence, bounded_observed, never certified. certified_recorded counts "
             "records that claim certification; they are not verified by this view and do "
             "not change coverage, readiness or any gate.")
    if rows is None:
        return {"status": "unavailable", "label": label}
    counts = {("bounded_observed", True): 0, ("bounded_observed", False): 0, ("certified", False): 0}
    for mode, synthetic, count in rows:
        counts[(mode, bool(synthetic))] = counts.get((mode, bool(synthetic)), 0) + int(count)
    return {"status": "observed", "label": label,
            "bounded_observed_synthetic": counts[("bounded_observed", True)],
            "bounded_observed_not_synthetic": counts[("bounded_observed", False)],
            "certified_recorded": sum(n for (mode, _), n in counts.items() if mode == "certified")}


def _served_view(groups):
    if groups is None:
        return {"status": "unavailable", "origin": "unverified", "rows_by_state": None,
                "truncated": None, "label": _SERVED_LABEL}
    return {"status": "observed", "origin": "unverified", "label": _SERVED_LABEL,
            "rows_by_state": [{"status": g[0], "provisional": g[1], "coverage_status": g[2],
                               "rows": int(g[3])} for g in groups[:MAX_STATE_GROUPS]],
            "truncated": len(groups) > MAX_STATE_GROUPS}


_SERVED_LABEL = ("Row origin is not recorded: rows may be fixture or synthetic, are not live "
                 "data, and are not certified. Provisional rows are not final.")


def _gates():
    """Gates need verifiable evidence; this read-only service has none, so none is passed."""
    return {
        "stateful_streaming": {
            "status": "not_qualified", "evidence": None,
            "meaning": "No qualification evidence is available to this service."},
        "live_source": {
            "status": "not_qualified", "evidence": None, "owner_approval": "not_granted",
            "meaning": "No live-source run is authorized or evidenced; local data is fixture only."},
    }


def _render_demo(payload):
    """Render status as HTML, escaping every interpolated value."""
    payload = redact(payload)
    qualification = payload["qualification"]
    served = payload.get("served_data") or {}
    states = served.get("rows_by_state")
    values = {
        "gate_stateful": payload["gates"]["stateful_streaming"]["status"],
        "gate_stateful_evidence": payload["gates"]["stateful_streaming"]["evidence"] or "none",
        "gate_live": payload["gates"]["live_source"]["status"],
        "gate_live_evidence": payload["gates"]["live_source"]["evidence"] or "none",
        "gate_live_meaning": payload["gates"]["live_source"]["meaning"],
        "served_origin": served.get("origin", "unavailable"),
        "served_label": served.get("label", _SERVED_LABEL),
        "qual_status": qualification["status"],
        "qual_synthetic": qualification.get("bounded_observed_synthetic", "unavailable"),
        "qual_not_synthetic": qualification.get("bounded_observed_not_synthetic", "unavailable"),
        "qual_certified": qualification.get("certified_recorded", "unavailable"),
        "qual_label": qualification["label"],
        "status": payload["status"],
        "status_meaning": payload["status_meaning"],
        "process": payload["availability"]["process"],
        "database": payload["availability"]["database"],
        "status_query": payload["status_query"],
        "readiness": payload["readiness"],
        "source": payload["source"]["status"],
        "coverage": payload["coverage"]["status"],
        "backup": payload["backup"],
        "limitation_mode": payload["interim_limitation"]["mode"],
        "limitation_stateful": payload["interim_limitation"]["stateful_streaming"],
        "limitation_meaning": payload["interim_limitation"]["meaning"],
        "health_meaning": payload["health_endpoint_meaning"],
        "feature_rows": payload.get("feature_rows", "unavailable"),
        "windows": payload.get("windows", "unavailable"),
        "freshness_status": payload["serving_freshness"]["status"],
        "freshness_scope": payload["serving_freshness"]["scope"],
        "latest_window_end": payload["serving_freshness"].get("latest_window_end") or "unavailable",
        "latest_serving_write_at": payload["serving_freshness"].get("latest_serving_write_at") or "unavailable",
        "serving_lag": payload["serving_freshness"]["lag"]["status"],
        "freshness_limitations": payload["serving_freshness"]["limitations"],
    }
    pending = payload["pending_work"]
    values["pending_outbox"] = pending["outbox"].get("pending", "unavailable")
    values["pending_outbox_age"] = pending["outbox"].get("oldest_pending_age_seconds")
    values["pending_outbox_age"] = "none" if (pending["outbox"].get("status") == "observed"
                                              and values["pending_outbox_age"] is None) \
        else values["pending_outbox_age"] if values["pending_outbox_age"] is not None else "unavailable"
    values["pending_repair"] = pending["repair"].get("pending_windows", "unavailable")
    for name, value in payload["operational_domains"].items():
        values[f"domain_{name}"] = value
    escaped = {name: escape(str(value), quote=True) for name, value in values.items()}
    escaped["served_states"] = "unavailable" if states is None else "none" if not states else "".join(
        "<li>status <code>{}</code>, provisional <code>{}</code>, coverage <code>{}</code>: "
        "<code>{}</code> rows</li>".format(*(escape(str(v), quote=True) for v in (
            g["status"], g["provisional"], g["coverage_status"], g["rows"]))) for g in states)
    template = Template(files("ton_sentinel").joinpath("demo.html").read_text(encoding="utf-8"))
    return template.substitute(escaped)


def _demo_csp_header():
    return _demo_csp(files("ton_sentinel").joinpath("demo.html").read_text(encoding="utf-8"))


def _respond_html(start_response, status, body):
    encoded = body.encode("utf-8")
    headers = [("Content-Type", "text/html; charset=utf-8"),
               ("Content-Length", str(len(encoded))), ("Cache-Control", "no-store"),
               ("Content-Security-Policy", _demo_csp_header())]
    start_response(status, headers)
    return [encoded]


def _query_int(query, name):
    value = query.get(name)
    if value is None:
        return None
    if not value.isascii() or not value.isdigit():
        raise ValueError("query values must be nonnegative integers")
    if len(value) > 19:
        raise ValueError("query integer is too large")
    number = int(value)
    if number > 9223372036854775807:
        raise ValueError("query integer is too large")
    return number


def _status_payload(*, database, query="available", feature_rows=None, windows=None,
                    latest_window_end=None, latest_serving_write_at=None, groups=None,
                    qualification=None, pending_work=None, telemetry=None):
    """Report measured availability separately from uninstrumented operations."""
    payload = {
        "status": "ok" if query == "available" else "unavailable",
        "status_meaning": "database_query_succeeded" if query == "available"
        else "database_query_unavailable",
        "availability": {"process": "available", "database": database},
        "status_query": query,
        "readiness": "unknown",
        "source": {"status": "unknown"},
        "coverage": {"status": "unknown"},
        "operational_domains": {
            "outbox": "unsupported",
            "repair": "unsupported",
            "spark_checkpoint": "unsupported",
            "spark_watermark": "unsupported",
            "spark_state": "unsupported",
            "database_storage": "unsupported",
            "disk": "unsupported",
            "maintenance": "unsupported",
            "rates": "unsupported",
        },
        "pending_work": pending_work or {"outbox": {"status": "unavailable"},
                                         "repair": {"status": "unavailable"}},
        "backup": "not_configured",
        "interim_limitation": {
            "mode": "stateless_batch_replacement",
            "stateful_streaming": "not_qualified",
            "meaning": "Features come from stateless full-window replacement; the stateful "
                       "streaming path is not qualified until it passes its own qualification.",
        },
        "health_endpoint_meaning": "process_and_database_connectivity_only",
        "gates": _gates(),
        "qualification": _qualification_view(qualification),
        "serving_freshness": {
            "status": "unavailable" if query != "available" else
            "observed" if feature_rows else "empty",
            "scope": "global_all_scopes",
            "latest_window_end": latest_window_end,
            "latest_serving_write_at": latest_serving_write_at,
            "lag": {"status": "unknown", "meaning": "not_measured"},
            "limitations": "Serving-row observations only; not pipeline/source freshness or per-scope freshness. The two maxima are independent and may describe different rows.",
        },
    }
    for name in ("outbox", "repair"):
        payload["operational_domains"][name] = payload["pending_work"][name]["status"]
    if query == "available":
        size = (telemetry or {}).get("storage", {}).get("database_size", {})
        payload["operational_domains"]["database_storage"] = (
            "observed" if size.get("status") == "observed" else "unavailable")
        # A domain is observed when its telemetry was read (a value or none recorded), unavailable
        # when the read failed, and stays unsupported while it is not measured at all.
        for domain, section in (("spark_checkpoint", "spark_progress"), ("disk", "disk"),
                                ("maintenance", "maintenance"), ("rates", "rates")):
            state = ((telemetry or {}).get(section) or {}).get("status")
            if state in ("observed", "none_recorded"):
                payload["operational_domains"][domain] = "observed"
            elif state == "unavailable" or telemetry is None:
                payload["operational_domains"][domain] = "unavailable"
        payload["telemetry"] = telemetry if telemetry is not None else {"status": "unavailable"}
    if query == "available":
        payload.update(feature_rows=feature_rows, windows=windows,
                       served_data=_served_view(groups))
    return payload


def _setup_read_transaction(connection):
    """Enforce read-only mode before any application SQL in this transaction."""
    connection.execute("SET TRANSACTION READ ONLY")
    connection.execute("SELECT set_config('statement_timeout', %s, true)", (STATEMENT_TIMEOUT,))


def _encode_cursor(filters, key):
    raw = json.dumps({"v": 1, "f": filters, "k": key}, separators=(",", ":"),
                     ensure_ascii=False).encode("utf-8")
    cursor = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    if len(cursor) > MAX_CURSOR_LENGTH:
        raise ValueError("cursor is too large")
    return cursor


def _decode_cursor(cursor, filters):
    if (type(cursor) is not str or not cursor or len(cursor) > MAX_CURSOR_LENGTH
            or re.fullmatch(r"[A-Za-z0-9_-]+", cursor) is None):
        raise ValueError("invalid cursor")
    try:
        raw = base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        if base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != cursor:
            raise ValueError("invalid cursor")
        payload = json.loads(raw.decode("utf-8"))
        if (json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8") != raw
                or type(payload) is not dict or set(payload) != {"v", "f", "k"}
                or type(payload["v"]) is not int or payload["v"] != 1
                or type(payload["f"]) is not dict or set(payload["f"]) != set(filters)
                or any(type(payload["f"][name]) is not type(value)
                       for name, value in filters.items())
                or payload["f"] != filters or type(payload["k"]) is not list
                or len(payload["k"]) != 4):
            raise ValueError("invalid cursor")
        network, start, end, token = payload["k"]
        if (type(network) is not str or network not in ("mainnet", "testnet")
                or type(start) is not int or type(end) is not int or start < 0
                or end > 9223372036854775807
                or end != start + 300 or type(token) is not str or not token):
            raise ValueError("invalid cursor")
        for name, value in zip(("network", "window_start", "window_end", "token"),
                               (network, start, end, token)):
            if filters[name] is not None and filters[name] != value:
                raise ValueError("cursor key conflicts with filters")
        return network, start, end, token
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError, RecursionError) as error:
        raise ValueError("invalid cursor") from error


def _respond(start_response, status, payload, extra=()):
    body = json.dumps(redact(payload), separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    headers = [("Content-Type", "application/json; charset=utf-8"),
               ("Content-Length", str(len(body))), ("Cache-Control", "no-store"), *extra]
    start_response(status, headers)
    return [body]
