"""GET /metrics rendering, label bounds, cardinality, redaction, omission rules (fake DB)."""

import io
import re
import threading
import time
from datetime import datetime, timezone
from unittest.mock import patch

from ton_sentinel import api
from ton_sentinel.db import serving

PASSWORD = "s3cr3tpw-DO-NOT-LEAK"
DSN = f"postgresql://sentinel_reader:{PASSWORD}@db.internal:5432/feasibility"

HEALTHY_FAMILIES = {
    "http_requests_total", "http_request_duration_seconds", "http_errors_total",
    "process_start_time_seconds", "build_info", "source_up", "db_probe_duration_seconds",
    "collector_age_seconds", "serving_feature_rows", "serving_windows",
    "serving_latest_window_end_timestamp_seconds", "serving_latest_write_timestamp_seconds",
    "outbox_pending", "outbox_oldest_pending_age_seconds", "repair_pending",
    "database_size_bytes", "table_size_bytes", "last_activity_timestamp_seconds", "served_rows",
    "pipeline_backlog", "quality_issues",
}
DB_GAUGES = HEALTHY_FAMILIES - {
    "http_requests_total", "http_request_duration_seconds", "http_errors_total",
    "process_start_time_seconds", "build_info", "source_up", "db_probe_duration_seconds",
    "collector_age_seconds"}
FORBIDDEN_LABELS = {"token", "address", "url", "cursor", "owner", "dsn", "error_text",
                    "exception", "path", "query", "lease_owner", "message"}


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _Conn:
    def __init__(self, routes, log):
        self.routes, self.log = routes, log

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        self.log.append((statement, parameters))
        for needle, rows in self.routes.items():
            if needle in statement:
                if rows is Exception:
                    raise RuntimeError(f"denied {DSN}")
                return _Result(rows)
        return _Result([])


ROUTES = {
    "count(DISTINCT (network": [(7, 3, 1_700_000_300,
                                 datetime(2026, 11, 15, 10, 0, tzinfo=timezone.utc))],
    "GROUP BY 1, 2, 3": [("provisional", True, "bounded_observed", 4),
                         ("weird_status_xyz", False, None, 2)],
    "FROM outbox WHERE published_at IS NULL": [(5, 12.5)],
    "FROM dirty_windows WHERE status = 'pending'": [(3,)],
}


def _obs(value=None, **extra):
    return dict({"status": "observed", "value": value}, **extra)


def _telemetry():
    return {
        "storage": {
            "database_size": {"status": "observed", "bytes": 123456},
            "largest_tables": _obs([{"table": "outbox", "total_bytes": 4096},
                                    {"table": "evil_table_EQabc", "total_bytes": 1}])},
        "last_recorded_activity": {
            "canonical_actions": {"status": "unavailable", "reason": "table_not_in_reader_allowlist",
                                  "at": None},
            "outbox_created": {"status": "observed", "at": "2026-11-15T10:00:00Z"},
            "outbox_published": {"status": "none_recorded", "at": None},
            "stream_commit": {"status": "observed", "at": "2026-11-15T10:02:00Z"}},
    }


def _pipeline(network, **_):
    return {"scoring_backlog": _obs({"unscored_served_windows": _obs(4),
                                     "stale_results": _obs(2, oldest_age_seconds=1.0)})}


def _quality(network, **_):
    return {"revision_quarantine_open": _obs(1), "delivery_quarantine": _obs(2),
            "dirty_windows_pending_by_role": _obs({"a": 1, "b": 2}),
            "scan_quarantine": {"status": "unavailable", "value": None}}


class Harness:
    def __init__(self, routes=None, telemetry=None, pipeline=_pipeline, quality=_quality,
                 connect_error=False, delay=0.0):
        self.routes = dict(ROUTES if routes is None else routes)
        self.log, self.connects = [], []
        self.telemetry, self.pipeline, self.quality = telemetry or _telemetry(), pipeline, quality
        self.connect_error, self.delay = connect_error, delay

    def connect(self, *args, **kwargs):
        self.connects.append((args, kwargs))
        time.sleep(self.delay)
        if self.connect_error:
            raise RuntimeError(f"cannot connect {DSN}")
        return _Conn(self.routes, self.log)

    def __enter__(self):
        self.patches = [
            patch("ton_sentinel.api.psycopg.connect", side_effect=self.connect),
            patch("ton_sentinel.api.serving_read_telemetry",
                  side_effect=lambda c: self._call(self.telemetry)),
            patch("ton_sentinel.api.read_pipeline", side_effect=lambda c, **k: self._call(
                self.pipeline, **k)),
            patch("ton_sentinel.api.read_quality", side_effect=lambda c, **k: self._call(
                self.quality, **k)),
        ]
        for p in self.patches:
            p.start()
        return self

    def __exit__(self, *_):
        for p in self.patches:
            p.stop()

    @staticmethod
    def _call(source, **kwargs):
        if isinstance(source, Exception):
            raise source
        return source(**kwargs) if callable(source) else source


def call(app, path="/metrics", method="GET", query=""):
    captured = {}

    def start_response(status, headers, *_):
        captured["status"], captured["headers"] = status, dict(headers)

    environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": query,
               "wsgi.input": io.BytesIO(b"")}
    body = b"".join(app(environ, start_response)).decode("utf-8")
    return int(captured["status"][:3]), captured["headers"], body


_SAMPLE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})? (-?[0-9][0-9.eE+-]*|NaN|\+Inf)$')
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\\n]|\\["\\n])*)"(,|$)')


def parse(text):
    """Strict text format 0.0.4 parser: returns (types, helps, samples[(name, labels, value)])."""
    assert text.endswith("\n") and "\r" not in text
    types, helps, samples = {}, {}, []
    for line in text.split("\n")[:-1]:
        assert line, "blank line"
        if line.startswith("# HELP "):
            name, _, doc = line[7:].partition(" ")
            assert name not in helps and doc
            helps[name] = doc
        elif line.startswith("# TYPE "):
            name, kind = line[7:].split(" ")
            assert name not in types and kind in ("counter", "gauge", "histogram")
            types[name] = kind
        else:
            match = _SAMPLE.match(line)
            assert match, f"unparsable sample: {line!r}"
            labels, rest = {}, match.group(3) or ""
            while rest:
                lm = _LABEL.match(rest)
                assert lm, f"bad labels: {line!r}"
                assert lm.group(1) not in labels
                labels[lm.group(1)] = lm.group(2)
                rest = rest[lm.end():]
            samples.append((match.group(1), labels, float(match.group(4))))
    return types, helps, samples


def family_of(sample_name, types):
    for suffix in ("_bucket", "_sum", "_count"):
        if sample_name.endswith(suffix) and sample_name[:-len(suffix)] in types:
            return sample_name[:-len(suffix)]
    return sample_name


def scraped(h, warm=("/health", "/nope")):
    app = api.create_app(dsn=DSN)
    for path in warm:
        call(app, path)
    status, headers, body = call(app)
    assert status == 200, body
    return app, headers, body, parse(body)


def values(samples, name, **labels):
    return [v for n, ls, v in samples if n == name and all(ls.get(k) == x for k, x in labels.items())]


def test_exposition_is_strict_text_format_with_catalogue_names_and_types():
    with Harness() as h:
        _, headers, body, (types, helps, samples) = scraped(h)
    assert headers["Content-Type"] == "text/plain; version=0.0.4; charset=utf-8"
    assert headers["Cache-Control"] == "no-store"
    assert {n.removeprefix("ton_sentinel_") for n in types} == HEALTHY_FAMILIES
    assert set(types) == set(helps)
    assert all(n.startswith("ton_sentinel_") for n in types)
    for name, kind in types.items():
        expected = ("counter" if name.endswith("_total") else
                    "histogram" if name.endswith("_seconds") and "duration" in name
                    and "probe" not in name else "gauge")
        assert kind == expected, name
    for name, _, _ in samples:
        assert family_of(name, types) in types, name


def test_label_values_are_closed_enums_and_no_forbidden_label_keys():
    with Harness() as h:
        app = api.create_app(dsn=DSN)
        for path in ("/health", "/status", "/", "/v1/features", "/v1/tokens", "/v1/zzz",
                     "/dashboard/", "/dashboard/assets/x.js", "/EQ" + "A" * 46, "/metrics"):
            call(app, path)
        call(app, "/status", method="POST")
        call(app, "/v1/features", query="token=EQ" + "B" * 46 + "&cursor=abc")
        _, _, body = call(app)
    types, _, samples = parse(body)
    allowed = {
        "route": api.METRICS_ROUTES, "method": {"GET", "other"},
        "status_class": {"2xx", "4xx", "5xx"}, "error": set(api.METRICS_ERRORS),
        "source": {"database", "status_query", "telemetry", "pipeline"},
        "network": {"mainnet", "testnet"}, "kind": set(api.METRICS_QUALITY_KINDS),
        "activity": {"outbox_created", "outbox_published", "stream_commit"},
        "table": set(serving.READER_TABLES), "le": None, "version": None,
        "schema_migration": None, "status": None, "coverage_status": None,
        "provisional": {"true", "false"},
    }
    for name, labels, _ in samples:
        assert not FORBIDDEN_LABELS & set(labels), name
        for key, value in labels.items():
            assert key in allowed, (name, key)
            if allowed[key] is not None:
                assert value in allowed[key], (name, key, value)
    routes = {ls["route"] for n, ls, _ in samples if "route" in ls}
    assert "other" in routes and "/v1/zzz" not in routes and "/dashboard" in routes
    assert not any(v.startswith("EQ") for _, ls, _ in samples for v in ls.values())
    assert sum(values(samples, "ton_sentinel_http_requests_total", route="/dashboard")) == 2.0
    assert values(samples, "ton_sentinel_http_requests_total", method="other") == [1.0]
    assert any(n == "ton_sentinel_served_rows" and ls["status"] == "other" for n, ls, _ in samples)


def test_http_counters_and_error_enum_use_body_code_not_message():
    with Harness() as h:
        app = api.create_app(dsn=DSN)
        call(app, "/health")
        call(app, "/nope")
        call(app, "/status", method="POST")
        call(app, "/v1/features", query="network=moon")
        _, _, body = call(app)
    _, _, samples = parse(body)
    assert values(samples, "ton_sentinel_http_errors_total", error="not_found") == [1.0]
    assert values(samples, "ton_sentinel_http_errors_total", error="read_only") == [1.0]
    assert values(samples, "ton_sentinel_http_errors_total", error="invalid_network") == [1.0]
    assert values(samples, "ton_sentinel_http_requests_total", route="/health",
                  status_class="2xx") == [1.0]
    assert values(samples, "ton_sentinel_http_requests_total", route="other",
                  status_class="4xx") == [1.0]


def test_histogram_buckets_are_fixed_monotonic_and_consistent():
    with Harness() as h:
        app = api.create_app(dsn=DSN)
        for _ in range(3):
            call(app, "/health")
        _, _, body = call(app)
    types, _, samples = parse(body)
    name = "ton_sentinel_http_request_duration_seconds"
    assert types[name] == "histogram"
    buckets = [(ls["le"], v) for n, ls, v in samples
               if n == name + "_bucket" and ls["route"] == "/health"]
    assert [le for le, _ in buckets] == ["0.01", "0.05", "0.1", "0.25", "0.5", "1", "2", "3.5",
                                         "5", "+Inf"]
    counts = [v for _, v in buckets]
    assert counts == sorted(counts)
    assert counts[-1] == values(samples, name + "_count", route="/health")[0] == 3.0
    assert values(samples, name + "_sum", route="/health")[0] >= 0


def test_series_budget_holds_in_the_worst_case():
    with Harness(routes=dict(ROUTES, **{"GROUP BY 1, 2, 3": [
            (f"s{i}", bool(i % 2), f"c{i}", 1) for i in range(60)]})) as h:
        app = api.create_app(dsn=DSN)
        registry = app.metrics_registry
        for route in api.METRICS_ROUTES:
            for method in ("GET", "other"):
                for code in (200, 404, 503):
                    registry.observe(route, method, code, None, 0.2)
        for error in api.METRICS_ERRORS:
            registry.observe("/status", "GET", 400, error, 0.2)
        status, _, body = call(app)
    assert status == 200
    _, _, samples = parse(body)
    assert len(samples) <= api.METRICS_SERIES_BUDGET == 200
    assert len(samples) > 100
    assert len(body.encode()) < api.METRICS_MAX_BYTES


def test_oversized_response_fails_visibly():
    with Harness() as h, patch("ton_sentinel.api.METRICS_MAX_BYTES", 50):
        status, _, body = call(api.create_app(dsn=DSN))
    assert status == 422 and '"response_too_large"' in body


def test_output_contains_no_dsn_password_url_or_address():
    err = RuntimeError(f"boom {DSN} token=abc")
    with Harness(telemetry=err, pipeline=err, quality=err) as h:
        app = api.create_app(dsn=DSN)
        call(app, "/v1/tokens", query="token=EQ" + "C" * 46)
        _, _, body = call(app)
    for needle in (PASSWORD, "postgresql://", "db.internal", "sentinel_reader", "boom",
                   "evil_table", "token=", "EQ" + "C" * 10):
        assert needle not in body
    assert not re.search(r"\b[EU]Q[A-Za-z0-9_-]{46}\b", body)
    from ton_sentinel.redaction import redact
    assert redact(body) == body


def test_unreadable_outbox_is_omitted_and_source_marked_down_never_zero():
    with Harness(routes=dict(ROUTES, **{"FROM outbox WHERE published_at IS NULL": Exception})) as h:
        _, _, body, (types, _, samples) = scraped(h)
    names = {n for n, _, _ in samples}
    assert "ton_sentinel_outbox_pending" not in names
    assert "ton_sentinel_outbox_oldest_pending_age_seconds" not in names
    assert "ton_sentinel_outbox_pending" not in types
    assert values(samples, "ton_sentinel_source_up", source="status_query") == [0.0]
    assert values(samples, "ton_sentinel_source_up", source="database") == [1.0]
    assert values(samples, "ton_sentinel_repair_pending") == [3.0]
    assert values(samples, "ton_sentinel_serving_feature_rows") == [7.0]


def test_unreadable_database_omits_every_database_gauge():
    with Harness(connect_error=True) as h:
        app = api.create_app(dsn=DSN)
        status, _, body = call(app)
    assert status == 200
    types, _, samples = parse(body)
    stripped = {n.removeprefix("ton_sentinel_") for n in types}
    assert not (DB_GAUGES & stripped), DB_GAUGES & stripped
    for source in ("database", "status_query", "telemetry", "pipeline"):
        assert values(samples, "ton_sentinel_source_up", source=source) == [0.0]
    assert values(samples, "ton_sentinel_process_start_time_seconds")[0] > 1e9
    assert PASSWORD not in body


def test_conditional_families_follow_reader_values():
    def pipeline(network, **_):
        if network == "testnet":
            raise RuntimeError("down")
        return _pipeline(network)
    with Harness(pipeline=pipeline) as h:
        _, _, body, (types, _, samples) = scraped(h)
    assert values(samples, "ton_sentinel_pipeline_backlog", network="mainnet") == [6.0]
    assert values(samples, "ton_sentinel_pipeline_backlog", network="testnet") == []
    assert values(samples, "ton_sentinel_source_up", source="pipeline") == [0.0]
    assert values(samples, "ton_sentinel_quality_issues", network="mainnet",
                  kind="dirty_windows_pending") == [3.0]
    assert values(samples, "ton_sentinel_quality_issues", network="mainnet",
                  kind="delivery_quarantine") == [2.0]
    assert "ton_sentinel_coverage_ratio" not in body


def test_unobserved_pipeline_value_is_omitted():
    def pipeline(network, **_):
        return {"scoring_backlog": _obs({"unscored_served_windows": _obs(4),
                                         "stale_results": {"status": "unavailable",
                                                           "value": None}})}
    with Harness(pipeline=pipeline) as h:
        _, _, body, (types, _, samples) = scraped(h)
    assert "ton_sentinel_pipeline_backlog" not in types


def test_values_come_from_the_database_snapshot():
    with Harness() as h:
        _, _, body, (types, _, samples) = scraped(h)
    assert values(samples, "ton_sentinel_serving_windows") == [3.0]
    assert values(samples, "ton_sentinel_serving_latest_window_end_timestamp_seconds") == [1700000300.0]
    assert values(samples, "ton_sentinel_serving_latest_write_timestamp_seconds") == [
        datetime(2026, 11, 15, 10, 0, tzinfo=timezone.utc).timestamp()]
    assert values(samples, "ton_sentinel_outbox_pending") == [5.0]
    assert values(samples, "ton_sentinel_outbox_oldest_pending_age_seconds") == [12.5]
    assert values(samples, "ton_sentinel_database_size_bytes") == [123456.0]
    assert values(samples, "ton_sentinel_table_size_bytes", table="outbox") == [4096.0]
    assert len(values(samples, "ton_sentinel_table_size_bytes")) == 1
    assert values(samples, "ton_sentinel_last_activity_timestamp_seconds",
                  activity="outbox_published") == []
    assert values(samples, "ton_sentinel_last_activity_timestamp_seconds",
                  activity="stream_commit") == [
        datetime(2026, 11, 15, 10, 2, tzinfo=timezone.utc).timestamp()]
    assert all(ls["status"] != "x" for n, ls, _ in samples if n == "ton_sentinel_served_rows")
    assert sum(values(samples, "ton_sentinel_served_rows")) == 6.0
    assert len(values(samples, "ton_sentinel_build_info")) == 1


def test_collector_is_cached_for_15_seconds_with_one_read_only_transaction():
    clock = [1000.0]
    with Harness() as h, patch("ton_sentinel.api._monotonic", side_effect=lambda: clock[0]):
        app = api.create_app(dsn=DSN)
        for _ in range(5):
            call(app)
        assert len(h.connects) == 1
        clock[0] += 14.0
        call(app)
        assert len(h.connects) == 1
        clock[0] += 2.0
        _, _, body = call(app)
        assert len(h.connects) == 2
    assert values(parse(body)[2], "ton_sentinel_collector_age_seconds") == [0.0]
    assert h.connects[0][1] == {"connect_timeout": 3}
    first = [s for s, _ in h.log[:2]]
    assert first[0] == "SET TRANSACTION READ ONLY" and "set_config" in first[1]
    assert ("3000ms",) in [p for _, p in h.log if p]
    assert not any(re.match(r"\s*(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE)", s, re.I)
                   for s, _ in h.log)


def test_concurrent_scrapes_share_one_refresh():
    with Harness(delay=0.2) as h:
        app = api.create_app(dsn=DSN)
        results = []
        threads = [threading.Thread(target=lambda: results.append(call(app)[0]))
                   for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    assert results == [200] * 6 and len(h.connects) == 1


def test_existing_routes_keep_their_contract():
    with Harness() as h:
        app = api.create_app(dsn=DSN)
        assert call(app, "/health")[0] == 200
        assert call(app, "/nope")[0] == 404
        status, headers, body = call(app, "/metrics", method="POST")
        assert status == 405 and headers["Allow"] == "GET"
        assert call(app, "/status")[0] == 200
