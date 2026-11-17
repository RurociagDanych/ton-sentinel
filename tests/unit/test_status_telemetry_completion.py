"""Kafka lag, Spark progress, database-volume space, maintenance,
rates and ingestion freshness in the /status telemetry block and /metrics (fake connection,
fake broker, fake statvfs). Unmeasured items stay explicit and are never zero."""
import json
import os
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ton_sentinel import api, telemetry_probes
from ton_sentinel.db import serving
from tests.unit.test_metrics_endpoint import ROUTES, Harness, call, parse, values
from tests.unit.test_status_telemetry import FULL, _Connection, _walk

PROGRESS = "DISTINCT ON (environment_id, query_id)"
MAINT = "FROM retention_runs"
RATES = "sum(record_count)"

PROGRESS_ROWS = [
    ("q-new", 7, "topic-a", "2026-11-15T10:02:00Z", True, {"0": 9, "1": 4}),
    ("q-old", 2, "topic-b", "2026-11-15T09:00:00Z", False, {"0": 1}),
]
MAINT_ROWS = [("guards", "mainnet", "2026-11-15T09:30:00Z", 2),
              ("payload", "mainnet", "2026-11-15T09:31:00Z", 3)]
RATES_ROWS = [(120, 4, 33, "2026-11-15T10:05:00Z")]


def _routes(**overrides):
    routes = {PROGRESS: PROGRESS_ROWS, MAINT: MAINT_ROWS, RATES: RATES_ROWS}
    routes.update(FULL)
    routes.update(overrides)
    return routes


def _telemetry(**overrides):
    return serving.read_telemetry(_Connection(_routes(**overrides)))


# --- PostgreSQL-derived sections -------------------------------------------------------------

def test_spark_progress_is_the_audited_position_per_query_with_provenance():
    progress = _telemetry()["spark_progress"]
    assert progress["status"] == "observed"
    assert "stream_batches" in progress["provenance"]
    assert "not the checkpoint directory" in progress["provenance"]
    first, second = progress["value"]
    assert first == {"query_id": "q-new", "batch_id": 7, "topic": "topic-a",
                     "committed_at": "2026-11-15T10:02:00Z", "offsets_audited": True,
                     "partitions": [{"partition": 0, "next_offset": 10},
                                    {"partition": 1, "next_offset": 5}]}
    assert second["offsets_audited"] is False and second["partitions"] is None


def test_spark_progress_empty_is_none_recorded_and_failure_is_unavailable():
    assert _telemetry(**{PROGRESS: []})["spark_progress"]["status"] == "none_recorded"
    failed = _telemetry(**{PROGRESS: Exception})["spark_progress"]
    assert failed["status"] == "unavailable" and failed["reason"] == "query_failed"
    assert failed["value"] is None


def test_spark_state_and_ingestion_freshness_stay_not_measured_with_reasons():
    t = _telemetry()
    assert t["spark_state"] == {"status": "not_measured", "value": None,
                                "reason": "no_persistent_checkpoint_location_in_stack",
                                "provenance": "not queried"}
    assert t["ingestion_freshness"] == {
        "status": "not_measured", "value": None,
        "reason": "ingestion_clock_tables_not_granted_to_reader", "provenance": "not queried"}


def test_unconfigured_probes_leave_kafka_lag_and_disk_not_measured():
    t = _telemetry()
    assert t["kafka_lag"] == {"status": "not_measured", "value": None,
                              "reason": "broker_probe_not_configured", "provenance": "not queried"}
    assert t["disk"] == {"status": "not_measured", "value": None,
                         "reason": "database_volume_not_mounted", "provenance": "not queried"}


def test_maintenance_is_last_completed_retention_run_per_kind_and_network():
    m = _telemetry()["maintenance"]
    assert m["status"] == "observed" and m["scope"] == "retention_cycle_only"
    assert m["not_covered"] == ["vacuum", "backup"]
    assert m["value"] == [
        {"kind": "guards", "network": "mainnet", "last_completed_at": "2026-11-15T09:30:00Z",
         "completed_runs": 2},
        {"kind": "payload", "network": "mainnet", "last_completed_at": "2026-11-15T09:31:00Z",
         "completed_runs": 3}]
    assert "retention_runs" in m["provenance"]
    assert _telemetry(**{MAINT: []})["maintenance"]["status"] == "none_recorded"
    failed = _telemetry(**{MAINT: Exception})["maintenance"]
    assert failed["status"] == "unavailable" and failed["value"] is None


def test_rates_are_trailing_window_counts_not_extrapolated():
    r = _telemetry()["rates"]
    assert r["status"] == "observed" and r["window_seconds"] == 3600
    assert r["as_of"] == "2026-11-15T10:05:00Z"
    assert r["value"] == {"stream_records": 120, "stream_batches": 4, "outbox_rows": 33}
    assert "not a per-second rate" in r["meaning"]
    failed = _telemetry(**{RATES: Exception})["rates"]
    assert failed["status"] == "unavailable" and failed["value"] is None
    malformed = _telemetry(**{RATES: [(None, "x")]})["rates"]
    assert malformed["status"] == "unavailable" and malformed["value"] is None


# --- probes: Kafka end offsets and database-volume statvfs ------------------------------------

class _Broker:
    def __init__(self, ends=None, error=None):
        self.ends, self.error, self.calls = ends or {}, error, []

    def __call__(self, topics):
        self.calls.append(tuple(topics))
        if self.error:
            raise self.error
        return {topic: self.ends.get(topic) for topic in topics}


def _probes(broker=None, volume=None, statvfs=None, ismount=lambda p: True):
    return telemetry_probes.TelemetryProbes(
        kafka_bootstrap="kafka:9092" if broker else None, database_volume=volume,
        end_offsets=broker, statvfs=statvfs, ismount=ismount)


def test_kafka_lag_is_log_end_minus_audited_next_offset():
    broker = _Broker({"topic-a": {0: 12, 1: 5}})
    lag = _probes(broker).apply(_telemetry())["kafka_lag"]
    assert broker.calls == [("topic-a",)]  # only audited queries are asked for
    assert lag["status"] == "observed"
    assert lag["semantics"] == "log_end_offset_minus_next_offset_after_newest_audited_record"
    assert "no consumer group" in lag["provenance"] and "no credentials" in lag["provenance"]
    new, old = lag["value"]
    assert new == {"query_id": "q-new", "topic": "topic-a", "status": "observed",
                   "total_lag_records": 2,
                   "partitions": [{"partition": 0, "log_end_offset": 12, "next_offset": 10,
                                   "lag_records": 2},
                                  {"partition": 1, "log_end_offset": 5, "next_offset": 5,
                                   "lag_records": 0}]}
    assert old == {"query_id": "q-old", "topic": "topic-b", "status": "unavailable",
                   "reason": "latest_batch_not_audited", "total_lag_records": None,
                   "partitions": None}


def test_partition_without_recorded_position_has_no_lag_and_no_total():
    lag = _probes(_Broker({"topic-a": {0: 12, 1: 5, 2: 3}})).apply(_telemetry())["kafka_lag"]
    entry = lag["value"][0]
    assert entry["total_lag_records"] is None
    assert entry["total_reason"] == "position_not_recorded_for_some_partitions"
    assert entry["partitions"][2] == {"partition": 2, "log_end_offset": 3, "next_offset": None,
                                      "lag_records": None}


def test_missing_topic_unreachable_broker_and_position_ahead_are_unavailable():
    gone = _probes(_Broker({})).apply(_telemetry())["kafka_lag"]
    assert gone["status"] == "unavailable" and gone["reason"] == "no_measurable_query"
    assert gone["value"] is None and gone["queries"][0]["reason"] == "topic_not_found"
    down = _probes(_Broker(error=RuntimeError("kafka:9092 password=x"))).apply(_telemetry())
    assert down["kafka_lag"] == {"status": "unavailable", "value": None,
                                 "reason": "broker_unreachable",
                                 "provenance": down["kafka_lag"]["provenance"]}
    assert "password" not in json.dumps(down)
    ahead = _probes(_Broker({"topic-a": {0: 3, 1: 5}})).apply(_telemetry())["kafka_lag"]
    assert ahead["status"] == "unavailable" and ahead["queries"][0]["status"] == "unavailable"
    assert ahead["queries"][0]["reason"] == "position_ahead_of_log_end"


def test_kafka_lag_follows_spark_progress_state():
    broker = _Broker({"topic-a": {0: 12, 1: 5}})
    none = _probes(broker).apply(_telemetry(**{PROGRESS: []}))["kafka_lag"]
    assert none["status"] == "none_recorded" and none["reason"] == "no_spark_batch_recorded"
    failed = _probes(broker).apply(_telemetry(**{PROGRESS: Exception}))["kafka_lag"]
    assert failed["status"] == "unavailable" and failed["reason"] == "consumer_position_unavailable"
    assert broker.calls == []


def test_disk_is_statvfs_of_the_mounted_database_volume():
    stat = SimpleNamespace(f_frsize=4096, f_blocks=1000, f_bfree=400, f_bavail=300)
    disk = _probes(volume="/telemetry/database-volume", statvfs=lambda p: stat).apply(
        _telemetry())["disk"]
    assert disk == {"status": "observed", "total_bytes": 4096000, "free_bytes": 1638400,
                    "available_bytes": 1228800, "scope": "filesystem_backing_database_volume",
                    "provenance": "statvfs on the read-only database volume mount"}


def test_disk_not_a_mount_or_failing_statvfs_is_unavailable_never_zero():
    def boom(path):
        raise OSError("denied /var/lib/postgresql")
    not_mount = _probes(volume="/x", statvfs=os.statvfs, ismount=lambda p: False).apply(
        _telemetry())["disk"]
    assert not_mount["status"] == "unavailable" and not_mount["reason"] == "path_is_not_a_mount"
    failed = _probes(volume="/x", statvfs=boom).apply(_telemetry())["disk"]
    assert failed["status"] == "unavailable" and failed["reason"] == "statvfs_failed"
    for part in (not_mount, failed):
        assert part.get("total_bytes") is None and part.get("available_bytes") is None
        assert "postgresql" not in json.dumps(part)


@pytest.mark.parametrize("bootstrap", [
    "user:pw@kafka:9092", "SASL_SSL://kafka:9093", "kafka", "kafka:9092;rm", "kafka:99999", ""])
def test_bootstrap_must_be_plain_host_port_without_credentials(bootstrap):
    with pytest.raises(ValueError):
        telemetry_probes.TelemetryProbes(kafka_bootstrap=bootstrap)


def test_probes_from_environ_is_explicit_opt_in():
    assert telemetry_probes.probes_from_environ({}) is None
    probes = telemetry_probes.probes_from_environ({
        "TON_SENTINEL_TELEMETRY_KAFKA_BOOTSTRAP": "kafka:9092",
        "TON_SENTINEL_TELEMETRY_DB_VOLUME": "/telemetry/database-volume"})
    assert probes.kafka_bootstrap == "kafka:9092"
    assert probes.database_volume == "/telemetry/database-volume"


def test_kafka_reader_uses_plaintext_no_group_no_commit():
    seen = {}

    class FakeConsumer:
        def __init__(self, **config):
            seen.update(config)

        def partitions_for_topic(self, topic):
            return {0, 1} if topic == "t" else None

        def end_offsets(self, partitions):
            return {p: 10 + p.partition for p in partitions}

        def close(self, **_):
            seen["closed"] = True

    with patch("kafka.KafkaConsumer", FakeConsumer):
        ends = telemetry_probes.kafka_end_offsets("kafka:9092", ["t", "gone"])
    assert ends == {"t": {0: 10, 1: 11}, "gone": None}
    assert seen["group_id"] is None and seen["enable_auto_commit"] is False
    assert seen["security_protocol"] == "PLAINTEXT" and seen["closed"] is True
    assert seen["allow_auto_create_topics"] is False
    assert not {k for k in seen if "sasl" in k or "ssl" in k}
    from kafka import KafkaConsumer as RealConsumer  # every option must be a real client option
    assert set(seen) - {"closed"} <= set(RealConsumer.DEFAULT_CONFIG)


def test_no_zero_for_unmeasured_states_with_probes_applied():
    t = _probes(_Broker(error=RuntimeError("x")), volume="/x",
                statvfs=lambda p: (_ for _ in ()).throw(OSError())).apply(
        serving.read_telemetry(_Connection({})))
    for node in _walk(t):
        if node.get("status") in ("not_measured", "unsupported", "unavailable", "none_recorded"):
            assert node.get("value") is None
            for key in ("bytes", "at", "total_bytes", "available_bytes", "total_lag_records"):
                assert node.get(key) is None


# --- /status and /metrics --------------------------------------------------------------------

def _status_with(probes, routes):
    harness_routes = dict(ROUTES, **routes)
    with patch("ton_sentinel.api.psycopg.connect",
               side_effect=lambda *a, **k: _Connection(harness_routes)):
        app = api.create_app(dsn="explicit-test-dsn", telemetry_probes=probes)
        status, _, body = call(app, "/status")
    return status, json.loads(body)


def test_status_domains_follow_measured_telemetry():
    stat = SimpleNamespace(f_frsize=1, f_blocks=10, f_bfree=5, f_bavail=4)
    probes = _probes(_Broker({"topic-a": {0: 12, 1: 5}}), volume="/v", statvfs=lambda p: stat)
    status, body = _status_with(probes, _routes())
    assert status == 200
    domains = body["operational_domains"]
    assert domains["spark_checkpoint"] == "observed"
    assert domains["disk"] == "observed"
    assert domains["maintenance"] == "observed" and domains["rates"] == "observed"
    assert domains["spark_watermark"] == "unsupported" and domains["spark_state"] == "unsupported"
    assert body["telemetry"]["kafka_lag"]["value"][0]["total_lag_records"] == 2
    assert body["telemetry"]["disk"]["available_bytes"] == 4


def test_status_without_probes_keeps_disk_unsupported_domain():
    status, body = _status_with(None, _routes())
    assert status == 200
    assert body["operational_domains"]["disk"] == "unsupported"
    assert body["telemetry"]["kafka_lag"]["reason"] == "broker_probe_not_configured"


def _full_telemetry():
    stat = SimpleNamespace(f_frsize=1, f_blocks=10, f_bfree=5, f_bavail=4)
    probes = _probes(_Broker({"topic-a": {0: 12, 1: 5}}), volume="/v", statvfs=lambda p: stat)
    return probes, probes.apply(serving.read_telemetry(_Connection(_routes())))


def test_metrics_export_measured_completion_families_with_closed_labels():
    probes, telemetry = _full_telemetry()
    with Harness(telemetry=telemetry):
        app = api.create_app(dsn="explicit-test-dsn", telemetry_probes=None)
        _, _, body = call(app)
    types, _, samples = parse(body)
    assert values(samples, "ton_sentinel_kafka_lag_records") == [2.0]
    assert values(samples, "ton_sentinel_database_volume_size_bytes") == [10.0]
    assert values(samples, "ton_sentinel_database_volume_available_bytes") == [4.0]
    maint = {ls["run_kind"]: v for n, ls, v in samples
             if n == "ton_sentinel_last_maintenance_timestamp_seconds"}
    assert set(maint) == {"guards", "payload"} and maint["payload"] == 1794735060.0
    rows = {ls["series"]: v for n, ls, v in samples if n == "ton_sentinel_recorded_rows_last_hour"}
    assert rows == {"stream_records": 120.0, "stream_batches": 4.0, "outbox_rows": 33.0}
    for name in ("kafka_lag_records", "database_volume_size_bytes",
                 "last_maintenance_timestamp_seconds", "recorded_rows_last_hour"):
        assert types["ton_sentinel_" + name] == "gauge"


def test_metrics_omit_unmeasured_completion_families_never_zero():
    telemetry = serving.read_telemetry(_Connection(_routes(**{
        PROGRESS: [], MAINT: Exception, RATES: Exception})))
    with Harness(telemetry=telemetry):
        _, _, body = call(api.create_app(dsn="explicit-test-dsn"))
    for name in ("kafka_lag_records", "database_volume_size_bytes",
                 "database_volume_available_bytes", "last_maintenance_timestamp_seconds",
                 "recorded_rows_last_hour"):
        assert "ton_sentinel_" + name not in body


def test_metrics_kafka_lag_is_omitted_when_the_total_is_incomplete():
    probes = _probes(_Broker({"topic-a": {0: 12, 1: 5, 2: 9}}))
    telemetry = probes.apply(serving.read_telemetry(_Connection(_routes())))
    with Harness(telemetry=telemetry):
        _, _, body = call(api.create_app(dsn="explicit-test-dsn"))
    assert "ton_sentinel_kafka_lag_records" not in body


def test_series_budget_holds_in_the_worst_case_with_completion_families():
    _, telemetry = _full_telemetry()
    telemetry["maintenance"]["value"] = [
        {"kind": k, "network": n, "last_completed_at": "2026-11-15T09:30:00Z",
         "completed_runs": 1} for k in ("payload", "guards", "derived")
        for n in ("mainnet", "testnet")]
    telemetry["storage"]["largest_tables"]["value"] = [
        {"table": t, "total_bytes": 1} for t in sorted(serving.READER_TABLES)[:5]]
    with Harness(routes=dict(ROUTES, **{"GROUP BY 1, 2, 3": [
            (f"s{i}", bool(i % 2), f"c{i}", 1) for i in range(60)]}), telemetry=telemetry):
        app = api.create_app(dsn="explicit-test-dsn")
        for route in api.METRICS_ROUTES:
            for method in ("GET", "other"):
                for code in (200, 404, 503):
                    app.metrics_registry.observe(route, method, code, None, 0.2)
        for error in api.METRICS_ERRORS:
            app.metrics_registry.observe("/status", "GET", 400, error, 0.2)
        status, _, body = call(app)
    assert status == 200
    _, _, samples = parse(body)
    assert len(samples) <= api.METRICS_SERIES_BUDGET == 200
    assert len([1 for n, _, _ in samples
                if n == "ton_sentinel_last_maintenance_timestamp_seconds"]) == 3


def test_cli_serve_passes_environment_probes_to_create_app(tmp_path, monkeypatch):
    from ton_sentinel import cli
    dsn_file = tmp_path / "dsn"
    dsn_file.write_text("postgresql://reader@db/x")
    monkeypatch.setenv("TON_SENTINEL_TELEMETRY_KAFKA_BOOTSTRAP", "kafka:9092")
    monkeypatch.delenv("TON_SENTINEL_TELEMETRY_DB_VOLUME", raising=False)
    captured = {}

    def fake_create_app(**kwargs):
        captured.update(kwargs)
        return lambda environ, start_response: []

    class _Server:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def serve_forever(self):
            return None

    monkeypatch.setattr(cli, "create_app", fake_create_app)
    monkeypatch.setattr(cli, "make_server", lambda *a, **k: _Server())
    monkeypatch.setattr(cli, "_configure_request_log", lambda: None)
    cli.main(["--dsn-file", str(dsn_file)])
    assert captured["telemetry_probes"].kafka_bootstrap == "kafka:9092"
    assert captured["telemetry_probes"].database_volume is None
