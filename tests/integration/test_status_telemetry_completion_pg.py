"""On real PostgreSQL and the real compose Kafka broker, over HTTP,
through sentinel_reader: Spark progress, Kafka lag, maintenance, rates and the volume probe."""
import hashlib
import json
import os
import threading
import uuid
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel.api import create_app
from ton_sentinel.telemetry_probes import TelemetryProbes
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401


def _bootstrap():
    value = os.environ.get("TON_SENTINEL_TEST_KAFKA_BOOTSTRAP")
    if not value:
        pytest.fail("TON_SENTINEL_TEST_KAFKA_BOOTSTRAP must point to the compose broker")
    return value


@pytest.fixture
def topic():
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    name = "probe_f1c_" + uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=_bootstrap(), client_id="f1c-test")
    admin.create_topics([NewTopic(name, num_partitions=2, replication_factor=1)])
    producer = KafkaProducer(bootstrap_servers=_bootstrap(), acks="all")
    for partition, count in ((0, 5), (1, 3)):
        for i in range(count):
            producer.send(name, key=f"k{i}".encode(), value=b"{}", partition=partition)
    producer.flush()
    producer.close()
    try:
        yield name
    finally:
        admin.delete_topics([name])
        admin.close()


def _serve(dsn, probes):
    server = make_server("127.0.0.1", 0, create_app(dsn=dsn, telemetry_probes=probes))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


def _get(base, path):
    with urlopen(base + path, timeout=15) as response:
        return response.status, response.read().decode("utf-8")


def _batch(db, *, query_id, batch_id, topic, offsets, records):
    db.execute(
        "INSERT INTO stream_batches (environment_id, query_id, pipeline_epoch, batch_id, topic, "
        "topic_incarnation, replay_epoch, offset_metadata, record_digest, record_count) "
        "VALUES ('f1c', %s, 'e1', %s, %s, 'inc', 0, %s::jsonb, %s, %s)",
        (query_id, batch_id, topic, json.dumps(offsets), hashlib.sha256(
            f"{query_id}{batch_id}".encode()).hexdigest(), records))


def _run(db, kind, network, done):
    digest = "sha256:" + hashlib.sha256(f"{kind}{network}{done}".encode()).hexdigest()
    db.execute(
        "INSERT INTO retention_runs (run_id, kind, network, now_at, cutoff, from_window, "
        "to_window, windows, plan_digest, counts_before, counts_after, deleted, lease_owner, "
        "lease_fence) VALUES (%s, %s, %s, 0, 0, 0, 300, '[]', %s, '{}', %s, %s, 'f1c', 1)",
        (digest, kind, network, digest, '{}' if done else None, '{}' if done else None))


def test_kafka_lag_equals_broker_end_minus_audited_position(db, reader_dsn, topic):  # noqa: F811
    entries = lambda p, last: [{"topic": topic, "partition": p, "offset": o}  # noqa: E731
                               for o in range(last + 1)]
    _batch(db, query_id="q-f1c", batch_id=0, topic=topic, offsets=entries(0, 1), records=2)
    _batch(db, query_id="q-f1c", batch_id=1, topic=topic,
           offsets=[{"topic": topic, "partition": 0, "offset": 2}] + entries(1, 2), records=4)
    db.commit()
    server, thread, base = _serve(reader_dsn, TelemetryProbes(kafka_bootstrap=_bootstrap()))
    try:
        status, body = _get(base, "/status")
        assert status == 200
        telemetry = json.loads(body)["telemetry"]
        progress = telemetry["spark_progress"]
        assert progress["status"] == "observed"
        assert progress["value"][0]["partitions"] == [{"partition": 0, "next_offset": 3},
                                                      {"partition": 1, "next_offset": 3}]
        lag = telemetry["kafka_lag"]
        assert lag["status"] == "observed"
        entry = lag["value"][0]
        assert entry["partitions"] == [
            {"partition": 0, "log_end_offset": 5, "next_offset": 3, "lag_records": 2},
            {"partition": 1, "log_end_offset": 3, "next_offset": 3, "lag_records": 0}]
        assert entry["total_lag_records"] == 2
        assert json.loads(body)["operational_domains"]["spark_checkpoint"] == "observed"
        status, metrics = _get(base, "/metrics")
        assert status == 200
        assert "\nton_sentinel_kafka_lag_records 2\n" in metrics
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_missing_topic_and_unreachable_broker_are_unavailable(db, reader_dsn):  # noqa: F811
    _batch(db, query_id="q-gone", batch_id=0, topic="probe_f1c_absent_" + uuid.uuid4().hex,
           offsets=[], records=0)
    db.commit()
    for probes, reason in ((TelemetryProbes(kafka_bootstrap=_bootstrap()), "no_measurable_query"),
                           (TelemetryProbes(kafka_bootstrap="127.0.0.1:1"), "broker_unreachable")):
        server, thread, base = _serve(reader_dsn, probes)
        try:
            lag = json.loads(_get(base, "/status")[1])["telemetry"]["kafka_lag"]
            assert lag["status"] == "unavailable" and lag["reason"] == reason
            assert lag["value"] is None
            metrics = _get(base, "/metrics")[1]
            assert "ton_sentinel_kafka_lag_records" not in metrics
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()


def test_maintenance_and_rates_equal_database_truth(db, reader_dsn):  # noqa: F811
    _run(db, "payload", "testnet", True)
    _run(db, "guards", "testnet", False)  # an open run is not a completed maintenance run
    _batch(db, query_id="q-r", batch_id=0, topic="t", offsets=None, records=7)
    _batch(db, query_id="q-r", batch_id=1, topic="t", offsets=None, records=5)
    db.commit()
    truth = db.execute(
        "SELECT to_char(max(recorded_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') "
        "FROM retention_runs WHERE counts_after IS NOT NULL").fetchone()[0]
    outbox = db.execute("SELECT count(*) FROM outbox WHERE created_at >= clock_timestamp() "
                        "- interval '3600 seconds'").fetchone()[0]
    db.commit()
    server, thread, base = _serve(reader_dsn, None)
    try:
        payload = json.loads(_get(base, "/status")[1])
        t = payload["telemetry"]
        assert t["maintenance"]["status"] == "observed"
        assert t["maintenance"]["value"] == [{"kind": "payload", "network": "testnet",
                                              "last_completed_at": truth, "completed_runs": 1}]
        assert t["rates"]["status"] == "observed"
        assert t["rates"]["value"] == {"stream_records": 12, "stream_batches": 2,
                                       "outbox_rows": outbox}
        progress = t["spark_progress"]["value"][0]
        assert progress["offsets_audited"] is False and progress["partitions"] is None
        assert t["kafka_lag"]["reason"] == "broker_probe_not_configured"
        assert t["disk"]["reason"] == "database_volume_not_mounted"
        assert t["spark_state"]["reason"] == "no_persistent_checkpoint_location_in_stack"
        assert t["ingestion_freshness"]["reason"] == "ingestion_clock_tables_not_granted_to_reader"
        domains = payload["operational_domains"]
        assert (domains["maintenance"], domains["rates"], domains["disk"]) == (
            "observed", "observed", "unsupported")
        metrics = _get(base, "/metrics")[1]
        assert 'ton_sentinel_last_maintenance_timestamp_seconds{run_kind="payload"}' in metrics
        assert 'run_kind="guards"' not in metrics
        assert 'ton_sentinel_recorded_rows_last_hour{series="stream_records"} 12' in metrics
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_revoked_retention_runs_is_unavailable_never_zero(db, reader_dsn):  # noqa: F811
    db.execute("REVOKE SELECT ON retention_runs FROM sentinel_reader")
    db.commit()
    try:
        server, thread, base = _serve(reader_dsn, None)
        try:
            payload = json.loads(_get(base, "/status")[1])
            maintenance = payload["telemetry"]["maintenance"]
            assert maintenance["status"] == "unavailable" and maintenance["value"] is None
            assert payload["operational_domains"]["maintenance"] == "unavailable"
            assert "last_maintenance_timestamp_seconds" not in _get(base, "/metrics")[1]
        finally:
            server.shutdown()
            thread.join(timeout=5)
            server.server_close()
    finally:
        db.execute("GRANT SELECT ON retention_runs TO sentinel_reader")
        db.commit()


def test_volume_probe_matches_statvfs_of_a_real_mount_and_refuses_a_plain_directory(
        db, reader_dsn, tmp_path):  # noqa: F811
    mount = "/workspace/src"  # read-only bind mount in the app container
    assert os.path.ismount(mount)
    server, thread, base = _serve(reader_dsn, TelemetryProbes(database_volume=mount))
    try:
        disk = json.loads(_get(base, "/status")[1])["telemetry"]["disk"]
        truth = os.statvfs(mount)
        assert disk["status"] == "observed"
        assert disk["total_bytes"] == truth.f_frsize * truth.f_blocks
        assert abs(disk["available_bytes"] - truth.f_frsize * truth.f_bavail) < 64 * 1024 * 1024
        assert "ton_sentinel_database_volume_size_bytes" in _get(base, "/metrics")[1]
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
    plain = str(tmp_path)
    assert not os.path.ismount(plain)
    server, thread, base = _serve(reader_dsn, TelemetryProbes(database_volume=plain))
    try:
        disk = json.loads(_get(base, "/status")[1])["telemetry"]["disk"]
        assert disk["status"] == "unavailable" and disk["reason"] == "path_is_not_a_mount"
        assert disk.get("total_bytes") is None
        assert "ton_sentinel_database_volume" not in _get(base, "/metrics")[1]
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
