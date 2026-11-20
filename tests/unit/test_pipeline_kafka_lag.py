"""G5b: /v1/pipeline kafka_lag uses the same telemetry probe as /status and /metrics."""

from unittest.mock import patch

from ton_sentinel import api, telemetry_probes
from tests.unit.test_dashboard_pipeline import Conn, PROV, call as _unused, pipeline_value  # noqa: F401
import json

PROGRESS = {"status": "observed", "provenance": "stream_batches", "value": [
    {"query_id": "q", "batch_id": 1, "topic": "t", "committed_at": "2026-11-15T10:00:00Z",
     "offsets_audited": True, "partitions": [{"partition": 0, "next_offset": 10}]}]}
TELEMETRY = {"spark_progress": PROGRESS,
             "kafka_lag": {"status": "not_measured", "value": None,
                           "reason": "broker_probe_not_configured"}}


def fetch(probes):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    pipeline = pipeline_value(kafka_lag={"status": "not_measured", "value": None,
                                         "reason": "kafka_not_queried_by_reader"})
    with patch("ton_sentinel.api.psycopg.connect", return_value=Conn()), \
         patch("ton_sentinel.api.read_pipeline", return_value=pipeline), \
         patch("ton_sentinel.api.serving_read_telemetry", return_value=dict(TELEMETRY)), \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        app = api.create_app(dsn="x", telemetry_probes=probes)
        body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/v1/pipeline",
                             "QUERY_STRING": "network=testnet"}, start_response))
    return out["status"], json.loads(body)


def test_observed_value_from_probe_with_provenance():
    probes = telemetry_probes.TelemetryProbes(kafka_bootstrap="kafka:9092",
                                              end_offsets=lambda topics: {"t": {0: 14}})
    status, body = fetch(probes)
    lag = body["pipeline"]["kafka_lag"]
    assert status == "200 OK" and lag["status"] == "observed"
    assert lag["value"][0]["total_lag_records"] == 4 and lag["provenance"]
    assert set(body["pipeline"]) >= {"outbox", "stream_progress", "ingestion_freshness"}


def test_broker_down_is_unavailable_with_reason_never_zero():
    def down(topics):
        raise OSError("down")
    status, body = fetch(telemetry_probes.TelemetryProbes(kafka_bootstrap="kafka:9092",
                                                          end_offsets=down))
    lag = body["pipeline"]["kafka_lag"]
    assert status == "200 OK" and lag["status"] == "unavailable"
    assert lag["reason"] == "broker_unreachable" and lag["value"] is None and lag["provenance"]


def test_not_measured_only_when_probe_not_configured():
    status, body = fetch(None)
    lag = body["pipeline"]["kafka_lag"]
    assert lag["status"] == "not_measured" and lag["value"] is None and lag["reason"]
    status, body = fetch(telemetry_probes.TelemetryProbes())
    assert body["pipeline"]["kafka_lag"]["status"] == "not_measured"


def test_telemetry_failure_is_unavailable_not_zero():
    with patch("ton_sentinel.api._safe_telemetry", return_value=None):
        status, body = fetch(telemetry_probes.TelemetryProbes(kafka_bootstrap="kafka:9092"))
    lag = body["pipeline"]["kafka_lag"]
    assert lag["status"] == "unavailable" and lag["value"] is None and lag["reason"]


def test_safety_filter_applies():
    probes = telemetry_probes.TelemetryProbes(kafka_bootstrap="kafka:9092",
                                              end_offsets=lambda topics: {"t": {0: 14}})
    real = api.dashboard_payload.ensure_safe
    with patch("ton_sentinel.api.dashboard_payload.ensure_safe", side_effect=real) as spy:
        status, body = fetch(probes)
    assert status == "200 OK"
    assert any("kafka_lag" in call.args[0] for call in spy.call_args_list
               if isinstance(call.args[0], dict))
