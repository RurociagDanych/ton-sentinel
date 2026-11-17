"""G5b: /v1/pipeline kafka_lag over HTTP through sentinel_reader, equal to /status."""
import json
import threading
from urllib.request import urlopen

from ton_sentinel.api import create_app
from ton_sentinel.telemetry_probes import TelemetryProbes
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401
from tests.integration.test_status_telemetry_completion_pg import (  # noqa: F401
    _batch, _bootstrap, _get, _serve, topic)


def test_pipeline_kafka_lag_equals_status_and_unavailable_when_broker_down(  # noqa: F811
        db, reader_dsn, topic):
    entries = lambda p, last: [{"topic": topic, "partition": p, "offset": o}  # noqa: E731
                               for o in range(last + 1)]
    _batch(db, query_id="q-g5b", batch_id=0, topic=topic,
           offsets=entries(0, 2) + entries(1, 2), records=6)
    db.commit()
    server, thread, base = _serve(reader_dsn, TelemetryProbes(kafka_bootstrap=_bootstrap()))
    try:
        status, body = _get(base, "/v1/pipeline?network=testnet")
        assert status == 200
        lag = json.loads(body)["pipeline"]["kafka_lag"]
        status_lag = json.loads(_get(base, "/status")[1])["telemetry"]["kafka_lag"]
        assert lag == status_lag and lag["status"] == "observed" and lag["provenance"]
        assert lag["value"][0]["total_lag_records"] == 2
        assert {"outbox", "stream_progress", "ingestion_freshness"} <= set(json.loads(body)["pipeline"])
    finally:
        server.shutdown(); thread.join(timeout=5); server.server_close()
    server, thread, base = _serve(reader_dsn, TelemetryProbes(kafka_bootstrap="127.0.0.1:1"))
    try:
        lag = json.loads(_get(base, "/v1/pipeline?network=testnet")[1])["pipeline"]["kafka_lag"]
        assert lag["status"] == "unavailable" and lag["reason"] == "broker_unreachable"
        assert lag["value"] is None
    finally:
        server.shutdown(); thread.join(timeout=5); server.server_close()
