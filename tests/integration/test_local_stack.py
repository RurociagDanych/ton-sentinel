"""Real local Kafka → stateful Spark → PostgreSQL connectivity evidence."""
from __future__ import annotations

import json
import platform
import uuid
from pathlib import Path

import psycopg
import pytest
from ton_sentinel.feasibility import local_dsn, run_probe


@pytest.mark.integration
def test_local_stack_real_roundtrip_and_readback(probe_cleanup):
    run_id = uuid.uuid4().hex
    runtime = Path("/runtime/local-stack") / run_id
    evidence = Path("/evidence") / run_id
    runtime.mkdir(parents=True, exist_ok=False)
    evidence.mkdir(parents=True, exist_ok=False)
    fixture = json.loads(Path("tests/fixtures/feasibility.json").read_text())
    settings = {
        "topic": f"probe_{run_id}",
        "schema": f"probe_{run_id}",
        "checkpoint": str(runtime / "checkpoint"),
        "evidence": str(evidence / "probe"),
        "fixture": fixture,
    }
    probe_cleanup(settings)                 # schema and topic are dropped after the test

    probe = run_probe(settings)
    with psycopg.connect(local_dsn()) as connection:
        server_version = connection.execute("SELECT version()").fetchone()[0]
        readback = connection.execute(
            f'SELECT token, window_start, window_end, swaps FROM "{settings["schema"]}".features '
            "ORDER BY token, window_start, window_end"
        ).fetchall()

    assert probe["pg_rows"] == fixture["expected_rows"]
    assert probe["restart_rows"] == fixture["expected_rows"]
    assert probe["idle_rows"] == fixture["expected_rows"]
    assert probe["state_operator_count"] >= 2
    assert len(probe["acknowledgements"]) == sum(map(len, fixture["phases"]))
    assert probe["correctness_checks"] == "true"
    spark_version = probe["phases"][0]["spark_version"]
    assert all(phase["spark_version"] == spark_version for phase in probe["phases"])
    assert [
        [token, start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z"), count]
        for token, start, end, count in readback
    ] == fixture["expected_rows"]

    evidence_payload = {
        "scope": "synthetic local Docker mechanics only; not K3s/PVC, ARM64, or target qualification",
        "architecture": platform.machine(),
        "spark_version": spark_version,
        "kafka_python_version": __import__("kafka").__version__,
        "postgres_server_version": server_version,
        "kafka_acknowledgements": probe["acknowledgements"],
        "state_operator_count": probe["state_operator_count"],
        "pg_rows": probe["pg_rows"],
        "restart_rows": probe["restart_rows"],
        "idle_rows": probe["idle_rows"],
        "watermark": probe["watermark"],
        "late_dropped": probe["late_dropped"],
        "correctness_checks": probe["correctness_checks"],
        "checkpoint_bytes": probe["checkpoint_bytes"],
        "probe_evidence_prefix": str(evidence),
    }
    (evidence / "readback.json").write_text(json.dumps(evidence_payload, indent=2) + "\n")
    print("LOCAL_STACK_EVIDENCE=" + json.dumps(evidence_payload, sort_keys=True))
