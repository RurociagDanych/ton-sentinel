"""The local Kafka healthcheck must not start a JVM inside the broker's CPU quota.

The broker runs with ``cpus: 0.5``. A healthcheck that runs a Kafka CLI tool
(``/opt/kafka/bin/kafka-*.sh``) starts a full JVM every interval: about 3.5 CPU-seconds and
11 s wall time per check, so the 8 s timeout is also exceeded. That burst takes most of the
quota. The broker is then throttled in about 63 % of CFS periods even when idle. Its KRaft
heartbeat to its own controller times out (2 s), the controller fences the broker, and every
partition loses its leader. A producer that is publishing at that moment gets
``NotLeaderForPartitionError`` (seen in the full integration run). A TCP probe of the
client listener costs no JVM. The listener only accepts connections after broker startup, so
the probe still gates readiness.
"""
from pathlib import Path

import yaml

COMPOSE = Path(__file__).resolve().parents[2] / "deploy" / "compose.yaml"


def _kafka():
    return yaml.safe_load(COMPOSE.read_text())["services"]["kafka"]


def test_kafka_healthcheck_starts_no_jvm_inside_the_broker_quota():
    check = " ".join(_kafka()["healthcheck"]["test"])
    assert "/opt/kafka/bin/" not in check and "java" not in check
    assert "nc -z" in check and "9092" in check


def test_kafka_healthcheck_stays_bounded_and_the_quota_is_unchanged():
    kafka = _kafka()
    health = kafka["healthcheck"]
    assert kafka["cpus"] == 0.5 and kafka["mem_limit"] == "1g"     # owner-set local ceilings
    assert health["retries"] * int(health["interval"].rstrip("s")) >= 120  # same startup budget
