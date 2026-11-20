"""The local Kafka healthcheck must not start a JVM inside the broker's CPU quota."""

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
    assert kafka["cpus"] == 0.5 and kafka["mem_limit"] == "1g"
    assert health["retries"] * int(health["interval"].rstrip("s")) >= 120
