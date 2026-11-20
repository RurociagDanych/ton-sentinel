"""The api service gets the credential-free telemetry probes and nothing more."""

from pathlib import Path

import yaml

from ton_sentinel.telemetry_probes import KAFKA_ENV, VOLUME_ENV, TelemetryProbes

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
API = yaml.safe_load((DEPLOY / "compose.dashboard.yaml").read_text())["services"]["api"]
BASE = yaml.safe_load((DEPLOY / "compose.yaml").read_text())


def test_api_probe_environment_is_plaintext_internal_listener_and_volume_path():
    env = API["environment"]
    assert env[KAFKA_ENV] == "kafka:9092"
    assert "PLAINTEXT://kafka:9092" in BASE["services"]["kafka"]["environment"][
        "KAFKA_ADVERTISED_LISTENERS"]
    TelemetryProbes(kafka_bootstrap=env[KAFKA_ENV], database_volume=env[VOLUME_ENV])
    assert not [k for k in env if any(w in k.upper() for w in ("SASL", "SSL", "JAAS", "PASSWORD"))]


def test_database_volume_is_mounted_read_only_at_the_probe_path():
    mounts = [v for v in API.get("volumes", []) if str(v).startswith("pgdata:")]
    assert mounts == [f"pgdata:{API['environment'][VOLUME_ENV]}:ro"]
    assert "pgdata" in BASE["volumes"]


def test_api_stays_read_only_without_new_secrets_or_capabilities():
    assert API["read_only"] is True and API["cap_drop"] == ["ALL"]
    assert API["secrets"] == ["api_dsn"]
    assert "no-new-privileges:true" in API["security_opt"]
    assert set(API["networks"]) == {"fixture", "edge"}
