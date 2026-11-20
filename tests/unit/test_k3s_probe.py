"""Focused unit tests for the bounded local-K3s probe entrypoints (no services needed)."""

import json

import pytest

from ton_sentinel import k3s_probe


class FakeFuture:
    def get(self, timeout=None):
        return object()


class FakeProducer:
    def __init__(self):
        self.sent = []
        self.flushed = False

    def send(self, topic, value=None, key=None):
        self.sent.append((topic, key, value))
        return FakeFuture()

    def flush(self, timeout=None):
        self.flushed = True


class Msg:
    def __init__(self, key, value):
        self.key, self.value = key, value


class FakeConsumer:
    def __init__(self, batches):
        self.batches = list(batches)

    def poll(self, timeout_ms=0):
        return {"tp": self.batches.pop(0)} if self.batches else {}


def test_kafka_roundtrip_returns_records_in_order():
    producer = FakeProducer()
    records = [("k1", {"n": 1}), ("k2", {"n": 2})]
    consumer = FakeConsumer([[Msg(b"k1", json.dumps({"n": 1}).encode())],
                             [Msg(b"k2", json.dumps({"n": 2}).encode())]])
    got = k3s_probe.kafka_roundtrip(producer, consumer, "t", records, deadline_s=5)
    assert got == records and producer.flushed
    assert [s[0] for s in producer.sent] == ["t", "t"]


def test_kafka_roundtrip_fails_when_records_missing():
    with pytest.raises(RuntimeError, match="round-trip"):
        k3s_probe.kafka_roundtrip(FakeProducer(), FakeConsumer([]), "t", [("k", {"n": 1})], deadline_s=0.2)


def test_kafka_client_config_requires_tls_material(monkeypatch):
    env = {"SENTINEL_KAFKA_BOOTSTRAP": "kafka:9092"}
    with pytest.raises(ValueError, match="SENTINEL_KAFKA_TLS_DIR"):
        k3s_probe.kafka_client_config(env)
    cfg = k3s_probe.kafka_client_config(env | {"SENTINEL_KAFKA_TLS_DIR": "/tls"})
    assert cfg["bootstrap_servers"] == "kafka:9092" and cfg["security_protocol"] == "SSL"
    assert cfg["ssl_cafile"] == "/tls/kafka-ca.crt" and cfg["ssl_check_hostname"] is True
    assert cfg["ssl_certfile"] == "/tls/kafka-client.crt" and cfg["ssl_keyfile"] == "/tls/kafka-client.key"


def test_postgres_roundtrip_uses_project_db_code(monkeypatch):
    calls = []
    monkeypatch.setattr(k3s_probe, "migrate", lambda c: calls.append("migrate"))
    monkeypatch.setattr(k3s_probe, "acquire_transition", lambda c, owner: calls.append("lease") or "LEASE")
    monkeypatch.setattr(k3s_probe, "replace_features",
                        lambda c, lease, rows: calls.append(("replace", lease, len(rows))) or len(rows))
    stored = [dict(r) for r in k3s_probe.sample_features()]
    monkeypatch.setattr(k3s_probe, "read_features", lambda c, **kw: calls.append("read") or stored[::-1])
    assert k3s_probe.postgres_roundtrip(object(), "probe") == stored[::-1]
    assert calls == ["migrate", "lease", ("replace", "LEASE", len(stored)), "read"]


def test_postgres_roundtrip_detects_mismatch(monkeypatch):
    monkeypatch.setattr(k3s_probe, "migrate", lambda c: None)
    monkeypatch.setattr(k3s_probe, "acquire_transition", lambda c, owner: "L")
    monkeypatch.setattr(k3s_probe, "replace_features", lambda c, lease, rows: len(rows))
    monkeypatch.setattr(k3s_probe, "read_features", lambda c, **kw: [])
    with pytest.raises(RuntimeError, match="read-back"):
        k3s_probe.postgres_roundtrip(object(), "probe")


def test_sample_actions_are_canonical_json():
    rows = k3s_probe.sample_actions()
    assert len(rows) >= 2 and all(json.loads(r)["identity"]["network"] == "testnet" for r in rows)


@pytest.mark.parametrize("role", ["ingestor", "api", "notifier", "spark"])
def test_main_marks_ready_only_after_role_success(role, tmp_path, monkeypatch):
    ready = tmp_path / "ready"
    monkeypatch.setenv("SENTINEL_READY_FILE", str(ready))
    monkeypatch.setitem(k3s_probe.ROLES, role, lambda env: {"ok": True})
    assert k3s_probe.main([role, "--once"]) == 0 and ready.exists()


def test_main_failure_does_not_mark_ready(tmp_path, monkeypatch):
    ready = tmp_path / "ready"
    monkeypatch.setenv("SENTINEL_READY_FILE", str(ready))

    def boom(env):
        raise RuntimeError("nope")
    monkeypatch.setitem(k3s_probe.ROLES, "ingestor", boom)
    assert k3s_probe.main(["ingestor", "--once"]) == 1 and not ready.exists()


def test_main_rejects_unknown_role():
    with pytest.raises(SystemExit) as exc:
        k3s_probe.main(["bogus", "--once"])
    assert exc.value.code == 2
