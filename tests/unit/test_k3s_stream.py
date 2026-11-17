"""Focused unit tests for the K3s Kafka->Spark->PostgreSQL bounded stream role glue."""
import json
import os
import stat

import pytest

from ton_sentinel import k3s_probe


def test_stream_role_registered():
    assert "stream" in k3s_probe.ROLES


def test_stream_expected_rows_are_exact_raw_sums():
    first, final = k3s_probe.STREAM_EXPECTED_FIRST, k3s_probe.STREAM_EXPECTED_FINAL
    assert first == [("JETTON:CANONICAL", 1, None, "33"), ("TON", 1, "110", None)]
    assert final == [("JETTON:CANONICAL", 2, None, "96"), ("TON", 2, "320", None)]


def test_stream_source_action_is_synthetic_and_deterministic():
    a = k3s_probe.stream_source_action(11, 15)
    assert a["synthetic"] is True and a["network"] == "testnet" and a["action_end"] == 15
    assert a["input"]["raw_amount"] == "110" and a["output"]["raw_amount"] == "33"
    assert a == k3s_probe.stream_source_action(11, 15)


def test_fixed_pages_serves_one_action_page():
    pages = k3s_probe.FixedPages({"x": 1})
    assert pages.fetch_page(None, 0, 10, 0, 5) == [{"x": 1}]
    assert pages.fetch_page(None, 0, 10, 1, 5) == []


def test_stream_kafka_options_writes_private_combined_keystore(tmp_path):
    tls = tmp_path / "tls"
    tls.mkdir()
    (tls / "kafka-client.crt").write_text("CERT\n")
    (tls / "kafka-client.key").write_text("KEY\n")
    (tls / "kafka-ca.crt").write_text("CA\n")
    out = tmp_path / "out"
    out.mkdir()
    opts = k3s_probe.stream_kafka_options({"SENTINEL_KAFKA_TLS_DIR": str(tls)}, out)
    ks = out / "kafka-client-keystore.pem"
    assert ks.read_text() == "KEY\nCERT\n"
    assert stat.S_IMODE(ks.stat().st_mode) == 0o600
    assert dict(opts) == {
        "security.protocol": "SSL",
        "ssl.truststore.type": "PEM", "ssl.truststore.location": str(tls / "kafka-ca.crt"),
        "ssl.keystore.type": "PEM", "ssl.keystore.location": str(ks)}
    assert "KEY" not in json.dumps(opts)  # secrets never travel in options/argv


def test_stream_kafka_options_requires_tls_dir(tmp_path):
    with pytest.raises(ValueError, match="SENTINEL_KAFKA_TLS_DIR"):
        k3s_probe.stream_kafka_options({}, tmp_path)


def test_stream_cli_argv_uses_project_cli_and_tls_options():
    argv = k3s_probe.stream_cli_argv(
        {"SENTINEL_DATABASE_DSN": "postgresql://u:p@h/d", "SENTINEL_KAFKA_BOOTSTRAP": "k:9092"},
        [("security.protocol", "SSL")], incarnation="inc", checkpoint="/var/sentinel/checkpoints/s")
    assert argv[argv.index("--topic") + 1] == "ton.swap_events.v1"
    assert argv[argv.index("--bootstrap-servers") + 1] == "k:9092"
    assert argv[argv.index("--checkpoint") + 1] == "/var/sentinel/checkpoints/s"
    assert argv[argv.index("--kafka-option") + 1] == "security.protocol=SSL"
    assert "--dsn" not in argv  # DSN password stays out of argv; passed via TON_SENTINEL_DSN env


def test_check_stream_rows_rejects_mismatch():
    k3s_probe.check_stream_rows(list(k3s_probe.STREAM_EXPECTED_FINAL), k3s_probe.STREAM_EXPECTED_FINAL)
    with pytest.raises(RuntimeError, match="stream read-back mismatch"):
        k3s_probe.check_stream_rows([("TON", 1, "110", None)], k3s_probe.STREAM_EXPECTED_FINAL)
