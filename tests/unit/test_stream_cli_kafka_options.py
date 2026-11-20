"""Unit tests for the TLS passthrough options of the bounded Kafka->Spark consumer."""

import pytest

from ton_sentinel import stream_cli
from ton_sentinel.stream_cli import ConsumerConfig


def _config(**kw):
    base = dict(dsn="postgresql://x", bootstrap_servers="kafka:9092", topic="ton.swap_events.v1",
                checkpoint="/var/sentinel/checkpoints/c", topic_incarnation="inc", replay_epoch=0,
                environment_id="e", query_id="q", pipeline_epoch="p")
    base.update(kw)
    return ConsumerConfig(**base)


def test_reader_options_default_has_no_security_options():
    opts = stream_cli.kafka_reader_options(_config())
    assert opts == {"kafka.bootstrap.servers": "kafka:9092", "subscribe": "ton.swap_events.v1",
                    "startingOffsets": "earliest", "failOnDataLoss": "true",
                    "maxOffsetsPerTrigger": "500"}


def test_reader_options_adds_prefixed_ssl_options():
    opts = stream_cli.kafka_reader_options(_config(kafka_options=(
        ("security.protocol", "SSL"), ("ssl.keystore.type", "PEM"))))
    assert opts["kafka.security.protocol"] == "SSL"
    assert opts["kafka.ssl.keystore.type"] == "PEM"
    assert opts["startingOffsets"] == "earliest"


@pytest.mark.parametrize("key", ["bootstrap.servers", "startingOffsets", "subscribe",
                                 "group.id", "ssl", "", "kafka.security.protocol"])
def test_validate_rejects_non_tls_option_keys(key):
    with pytest.raises(ValueError, match="kafka option"):
        _config(kafka_options=((key, "x"),)).validate()


def test_validate_rejects_non_text_values():
    with pytest.raises(ValueError, match="kafka option"):
        _config(kafka_options=(("security.protocol", 1),)).validate()


def test_cli_parses_repeated_kafka_option(monkeypatch):
    seen = []
    monkeypatch.setattr(stream_cli, "run_consumer", seen.append)
    rc = stream_cli.main([
        "--dsn", "postgresql://x", "--bootstrap-servers", "kafka:9092", "--topic", "t",
        "--checkpoint", "/var/c", "--topic-incarnation", "i", "--replay-epoch", "0",
        "--environment-id", "e", "--query-id", "q", "--pipeline-epoch", "p",
        "--kafka-option", "security.protocol=SSL", "--kafka-option", "ssl.truststore.type=PEM"])
    assert rc == 0
    assert seen[0].kafka_options == (("security.protocol", "SSL"), ("ssl.truststore.type", "PEM"))


def test_cli_rejects_malformed_kafka_option():
    with pytest.raises(SystemExit):
        stream_cli.main(["--kafka-option", "no-equals-sign"])
