"""The resumed Spark consumer lineage starts exactly at the cut's end offsets E."""

import json
from datetime import datetime, timezone

import pytest

from ton_sentinel import stream_cli
from ton_sentinel.contracts import Lease
from ton_sentinel.recovery import (RecoveryResumption, resumed_consumer_config,
                                   spark_starting_offsets)
from ton_sentinel.stream_cli import ConsumerConfig


def _config(**overrides):
    base = dict(dsn="postgresql://x", bootstrap_servers="kafka:9092", topic="canonical",
                checkpoint="/tmp/cp-resumed", topic_incarnation="inc", replay_epoch=0,
                environment_id="env", query_id="query", pipeline_epoch="epoch-a")
    base.update(overrides)
    return ConsumerConfig(**base)


def _resumption(**overrides):
    base = dict(cut_id="c" * 8, pause_id=1, recovery_epoch=1, pipeline_epoch="pe-resumed",
                lease=Lease("resumer", 7, datetime(2030, 1, 1, tzinfo=timezone.utc)), topic="canonical",
                start_offsets={0: 3, 1: 0}, resolutions={}, duplicates=(), digest="d" * 64)
    base.update(overrides)
    return RecoveryResumption(**base)


def test_default_consumer_still_starts_at_earliest():
    assert _config().starting_offsets == "earliest"
    assert stream_cli.kafka_reader_options(_config())["startingOffsets"] == "earliest"


def test_resumed_config_binds_the_new_lineage_and_starts_exactly_at_the_cut_end():
    resumption = _resumption()
    config = resumed_consumer_config(_config(), resumption)
    assert config.pipeline_epoch == "pe-resumed"
    assert config.starting_offsets == spark_starting_offsets(resumption)
    config.validate()
    options = stream_cli.kafka_reader_options(config)
    assert json.loads(options["startingOffsets"]) == {"canonical": {"0": 3, "1": 0}}
    assert options["failOnDataLoss"] == "true"
    assert stream_cli.starting_offsets_by_partition(config) == {0: 3, 1: 0}
    assert stream_cli.starting_offsets_by_partition(_config()) is None


def test_resumed_config_refuses_another_topic_or_a_non_resumption():
    with pytest.raises(ValueError, match="topic"):
        resumed_consumer_config(_config(topic="other"), _resumption())
    with pytest.raises(ValueError, match="RecoveryResumption"):
        resumed_consumer_config(_config(), {"pipeline_epoch": "pe", "start_offsets": {0: 1}})
    with pytest.raises(ValueError, match="ConsumerConfig"):
        resumed_consumer_config(object(), _resumption())


@pytest.mark.parametrize("value", [
    "latest",
    "",
    "{",
    json.dumps({"other": {"0": 1}}),
    json.dumps({"canonical": {}}),
    json.dumps({"canonical": {"0": -1}}),
    json.dumps({"canonical": {"0": -2}}),
    json.dumps({"canonical": {"0": 1.5}}),
    json.dumps({"canonical": {"0": True}}),
    json.dumps({"canonical": {"1": 4}}),
    json.dumps({"canonical": {"x": 4}}),
    json.dumps({"canonical": {"0": 1}, "other": {"0": 1}}),
    json.dumps(["canonical"]),
])
def test_explicit_starting_offsets_must_be_exact_nonnegative_cut_coordinates(value):
    with pytest.raises(ValueError, match="starting_offsets"):
        _config(starting_offsets=value).validate()


def test_cli_accepts_explicit_starting_offsets_and_defaults_to_earliest():
    parser = stream_cli._parser()
    assert parser.parse_args([]).starting_offsets == "earliest"
    text = json.dumps({"canonical": {"0": 2}})
    assert parser.parse_args(["--starting-offsets", text]).starting_offsets == text
