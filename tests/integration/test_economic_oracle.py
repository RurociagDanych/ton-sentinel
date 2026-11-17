"""Compare real Spark feature output with the independent raw-source oracle."""
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from tests.oracle import expected_economic_features
from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.normalize import normalize
from ton_sentinel.spark.batch import compute_features


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("economic-oracle-test")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def test_fixture_economic_features_match_independent_oracle(spark):
    fixture_path = Path(__file__).parents[1] / "fixtures" / "truth" / "actions.json"
    fixture = json.loads(fixture_path.read_text())
    source_actions = fixture["actions"]
    # Exact replay of an eligible event must not increase the economic totals.
    source_actions = source_actions + [source_actions[0]]
    # An explicit eligible synthetic event with zero endpoints is still one swap.
    zero_action = json.loads(json.dumps(source_actions[0]))
    zero_action.update(
        trace_id="AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
        action_end=1201,
        route=["jetton:zero-input", "jetton:zero-output"],
    )
    zero_action["input"].update(asset="jetton:zero-input", raw_amount="0")
    zero_action["output"].update(asset="jetton:zero-output", raw_amount="0")
    source_actions.append(zero_action)
    expected = expected_economic_features(source_actions)

    epoch = SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                        "synthetic:build", "synthetic:policy")
    canonical_json = [normalize(action, epoch).to_json() for action in source_actions]
    actions = spark.createDataFrame([(item,) for item in canonical_json], "action_json string")
    coverage = spark.createDataFrame(
        [("testnet", start, start + 300, "bounded_observed", False)
         for start in (0, 300, 600, 900, 1200)],
        "network string, window_start long, window_end long, status string, eligible_zero boolean",
    )
    actual = []
    for row in compute_features(
            spark, actions, coverage,
            {"schema_version": 1, "synthetic": True}).collect():
        item = row.asDict()
        for field in ("input_raw", "output_raw"):
            item[field] = int(item[field]) if item[field] is not None else None
        actual.append({key: item[key] for key in (
            "network", "window_start", "window_end", "token", "generation",
            "swaps", "input_raw", "output_raw", "known_traders", "unknown_count",
            "status", "provisional")})
    assert sorted(actual, key=lambda row: (row["window_start"], row["token"])) == expected
    zero_rows = sorted((row for row in actual if row["window_start"] == 1200),
                       key=lambda row: row["token"])
    assert [(row["token"], row["swaps"], row["input_raw"], row["output_raw"])
            for row in zero_rows] == [
                ("jetton:zero-input", 1, 0, None),
                ("jetton:zero-output", 1, None, 0),
            ]
