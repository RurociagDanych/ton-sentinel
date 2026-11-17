"""Real Spark regressions for canonical action full-window recomputation."""
import json

import pytest
from pyspark.sql import SparkSession

from ton_sentinel.spark.batch import compute_features


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("spark-feature-test")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def action(action_id, *, end, input_asset, input_raw, output_asset, output_raw,
           trader=None, method=None, unambiguous=None, protocol="STON.fi", status="success"):
    return json.dumps({
        "schema_version": 1, "kind": "synthetic_action",
        "identity": {"network": "testnet", "trace_id": "trace-1", "action_id": action_id},
        "epoch": {"network": "testnet", "source_fingerprint": "src", "filter_fingerprint": "filter",
                  "build_fingerprint": "build-1", "action_policy_fingerprint": "policy"},
        "protocol": protocol, "protocol_version": "synthetic-v1", "action_end": end, "status": status,
        "source_eligible": True, "route_proven": True, "route": [input_asset, output_asset],
        "input": {"asset": input_asset, "raw_amount": input_raw, "decimals": None, "decimal_provenance": None},
        "output": {"asset": output_asset, "raw_amount": output_raw, "decimals": None, "decimal_provenance": None},
        "trader": trader, "trader_method": method, "trader_unambiguous": unambiguous,
    }, separators=(",", ":"))


def test_recomputes_both_sides_exact_amounts_and_unknown_traders(spark):
    rows = [
        action("id-1", end=299, input_asset="TON", input_raw="10000000000000000001",
               output_asset="jetton:A", output_raw="250", trader="wallet-1", method="explicit", unambiguous=True),
        action("id-2", end=300, input_asset="jetton:A", input_raw="250", output_asset="TON",
               output_raw="10000000000000000001", trader="wallet-1", method="explicit", unambiguous=True, protocol="DeDust"),
        action("id-3", end=301, input_asset="jetton:A", input_raw="5", output_asset="jetton:A",
               output_raw="3", trader="wallet-2", method="ambiguous", unambiguous=False),
        action("id-4", end=302, input_asset="TON", input_raw="9", output_asset="jetton:B",
               output_raw="7", status="pending"),
        # Exact replay of a canonical observation must not add volume.
        action("id-1", end=299, input_asset="TON", input_raw="10000000000000000001",
               output_asset="jetton:A", output_raw="250", trader="wallet-1", method="explicit", unambiguous=True),
    ]
    actions = spark.createDataFrame([(row,) for row in rows], "action_json string")
    coverage = spark.createDataFrame([("testnet", 0, 300, "bounded_observed", False),
                                      ("testnet", 300, 600, "bounded_observed", False)],
                                     "network string, window_start long, window_end long, status string, eligible_zero boolean")
    policy = {"schema_version": 1, "synthetic": True,
              "mappings": [{"action_start": 0, "action_end": 600, "status": "bounded_observed"}]}
    first = compute_features(spark, actions, coverage, policy)
    second = compute_features(spark, actions, coverage, policy)
    observed = {(r.window_start, r.token): r.asDict() for r in first.collect()}
    observed_second = sorted((r.window_start, r.token, r.input_raw, r.output_raw, r.swaps) for r in second.collect())
    assert observed_second == sorted((r["window_start"], r["token"], r["input_raw"], r["output_raw"], r["swaps"]) for r in observed.values())
    ton0 = observed[(0, "TON")]
    assert (ton0["swaps"], ton0["input_raw"], ton0["output_raw"], ton0["known_traders"], ton0["unknown_count"]) == (1, 10000000000000000001, None, 1, 0)
    token0 = observed[(0, "jetton:A")]
    assert (token0["swaps"], token0["input_raw"], token0["output_raw"], token0["known_traders"], token0["unknown_count"]) == (1, None, 250, 1, 0)
    token300 = observed[(300, "jetton:A")]
    assert (token300["swaps"], token300["input_raw"], token300["output_raw"], token300["known_traders"], token300["unknown_count"]) == (2, 255, 3, 1, 1)
    assert all(row["status"] == "provisional" and row["provisional"] for row in observed.values())


def test_explicit_eligible_zero_requires_policy_and_coverage(spark):
    actions = spark.createDataFrame([], "action_json string")
    coverage = spark.createDataFrame([("testnet", 0, 300, "bounded_observed", True),
                                      ("testnet", 300, 600, "bounded_observed", True),
                                      ("testnet", 600, 900, "certified", True)],
                                     "network string, window_start long, window_end long, status string, eligible_zero boolean")
    policy = {"schema_version": 1, "synthetic": True,
              "mappings": [{"action_start": 0, "action_end": 300, "status": "bounded_observed"}]}
    result = compute_features(spark, actions, coverage, policy).collect()
    assert result == []


def test_missing_required_action_input_fails_fast(spark):
    actions = spark.createDataFrame([], "not_action string")
    coverage = spark.createDataFrame([], "network string, window_start long, window_end long, status string, eligible_zero boolean")
    with pytest.raises(ValueError, match="action_json"):
        compute_features(spark, actions, coverage, {})


def _coverage(spark):
    return spark.createDataFrame([("testnet", 0, 300, "bounded_observed", False)],
                                 "network string, window_start long, window_end long, status string, eligible_zero boolean")


def test_conflicting_payloads_for_one_identity_fail_closed(spark):
    first = action("same-id", end=20, input_asset="TON", input_raw="10",
                   output_asset="jetton:A", output_raw="2")
    conflict = action("same-id", end=20, input_asset="TON", input_raw="11",
                      output_asset="jetton:A", output_raw="2")
    actions = spark.createDataFrame([(first,), (conflict,)], "action_json string")
    with pytest.raises(ValueError, match="conflicting canonical"):
        compute_features(spark, actions, _coverage(spark), {}).collect()


@pytest.mark.parametrize("payload", [
    "not-json",
    '{"input":{"asset":"TON","raw_amount":"1"},"output":{"asset":"jetton:A","raw_amount":"2"}}',
    '{"identity":{"network":"testnet","trace_id":"trace-1","action_id":"id-1"}}',
    action("id-1", end=20, input_asset="TON", input_raw="10",
           output_asset="jetton:A", output_raw="2").replace('"raw_amount":"10"', '"raw_amount":null'),
    *[action("id-1", end=20, input_asset="TON", input_raw=value,
             output_asset="jetton:A", output_raw="2") for value in ("-1", "1.5", "abc", "01", "9" * 39)],
])
def test_malformed_or_noncanonical_observations_fail_closed(spark, payload):
    actions = spark.createDataFrame([(payload,)], "action_json string")
    with pytest.raises(ValueError, match="invalid canonical action_json"):
        compute_features(spark, actions, _coverage(spark), {}).collect()


def test_raw_sum_over_decimal38_limit_fails_closed(spark):
    rows = [action(f"id-{index}", end=20, input_asset="TON", input_raw=value,
                   output_asset="jetton:A", output_raw="1")
            for index, value in enumerate(("9" * 38, "1"))]
    actions = spark.createDataFrame([(row,) for row in rows], "action_json string")
    with pytest.raises(ValueError, match=r"Decimal\(38,0\)"):
        compute_features(spark, actions, _coverage(spark), {}).collect()
