"""Deterministic, stateless Spark recomputation for complete action windows."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ton_sentinel.contracts import (
    LIVE_PROTOCOL_VERSIONS, SYNTHETIC_VERSION, refuse_synthetic, synthetic_fixtures_allowed,
)


_ACTION_SCHEMA = T.StructType([
    T.StructField("identity", T.StructType([T.StructField("network", T.StringType()), T.StructField("trace_id", T.StringType()), T.StructField("action_id", T.StringType())])),
    T.StructField("epoch", T.StructType([T.StructField(k, T.StringType()) for k in ("network", "source_fingerprint", "filter_fingerprint", "build_fingerprint", "action_policy_fingerprint")])),
    T.StructField("protocol", T.StringType()), T.StructField("protocol_version", T.StringType()), T.StructField("action_end", T.LongType()), T.StructField("status", T.StringType()),
    T.StructField("source_eligible", T.BooleanType()), T.StructField("route_proven", T.BooleanType()), T.StructField("route", T.ArrayType(T.StringType())),
    T.StructField("input", T.StructType([T.StructField("asset", T.StringType()), T.StructField("raw_amount", T.StringType()), T.StructField("decimals", T.IntegerType()), T.StructField("decimal_provenance", T.StringType())])),
    T.StructField("output", T.StructType([T.StructField("asset", T.StringType()), T.StructField("raw_amount", T.StringType()), T.StructField("decimals", T.IntegerType()), T.StructField("decimal_provenance", T.StringType())])),
    T.StructField("trader", T.StringType()), T.StructField("trader_method", T.StringType()), T.StructField("trader_unambiguous", T.BooleanType()),
])


def compute_features(spark: SparkSession, actions_df: DataFrame, coverage_df: DataFrame, policy: dict) -> DataFrame:
    """Recompute eligible actions grouped by network/token/window.

    actions_df requires canonical ``Action.to_json()`` values in ``action_json``.
    Coverage columns: network, window_start, window_end, status, eligible_zero.
    Each endpoint is a separate token contribution; quantities remain exact raw
    integers (Spark Decimal(38,0)); feature status is always provisional.
    """
    if "action_json" not in actions_df.columns:
        raise ValueError("actions_df requires canonical action_json column")
    required = {"network", "window_start", "window_end", "status", "eligible_zero"}
    if not required.issubset(coverage_df.columns):
        raise ValueError("coverage_df lacks action-window coverage fields")
    # Parse and validate the entire bounded input, including rows that will later
    # be filtered as pending or unsupported. A malformed observation must not
    # disappear behind permissive JSON parsing or eligibility filtering.
    parsed = actions_df.select("action_json").distinct().select(
        F.from_json("action_json", _ACTION_SCHEMA).alias("a")).select("a.*")
    bad_raw = lambda name: ~F.col(name).rlike(r"^(0|[1-9][0-9]{0,37})$")
    invalid = parsed.where(
        F.col("identity.network").isNull() | (F.length("identity.network") == 0)
        | F.col("identity.trace_id").isNull() | (F.length("identity.trace_id") == 0)
        | F.col("identity.action_id").isNull() | (F.length("identity.action_id") == 0)
        | F.col("input.asset").isNull() | (F.length("input.asset") == 0)
        | F.col("output.asset").isNull() | (F.length("output.asset") == 0)
        | F.col("input.raw_amount").isNull() | bad_raw("input.raw_amount")
        | F.col("output.raw_amount").isNull() | bad_raw("output.raw_amount"))
    if invalid.limit(1).count():
        raise ValueError("invalid canonical action_json: malformed JSON or missing identity/required raw fields, or invalid raw amount")
    # Production accepts only registered live adapter versions; synthetic-v1 fixture
    # rows are refused explicitly (never filtered away silently) outside the test-only switch.
    fixtures = synthetic_fixtures_allowed()
    if not fixtures and parsed.where(F.col("protocol_version") == SYNTHETIC_VERSION).limit(1).count():
        refuse_synthetic("synthetic-v1 action_json in the Spark batch")

    # Exact replays have already been removed. Distinct payloads for one event
    # are contradictory canonical observations; never choose one arbitrarily.
    identity = ["identity.network", "identity.trace_id", "identity.action_id"]
    canonical = F.to_json(F.struct(*[F.col(c) for c in parsed.columns]))
    conflicts = parsed.withColumn("_canonical", canonical).groupBy(*identity).agg(
        F.countDistinct("_canonical").alias("payloads")).where(F.col("payloads") > 1)
    if conflicts.limit(1).count():
        raise ValueError("conflicting canonical action_json payloads share an action identity")
    # JSON whitespace/key order are not semantic payload differences. Dedup
    # parsed canonical structs as well as byte-identical source strings.
    parsed = parsed.dropDuplicates(parsed.columns)

    # Replayed canonical JSON is the same observation, not a second trade.
    registered = F.lit(False)
    for version, protocol in sorted(LIVE_PROTOCOL_VERSIONS.items()):
        registered = registered | ((F.col("protocol_version") == version) & (F.col("protocol") == protocol))
    if fixtures:
        registered = registered | (F.col("protocol").isin("STON.fi", "DeDust")
                                   & (F.col("protocol_version") == SYNTHETIC_VERSION))
    eligible = parsed.where(registered
        & (F.col("status") == "success") & (F.col("source_eligible") == True) & (F.col("route_proven") == True))
    trader = F.when((F.col("trader_unambiguous") == True) & F.col("trader_method").isNotNull(), F.col("trader"))
    base = eligible.select(F.col("identity.network").alias("network"), F.col("identity.trace_id").alias("trace_id"),
        F.col("identity.action_id").alias("action_id"), (F.floor(F.col("action_end") / 300) * 300).cast("long").alias("window_start"),
        (F.floor(F.col("action_end") / 300) * 300 + 300).cast("long").alias("window_end"), F.col("epoch.build_fingerprint").alias("generation"),
        trader.alias("trader"), F.col("input"), F.col("output"))
    side_array = F.array(
        F.struct(F.col("input.asset").alias("token"), F.col("input.raw_amount").alias("input_raw"), F.lit(None).cast("string").alias("output_raw")),
        F.struct(F.col("output.asset").alias("token"), F.lit(None).cast("string").alias("input_raw"), F.col("output.raw_amount").alias("output_raw")),
    )
    sides = base.select("network", "window_start", "window_end", "generation", "trader", "trace_id", "action_id", F.explode(side_array).alias("side"))
    sides = sides.select("network", "window_start", "window_end", "generation", "trader", "trace_id", "action_id", "side.*")
    event_token = sides.groupBy("network", "window_start", "window_end", "generation", "token", "trace_id", "action_id").agg(
        F.first("trader", ignorenulls=True).alias("trader"), F.sum(F.col("input_raw").cast(T.DecimalType(38, 0))).alias("input_raw"),
        F.sum(F.col("output_raw").cast(T.DecimalType(38, 0))).alias("output_raw"))
    computed = event_token.groupBy("network", "window_start", "window_end", "token", "generation").agg(
        F.count(F.lit(1)).cast("long").alias("swaps"), F.sum("input_raw").cast(T.DecimalType(38, 0)).alias("input_raw"),
        F.sum("output_raw").cast(T.DecimalType(38, 0)).alias("output_raw"), F.countDistinct("trader").cast("long").alias("known_traders"),
        F.sum(F.when(F.col("trader").isNull(), 1).otherwise(0)).cast("long").alias("unknown_count"))
    aggregate_keys = ["network", "window_start", "window_end", "token", "generation"]
    arithmetic_overflow = event_token.groupBy(*aggregate_keys).agg(
        F.sum("input_raw").alias("_input_sum"), F.count("input_raw").alias("_input_count"),
        F.sum("output_raw").alias("_output_sum"), F.count("output_raw").alias("_output_count"))
    try:
        overflow_found = arithmetic_overflow.where(
            ((F.col("_input_count") > 0) & F.col("_input_sum").isNull())
            | ((F.col("_output_count") > 0) & F.col("_output_sum").isNull())
        ).limit(1).count()
    except Exception as exc:
        if "OVERFLOW" not in str(exc).upper():
            raise
        raise ValueError("raw amount aggregation exceeds Spark Decimal(38,0) precision") from exc
    if overflow_found:
        raise ValueError("raw amount aggregation exceeds Spark Decimal(38,0) precision")
    cov = coverage_df.select("network", F.col("window_start").cast("long").alias("window_start"), F.col("window_end").cast("long").alias("window_end"),
        F.col("status").alias("coverage_status"), F.col("eligible_zero").cast("boolean").alias("eligible_zero"))
    joined = computed.join(cov, ["network", "window_start", "window_end"], "left")
    # A bounded mapping proves observation bounds, not eligible zero volume.
    return joined.select("network", "window_start", "window_end", "token", "generation", "swaps", "input_raw", "output_raw", "known_traders", "unknown_count",
        "coverage_status", F.lit("provisional").alias("status"), F.lit(True).alias("provisional"))
