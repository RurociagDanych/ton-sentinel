"""Real Spark full-window feature replacement into real PostgreSQL.

Provisional/non-certified synthetic fixtures only; no live source claim.
"""
import json
import uuid
from pathlib import Path

import psycopg
import pytest

from tests.oracle import expected_economic_features
from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.serving import WindowKey, read_features, replace_features
from ton_sentinel.normalize import normalize
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.spark.batch import compute_features
from ton_sentinel.spark.streaming import process_canonical_microbatch

from tests.integration.test_canonical_stream import (  # noqa: F401  (fixtures + helpers)
    _accepted, _audited_batch, _audited_row, _batch, database, spark,
)

COLUMNS = ("network", "window_start", "window_end", "token", "generation", "swaps", "input_raw",
           "output_raw", "known_traders", "unknown_count", "coverage_status", "status", "provisional")
COVERAGE = "network string, window_start long, window_end long, status string, eligible_zero boolean"


def _fixture_actions():
    path = Path(__file__).parents[1] / "fixtures" / "truth" / "actions.json"
    actions = json.loads(path.read_text())["actions"]
    return actions + [actions[0]]  # exact replay must not add volume


def _spark_features(spark, source_actions, windows):
    epoch = SourceEpoch("testnet", "synthetic:source", "synthetic:filter", "synthetic:build", "synthetic:policy")
    rows = [(normalize(a, epoch).to_json(),) for a in source_actions]
    cov = [("testnet", s, s + 300, "bounded_observed", False) for s in windows]
    return compute_features(spark, spark.createDataFrame(rows, "action_json string"),
                            spark.createDataFrame(cov, COVERAGE), {"schema_version": 1, "synthetic": True}).collect()


def _stored(connection):
    return [dict(zip(COLUMNS, row)) for row in connection.execute(
        "SELECT network,window_start,window_end,token,generation,swaps,input_raw::text,output_raw::text,"
        "known_traders,unknown_count,coverage_status,status,provisional FROM served_features "
        "ORDER BY window_start, token COLLATE \"C\"").fetchall()]


def test_persisted_spark_features_equal_independent_oracle_with_directions_and_routes(spark, database):
    source = _fixture_actions()
    expected = expected_economic_features(source)
    features = _spark_features(spark, source, (0, 300, 600))
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "task61-oracle")
        assert replace_features(connection, lease, features) == len(expected)
        stored = _stored(connection)
    projected = [{k: (int(r[k]) if k in ("input_raw", "output_raw") and r[k] is not None else r[k])
                  for k in ("network", "window_start", "window_end", "token", "generation", "swaps",
                            "input_raw", "output_raw", "known_traders", "unknown_count", "status", "provisional")}
                 for r in stored]
    assert projected == expected  # exact keys, values, sorted order
    by_key = {(r["window_start"], r["token"]): r for r in stored}
    # Both directions of the TON<->jetton:A pair: input-only and output-only endpoints.
    assert by_key[(0, "TON")]["input_raw"] == "1000000000" and by_key[(0, "TON")]["output_raw"] is None
    assert by_key[(0, "jetton:A")]["output_raw"] == "250" and by_key[(0, "jetton:A")]["input_raw"] is None
    assert by_key[(300, "TON")]["output_raw"] == "1000000000" and by_key[(300, "TON")]["input_raw"] is None
    # Jetton->jetton route: only the swapped endpoints count; intermediate hops are not tokens.
    assert (600, "jetton:A") in by_key and (600, "jetton:B") in by_key
    assert not any(token == "jetton:intermediate" for _, token in by_key)
    assert all(r["status"] == "provisional" and r["provisional"] and r["coverage_status"] == "bounded_observed"
               for r in stored)


def test_recomputation_replaces_full_window_instead_of_incrementing(spark, database):
    source = _fixture_actions()
    full = _spark_features(spark, source, (0, 300, 600))
    # Window 600 later shrinks to its first remaining action only (drops jetton:A/B rows, recomputed from fewer inputs).
    shrunk_source = [a for a in source if a["action_end"] < 600]
    shrunk = _spark_features(spark, shrunk_source, (0, 300))
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "task61-replace")
        replace_features(connection, lease, full)
        first = _stored(connection)
        replace_features(connection, lease, full)  # identical recompute: never doubles
        assert _stored(connection) == first
        replace_features(connection, lease, shrunk, windows=[WindowKey("testnet", s, s + 300) for s in (0, 300, 600)])
        after = _stored(connection)
    assert {r["window_start"] for r in after} == {0, 300}  # emptied touched window removed
    assert [r for r in after if r["window_start"] < 600] == [r for r in first if r["window_start"] < 600]
    assert {r["swaps"] for r in after if r["window_start"] == 300} == {1}


def _make_events(connection, lease, incarnation):
    # three accepted actions: two in window [0,300) and one in window [300,600)
    _accepted(connection, lease, 10, 20, 11, 15)
    _accepted(connection, lease, 20, 30, 21, 25)
    _accepted(connection, lease, 300, 310, 31, 305)
    rows = connection.execute("SELECT event_id,payload,accepted_generation FROM outbox ORDER BY event_id").fetchall()
    connection.commit()
    assert len(rows) == 3
    return rows


def _audited_rows(events, incarnation, topic="t61"):
    return [_audited_row(topic, offset, event, payload, incarnation, generation)
            for offset, (event, payload, generation) in enumerate(events)]


def _business(connection):
    return [{k: v for k, v in row.items()} for row in _stored(connection)]


def _run(connection, lease, spark, incarnation, rows, batch_id, query="q1", epoch="pe1"):
    return process_canonical_microbatch(
        connection, lease, spark, _audited_batch(spark, rows), topic_incarnation=incarnation, replay_epoch=0,
        batch_id=batch_id, environment_id="env", query_id=query, pipeline_epoch=epoch, topic="t61")


def test_changed_batch_boundaries_and_post_commit_retry_yield_identical_results(spark, database):
    incarnation = "t61-" + uuid.uuid4().hex
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "task61-boundaries", ttl_seconds=300)
        configure_delivery(connection, topic="t61", topic_incarnation=incarnation)
        events = _make_events(connection, lease, incarnation)
        rows = _audited_rows(events, incarnation)

        # Layout A: one batch with every trigger.
        _run(connection, lease, spark, incarnation, rows, 0)
        reference = _business(connection)
        connection.commit()
        assert reference and {r["window_start"] for r in reference} == {0, 300}
        assert {r["swaps"] for r in reference if r["window_start"] == 0} == {2}

        # Commit-before-checkpoint retry: identical batch identity is processed again after its SQL commit.
        assert _run(connection, lease, spark, incarnation, rows, 0) == len(reference)
        assert _business(connection) == reference
        connection.commit()
        assert connection.execute("SELECT count(*) FROM stream_batches WHERE batch_id=0").fetchone()[0] == 1

        # Layout B: wipe serving rows, process one trigger per batch in reverse order (new pipeline epoch).
        connection.execute("DELETE FROM served_features")
        connection.commit()
        for batch_id, row in enumerate(reversed(rows)):
            _run(connection, lease, spark, incarnation, [row], batch_id, query="q2", epoch="pe2")
            connection.commit()
        assert _business(connection) == reference

        # Layout C: different split (2+1) and a replay of the first split.
        connection.execute("DELETE FROM served_features")
        connection.commit()
        _run(connection, lease, spark, incarnation, rows[2:], 0, query="q3", epoch="pe3")
        _run(connection, lease, spark, incarnation, rows[:2], 1, query="q3", epoch="pe3")
        _run(connection, lease, spark, incarnation, rows[:2], 1, query="q3", epoch="pe3")
        connection.commit()
        assert _business(connection) == reference
