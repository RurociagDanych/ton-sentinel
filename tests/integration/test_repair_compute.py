"""Compute_replacement(spark, frozen) reads only the frozen pins."""
import json

import pytest
from pyspark.sql import SparkSession

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.db.work import compute_replacement, promote
from tests.integration.test_repair_fencing import (  # noqa: F401  (fixtures + helpers)
    W0, W1, db, freeze, promote_idle, served, world_with_correction,
)


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("repair-compute-test")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


def expected_pairs(frozen, window):
    """Independent oracle: (token, swaps) per window straight from the frozen action JSON."""
    counts = {}
    for _, _, _, text in frozen.inputs:
        action = json.loads(text)
        if not window.window_start <= action["action_end"] < window.window_end:
            continue
        if action["status"] != "success":
            continue
        for side in ("input", "output"):
            counts[action[side]["asset"]] = counts.get(action[side]["asset"], 0) + 1
    return sorted(counts.items())


def test_compute_replacement_uses_only_frozen_inputs_and_promotes(db, spark):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    assert expected_pairs(frozen, W0), "fixture must put an action in window 0"
    # a correction moves X out of window 0 while the repair computes; the result is
    # still derived only from the frozen pins ...
    decide_revision(db, lease, evidence, auth)
    db.commit()
    result = compute_replacement(spark, frozen)
    assert result.work_id == frozen.work_id
    assert sorted((r["token"], r["swaps"]) for r in result.rows) == expected_pairs(frozen, W0)
    assert {(r["network"], r["window_start"], r["window_end"]) for r in result.rows} == {
        ("testnet", 0, 300)}
    # ... and therefore can never be promoted: the pins moved
    assert promote_idle(db, lease, frozen, result.rows) is False
    assert served(db) == []


def test_compute_replacement_result_promotes_when_pins_unchanged(db, spark):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W0])
    result = compute_replacement(spark, frozen)
    assert promote_idle(db, lease, frozen, result.rows) is True
    assert sorted((r[1], r[3]) for r in served(db)) == expected_pairs(frozen, W0)


def test_compute_replacement_of_empty_window_is_empty(db, spark):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "repair")
    frozen = freeze(db, lease, [W1])
    assert compute_replacement(spark, frozen).rows == ()


def test_compute_replacement_rejects_non_frozen_input(db, spark):
    with pytest.raises(ValueError):
        compute_replacement(spark, object())
