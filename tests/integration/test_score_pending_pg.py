"""Dashboard API on real PostgreSQL: ``score_pending`` first-scores promoted windows, rescores stale results, and a second run is a no-op."""

import pytest

from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.serving import read_detector_results
from ton_sentinel.db.work import score_pending
from tests.integration.test_baseline_grid_pg import promote_rows, row
from tests.integration.test_detection import COUNT, NET, SCORED, W, counts, history
from tests.integration.test_detector_synthetic_pg import build, real_windows
from tests.integration.test_revisions import db  # noqa: F401

EMPTY = {"first_scored": 0, "rescored": 0, "remaining_unscored": 0, "remaining_stale": 0,
         "refused": 0}


def fresh_world(db):
    real = real_windows()
    lease, tokens = build(db, real, {})
    db.commit()
    return real, lease, tokens


def test_fresh_promoted_window_gets_a_first_score_and_second_run_is_a_no_op(db):
    real, lease, tokens = fresh_world(db)
    assert counts(db)[1:3] == (0, 0)
    total = 25 * len(tokens)

    first = score_pending(db, lease, network=NET, limit=1000)
    assert first == {**EMPTY, "first_scored": total}

    db.commit()
    docs = read_detector_results(db, network=NET, token=tokens[0], window_start=SCORED)
    assert sorted(d["detector"] for d in docs) == ["active_wallets_spike.v1", COUNT]
    db.commit()
    after_first = counts(db)

    assert score_pending(db, lease, network=NET, limit=1000) == EMPTY
    assert counts(db) == after_first


def test_limit_bounds_the_run_and_the_backlog_drains_across_runs(db):
    real, lease, tokens = fresh_world(db)
    total = 25 * len(tokens)
    one = score_pending(db, lease, network=NET, limit=10)
    assert one["first_scored"] == 10 and one["remaining_unscored"] == total - 10
    db.commit()
    two = score_pending(db, lease, network=NET, limit=1000)
    assert two["first_scored"] == total - 10 and two["remaining_unscored"] == 0


def test_correction_makes_results_stale_then_score_pending_refreshes(db):
    real, lease, tokens = fresh_world(db)
    score_pending(db, lease, network=NET, limit=1000)
    spike = real[tokens[0]][0]
    promote_rows(db, lease, SCORED, [row(SCORED, t, spike[0] + 50 if t == tokens[0] else
                                         real[t][0][0], real[t][0][1], real[t][0][2])
                                     for t in tokens])
    db.commit()
    assert read_detector_results(db, network=NET, token=tokens[0], window_start=SCORED) == []
    db.commit()
    report = score_pending(db, lease, network=NET, limit=1000)
    assert report["first_scored"] == 0 and report["rescored"] >= 1
    assert report["remaining_stale"] == 0
    db.commit()
    assert len(read_detector_results(db, network=NET, token=tokens[0],
                                     window_start=SCORED)) == 2
    db.commit()
    assert score_pending(db, lease, network=NET, limit=1000) == EMPTY


def test_lost_lease_stops_without_partial_promotion(db):
    real, lease, tokens = fresh_world(db)
    acquire_transition(db, "usurper", 300)
    db.commit()
    with pytest.raises(LeaseRejected):
        score_pending(db, lease, network=NET, limit=1000)
    db.rollback()
    assert counts(db)[1:3] == (0, 0)
