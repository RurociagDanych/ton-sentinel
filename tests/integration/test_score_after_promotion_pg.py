"""Dashboard API on real PostgreSQL: the post-promotion hook scores promoted windows; a failed
promotion schedules nothing; a scoring failure leaves the promoted features served.
SYNTHETIC grid (behaviour check, not quality)."""
import pytest

from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.serving import read_detector_results
from ton_sentinel.stream_cli import promote_then_score, score_after_promotion
from tests.integration.test_baseline_grid_pg import promote_rows, row
from tests.integration.test_detection import COUNT, NET, SCORED, W, counts
from tests.integration.test_detector_synthetic_pg import build, real_windows
from tests.integration.test_revisions import db  # noqa: F401


def served(db):
    db.commit()
    return db.execute("SELECT count(*) FROM served_features").fetchone()[0]


def test_promoted_window_is_scored_by_the_post_promotion_hook(db):
    real = real_windows()
    holder = {}

    def promote():                                     # the real fenced promotion path
        holder["lease"], holder["tokens"] = build(db, real, {})
        db.commit()
        return 25 * len(holder["tokens"])

    assert counts(db)[1:3] == (0, 0)
    spy = []

    def score(connection, lease, *, network, limit):
        from ton_sentinel.db.work import score_pending
        spy.append(limit)
        return score_pending(connection, lease, network=network, limit=limit)

    promoted, report = promote_then_score(db, promote, limit=1000, owner="stream-test",
                                          score=score)
    assert promoted > 0 and spy == [1000]
    assert report["status"] == "scored" and report["first_scored"] == promoted
    assert report["remaining_unscored"] == 0
    db.commit()
    docs = read_detector_results(db, network=NET, token=holder["tokens"][0], window_start=SCORED)
    assert COUNT in {d["detector"] for d in docs}     # current detections without manual calls


def test_hook_is_bounded_by_the_configured_limit(db):
    real = real_windows()
    lease, tokens = build(db, real, {})
    db.commit()
    report = score_after_promotion(db, 1, limit=5, owner="stream-test")
    assert report["status"] == "scored" and report["first_scored"] == 5
    assert report["remaining_unscored"] == 25 * len(tokens) - 5


def test_disabled_hook_scores_nothing(db):
    real = real_windows()
    build(db, real, {})
    db.commit()
    before = counts(db)
    assert score_after_promotion(db, 3, limit=None, owner="stream-test") is None
    assert counts(db) == before and before[1:3] == (0, 0)


def test_failed_promotion_schedules_nothing(db):
    real = real_windows()
    lease, tokens = build(db, real, {})
    db.commit()
    stale = lease
    acquire_transition(db, "newer-owner", 300)          # supersedes ``stale``
    before_served, before_counts = served(db), counts(db)
    spy = []

    def promote():
        promote_rows(db, stale, SCORED + 10 * W, [row(SCORED + 10 * W, tokens[0], 3)])
        return 1

    with pytest.raises(LeaseRejected):
        promote_then_score(db, promote, limit=1000, owner="stream-test",
                           score=lambda *a, **k: spy.append(1))
    db.rollback()
    assert spy == []
    assert served(db) == before_served and counts(db) == before_counts


def test_scoring_failure_leaves_the_promotion_served(db):
    real = real_windows()
    lease, tokens = build(db, real, {})
    db.commit()
    before = served(db)

    def broken(connection, lease, *, network, limit):
        connection.execute("SELECT 1")                 # open a transaction, then fail
        raise RuntimeError("scoring blew up")

    report = score_after_promotion(db, 1, limit=10, owner="stream-test", score=broken)
    assert report == {"status": "failed", "error": "RuntimeError"}
    assert served(db) == before > 0                    # features still served
    assert counts(db)[1:3] == (0, 0)                   # backlog visible, nothing half-written
    assert db.info.transaction_status.name in ("IDLE", "INTRANS")
