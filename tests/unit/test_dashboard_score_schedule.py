"""Scoring after a promotion (bounded, non-fatal) and the opt-in score schedule."""
import inspect
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import ton_sentinel.stream_cli as stream_cli
from ton_sentinel.db.fencing import LeaseRejected
from ton_sentinel.stream_cli import ConsumerConfig, score_after_promotion


LEASE = SimpleNamespace(owner="o", fence=1)


class Conn:
    def __init__(self):
        self.calls = []

    def commit(self):
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")


def recorder(result=None, exc=None):
    calls = []

    def score(connection, lease, *, network, limit):
        calls.append({"network": network, "limit": limit})
        if exc is not None:
            raise exc
        return result if result is not None else {"first_scored": 3}
    score.calls = calls
    return score


def hook(conn, score, *, promoted=2, limit=50):
    return score_after_promotion(conn, promoted, limit=limit, owner="stream",
                                 acquire=lambda c, owner, ttl: LEASE, score=score)


def base_config(**extra):
    base = dict(dsn="postgresql://x", bootstrap_servers="k:9092", topic="t",
                checkpoint="/tmp/ckpt", topic_incarnation="i", replay_epoch=0,
                environment_id="e", query_id="q", pipeline_epoch="p")
    base.update(extra)
    return ConsumerConfig(**base)


# ---- hook: scoring is invoked after a successful promotion, within budget -------------------
def test_promotion_callback_scores_new_windows_within_budget():
    score = recorder()
    report = hook(Conn(), score, promoted=2, limit=25)
    assert score.calls == [{"network": None, "limit": 25}]
    assert report == {"status": "scored", "first_scored": 3}


def test_nothing_promoted_means_no_scoring_call():
    score = recorder()
    assert hook(Conn(), score, promoted=0) is None
    assert score.calls == []


def test_disabled_hook_makes_no_scoring_call():
    score = recorder()
    assert score_after_promotion(Conn(), 5, limit=None, owner="s", score=score,
                                 acquire=lambda c, o, t: LEASE) is None
    assert score.calls == []


# ---- a scoring failure never propagates and never touches the promotion --------------------
@pytest.mark.parametrize("exc", [RuntimeError("boom postgresql://u:secret@h/db"),
                                 LeaseRejected("superseded"), ValueError("x")])
def test_scoring_failure_does_not_roll_back_feature_promotion(exc):
    conn = Conn()
    report = hook(conn, recorder(exc=exc))
    assert report == {"status": "failed", "error": type(exc).__name__}   # class only, no text
    assert "secret" not in repr(report)
    assert conn.calls[-1] == "rollback"      # only the scoring transaction is abandoned


def test_lease_acquisition_failure_is_also_contained():
    def acquire(c, owner, ttl):
        raise RuntimeError("db down")
    report = score_after_promotion(Conn(), 1, limit=5, owner="s", acquire=acquire,
                                   score=recorder())
    assert report == {"status": "failed", "error": "RuntimeError"}


def test_hook_runs_after_the_promotion_and_is_wrapped_in_apply_batch():
    src = inspect.getsource(stream_cli.run_consumer)
    assert "promote_then_score(" in src and "process_canonical_microbatch(" in src
    assert "score_limit_per_cycle" in src


def test_hook_uses_a_short_bounded_lease():
    seen = []
    score_after_promotion(Conn(), 1, limit=5, owner="s", score=recorder(),
                          acquire=lambda c, o, ttl: seen.append(ttl) or LEASE)
    assert seen and 1 <= seen[0] <= 300


def test_failed_promotion_schedules_nothing():
    score = recorder()

    def promote():
        raise LeaseRejected("lost")
    with pytest.raises(LeaseRejected):
        stream_cli.promote_then_score(Conn(), promote, limit=5, owner="s", score=score,
                                      acquire=lambda c, o, t: LEASE)
    assert score.calls == []
    assert stream_cli.promote_then_score(Conn(), lambda: 0, limit=5, owner="s", score=score,
                                         acquire=lambda c, o, t: LEASE) == (0, None)
    assert score.calls == []


# ---- config: scoring.limit_per_cycle ---------------------------------------------------------
def test_score_limit_defaults_to_a_small_bound_and_validates():
    assert base_config().score_limit_per_cycle == 50
    assert base_config(score_limit_per_cycle=None).score_limit_per_cycle is None
    for good in (1, 10000):
        base_config(score_limit_per_cycle=good).validate()
    for bad in (0, -1, 10001, True, "5", 1.5):
        with pytest.raises(ValueError, match="score_limit_per_cycle"):
            base_config(score_limit_per_cycle=bad).validate()


def test_cli_flags_map_to_config(monkeypatch):
    seen = {}
    monkeypatch.setattr(stream_cli, "run_consumer", lambda c: seen.setdefault("c", c))
    common = ["--dsn", "d", "--bootstrap-servers", "b", "--topic", "t", "--checkpoint", "/tmp/c",
              "--topic-incarnation", "i", "--replay-epoch", "0", "--environment-id", "e",
              "--query-id", "q", "--pipeline-epoch", "p"]
    assert stream_cli.main(common) == 0
    assert seen["c"].score_limit_per_cycle == 50
    seen.clear()
    assert stream_cli.main(common + ["--score-limit-per-cycle", "7"]) == 0
    assert seen["c"].score_limit_per_cycle == 7
    seen.clear()
    assert stream_cli.main(common + ["--no-score-after-promotion"]) == 0
    assert seen["c"].score_limit_per_cycle is None
