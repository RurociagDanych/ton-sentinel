"""Bounded score runner (first-score + rescore) and the ``score`` CLI."""

import inspect
import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

import ton_sentinel.cli as cli
from ton_sentinel.db import work
from ton_sentinel.db.fencing import LeaseRejected


class Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None


class FakeConn:
    """Answers the runner's SELECTs; records every statement."""
    def __init__(self, unscored=(), stale_count=0, unreadable=False):
        self.unscored = list(unscored)
        self.stale_count = stale_count
        self.unreadable = unreadable
        self.sql = []
        self.info = SimpleNamespace(transaction_status=SimpleNamespace(name="IDLE"))

    def commit(self):
        return None

    @contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=None):
        self.sql.append(sql)
        text = " ".join(sql.split())
        if "count(" in text:
            if self.unreadable:
                import psycopg
                raise psycopg.errors.InsufficientPrivilege("denied")
            if "FROM served_features" in text:
                return Result([(len(self.unscored),)])
            return Result([(self.stale_count,)])
        assert "FROM served_features" in text, text
        return Result(self.unscored[:params["l"]])


LEASE = object()


@pytest.fixture
def calls(monkeypatch):
    log = SimpleNamespace(scored=[], promoted=[], stale_limits=[])

    def score_window(connection, network, token, start):
        log.scored.append((network, token, start))
        return ("baseline", token, start), ("r1", "r2")

    def promote_scores(connection, lease, baseline, results):
        log.promoted.append(baseline[1:])
        connection.unscored[:] = [k for k in connection.unscored
                                  if (k[1], k[2]) != (baseline[1], baseline[2])]
        return True

    def rescore_stale(connection, lease, *, network=None, limit=100):
        log.stale_limits.append(limit)
        n = min(limit, connection.stale_count)
        connection.stale_count -= n
        return [("mainnet", f"t{i}", 300 * i) for i in range(n)]

    monkeypatch.setattr(work, "score_window", score_window)
    monkeypatch.setattr(work, "promote_scores", promote_scores)
    monkeypatch.setattr(work, "rescore_stale", rescore_stale)
    return log


def keys(n):
    return [("mainnet", f"tok{i}", 300 * i) for i in range(n)]


def test_limit_bounds_work_and_reports_remaining(calls):
    conn = FakeConn(unscored=keys(5), stale_count=4)
    report = work.score_pending(conn, LEASE, network="mainnet", limit=3)
    assert len(calls.scored) == 3 and calls.stale_limits == []
    assert report == {"first_scored": 3, "rescored": 0, "remaining_unscored": 2,
                      "remaining_stale": 4, "refused": 0}


def test_leftover_budget_goes_to_stale_rescoring(calls):
    conn = FakeConn(unscored=keys(1), stale_count=5)
    report = work.score_pending(conn, LEASE, network="mainnet", limit=3)
    assert calls.stale_limits == [2]
    assert report["first_scored"] == 1 and report["rescored"] == 2
    assert report["remaining_unscored"] == 0 and report["remaining_stale"] == 3


def test_stale_results_are_rescored_when_nothing_is_unscored(calls):
    conn = FakeConn(unscored=[], stale_count=2)
    report = work.score_pending(conn, LEASE, network=None, limit=10)
    assert calls.stale_limits == [10] and calls.scored == []
    assert report["rescored"] == 2 and report["remaining_stale"] == 0


def test_second_run_is_a_no_op(calls):
    conn = FakeConn(unscored=keys(2), stale_count=0)
    first = work.score_pending(conn, LEASE, network="mainnet", limit=10)
    scored_once = list(calls.scored)
    second = work.score_pending(conn, LEASE, network="mainnet", limit=10)
    assert first["first_scored"] == 2 and len(scored_once) == 2
    assert second == {"first_scored": 0, "rescored": 0, "remaining_unscored": 0,
                      "remaining_stale": 0, "refused": 0}
    assert calls.scored == scored_once


def test_refused_promotions_are_counted_not_guessed(calls, monkeypatch):
    monkeypatch.setattr(work, "promote_scores", lambda *a: False)
    monkeypatch.setattr(work, "validate_transition", lambda c, l: None)
    conn = FakeConn(unscored=keys(2))
    report = work.score_pending(conn, LEASE, network="mainnet", limit=5)
    assert report["first_scored"] == 0 and report["refused"] == 2
    assert report["remaining_unscored"] == 2


def test_unreadable_backlog_is_none_not_zero(calls):
    conn = FakeConn(unscored=keys(1), unreadable=True)
    report = work.score_pending(conn, LEASE, network="mainnet", limit=5)
    assert report["remaining_unscored"] is None and report["remaining_stale"] is None


def test_lost_lease_stops_without_partial_promotion(calls, monkeypatch):
    def lost(connection, lease, baseline, results):
        calls.promoted.append(baseline[1:])
        return False

    def reject(connection, lease):
        raise LeaseRejected("lost")

    monkeypatch.setattr(work, "promote_scores", lost)
    monkeypatch.setattr(work, "validate_transition", reject)
    conn = FakeConn(unscored=keys(3), stale_count=1)
    with pytest.raises(LeaseRejected):
        work.score_pending(conn, LEASE, network="mainnet", limit=5)
    assert len(calls.scored) == 1 and calls.stale_limits == []


@pytest.mark.parametrize("kwargs", [{"limit": 0}, {"limit": 10001}, {"limit": True},
                                    {"limit": "5"}, {"limit": 5, "network": "devnet"}])
def test_bad_limit_or_network_is_rejected_before_any_query(calls, kwargs):
    conn = FakeConn()
    params = {"network": "mainnet", **kwargs}
    with pytest.raises(ValueError):
        work.score_pending(conn, LEASE, **params)
    assert conn.sql == []


def test_no_direct_writes_outside_promote_scores(calls):
    source = inspect.getsource(work.score_pending).upper()
    for verb in ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE", "ALTER ", "DROP ", "CREATE "):
        assert verb not in source
    conn = FakeConn(unscored=keys(2), stale_count=1)
    work.score_pending(conn, LEASE, network="mainnet", limit=5)
    for sql in conn.sql:
        assert " ".join(sql.split()).upper().startswith("SELECT"), sql


class FakePg:
    def __init__(self):
        self.dsn = None

    def __call__(self, dsn, *a, **k):
        self.dsn = dsn
        return self

    def __enter__(self):
        return FakeConn()

    def __exit__(self, *exc):
        return False


@pytest.fixture
def cli_env(monkeypatch):
    import psycopg
    pg = FakePg()
    monkeypatch.setattr(psycopg, "connect", pg)
    monkeypatch.setattr("ton_sentinel.db.fencing.acquire_transition",
                        lambda conn, owner, ttl=30: LEASE)
    monkeypatch.delenv("SENTINEL_DATABASE_DSN", raising=False)
    return pg


def test_cli_prints_one_json_report_and_exits_zero(cli_env, monkeypatch, capsys):
    seen = {}

    def fake(connection, lease, *, network, limit):
        seen.update(network=network, limit=limit)
        return {"first_scored": 1, "rescored": 0, "remaining_unscored": 0,
                "remaining_stale": 0, "refused": 0}

    monkeypatch.setattr(work, "score_pending", fake)
    code = cli.main(["score", "--owner", "o", "--network", "mainnet", "--limit", "7",
                     "--dsn", "postgresql://u:pw@h/db"])
    out = capsys.readouterr().out
    assert code == 0 and json.loads(out)["first_scored"] == 1
    assert seen == {"network": "mainnet", "limit": 7}


@pytest.mark.parametrize("extra", [["--limit", "0"], ["--limit", "10001"], ["--limit", "x"],
                                   ["--limit", "5", "--network", "devnet"]])
def test_cli_rejects_bad_limit_and_network(cli_env, extra, capsys):
    argv = ["score", "--owner", "o", "--dsn", "postgresql://h/db"]
    if "--network" not in extra:
        argv += ["--network", "mainnet"]
    with pytest.raises(SystemExit) as exc:
        cli.main(argv + extra)
    assert exc.value.code == 2
    assert cli_env.dsn is None


def test_cli_lost_lease_exits_two(cli_env, monkeypatch, capsys):
    def lost(*a, **k):
        raise LeaseRejected("lost")

    monkeypatch.setattr(work, "score_pending", lost)
    code = cli.main(["score", "--owner", "o", "--network", "mainnet", "--limit", "5",
                     "--dsn", "postgresql://h/db"])
    assert code == 2
    assert json.loads(capsys.readouterr().out)["error"] == "LeaseRejected"


def test_cli_error_output_has_no_dsn(cli_env, monkeypatch, capsys):
    def boom(*a, **k):
        raise RuntimeError("could not connect to postgresql://user:SECRETPW@host/db")

    monkeypatch.setattr(work, "score_pending", boom)
    code = cli.main(["score", "--owner", "o", "--network", "mainnet", "--limit", "5",
                     "--dsn", "postgresql://user:SECRETPW@host/db"])
    captured = capsys.readouterr()
    assert code == 1
    assert "SECRETPW" not in captured.out + captured.err
    assert json.loads(captured.out)["error"] == "RuntimeError"


def test_cli_requires_a_dsn(cli_env, capsys):
    code = cli.main(["score", "--owner", "o", "--network", "mainnet", "--limit", "5"])
    assert code == 2
