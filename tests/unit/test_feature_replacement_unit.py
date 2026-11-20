"""Unit contract: full-window replacement is delete-then-insert, never additive."""

from contextlib import contextmanager

import pytest

import ton_sentinel.db.serving as serving
from ton_sentinel.contracts import Lease
from ton_sentinel.db.serving import WindowKey, replace_features


class _Result:
    def __init__(self, row=None):
        self.row = row

    def fetchone(self):
        return self.row


class _Cursor:
    def __init__(self, log):
        self.log = log

    def executemany(self, sql, rows):
        self.log.append(("insert", [(r["window_start"], r["token"], r["swaps"], r["write_fence"]) for r in rows]))


class _Connection:
    def __init__(self):
        self.log = []
        self.committed = False

    @contextmanager
    def transaction(self):
        self.log.append(("begin",))
        yield
        self.log.append(("commit",))

    def execute(self, sql, params=None):
        if sql.startswith("SELECT max(write_fence)"):
            return _Result((None,))
        if sql.startswith("DELETE"):
            self.log.append(("delete", params))
            return _Result()
        raise AssertionError(sql)

    def cursor(self):
        return _Cursor(self.log)


def _row(start, token, swaps):
    return {"network": "testnet", "window_start": start, "window_end": start + 300, "token": token,
            "generation": "g", "swaps": swaps, "input_raw": 5, "output_raw": None, "known_traders": 1,
            "unknown_count": 0, "coverage_status": "bounded_observed", "status": "provisional",
            "provisional": True}


@pytest.fixture(autouse=True)
def _lease(monkeypatch):
    monkeypatch.setattr(serving, "validate_transition", lambda connection, lease: None)


LEASE = Lease("o", 3, __import__("datetime").datetime(2030, 1, 1, tzinfo=__import__("datetime").timezone.utc))


def test_each_touched_window_is_deleted_before_its_rows_are_inserted():
    connection = _Connection()
    assert replace_features(connection, LEASE, [_row(300, "B", 1), _row(0, "A", 2)]) == 2
    kinds = [entry[0] for entry in connection.log]
    assert kinds == ["begin", "delete", "insert", "delete", "insert", "commit"]
    assert connection.log[1][1] == ("testnet", 0, 300)
    assert connection.log[3][1] == ("testnet", 300, 600)
    assert connection.log[2][1] == [(0, "A", 2, 3)]


def test_explicit_empty_recomputation_still_deletes_prior_window():
    connection = _Connection()
    assert replace_features(connection, LEASE, [], windows=[WindowKey("testnet", 0, 300)]) == 0
    assert [entry[0] for entry in connection.log] == ["begin", "delete", "insert", "commit"]
    assert connection.log[2][1] == []


def test_rows_outside_declared_windows_and_duplicate_tokens_never_touch_the_database():
    connection = _Connection()
    with pytest.raises(ValueError, match="undeclared target"):
        replace_features(connection, LEASE, [_row(0, "A", 1)], windows=[WindowKey("testnet", 300, 600)])
    with pytest.raises(ValueError, match="duplicate token"):
        replace_features(connection, LEASE, [_row(0, "A", 1), _row(0, "A", 1)])
    assert connection.log == []
