"""/status reports pending outbox and pending repair from PostgreSQL.

An unreadable source is ``unavailable`` (never zero); uninstrumented domains stay ``unsupported``.
"""
import json
from unittest.mock import patch

from ton_sentinel.api import create_app

# Spark_checkpoint, maintenance and rates are read from PostgreSQL; disk only
# with the volume probe (not configured here).
UNINSTRUMENTED = {"spark_watermark", "spark_state", "disk"}


class _Result:
    def __init__(self, one=None, many=None):
        self._one, self._many = one, many

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _Connection:
    def __init__(self, outbox=(0, None), repair=(0,), fail=()):
        self.outbox, self.repair, self.fail = outbox, repair, set(fail)

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        if "FROM outbox" in statement:
            if "outbox" in self.fail:
                raise RuntimeError("permission denied postgres://u:hunter2secret@h/db")
            return _Result(many=[self.outbox])
        if "FROM dirty_windows" in statement:
            if "repair" in self.fail:
                raise RuntimeError("permission denied")
            return _Result(many=[self.repair])
        if "max(window_end)" in statement:
            return _Result(one=(0, 0, None, None))
        return _Result(many=[])


def _status(connection):
    app = create_app(dsn="explicit-test-dsn")
    out = {}
    with patch("ton_sentinel.api.psycopg.connect", return_value=connection):
        body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/status", "QUERY_STRING": ""},
                            lambda status, headers: out.update(status=status)))
    return out["status"], json.loads(body), body.decode("utf-8")


def test_pending_outbox_and_repair_are_observed_counts():
    status, payload, _ = _status(_Connection(outbox=(3, 42.5), repair=(2,)))
    assert status == "200 OK"
    assert payload["operational_domains"]["outbox"] == "observed"
    assert payload["operational_domains"]["repair"] == "observed"
    assert payload["pending_work"]["outbox"] == {
        "status": "observed", "pending": 3, "oldest_pending_age_seconds": 42.5}
    assert payload["pending_work"]["repair"] == {"status": "observed", "pending_windows": 2}


def test_zero_pending_is_a_real_observed_zero_with_no_age():
    _, payload, _ = _status(_Connection(outbox=(0, None), repair=(0,)))
    assert payload["pending_work"]["outbox"] == {
        "status": "observed", "pending": 0, "oldest_pending_age_seconds": None}
    assert payload["pending_work"]["repair"]["pending_windows"] == 0


def test_unreadable_outbox_or_repair_is_unavailable_not_zero_and_leaks_nothing():
    status, payload, raw = _status(_Connection(fail=("outbox", "repair")))
    assert status == "200 OK"
    assert payload["operational_domains"]["outbox"] == "unavailable"
    assert payload["operational_domains"]["repair"] == "unavailable"
    assert payload["pending_work"]["outbox"] == {"status": "unavailable"}
    assert payload["pending_work"]["repair"] == {"status": "unavailable"}
    assert "hunter2secret" not in raw


def test_one_unreadable_source_does_not_hide_the_other():
    _, payload, _ = _status(_Connection(outbox=(1, 5.0), fail=("repair",)))
    assert payload["pending_work"]["outbox"]["pending"] == 1
    assert payload["pending_work"]["repair"] == {"status": "unavailable"}


def test_uninstrumented_domains_stay_unsupported():
    _, payload, _ = _status(_Connection())
    domains = payload["operational_domains"]
    assert {name: domains[name] for name in UNINSTRUMENTED} == {n: "unsupported" for n in UNINSTRUMENTED}
    # This fake returns no stream_batches / retention_runs rows (none recorded: observed) and
    # routes the rates statement to its outbox row, which does not parse (unavailable).
    assert (domains["spark_checkpoint"], domains["maintenance"], domains["rates"]) == (
        "observed", "observed", "unavailable")
    assert payload["backup"] == "not_configured"


def test_database_down_omits_pending_work_and_marks_domains_unavailable():
    app = create_app(dsn="explicit-test-dsn")
    out = {}
    with patch("ton_sentinel.api.psycopg.connect", side_effect=RuntimeError("down")):
        b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": "/status", "QUERY_STRING": ""},
                     lambda status, headers: out.update(status=status)))
    assert out["status"] == "503 Service Unavailable"
