"""Demo and /status never inflate certification; gates need evidence; labels present."""
import json
import re
from datetime import datetime, timezone
from unittest.mock import patch

from ton_sentinel.api import create_app

LATEST = datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc)


class _Result:
    def __init__(self, one=None, many=None):
        self._one, self._many = one, many

    def fetchone(self):
        return self._one

    def fetchall(self):
        return self._many


class _Connection:
    """Answers by statement text, like the real tables would."""

    def __init__(self, groups=(), qualification=(), fail_qualification=False):
        self.groups, self.qualification = list(groups), list(qualification)
        self.fail_qualification = fail_qualification
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        self.statements.append(statement)
        if "action_window_qualification" in statement:
            if self.fail_qualification:
                raise RuntimeError("relation missing postgres://u:hunter2pw@h/db")
            return _Result(many=self.qualification)
        if "GROUP BY" in statement and "served_features" in statement:
            return _Result(many=self.groups)
        if "max(window_end)" in statement:
            return _Result(one=(sum(g[3] for g in self.groups), 1, 300, LATEST))
        return _Result()


def _get(app, path):
    out = {}
    body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": path, "QUERY_STRING": ""},
                        lambda status, headers: out.update(status=status)))
    return out["status"], body.decode("utf-8")


def _status(connection):
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", return_value=connection):
        status, body = _get(app, "/status")
    return status, json.loads(body), body


def _words(payload):
    """Every string value and key anywhere in the payload."""
    if isinstance(payload, dict):
        for k, v in payload.items():
            yield k
            yield from _words(v)
    elif isinstance(payload, list):
        for v in payload:
            yield from _words(v)
    elif isinstance(payload, str):
        yield payload


def test_gates_without_evidence_are_not_qualified_and_nothing_says_passed():
    status, payload, body = _status(_Connection())
    assert status == "200 OK"
    gates = payload["gates"]
    assert gates["stateful_streaming"]["status"] == "not_qualified"
    assert gates["live_source"]["status"] == "not_qualified"
    assert gates["stateful_streaming"]["evidence"] is None
    assert gates["live_source"]["evidence"] is None
    forbidden = {"passed", "pass", "certified", "qualified", "live_certified", "ok_certified"}
    assert not forbidden & {w.lower() for w in _words(payload["gates"])}
    # the older interim limitation and the new gate must agree
    assert payload["interim_limitation"]["stateful_streaming"] == "not_qualified"


def test_qualification_records_are_labelled_synthetic_and_never_inflate_coverage():
    rows = [("bounded_observed", True, 7), ("bounded_observed", False, 2), ("certified", False, 1)]
    _, payload, _ = _status(_Connection(qualification=rows))
    q = payload["qualification"]
    assert q["status"] == "observed"
    assert q["bounded_observed_synthetic"] == 7
    assert q["bounded_observed_not_synthetic"] == 2
    assert q["certified_recorded"] == 1
    assert "synthetic" in q["label"] and "not verified" in q["label"]
    # recorded counts do not move overall coverage, readiness or gates
    assert payload["coverage"]["status"] == "unknown"
    assert payload["readiness"] == "unknown"
    assert payload["gates"]["live_source"]["status"] == "not_qualified"


def test_served_rows_are_grouped_by_state_with_origin_unverified_label():
    groups = [("final", False, "bounded_observed", 3), ("provisional", True, None, 4)]
    _, payload, _ = _status(_Connection(groups=groups))
    served = payload["served_data"]
    assert served["rows_by_state"] == [
        {"status": "final", "provisional": False, "coverage_status": "bounded_observed", "rows": 3},
        {"status": "provisional", "provisional": True, "coverage_status": None, "rows": 4},
    ]
    assert payload["feature_rows"] == 7
    assert served["origin"] == "unverified"
    assert "fixture" in served["label"] and "not live" in served["label"]


def test_empty_database_reports_empty_not_invented_data():
    _, payload, _ = _status(_Connection())
    assert payload["served_data"]["rows_by_state"] == []
    assert payload["qualification"]["bounded_observed_synthetic"] == 0
    assert payload["qualification"]["certified_recorded"] == 0


def test_unreadable_qualification_is_unavailable_not_zero_and_leaks_nothing():
    status, payload, body = _status(_Connection(fail_qualification=True))
    assert status == "200 OK"
    q = payload["qualification"]
    assert q["status"] == "unavailable"
    assert "bounded_observed_synthetic" not in q
    assert "hunter2pw" not in body


def test_database_down_keeps_gates_not_qualified_and_served_data_unavailable():
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", side_effect=RuntimeError("down")):
        status, body = _get(app, "/status")
    payload = json.loads(body)
    assert status == "503 Service Unavailable"
    assert payload["gates"]["stateful_streaming"]["status"] == "not_qualified"
    assert payload["gates"]["live_source"]["status"] == "not_qualified"
    assert "served_data" not in payload or payload["served_data"].get("rows_by_state") is None
    assert payload["qualification"]["status"] == "unavailable"


def test_demo_renders_gates_labels_and_only_served_counts():
    groups = [("final", False, "bounded_observed", 3), ("provisional", True, None, 4)]
    rows = [("bounded_observed", True, 7)]
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect",
               return_value=_Connection(groups=groups, qualification=rows)):
        status, html = _get(app, "/")
    text = " ".join(re.sub(r"<[^>]+>", "", html).split())
    assert status == "200 OK"
    assert "Stateful streaming gate" in text and "Live source gate" in text
    assert text.count("not_qualified") >= 2
    assert "synthetic" in text and "7" in text
    assert "provisional" in text and "unverified" in text
    assert "certified_recorded" in text
    for forbidden in ("PASS", "passed", "live-certified)"):
        assert forbidden not in text.replace("not live-certified", "")


def test_demo_escapes_and_redacts_database_text():
    groups = [("<script>alert(1)</script>", False, "password=hunter2pw", 1)]
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", return_value=_Connection(groups=groups)):
        _, html = _get(app, "/")
        _, body = _get(app, "/status")
    assert "<script>alert" not in html
    assert "hunter2pw" not in html and "hunter2pw" not in body


def test_demo_with_database_down_shows_gates_and_no_served_numbers():
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", side_effect=RuntimeError("down")):
        status, html = _get(app, "/")
    text = " ".join(re.sub(r"<[^>]+>", "", html).split())
    assert status == "503 Service Unavailable"
    assert text.count("not_qualified") >= 2
    assert "unavailable" in text
