"""Truthful demo + /status over real HTTP and real PostgreSQL (no certification inflation)."""
import json
import os
import re
import threading
from urllib.error import HTTPError
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest

from ton_sentinel.api import create_app
from ton_sentinel.db.work import promote_scores, score_window
from tests.integration.test_detection import NET, SCORED, TOK, ready_world  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401


def _dsn(db_connection):
    return psycopg.conninfo.make_conninfo(
        os.environ["TON_SENTINEL_TEST_DSN"], options=f"-c search_path={db_connection.schema_name}")


@pytest.fixture
def served(db):
    server = make_server("127.0.0.1", 0, create_app(dsn=_dsn(db)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def fetch(base, path):
    try:
        with urlopen(base + path, timeout=8) as response:
            return response.status, response.read().decode("utf-8")
    except HTTPError as error:
        return error.code, error.read().decode("utf-8")


def status_json(base):
    code, body = fetch(base, "/status")
    return code, json.loads(body), body


def demo_text(base):
    code, html = fetch(base, "/")
    return code, " ".join(re.sub(r"<[^>]+>", " ", html).split()), html


def assert_gates_honest(payload):
    assert payload["gates"]["stateful_streaming"] == {
        **payload["gates"]["stateful_streaming"], "status": "not_qualified", "evidence": None}
    assert payload["gates"]["live_source"]["status"] == "not_qualified"
    assert payload["gates"]["live_source"]["evidence"] is None
    assert payload["coverage"]["status"] == "unknown"
    assert payload["readiness"] == "unknown"


def test_empty_database_reports_empty_and_unqualified_gates(served):
    db, base = served
    code, payload, _ = status_json(base)
    assert code == 200
    assert payload["served_data"]["rows_by_state"] == []
    assert payload["feature_rows"] == 0
    assert payload["qualification"]["status"] == "observed"
    assert payload["qualification"]["bounded_observed_synthetic"] == 0
    assert payload["qualification"]["certified_recorded"] == 0
    assert_gates_honest(payload)
    code, text, _ = demo_text(base)
    assert code == 200 and text.count("not_qualified") >= 2


def test_demo_and_status_show_exactly_the_served_rows_and_label_synthetic_evidence(served):
    db, base = served
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()
    total = db.execute("SELECT count(*) FROM served_features").fetchone()[0]
    synthetic = db.execute("SELECT count(*) FROM action_window_qualification "
                           "WHERE synthetic AND mode='bounded_observed'").fetchone()[0]
    expected_groups = db.execute(
        "SELECT status, provisional, coverage_status, count(*) FROM served_features "
        "GROUP BY 1,2,3").fetchall()
    db.commit()
    assert total > 0 and synthetic > 0
    code, payload, _ = status_json(base)
    assert code == 200
    assert payload["feature_rows"] == total
    got = {(g["status"], g["provisional"], g["coverage_status"]): g["rows"]
           for g in payload["served_data"]["rows_by_state"]}
    assert got == {(s, p, c): n for s, p, c, n in expected_groups}
    assert sum(got.values()) == total
    q = payload["qualification"]
    assert q["bounded_observed_synthetic"] == synthetic
    assert q["bounded_observed_not_synthetic"] == 0 and q["certified_recorded"] == 0
    assert "synthetic" in q["label"]
    assert payload["served_data"]["origin"] == "unverified"
    assert_gates_honest(payload)
    code, text, html = demo_text(base)
    assert code == 200
    for label, value in (("Observed served-feature rows", total),
                         ("Synthetic (fixture) bounded_observed", synthetic),
                         ("Non-synthetic bounded_observed", 0),
                         ("certified_recorded (unverified claims)", 0)):
        assert re.search(re.escape(label) + rf"\s*{value}(?!\d)", text), (label, value)
    assert "unverified" in text and text.count("not_qualified") >= 2
    assert "jetton:" not in html  # aggregate counts only; no per-token rows


def test_a_recorded_certified_claim_is_counted_but_never_raises_coverage_or_gates(served):
    db, base = served
    world, lease = ready_world(db)
    db.commit()
    epoch = db.execute("SELECT source_epoch, dependency_epoch, lease_owner, lease_fence "
                       "FROM action_window_qualification LIMIT 1").fetchone()
    db.commit()
    try:
        db.execute(
            "INSERT INTO action_window_qualification(network,window_start,window_end,mode,"
            "mapping_version,delta_seconds,trace_ranges,source_epoch,synthetic,certification,"
            "dependency_epoch,lease_owner,lease_fence) VALUES (%s,%s,%s,'certified','m.v1',60,"
            "'[[0,1]]'::jsonb,%s,false,'{\"claim\":\"x\"}'::jsonb,%s,%s,%s)",
            (NET, 99 * 300 + 3000000, 99 * 300 + 3000300, json.dumps(epoch[0]), epoch[1], epoch[2], epoch[3]))
        db.commit()
    except psycopg.Error as error:  # a schema guard that forbids it also keeps the claim out
        db.rollback()
        pytest.skip(f"schema refuses a direct certified record: {error.sqlstate}")
    _, payload, _ = status_json(base)
    assert payload["qualification"]["certified_recorded"] == 1
    assert "not verified" in payload["qualification"]["label"]
    assert_gates_honest(payload)
    _, text, _ = demo_text(base)
    assert "not_qualified" in text and "certified_recorded" in text


def test_planted_secret_in_served_text_and_dsn_password_never_render(served):
    db, base = served
    world, lease = ready_world(db)
    db.commit()
    db.execute("UPDATE served_features SET coverage_status='password=planted-pw-31' "
               "WHERE (network,window_start,token)=(%s,%s,%s)", (NET, SCORED, TOK))
    db.commit()
    password = psycopg.conninfo.conninfo_to_dict(os.environ["TON_SENTINEL_TEST_DSN"]).get("password") or ""
    _, payload, body = status_json(base)
    _, _, html = demo_text(base)
    for rendered in (body, html):
        assert "planted-pw-31" not in rendered
        assert not password or password not in rendered


def test_unreachable_database_demo_and_status_are_503_with_unqualified_gates_and_no_password():
    dsn = "postgresql://probe:leakcheck-pw-77@127.0.0.1:1/none?connect_timeout=1"
    server = make_server("127.0.0.1", 0, create_app(dsn=dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        code, payload, body = status_json(base)
        dcode, text, html = demo_text(base)
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
    assert code == 503 and dcode == 503
    assert payload["gates"]["stateful_streaming"]["status"] == "not_qualified"
    assert payload["gates"]["live_source"]["status"] == "not_qualified"
    assert payload["qualification"]["status"] == "unavailable"
    assert "leakcheck-pw-77" not in body and "leakcheck-pw-77" not in html
