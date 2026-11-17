"""Bounded read-only detector/alert views over real HTTP + real PostgreSQL."""
import json
import threading
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest

from ton_sentinel.api import create_app
from ton_sentinel.db.work import promote_scores, rescore_stale, score_window
from tests.integration.test_baseline_grid_pg import promote_rows, row
from tests.integration.test_detection import (  # noqa: F401  (fixture + helpers)
    COUNT, NET, SCORED, TOK, WALLETS, alert_insert, ready_world,
)
from tests.integration.test_revisions import db  # noqa: F401

SECRET_REASON_PASSWORD = "reason-pw-9f3"
SECRET_OWNER = "lease-owner-secret-77"


def _dsn(db_connection):
    import os
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


def get(base, path, query=None, method="GET"):
    url = base + path + ("?" + urlencode(query) if query else "")
    try:
        with urlopen(Request(url, method=method), timeout=8) as response:
            return response.status, json.load(response), dict(response.headers)
    except HTTPError as error:
        return error.code, json.load(error), dict(error.headers)


def key(**extra):
    return {"network": NET, "token": TOK, "window_start": SCORED, **extra}


def promote_first(db):
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()
    return lease, inp


def test_current_views_serve_fresh_results_and_active_alerts_exactly(served):
    db, base = served
    promote_first(db)
    status, body, headers = get(base, "/v1/detections", key())
    assert status == 200 and headers["Cache-Control"] == "no-store"
    docs = {d["detector"]: d for d in body["detections"]}
    assert set(docs) == {COUNT, WALLETS} and body["truncated"] is False
    assert all(d["token"] == TOK and d["triggered"] is True for d in docs.values())
    status, body, _ = get(base, "/v1/alerts", key())
    assert status == 200
    assert {a["detector"] for a in body["alerts"]} == {COUNT, WALLETS}
    assert all(a["state"] == "active" and a["token"] == TOK and a["window_start"] == SCORED
               for a in body["alerts"])
    # exact business identifier filter: another token matches nothing
    assert get(base, "/v1/alerts", key(token="jetton:other"))[1]["alerts"] == []


def test_current_excludes_pending(served):
    """Stale/pending revisions are never current: a correction pends the alert at once."""
    db, base = served
    lease, _ = promote_first(db)
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    db.commit()
    state = db.execute("SELECT state FROM alert_revisions WHERE detector_id=%s "
                       "ORDER BY alert_revision DESC LIMIT 1", (COUNT,)).fetchone()[0]
    db.commit()
    assert state == "pending"
    assert get(base, "/v1/alerts", key())[1]["alerts"] == []
    assert get(base, "/v1/detections", key())[1]["detections"] == []  # stale result excluded
    # history still shows the pending revision (audit), readable
    history = get(base, "/v1/alerts/history", key(detector=COUNT))[1]["history"]
    assert [h["state"] for h in history] == ["active", "pending"]


def test_recomputed_retraction_is_current_again_but_alert_is_gone(served):
    db, base = served
    lease, _ = promote_first(db)
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    db.commit()
    detections = get(base, "/v1/detections", key())[1]["detections"]
    assert {d["detector"] for d in detections} == {COUNT, WALLETS}
    assert all(d["triggered"] is False for d in detections)
    assert get(base, "/v1/alerts", key())[1]["alerts"] == []
    history = get(base, "/v1/alerts/history", key(detector=COUNT))[1]["history"]
    assert [h["state"] for h in history] == ["active", "pending", "retracted"]
    assert [h["alert_revision"] for h in history] == [1, 2, 3]


def test_pending_status_result_row_is_not_current(served):
    """A fresh pointer at a detector_results row with status 'pending' is not served."""
    db, base = served
    promote_first(db)
    db.execute(
        "INSERT INTO detector_results(network,token,direction,window_start,detector_id,"
        "result_revision,input_digest,policy_id,dependency_epoch,status,triggered,score,result,"
        "lease_owner,lease_fence) SELECT network,token,direction,window_start,detector_id,2,"
        "input_digest,policy_id,dependency_epoch,'pending',false,NULL,"
        "jsonb_set(jsonb_set(jsonb_set(result,'{status}','\"pending\"'),'{triggered}','false'),"
        "'{score}','null'),'raw',1 FROM detector_results WHERE detector_id=%s AND result_revision=1",
        (COUNT,))
    db.execute("UPDATE detector_current SET result_revision=2 WHERE detector_id=%s", (COUNT,))
    db.commit()
    stale = db.execute("SELECT stale FROM detector_current WHERE detector_id=%s",
                       (COUNT,)).fetchone()[0]
    db.commit()
    assert stale is False
    served_ids = {d["detector"] for d in get(base, "/v1/detections", key())[1]["detections"]}
    assert served_ids == {WALLETS}


def test_alert_history_is_readable_and_redacted(served):
    db, base = served
    lease, inp = promote_first(db)
    db.execute("INSERT INTO alert_revisions(network,token,direction,window_start,detector_id,"
               "alert_revision,state,result_revision,input_digest,dependency_epoch,reason,"
               "lease_owner,lease_fence) VALUES (%s,%s,'both',%s,%s,2,'pending',1,%s,%s,%s,%s,9)",
               (NET, TOK, SCORED, COUNT, inp.digest, inp.dependency_epoch,
                f"retry postgresql://sentinel:{SECRET_REASON_PASSWORD}@db:5432/x password={SECRET_REASON_PASSWORD}",
                SECRET_OWNER))
    db.commit()
    status, body, _ = get(base, "/v1/alerts/history", key(detector=COUNT))
    text = json.dumps(body)
    assert status == 200 and [h["alert_revision"] for h in body["history"]] == [1, 2]
    assert SECRET_REASON_PASSWORD not in text and SECRET_OWNER not in text
    assert "lease_owner" not in text and "lease_fence" not in text
    assert body["history"][1]["input_digest"] == inp.digest  # identifiers exact
    assert "[REDACTED]" in body["history"][1]["reason"]


def test_every_new_route_is_get_only_over_http_and_changes_nothing(served):
    db, base = served
    promote_first(db)
    before = db.execute("SELECT (SELECT count(*) FROM alert_revisions),"
                        "(SELECT count(*) FROM detector_results),"
                        "(SELECT count(*) FROM detector_current)").fetchone()
    db.commit()
    for path in ("/v1/detections", "/v1/alerts", "/v1/alerts/history"):
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            status, body, headers = get(base, path, key(detector=COUNT), method=method)
            assert status == 405 and headers["Allow"] == "GET" and body == {"error": "read_only"}
    after = db.execute("SELECT (SELECT count(*) FROM alert_revisions),"
                       "(SELECT count(*) FROM detector_results),"
                       "(SELECT count(*) FROM detector_current)").fetchone()
    db.commit()
    assert after == before


def test_limits_are_enforced_and_truncation_is_reported(served):
    db, base = served
    promote_first(db)
    status, body, _ = get(base, "/v1/detections", key(limit=1))
    assert status == 200 and len(body["detections"]) == 1 and body["truncated"] is True
    status, body, _ = get(base, "/v1/alerts", key(limit=1))
    assert len(body["alerts"]) == 1 and body["truncated"] is True
    status, body, _ = get(base, "/v1/alerts", key(limit=2))
    assert len(body["alerts"]) == 2 and body["truncated"] is False
    for bad in (0, 1001):
        assert get(base, "/v1/alerts", key(limit=bad))[0] == 400
        assert get(base, "/v1/detections", key(limit=bad))[0] == 400
        assert get(base, "/v1/alerts/history", key(detector=COUNT, limit=bad))[0] == 400
    # deterministic order: repeated reads are identical and detector-ordered
    first = get(base, "/v1/alerts", key())[1]
    assert first == get(base, "/v1/alerts", key())[1]
    assert [a["detector"] for a in first["alerts"]] == sorted(a["detector"] for a in first["alerts"])


def test_history_limit_truncates_oldest_first(served):
    db, base = served
    lease, _ = promote_first(db)
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    status, body, _ = get(base, "/v1/alerts/history", key(detector=COUNT, limit=1))
    assert status == 200 and [h["alert_revision"] for h in body["history"]] == [1]
    assert body["truncated"] is True


def test_errors_never_contain_the_dsn_password(db):
    base_dsn = f"postgresql://probe:dsn-secret-pw-1@127.0.0.1:1/none"
    server = make_server("127.0.0.1", 0, create_app(dsn=base_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_port}"
        for path, query in (("/v1/detections", {"network": NET}), ("/v1/alerts", {"network": NET}),
                            ("/v1/alerts/history", key(detector=COUNT))):
            status, body, headers = get(base, path, query)
            assert status == 503 and body == {"error": "unavailable"}
            assert "dsn-secret-pw-1" not in json.dumps(body) + json.dumps(headers)
        # the unauthenticated status payload still reports honest limitation without secrets
        status, body, _ = get(base, "/status")
        assert "dsn-secret-pw-1" not in json.dumps(body) and "interim_limitation" in body
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
