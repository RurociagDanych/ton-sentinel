"""Dashboard API on real PostgreSQL over HTTP, through the sentinel_reader login."""

import json
import threading
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from ton_sentinel.db.work import promote_scores, rescore_stale, score_window
from tests.integration.test_baseline_grid_pg import promote_rows, row
from tests.integration.test_detection import COUNT, NET, SCORED, TOK, WALLETS, ready_world
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401

RANGE = {"from": SCORED - 3600, "to": SCORED + 3600}


@pytest.fixture
def served(db, reader_dsn):  # noqa: F811
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get(base, path, query):
    url = base + path + "?" + urlencode(query)
    try:
        with urlopen(Request(url), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def promote_first(db):
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()
    return lease


def rng(**over):
    return {"network": NET, "token": TOK, **RANGE, **over}


def rev(detector=COUNT, **over):
    return {"network": NET, "token": TOK, "window_start": SCORED, "detector": detector, **over}


def walk(base, path, name, query):
    items, cursor = [], None
    for _ in range(20):
        status, body = get(base, path, {**query, **({"cursor": cursor} if cursor else {})})
        assert status == 200, body
        items += body[name]
        cursor = body.get("next_cursor")
        if cursor is None:
            return items
    raise AssertionError("cursor walk did not terminate")


def test_range_returns_current_results_with_provenance_and_half_open_bounds(served):
    db, base = served
    promote_first(db)
    status, body = get(base, "/v1/detections/range", rng())
    assert status == 200
    assert {d["detector"] for d in body["detections"]} == {COUNT, WALLETS}
    assert all(d["window_start"] == SCORED and d["token"] == TOK for d in body["detections"])
    assert body["provenance"]["certification_state"] == "not_certified"
    assert body["provenance"]["qualification_class"] != "unavailable"
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert "lease_owner" not in json.dumps(body)
    assert get(base, "/v1/detections/range", rng(**{"to": SCORED}))[1]["detections"] == []
    assert len(get(base, "/v1/detections/range",
                   rng(**{"from": SCORED, "to": SCORED + 300}))[1]["detections"]) == 2
    assert get(base, "/v1/detections/range",
               rng(**{"from": SCORED + 300, "to": SCORED + 600}))[1]["detections"] == []
    assert get(base, "/v1/detections/range", rng(token="jetton:none"))[1]["detections"] == []
    assert get(base, "/v1/detections/range", rng(network="mainnet"))[1]["detections"] == []


def test_range_bounds_are_400_and_need_token(served):
    _, base = served
    for bad in (rng(token=""), {k: v for k, v in rng().items() if k != "token"},
                rng(**{"to": SCORED - 3600}), rng(**{"from": SCORED + 1}),
                rng(**{"to": SCORED - 3600 + 604800 + 300}), rng(limit=0), rng(limit=1001)):
        assert get(base, "/v1/detections/range", bad)[0] == 400, bad


def test_range_pages_each_row_once_and_cursor_is_bound(served):
    db, base = served
    promote_first(db)
    status, first = get(base, "/v1/detections/range", rng(limit=1))
    assert status == 200 and first["truncated"] is True and len(first["detections"]) == 1
    paged = walk(base, "/v1/detections/range", "detections", rng(limit=1))
    full = walk(base, "/v1/detections/range", "detections", rng(limit=1000))
    assert paged == full and len(paged) == 2
    assert len({(d["window_start"], d["detector"]) for d in paged}) == 2
    cursor = first["next_cursor"]
    assert get(base, "/v1/detections/range", rng(limit=1, cursor=cursor))[0] == 200
    assert get(base, "/v1/detections/range",
               rng(limit=1, cursor=cursor, **{"to": SCORED + 7200}))[0] == 400
    assert get(base, "/v1/detections/range", rng(limit=1, cursor=cursor, token="jetton:x"))[0] == 400


def test_detection_range_still_hides_stale_and_pending_real_data(served):
    db, base = served
    lease = promote_first(db)
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    db.commit()
    assert get(base, "/v1/detections/range", rng())[1]["detections"] == []
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    db.commit()
    now = get(base, "/v1/detections/range", rng())[1]["detections"]
    assert {d["detector"] for d in now} == {COUNT, WALLETS}
    assert all(d["triggered"] is False for d in now)
    db.execute(
        "INSERT INTO detector_results(network,token,direction,window_start,detector_id,"
        "result_revision,input_digest,policy_id,dependency_epoch,status,triggered,score,result,"
        "lease_owner,lease_fence) SELECT network,token,direction,window_start,detector_id,3,"
        "input_digest,policy_id,dependency_epoch,'pending',false,NULL,"
        "jsonb_set(jsonb_set(jsonb_set(result,'{status}','\"pending\"'),'{triggered}','false'),"
        "'{score}','null'),'raw',1 FROM detector_results WHERE detector_id=%s "
        "AND result_revision=2", (COUNT,))
    db.execute("UPDATE detector_current SET result_revision=3 WHERE detector_id=%s", (COUNT,))
    db.commit()
    assert {d["detector"] for d in get(base, "/v1/detections/range", rng())[1]["detections"]} \
        == {WALLETS}
    status, body = get(base, "/v1/detections/revisions", rev())
    labels = {r["result_revision"]: r["label"] for r in body["revisions"]}
    assert status == 200 and labels == {1: "superseded", 2: "superseded", 3: "pending"}


def test_revisions_listing_labels_stale_current_and_superseded(served):
    db, base = served
    lease = promote_first(db)
    status, body = get(base, "/v1/detections/revisions", rev())
    assert status == 200 and [r["result_revision"] for r in body["revisions"]] == [1]
    only = body["revisions"][0]
    assert only["is_current"] is True and only["stale"] is False and only["label"] == "current"
    assert only["status"] == "scored" and only["triggered"] is True and only["alert_state"]
    assert only["input_digest"].startswith("sha256:") and only["promoted_at"].endswith("Z")
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    db.commit()
    only = get(base, "/v1/detections/revisions", rev())[1]["revisions"][0]
    assert only["is_current"] is True and only["stale"] is True and only["label"] == "stale"
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    db.commit()
    status, body = get(base, "/v1/detections/revisions", rev())
    revs = body["revisions"]
    assert [r["result_revision"] for r in revs] == [1, 2]
    assert [(r["is_current"], r["stale"], r["label"]) for r in revs] == [
        (False, True, "superseded"), (True, False, "current")]
    assert revs[0]["triggered"] is True and revs[1]["triggered"] is False
    assert revs[1]["alert_state"] == "retracted"
    assert body["provenance"]["certification_state"] == "not_certified"
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    blob = json.dumps(body)
    assert "lease_owner" not in blob and "lease_fence" not in blob


def test_revisions_ordered_cursor_bound_and_validated(served):
    db, base = served
    lease = promote_first(db)
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    rescore_stale(db, lease, network=NET)
    db.commit()
    status, first = get(base, "/v1/detections/revisions", rev(limit=1))
    assert status == 200 and first["truncated"] and [r["result_revision"]
                                                    for r in first["revisions"]] == [1]
    paged = walk(base, "/v1/detections/revisions", "revisions", rev(limit=1))
    assert [r["result_revision"] for r in paged] == [1, 2]
    cursor = first["next_cursor"]
    assert get(base, "/v1/detections/revisions", rev(limit=1, cursor=cursor))[0] == 200
    assert get(base, "/v1/detections/revisions", rev(WALLETS, limit=1, cursor=cursor))[0] == 400
    assert get(base, "/v1/detections/revisions", rev(limit=1, cursor=cursor,
                                                      window_start=SCORED + 300))[0] == 400
    assert get(base, "/v1/detections/revisions", rev(window_start=SCORED + 300))[1][
        "revisions"] == []
    assert [r["result_revision"] for r in get(base, "/v1/detections/revisions",
                                              rev(WALLETS))[1]["revisions"]] == [1, 2]
    for bad in (rev(detector="nope"), rev(window_start=SCORED + 1), rev(limit=0),
                {k: v for k, v in rev().items() if k != "token"},
                {k: v for k, v in rev().items() if k != "detector"}):
        assert get(base, "/v1/detections/revisions", bad)[0] == 400, bad


def test_new_routes_are_get_only(served):
    db, base = served
    promote_first(db)
    before = db.execute("SELECT count(*) FROM detector_results").fetchone()[0]
    db.commit()
    for path in ("/v1/detections/range", "/v1/detections/revisions"):
        request = Request(base + path + "?" + urlencode(rng()), data=b"x", method="POST")
        with pytest.raises(HTTPError) as error:
            urlopen(request, timeout=8)
        assert error.value.code in (405, 404)
    assert db.execute("SELECT count(*) FROM detector_results").fetchone()[0] == before
    db.commit()
