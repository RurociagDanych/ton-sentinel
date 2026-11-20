"""Dashboard API on real PostgreSQL over HTTP, through the sentinel_reader login: /v1/alerts/feed."""

import json
import threading
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.work import promote_scores, rescore_stale, score_window
from tests.integration.test_baseline_grid_pg import ev, promote_rows, qualify, row
from tests.integration.test_detection import COUNT, NET, SCORED, W, WALLETS
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import World, db, val  # noqa: F401

TOK_A, TOK_B = "jetton:A", "jetton:B"
RANGE = {"from": SCORED - 3600, "to": SCORED + 3600}
FEED = "/v1/alerts/feed"


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


def get(base, query, path=FEED):
    url = base + path + "?" + urlencode(query)
    try:
        with urlopen(Request(url), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def q(**over):
    return {"network": NET, **RANGE, **over}


def walk(base, query):
    items, cursor = [], None
    for _ in range(40):
        status, body = get(base, {**query, **({"cursor": cursor} if cursor else {})})
        assert status == 200, body
        items += body["alerts"]
        cursor = body.get("next_cursor")
        if cursor is None:
            return items
    raise AssertionError("cursor walk did not terminate")


def seed_two_tokens(db):
    """Two tokens scored at SCORED. Token B is then corrected and rescored (retracted), so the
    fixture has active, pending-then-retracted and current revisions for several keys."""
    world = World(db)
    world.seed({ev(0): val(end=7)})
    lease = acquire_transition(db, "detect", 300)
    for i in range(24):
        assert qualify(db, lease, world, i * W) is True
    assert qualify(db, lease, world, SCORED) is True
    promote_rows(db, lease, 0, [row(0, TOK_A, 1), row(0, TOK_B, 1)])
    promote_rows(db, lease, SCORED, [row(SCORED, TOK_A, 12, 6, 0), row(SCORED, TOK_B, 12, 6, 0)])
    for token in (TOK_A, TOK_B):
        inp, results = score_window(db, NET, token, SCORED)
        db.commit()
        assert promote_scores(db, lease, inp, results) is True
        db.commit()
    return lease


def correct_and_rescore_b(db, lease):
    """Correct window SCORED (token B drops below the spike; A is re-promoted unchanged)."""
    promote_rows(db, lease, SCORED, [row(SCORED, TOK_A, 12, 6, 0), row(SCORED, TOK_B, 2, 2, 0)])
    db.commit()
    assert rescore_stale(db, lease, network=NET)
    db.commit()


def test_feed_lists_every_revision_across_tokens_with_required_fields_and_provenance(served):
    db, base = served
    lease = seed_two_tokens(db)
    status, body = get(base, q())
    assert status == 200 and body["truncated"] is False
    items = body["alerts"]
    assert {i["token"] for i in items} == {TOK_A, TOK_B}
    assert {i["detector"] for i in items} == {COUNT, WALLETS}
    assert len(items) == 4 and {i["state"] for i in items} == {"active"}
    for i in items:
        assert set(i) == {"alert_revision", "token", "window_start", "detector", "state",
                          "result_revision", "recorded_at", "label"}
        assert i["window_start"] == SCORED and i["alert_revision"] == 1
        assert i["recorded_at"].endswith("Z") and i["label"] == "current"
    assert [i["recorded_at"] for i in items] == sorted((i["recorded_at"] for i in items),
                                                       reverse=True)
    assert body["provenance"]["certification_state"] == "not_certified"
    assert body["provenance"]["qualification_class"] != "unavailable"
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    blob = json.dumps(body)
    assert "lease_owner" not in blob and "lease_fence" not in blob
    correct_and_rescore_b(db, lease)
    items = get(base, q())[1]["alerts"]
    b = [i for i in items if i["token"] == TOK_B and i["detector"] == COUNT]
    assert sorted((i["alert_revision"], i["state"], i["label"]) for i in b) == [
        (1, "active", "superseded"), (2, "pending", "superseded"), (3, "retracted", "current")]
    a = [i for i in items if i["token"] == TOK_A and i["detector"] == COUNT]
    assert sorted((i["alert_revision"], i["state"], i["label"]) for i in a) == [
        (1, "active", "superseded"), (2, "pending", "superseded"), (3, "active", "current")]
    assert len(items) == 12 and {i["state"] for i in items} == {"active", "pending", "retracted"}
    assert sum(i["label"] == "current" for i in items) == 4


def test_feed_range_is_half_open_per_network_and_bad_bounds_are_400(served):
    db, base = served
    seed_two_tokens(db)
    assert len(get(base, q())[1]["alerts"]) == 4
    assert get(base, q(**{"to": SCORED}))[1]["alerts"] == []
    assert len(get(base, q(**{"from": SCORED, "to": SCORED + W}))[1]["alerts"]) == 4
    assert get(base, q(**{"from": SCORED + W, "to": SCORED + 2 * W}))[1]["alerts"] == []
    assert get(base, q(network="mainnet"))[1]["alerts"] == []
    for bad in (q(**{"to": SCORED - 3600}), q(**{"from": SCORED + 1}),
                q(**{"to": SCORED - 3600 + 604800 + 300}), q(limit=0), q(limit=1001),
                {"network": NET}, q(network="devnet"), q(token=TOK_A)):
        assert get(base, bad)[0] == 400, bad


def test_feed_pages_each_row_once_newest_first_and_cursor_is_bound(served):
    db, base = served
    lease = seed_two_tokens(db)
    correct_and_rescore_b(db, lease)
    full = walk(base, q(limit=1000))
    assert len(full) == 12
    for size in (1, 3):
        paged = walk(base, q(limit=size))
        assert paged == full, size
    keys = [(i["token"], i["window_start"], i["detector"], i["alert_revision"]) for i in full]
    assert len(set(keys)) == len(keys)
    stamps = [i["recorded_at"] for i in full]
    assert stamps == sorted(stamps, reverse=True)
    status, first = get(base, q(limit=3))
    assert status == 200 and first["truncated"] is True and len(first["alerts"]) == 3
    cursor = first["next_cursor"]
    assert get(base, q(limit=3, cursor=cursor))[0] == 200
    assert get(base, q(limit=3, cursor=cursor, **{"to": SCORED + 7200}))[0] == 400
    assert get(base, q(limit=3, cursor=cursor, network="mainnet"))[0] == 400
    assert get(base, q(limit=4, cursor=cursor))[0] == 400
    assert get(base, q(cursor=cursor))[0] == 400


def test_feed_is_read_only_and_leaves_existing_alert_routes_unchanged(served):
    db, base = served
    seed_two_tokens(db)
    before = db.execute("SELECT count(*) FROM alert_revisions").fetchone()[0]
    db.commit()
    request = Request(base + FEED + "?" + urlencode(q()), data=b"x", method="POST")
    with pytest.raises(HTTPError) as error:
        urlopen(request, timeout=8)
    assert error.value.code in (405, 404)
    assert db.execute("SELECT count(*) FROM alert_revisions").fetchone()[0] == before
    db.commit()
    status, body = get(base, {"network": NET, "token": TOK_A, "window_start": SCORED},
                       path="/v1/alerts")
    assert status == 200 and set(body) == {"alerts", "limit", "truncated"}
    assert [a["detector"] for a in body["alerts"]] and "provenance" not in body
    status, body = get(base, {"network": NET, "token": TOK_A, "window_start": SCORED,
                              "detector": COUNT}, path="/v1/alerts/history")
    assert status == 200 and set(body) == {"history", "limit", "truncated"}
    assert [h["alert_revision"] for h in body["history"]] == [1]
