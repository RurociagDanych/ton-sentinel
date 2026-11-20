"""Keyset cursors on detector/alert views over real HTTP + PostgreSQL."""

from tests.integration.test_api_alert_views_pg import (  # noqa: F401  (fixtures + helpers)
    _dsn, get, key, promote_first, served,
)
from tests.integration.test_baseline_grid_pg import promote_rows, row
from tests.integration.test_detection import COUNT, NET, SCORED, TOK, WALLETS
from tests.integration.test_revisions import db  # noqa: F401


def walk(base, path, name, query):
    pages, cursor = [], None
    for _ in range(10):
        status, body, _ = get(base, path, {**query, **({"cursor": cursor} if cursor else {})})
        assert status == 200, body
        pages.append(body)
        cursor = body.get("next_cursor")
        assert ("next_cursor" in body) is body["truncated"]
        if cursor is None:
            return [item for page in pages for item in page[name]], pages
    raise AssertionError("cursor walk did not terminate")


def test_cursor_walk_over_current_views_equals_the_unpaged_read_exactly(served):
    db, base = served
    promote_first(db)
    for path, name in (("/v1/alerts", "alerts"), ("/v1/detections", "detections")):
        full = get(base, path, {"network": NET})[1]
        assert full["truncated"] is False and len(full[name]) == 2
        walked, pages = walk(base, path, name, {"network": NET, "limit": 1})
        assert walked == full[name] and len(pages) == 2
        assert {item["detector"] for item in walked} == {COUNT, WALLETS}


def test_cursor_walk_over_alert_history_equals_the_unpaged_read(served):
    db, base = served
    lease, _ = promote_first(db)
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    db.commit()
    full = get(base, "/v1/alerts/history", key(detector=COUNT))[1]["history"]
    walked, pages = walk(base, "/v1/alerts/history", "history", key(detector=COUNT, limit=1))
    assert walked == full and [h["alert_revision"] for h in walked] == [1, 2]
    assert len(pages) == 2


def test_cursor_from_one_view_or_filter_set_is_rejected_by_another(served):
    db, base = served
    promote_first(db)
    cursor = get(base, "/v1/alerts", {"network": NET, "limit": 1})[1]["next_cursor"]
    for path, query in (("/v1/detections", {"network": NET, "limit": 1}),
                        ("/v1/alerts", {"network": NET, "token": TOK, "limit": 1}),
                        ("/v1/alerts", {"network": "mainnet", "limit": 1})):
        status, body, _ = get(base, path, {**query, "cursor": cursor})
        assert status == 400 and body == {"error": "invalid_query"}


def test_second_page_completes_the_set_in_bytewise_detector_order(served):
    db, base = served
    promote_first(db)
    first = get(base, "/v1/alerts", {"network": NET, "limit": 1})[1]
    second = get(base, "/v1/alerts", {"network": NET, "limit": 1, "cursor": first["next_cursor"]})[1]
    assert second["truncated"] is False and "next_cursor" not in second
    assert [a["detector"] for a in first["alerts"] + second["alerts"]] == sorted([COUNT, WALLETS])
