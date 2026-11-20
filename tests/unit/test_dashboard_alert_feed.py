"""Dashboard API unit tests: GET /v1/alerts/feed (no database)."""

import json
from unittest.mock import patch

import pytest

from ton_sentinel import api, dashboard_payload
from ton_sentinel.api import create_app

PROV = dashboard_payload.build_provenance(qualification=[], scored=0)
T0 = 1_710_000_000 - (1_710_000_000 % 300)
DET = "swap_count_spike.v1"
OTHER = "active_wallets_spike.v1"
FEED = "/v1/alerts/feed"
STAMP = "2026-11-13T10:00:00.000001Z"


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, *_a, **_k):
        return self


def call(query, path=FEED, method="GET"):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    body = b"".join(create_app(dsn="x")({"REQUEST_METHOD": method, "PATH_INFO": path,
                                         "QUERY_STRING": query}, start_response))
    return out["status"], json.loads(body)


def run(query, rows):
    with patch("ton_sentinel.api.psycopg.connect", return_value=FakeConnection()), \
         patch("ton_sentinel.api.read_alert_feed", return_value=rows) as mock, \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = call(query)
    return status, body, mock


def item(start=T0, revision=1, token="jetton:a", detector=DET, **extra):
    doc = {"token": token, "window_start": start, "detector": detector,
           "alert_revision": revision, "state": "active", "result_revision": 1,
           "recorded_at": STAMP, "label": "current"}
    doc.update(extra)
    return doc


def feed_query(**over):
    q = {"network": "testnet", "from": T0, "to": T0 + 3600}
    q.update(over)
    return "&".join(f"{k}={v}" for k, v in q.items() if v is not None)


@pytest.mark.parametrize("over", [
    {"network": None}, {"network": "devnet"}, {"network": ""},
    {"from": None}, {"to": None},
    {"to": T0}, {"to": T0 - 300},
    {"to": T0 + 604800 + 300},
    {"from": T0 + 1}, {"to": T0 + 3601}, {"from": "-300"}, {"from": "abc"},
    {"limit": 0}, {"limit": 1001}, {"limit": "x"}, {"limit": -1},
    {"token": "jetton:a"},
])
def test_alert_feed_rejects_invalid_range_and_params(over):
    status, body, mock = run(feed_query(**over), [])
    assert status == "400 Bad Request" and "provenance" not in body
    mock.assert_not_called()


def test_alert_feed_rejects_duplicate_and_unknown_params():
    for extra in ("&bogus=1", "&limit=5&limit=6", "&network=mainnet"):
        assert run(feed_query() + extra, [])[0] == "400 Bad Request"


def test_alert_feed_accepts_seven_days_default_limit_and_max_limit():
    status, body, mock = run(feed_query(to=T0 + 604800), [item()])
    kw = mock.call_args.kwargs
    assert status == "200 OK" and body["limit"] == 100 and kw["limit"] == 100
    assert (kw["network"], kw["window_from"], kw["window_to"]) == ("testnet", T0, T0 + 604800)
    assert kw["fetch_extra"] is True and kw["after"] is None
    assert run(feed_query(limit=1000), [])[2].call_args.kwargs["limit"] == 1000
    assert run(feed_query(limit=1), [])[2].call_args.kwargs["limit"] == 1


def test_alert_feed_is_get_only_and_legacy_alert_routes_are_untouched():
    assert call(feed_query(), method="POST")[0] == "405 Method Not Allowed"
    assert "/v1/alerts/feed" not in api._DETECTOR_VIEWS
    assert set(api._DETECTOR_VIEWS) >= {"/v1/alerts", "/v1/alerts/history"}


def test_alert_feed_truncation_flag_and_page_size():
    rows = [item(revision=n) for n in range(3)]
    status, body, _ = run(feed_query(limit=2), rows)
    assert len(body["alerts"]) == 2 and body["truncated"] is True and "next_cursor" in body
    status, body, _ = run(feed_query(limit=3), rows)
    assert len(body["alerts"]) == 3 and body["truncated"] is False
    assert "next_cursor" not in body


def test_alert_feed_items_carry_required_fields_label_and_provenance():
    rows = [item(label="superseded", state="pending"), item(revision=2, state="retracted")]
    status, body, _ = run(feed_query(), rows)
    assert status == "200 OK"
    for got in body["alerts"]:
        assert set(got) >= {"alert_revision", "token", "window_start", "detector", "state",
                            "recorded_at", "label"}
    assert [g["state"] for g in body["alerts"]] == ["pending", "retracted"]
    assert body["provenance"]["certification_state"] == "not_certified"
    assert body["provenance"]["source_kind"] == "unverified_origin"
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert (body["network"], body["from"], body["to"]) == ("testnet", T0, T0 + 3600)


def test_alert_feed_safety_filter_strips_forbidden_keys():
    row = item(lease_owner="secret-owner", lease_fence=3, wallet="EQ" + "A" * 46)
    status, body, _ = run(feed_query(), [row])
    blob = json.dumps(body)
    assert status == "200 OK" and "lease_owner" not in blob and "lease_fence" not in blob
    assert "wallet" not in blob and "secret-owner" not in blob


def test_alert_feed_safety_filter_refuses_unsafe_values():
    for bad in ("https://example.test/x", "postgresql://u:p@h/db", "EQ" + "A" * 46):
        status, body, _ = run(feed_query(), [item(token=bad)])
        assert status == "503 Service Unavailable" and body == {"error": "unavailable"}


def test_alert_feed_cursor_is_bound_to_network_range_and_limit():
    rows = [item(revision=3), item(revision=2), item(revision=1)]
    q = feed_query(limit=2)
    status, body, _ = run(q, rows)
    cursor = body["next_cursor"]
    status, body2, mock = run(q + f"&cursor={cursor}", rows[2:])
    assert status == "200 OK" and "next_cursor" not in body2
    assert mock.call_args.kwargs["after"] == (STAMP, "jetton:a", T0, DET, 2)
    for changed in (feed_query(limit=2, to=T0 + 7200), feed_query(limit=2, **{"from": T0 - 300}),
                    feed_query(limit=2, network="mainnet"), feed_query(limit=3),
                    feed_query()):
        assert run(changed + f"&cursor={cursor}", [])[0] == "400 Bad Request", changed
    for bad in ("!!!", "a" * 5000, "e30"):
        assert run(q + f"&cursor={bad}", [])[0] == "400 Bad Request"


def test_alert_feed_cursor_key_is_the_full_keyset_tuple():
    rows = [item(start=T0 + 300, revision=7, detector=OTHER, token="jetton:b"), item()]
    cursor = run(feed_query(limit=1), rows)[1]["next_cursor"]
    key = api._decode_endpoint_cursor(
        cursor, "alert_feed",
        {"network": "testnet", "from": T0, "to": T0 + 3600, "limit": 1})
    assert key == [STAMP, "jetton:b", T0 + 300, OTHER, 7]


def test_alert_feed_cursor_with_out_of_range_or_malformed_key_is_refused():
    filters = {"network": "testnet", "from": T0, "to": T0 + 3600, "limit": 2}
    bad_keys = ([STAMP, "t", T0 + 3600, DET, 1], [STAMP, "t", T0 - 300, DET, 1],
                [STAMP, "t", T0 + 1, DET, 1], [STAMP, "t", T0, "bad", 1],
                [STAMP, "t", T0, DET, 0], [STAMP, "", T0, DET, 1], ["yesterday", "t", T0, DET, 1],
                [STAMP, "t", T0, DET], [STAMP, 5, T0, DET, 1], [STAMP, "t", T0, DET, "1"])
    for key in bad_keys:
        cursor = api._encode_endpoint_cursor("alert_feed", filters, key)
        status, _, mock = run(feed_query(limit=2) + f"&cursor={cursor}", [])
        assert status == "400 Bad Request", key
        mock.assert_not_called()


def test_alert_feed_cursor_is_not_interchangeable_with_other_endpoints():
    filters = {"network": "testnet", "from": T0, "to": T0 + 3600, "limit": 1}
    foreign = api._encode_endpoint_cursor("detection_range", filters, [T0, DET, "both"])
    assert run(feed_query(limit=1) + f"&cursor={foreign}", [])[0] == "400 Bad Request"
