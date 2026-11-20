"""Dashboard API unit tests: /v1/detections/range and /v1/detections/revisions (no database)."""

import json
from unittest.mock import patch

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app

PROV = dashboard_payload.build_provenance(qualification=[], scored=0)
T0 = 1_710_000_000 - (1_710_000_000 % 300)
DET = "swap_count_spike.v1"
OTHER = "active_wallets_spike.v1"


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, *_a, **_k):
        return self


def call(query, path):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    body = b"".join(create_app(dsn="x")({"REQUEST_METHOD": "GET", "PATH_INFO": path,
                                         "QUERY_STRING": query}, start_response))
    return out["status"], json.loads(body)


def run(path, query, rows, reader):
    with patch("ton_sentinel.api.psycopg.connect", return_value=FakeConnection()), \
         patch(f"ton_sentinel.api.{reader}", return_value=rows) as mock, \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = call(query, path)
    return status, body, mock


def detection(start, detector=DET, token="jetton:a", **extra):
    doc = {"network": "testnet", "token": token, "direction": "both", "window_start": start,
           "detector": detector, "status": "scored", "triggered": True, "score": "9.5",
           "explanation": "swaps above baseline"}
    doc.update(extra)
    return doc


def revision(number, **extra):
    row = {"result_revision": number, "status": "scored", "triggered": False, "score": "1.0",
           "input_digest": "sha256:" + "a" * 64, "policy_id": "sha256:" + "b" * 64,
           "dependency_epoch": 1, "promoted_at": "2026-11-13T00:00:00.000000Z",
           "explanation": "x", "is_current": False, "stale": True, "label": "superseded",
           "alert_state": None}
    row.update(extra)
    return row


def range_query(**over):
    q = {"network": "testnet", "token": "jetton:a", "from": T0, "to": T0 + 3600}
    q.update(over)
    return "&".join(f"{k}={v}" for k, v in q.items() if v is not None)


def rev_query(**over):
    q = {"network": "testnet", "token": "jetton:a", "window_start": T0, "detector": DET}
    q.update(over)
    return "&".join(f"{k}={v}" for k, v in q.items() if v is not None)


RANGE = "/v1/detections/range"
REVS = "/v1/detections/revisions"


@pytest.mark.parametrize("over", [
    {"token": None},
    {"token": ""}, {"network": None}, {"network": "devnet"},
    {"from": None}, {"to": None},
    {"to": T0}, {"to": T0 - 300},
    {"to": T0 + 604800 + 300},
    {"from": T0 + 1}, {"to": T0 + 3601}, {"from": "-300"}, {"from": "abc"},
    {"limit": 0}, {"limit": 1001}, {"limit": "x"},
])
def test_detection_range_requires_token_and_validates_bounds(over):
    status, body, mock = run(RANGE, range_query(**over), [], "read_detection_range")
    assert status == "400 Bad Request" and "provenance" not in body
    mock.assert_not_called()


def test_detection_range_accepts_seven_days_half_open_and_default_limit():
    status, body, mock = run(RANGE, range_query(to=T0 + 604800), [detection(T0)],
                             "read_detection_range")
    assert status == "200 OK" and body["limit"] == 100
    kw = mock.call_args.kwargs
    assert (kw["window_from"], kw["window_to"], kw["token"]) == (T0, T0 + 604800, "jetton:a")
    assert kw["fetch_extra"] is True and kw["network"] == "testnet"


def test_range_and_revisions_reject_unknown_duplicate_and_post_params():
    for path, base in ((RANGE, range_query()), (REVS, rev_query())):
        for extra in ("&bogus=1", "&limit=5&limit=6"):
            assert run(path, base + extra, [], "read_detection_range")[0] == "400 Bad Request"


@pytest.mark.parametrize("over", [
    {"token": None}, {"token": ""}, {"network": None}, {"network": "devnet"},
    {"window_start": None}, {"window_start": T0 + 1}, {"window_start": "-300"},
    {"detector": None}, {"detector": "Bad Detector"}, {"detector": "x"},
    {"limit": 0}, {"limit": 1001},
])
def test_revisions_require_full_key_and_validate(over):
    status, body, mock = run(REVS, rev_query(**over), [], "read_result_revisions")
    assert status == "400 Bad Request" and "provenance" not in body
    mock.assert_not_called()


def test_revisions_pass_exact_key_to_reader():
    status, body, mock = run(REVS, rev_query(), [revision(1)], "read_result_revisions")
    kw = mock.call_args.kwargs
    assert status == "200 OK"
    assert (kw["network"], kw["token"], kw["window_start"], kw["detector"]) == (
        "testnet", "jetton:a", T0, DET)
    assert kw["after"] is None and kw["fetch_extra"] is True and kw["limit"] == 100


def test_result_revisions_show_stale_flag_and_current_pointer():
    rows = [revision(1), revision(2, is_current=True, stale=False, label="current",
                                  triggered=True),
            revision(3, status="pending", label="pending", stale=True, alert_state="pending")]
    status, body, _ = run(REVS, rev_query(), rows, "read_result_revisions")
    assert status == "200 OK"
    got = body["revisions"]
    assert [r["result_revision"] for r in got] == [1, 2, 3]
    assert [r["is_current"] for r in got] == [False, True, False]
    assert [r["stale"] for r in got] == [True, False, True]
    assert {r["label"] for r in got} == {"superseded", "current", "pending"}
    assert all(set(r) >= {"is_current", "stale", "label"} for r in got)


def test_range_cursor_bound_to_filters_and_issues_only_when_truncated():
    rows = [detection(T0), detection(T0, OTHER), detection(T0 + 300)]
    q = range_query(limit=2)
    status, body, mock = run(RANGE, q, rows, "read_detection_range")
    assert status == "200 OK" and len(body["detections"]) == 2 and body["truncated"] is True
    assert mock.call_args.kwargs["limit"] == 2
    cursor = body["next_cursor"]
    status, body2, mock = run(RANGE, q + f"&cursor={cursor}", rows[2:], "read_detection_range")
    assert status == "200 OK" and "next_cursor" not in body2
    assert mock.call_args.kwargs["after"] == (T0, OTHER, "both")
    for changed in (range_query(limit=2, to=T0 + 7200), range_query(limit=2, **{"from": T0 - 300}),
                    range_query(limit=2, token="jetton:b"), range_query(limit=2, network="mainnet"),
                    range_query(limit=3)):
        assert run(RANGE, changed + f"&cursor={cursor}", [], "read_detection_range")[0] \
            == "400 Bad Request"
    for bad in ("!!!", "a" * 5000, "e30"):
        assert run(RANGE, q + f"&cursor={bad}", [], "read_detection_range")[0] == "400 Bad Request"


def test_range_cursor_key_outside_range_is_refused():
    from ton_sentinel import api
    filters = {"network": "testnet", "token": "jetton:a", "from": T0, "to": T0 + 3600, "limit": 2}
    for key in ([T0 + 3600, DET, "both"], [T0 - 300, DET, "both"], [T0 + 1, DET, "both"],
                [T0, "bad", "both"], [T0, DET, "up"], [T0, DET]):
        cursor = api._encode_endpoint_cursor("detection_range", filters, key)
        status, _, _ = run(RANGE, range_query(limit=2) + f"&cursor={cursor}", [],
                           "read_detection_range")
        assert status == "400 Bad Request", key


def test_revisions_cursor_ordered_and_bound_to_key():
    rows = [revision(1), revision(2), revision(3)]
    q = rev_query(limit=2)
    status, body, _ = run(REVS, q, rows, "read_result_revisions")
    assert [r["result_revision"] for r in body["revisions"]] == [1, 2] and body["truncated"]
    cursor = body["next_cursor"]
    status, body2, mock = run(REVS, q + f"&cursor={cursor}", rows[2:], "read_result_revisions")
    assert status == "200 OK" and mock.call_args.kwargs["after"] == 2
    assert "next_cursor" not in body2
    for changed in (rev_query(limit=2, detector=OTHER), rev_query(limit=2, token="jetton:b"),
                    rev_query(limit=2, window_start=T0 + 300), rev_query(limit=2, network="mainnet"),
                    rev_query(limit=3)):
        assert run(REVS, changed + f"&cursor={cursor}", [], "read_result_revisions")[0] \
            == "400 Bad Request"


def test_cursors_are_not_interchangeable_between_detection_endpoints():
    rows = [detection(T0), detection(T0 + 300)]
    range_cursor = run(RANGE, range_query(limit=1), rows, "read_detection_range")[1]["next_cursor"]
    rev_cursor = run(REVS, rev_query(limit=1), [revision(1), revision(2)],
                     "read_result_revisions")[1]["next_cursor"]
    assert run(REVS, rev_query(limit=1) + f"&cursor={range_cursor}", [],
               "read_result_revisions")[0] == "400 Bad Request"
    assert run(RANGE, range_query(limit=1) + f"&cursor={rev_cursor}", [],
               "read_detection_range")[0] == "400 Bad Request"
    assert run("/v1/detections", f"network=testnet&limit=1&cursor={range_cursor}", [],
               "read_detector_results")[0] == "400 Bad Request"


def test_unicode_token_cursor_roundtrip():
    from urllib.parse import quote
    token = "jetton:é\u202e日本"
    q = range_query(token=quote(token), limit=1)
    rows = [detection(T0, token=token), detection(T0 + 300, token=token)]
    cursor = run(RANGE, q, rows, "read_detection_range")[1]["next_cursor"]
    status, _, mock = run(RANGE, q + f"&cursor={cursor}", rows[1:], "read_detection_range")
    assert status == "200 OK" and mock.call_args.kwargs["token"] == token


def test_new_responses_carry_provenance_and_banner():
    status, body, _ = run(RANGE, range_query(), [detection(T0)], "read_detection_range")
    assert status == "200 OK" and body["provenance"] == PROV
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert body["provenance"]["certification_state"] == "not_certified"
    status, body, _ = run(REVS, rev_query(), [], "read_result_revisions")
    assert status == "200 OK" and body["provenance"] == PROV and body["revisions"] == []
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER


def test_provenance_failure_is_not_served_as_empty():
    for path, query, reader in ((RANGE, range_query(), "read_detection_range"),
                                (REVS, rev_query(), "read_result_revisions")):
        with patch("ton_sentinel.api.psycopg.connect", return_value=FakeConnection()), \
             patch(f"ton_sentinel.api.{reader}", return_value=[]), \
             patch("ton_sentinel.api.read_provenance", side_effect=RuntimeError("boom")):
            status, body = call(query, path)
        assert status == "503 Service Unavailable" and body == {"error": "unavailable"}


def test_safety_filter_strips_forbidden_keys_and_refuses_forbidden_values():
    bad = detection(T0, lease_owner="worker-7")
    status, body, _ = run(RANGE, range_query(), [bad], "read_detection_range")
    assert status == "200 OK" and "worker-7" not in json.dumps(body)
    leaky = detection(T0, explanation="postgresql://u:pw@h/db")
    status, body, _ = run(RANGE, range_query(), [leaky], "read_detection_range")
    assert status == "503 Service Unavailable" and "postgresql" not in json.dumps(body)
    wallet = revision(1, explanation="trader 0:" + "ab" * 32)
    status, body, _ = run(REVS, rev_query(), [wallet], "read_result_revisions")
    assert status == "503 Service Unavailable" and "ab" * 32 not in json.dumps(body)
    url = revision(1, explanation="see https://evil.example/x")
    assert run(REVS, rev_query(), [url], "read_result_revisions")[0] == "503 Service Unavailable"


def test_existing_detections_route_body_and_filters_unchanged():
    status, body, mock = run("/v1/detections", "network=testnet&token=jetton:a",
                             [detection(T0)], "read_detector_results")
    assert status == "200 OK" and set(body) == {"detections", "limit", "truncated"}
    assert "window_from" not in mock.call_args.kwargs
    assert run("/v1/detections", f"network=testnet&token=jetton:a&from={T0}&to={T0 + 300}", [],
               "read_detector_results")[0] == "400 Bad Request"


def test_unknown_path_under_detections_is_404():
    assert run("/v1/detections/other", "network=testnet", [], "read_detector_results")[0] \
        == "404 Not Found"
