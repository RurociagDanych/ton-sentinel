"""Dashboard API unit tests: /v1/tokens, /v1/series and the /v1/windows range filter (no database)."""

import json
from unittest.mock import patch

import pytest

from ton_sentinel import api, dashboard_payload
from ton_sentinel.api import create_app

PROV = dashboard_payload.build_provenance(qualification=[], scored=0)
T0 = 1_710_000_000 - (1_710_000_000 % 300)


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


def feature(start, token="jetton:a", qualification=None):
    return {"window_start": start, "window_end": start + 300, "generation": "g", "swaps": 1,
            "input_raw": "1", "output_raw": None, "known_traders": 1, "unknown_count": 0,
            "coverage_status": "unknown", "status": "provisional", "provisional": True,
            "qualification": qualification}


def run(path, query, rows, reader="read_series"):
    with patch("ton_sentinel.api.psycopg.connect", return_value=FakeConnection()), \
         patch(f"ton_sentinel.api.{reader}", return_value=rows) as mock, \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = call(query, path)
    return status, body, mock


def series_query(**over):
    q = {"network": "testnet", "token": "jetton:a", "from": T0, "to": T0 + 3600}
    q.update(over)
    return "&".join(f"{k}={v}" for k, v in q.items() if v is not None)


@pytest.mark.parametrize("over", [
    {"to": T0},
    {"to": T0 - 300},
    {"to": T0 + 604800 + 300},
    {"from": T0 + 1},
    {"to": T0 + 3601},
    {"from": None}, {"to": None}, {"token": None}, {"network": None},
    {"network": "devnet"}, {"token": ""}, {"from": "-300"}, {"from": "abc"},
    {"limit": 0}, {"limit": 1001}, {"limit": "x"},
])
def test_series_range_bounds_and_alignment(over):
    status, body, mock = run("/v1/series", series_query(**over), [])
    assert status == "400 Bad Request" and "provenance" not in body
    mock.assert_not_called()


def test_series_accepts_exactly_seven_days_and_default_limit():
    status, body, mock = run("/v1/series", series_query(to=T0 + 604800), [feature(T0)])
    assert status == "200 OK"
    assert mock.call_args.kwargs["limit"] == 288 and mock.call_args.kwargs["fetch_extra"] is True
    assert body["limit"] == 288


def test_series_rejects_unknown_and_duplicate_params():
    for extra in ("&bogus=1", "&limit=5&limit=6"):
        assert run("/v1/series", series_query() + extra, [])[0] == "400 Bad Request"


@pytest.mark.parametrize("query", [
    "network=testnet&from=1&to=300", "network=testnet&from=0&to=0",
    "network=testnet&from=0&to=700000", "network=testnet&from=0", "to=300&network=testnet",
    "network=testnet&from=0&to=300&limit=1001", "network=nope&from=0&to=300",
])
def test_tokens_range_and_page_bounds(query):
    status, _, mock = run("/v1/tokens", query, [], reader="read_tokens")
    assert status == "400 Bad Request"
    mock.assert_not_called()


def test_series_page_bound_truncates_and_issues_cursor():
    rows = [feature(T0 + 300 * i) for i in range(3)]
    status, body, mock = run("/v1/series", series_query(limit=2), rows)
    assert status == "200 OK" and len(body["series"]) == 2 and "next_cursor" in body
    assert mock.call_args.kwargs["limit"] == 2
    status, body, _ = run("/v1/series", series_query(limit=2), rows[:2])
    assert len(body["series"]) == 2 and "next_cursor" not in body


def _cursor(query, rows):
    return run("/v1/series", query, rows)[1]["next_cursor"]


def test_series_cursor_bound_to_filters_and_unicode_token_roundtrip():
    token = "jetton:é\u202e<img onerror=x>日本"
    from urllib.parse import quote
    q = series_query(token=quote(token), limit=1)
    rows = [feature(T0), feature(T0 + 300)]
    cursor = _cursor(q, rows)
    status, body, mock = run("/v1/series", q + f"&cursor={cursor}", rows[1:])
    assert status == "200 OK" and mock.call_args.kwargs["after"] == T0
    assert mock.call_args.kwargs["token"] == token
    for changed in (series_query(token=quote(token), limit=1, to=T0 + 7200),
                    series_query(token=quote(token), limit=1, **{"from": T0 - 300}),
                    series_query(token="jetton:b", limit=1),
                    series_query(token=quote(token), limit=1, network="mainnet"),
                    series_query(token=quote(token), limit=2)):
        assert run("/v1/series", changed + f"&cursor={cursor}", [])[0] == "400 Bad Request"
    for bad in ("!!!", "a" * 5000, "e30"):
        assert run("/v1/series", q + f"&cursor={bad}", [])[0] == "400 Bad Request"


def test_cursors_are_not_interchangeable_between_endpoints():
    cursor = _cursor(series_query(limit=1), [feature(T0), feature(T0 + 300)])
    status, _, _ = run("/v1/tokens", f"network=testnet&from={T0}&to={T0 + 3600}&limit=1"
                       f"&cursor={cursor}", [], reader="read_tokens")
    assert status == "400 Bad Request"
    status, _, _ = run("/v1/windows", f"network=testnet&limit=1&cursor={cursor}", [],
                       reader="read_windows")
    assert status == "400 Bad Request"


def test_tokens_cursor_bound_to_filters():
    rows = [{"token": "a", "feature_rows": 1, "window_count": 1, "swaps": 1,
             "first_window_start": T0, "latest_window_end": T0 + 300},
            {"token": "b", "feature_rows": 1, "window_count": 1, "swaps": 1,
             "first_window_start": T0, "latest_window_end": T0 + 300}]
    q = f"network=testnet&from={T0}&to={T0 + 3600}&limit=1"
    status, body, _ = run("/v1/tokens", q, rows, reader="read_tokens")
    assert status == "200 OK" and [t["token"] for t in body["tokens"]] == ["a"]
    cursor = body["next_cursor"]
    status, _, mock = run("/v1/tokens", q + f"&cursor={cursor}", rows[1:], reader="read_tokens")
    assert status == "200 OK" and mock.call_args.kwargs["after"] == "a"
    other = f"network=testnet&from={T0}&to={T0 + 7200}&limit=1&cursor={cursor}"
    assert run("/v1/tokens", other, [], reader="read_tokens")[0] == "400 Bad Request"


def window(start):
    return {"network": "testnet", "window_start": start, "window_end": start + 300,
            "feature_rows": 1, "provisional_rows": 1, "generation_count": 1, "statuses": ["x"]}


@pytest.mark.parametrize("query", [
    "from=1&to=300", "from=0&to=0", "from=600&to=300", "from=0&to=700000", "from=0", "to=300",
])
def test_windows_range_validation(query):
    status, _, mock = run("/v1/windows", "network=testnet&" + query, [], reader="read_windows")
    assert status == "400 Bad Request"
    mock.assert_not_called()


def test_windows_range_filter_with_cursor_and_unchanged_body():
    q = f"network=testnet&from={T0}&to={T0 + 3600}&limit=1"
    rows = [window(T0), window(T0 + 300)]
    status, body, mock = run("/v1/windows", q, rows, reader="read_windows")
    assert status == "200 OK" and set(body) == {"windows", "next_cursor"}
    assert mock.call_args.kwargs["window_from"] == T0 and mock.call_args.kwargs["window_to"] == T0 + 3600
    cursor = body["next_cursor"]
    assert run("/v1/windows", q + f"&cursor={cursor}", rows[1:], reader="read_windows")[0] == "200 OK"
    wider = f"network=testnet&from={T0}&to={T0 + 7200}&limit=1&cursor={cursor}"
    assert run("/v1/windows", wider, [], reader="read_windows")[0] == "400 Bad Request"
    plain = run("/v1/windows", "network=testnet&limit=1", rows, reader="read_windows")[1]["next_cursor"]
    assert run("/v1/windows", q + f"&cursor={plain}", [], reader="read_windows")[0] == "400 Bad Request"


def test_windows_without_range_keeps_old_filter_contract():
    _, _, mock = run("/v1/windows", "network=testnet&limit=1", [window(T0)], reader="read_windows")
    assert mock.call_args.kwargs["window_from"] is None and mock.call_args.kwargs["window_to"] is None


def test_new_responses_carry_provenance_blocks():
    status, body, _ = run("/v1/series", series_query(), [feature(T0), feature(T0 + 300, qualification={"mode": "bounded_observed", "synthetic": True})])
    assert status == "200 OK" and body["provenance"] == PROV
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    for point in body["series"]:
        assert point["provenance"]["honesty_banner"] == dashboard_payload.HONESTY_BANNER
        assert point["provenance"]["certification_state"] == "not_certified"
    unrecorded, synthetic = body["series"]
    assert unrecorded["qualification"] is None
    assert unrecorded["provenance"]["qualification_class"] == "unrecorded"
    assert synthetic["provenance"]["qualification_class"] == "synthetic"
    status, body, _ = run("/v1/tokens", f"network=testnet&from={T0}&to={T0 + 300}", [],
                          reader="read_tokens")
    assert status == "200 OK" and body["provenance"] == PROV and body["tokens"] == []


def test_series_never_marks_unrecorded_as_live_or_certified():
    _, body, _ = run("/v1/series", series_query(), [feature(T0)])
    point = body["series"][0]
    assert point["qualification"] is None
    assert point["provenance"]["source_kind"] == "unverified_origin"
    assert point["provenance"]["certified_claimed"] == 0


def test_provenance_failure_is_not_served_as_empty():
    with patch("ton_sentinel.api.psycopg.connect", return_value=FakeConnection()), \
         patch("ton_sentinel.api.read_series", return_value=[]), \
         patch("ton_sentinel.api.read_provenance", side_effect=RuntimeError("boom")):
        status, body = call(series_query(), "/v1/series")
    assert status == "503 Service Unavailable" and body == {"error": "unavailable"}


def test_safety_filter_applied_to_series_and_tokens():
    bad = feature(T0)
    bad["lease_owner"] = "worker-7"
    status, body, _ = run("/v1/series", series_query(), [bad])
    assert status == "200 OK" and "lease_owner" not in json.dumps(body)
    leaky = feature(T0)
    leaky["status"] = "postgresql://u:p@h/db"
    status, body, _ = run("/v1/series", series_query(), [leaky])
    assert status == "503 Service Unavailable" and "postgresql" not in json.dumps(body)
    tok = {"token": "https://evil.example/x", "feature_rows": 1, "window_count": 1, "swaps": 1,
           "first_window_start": T0, "latest_window_end": T0 + 300}
    status, body, _ = run("/v1/tokens", f"network=testnet&from={T0}&to={T0 + 300}", [tok],
                          reader="read_tokens")
    assert status == "503 Service Unavailable" and "evil" not in json.dumps(body)


def test_full_series_page_fits_response_cap():
    rows = [feature(T0 + 300 * i) for i in range(1000)]
    status, body, _ = run("/v1/series", series_query(to=T0 + 300 * 1000, limit=1000), rows)
    assert status == "200 OK" and len(body["series"]) == 1000


def test_limits_constants_match_policy_limits():
    assert api.SERIES_MAX_SPAN == dashboard_payload.LIMITS["time_range_seconds_max"] == 604800
    assert api.SERIES_DEFAULT_LIMIT == 288
    assert api.MAX_FEATURE_LIMIT == dashboard_payload.LIMITS["page_size_max"]
