"""Dashboard API unit tests: /v1/coverage validation, bounds, cursor binding, provenance, safety filter."""
import json
from unittest.mock import patch

import pytest

from ton_sentinel import api, dashboard_payload
from ton_sentinel.db import serving
from ton_sentinel.db.serving import coverage_state, read_coverage

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
        raise AssertionError("validation must happen before any statement")


def call(query, path="/v1/coverage", method="GET"):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    body = b"".join(api.create_app(dsn="x")({"REQUEST_METHOD": method, "PATH_INFO": path,
                                             "QUERY_STRING": query}, start_response))
    return out["status"], json.loads(body)


def row(start, state="missing", **over):
    item = {"window_start": start, "window_end": start + 300, "state": state,
            "reasons": ["no_qualification"], "qualification": None, "missing": [],
            "admission_refused": False, "lineage_records": 0,
            "served": {"rows": 0, "coverage_status_counts": {}, "known_traders": None,
                       "unknown_count": None}}
    item.update(over)
    return item


def run(query, rows):
    with patch("ton_sentinel.api.psycopg.connect", return_value=FakeConnection2()), \
         patch("ton_sentinel.api.read_coverage", return_value=rows) as mock, \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = call(query)
    return status, body, mock


class FakeConnection2(FakeConnection):
    def execute(self, *_a, **_k):
        return self


def q(**over):
    base = {"network": "testnet", "token": "jetton:a", "from": T0, "to": T0 + 3600}
    base.update(over)
    return "&".join(f"{k}={v}" for k, v in base.items() if v is not None)


# ---- validation and bounds ---------------------------------------------------------------
@pytest.mark.parametrize("over", [
    {"to": T0}, {"to": T0 - 300}, {"to": T0 + 604800 + 300}, {"from": T0 + 1},
    {"to": T0 + 3601}, {"from": None}, {"to": None}, {"token": None}, {"network": None},
    {"network": "devnet"}, {"token": ""}, {"from": "-300"}, {"from": "abc"}, {"limit": 0},
    {"limit": 1001}, {"limit": "x"}, {"extra": "1"}, {"cursor": "!!"},
])
def test_coverage_range_bounds_and_params_are_400(over):
    status, body, mock = run(q(**over), [])
    assert status == "400 Bad Request" and "provenance" not in body
    mock.assert_not_called()


def test_coverage_accepts_seven_days_default_limit_288_and_max_1000():
    status, body, mock = run(q(to=T0 + 604800), [row(T0)])
    assert status == "200 OK"
    kwargs = mock.call_args.kwargs
    assert kwargs["limit"] == 288 and kwargs["fetch_extra"] is True and kwargs["after"] is None
    assert (kwargs["window_from"], kwargs["window_to"]) == (T0, T0 + 604800)
    assert body["limit"] == 288 and body["from"] == T0 and body["to"] == T0 + 604800
    assert run(q(limit=1000), [])[0] == "200 OK"


def test_coverage_only_get_is_served():
    status, body = call(q(), method="POST")
    assert status == "405 Method Not Allowed"


# ---- cursor ----------------------------------------------------------------------------
def test_cursor_issued_only_when_truncated_and_binds_filters():
    rows = [row(T0 + 300 * i) for i in range(3)]
    status, body, _ = run(q(limit=2), rows)
    assert status == "200 OK" and len(body["coverage"]) == 2 and "next_cursor" in body
    cursor = body["next_cursor"]
    status, body, _ = run(q(limit=2), rows[:2])
    assert "next_cursor" not in body
    status, body, mock = run(q(limit=2, cursor=cursor), [])
    assert status == "200 OK" and mock.call_args.kwargs["after"] == T0 + 300
    for over in ({"limit": 3}, {"token": "jetton:b"}, {"to": T0 + 3000}, {"from": T0 + 300},
                 {"network": "mainnet"}):
        status, body, mock = run(q(cursor=cursor, **{"limit": 2, **over}), [])
        assert status == "400 Bad Request", over
        mock.assert_not_called()


def test_cursor_key_outside_range_or_unaligned_is_refused():
    filters = {"network": "testnet", "token": "jetton:a", "from": T0, "to": T0 + 3600,
               "limit": 288}
    for key in ([T0 - 300], [T0 + 3600], [T0 + 1], [T0, T0], ["x"]):
        cursor = api._encode_endpoint_cursor("coverage", filters, key)
        assert run(q(cursor=cursor), [])[0] == "400 Bad Request", key


def test_cursor_is_not_interchangeable_with_series():
    filters = {"network": "testnet", "token": "jetton:a", "from": T0, "to": T0 + 3600,
               "limit": 288}
    cursor = api._encode_endpoint_cursor("series", filters, [T0])
    assert run(q(cursor=cursor), [])[0] == "400 Bad Request"


def test_unicode_token_roundtrips_through_cursor():
    token = "jetton:é日本"
    rows = [row(T0), row(T0 + 300)]
    _, body, _ = run(q(token="jetton%3A%C3%A9%E6%97%A5%E6%9C%AC", limit=1), rows)
    assert body["token"] == token and "next_cursor" in body


# ---- provenance and safety filter ----------------------------------------------------------
def test_response_carries_provenance_banner_and_states():
    _, body, _ = run(q(), [row(T0)])
    assert body["provenance"] == PROV
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert body["coverage"][0]["state"] == "missing"
    assert body["network"] == "testnet" and body["token"] == "jetton:a"


def test_forbidden_keys_are_stripped_and_forbidden_values_refused():
    dirty = row(T0, lineage=[{"a": 1}], lease_owner="o", wallet="EQ" + "A" * 46)
    status, body, _ = run(q(), [dirty])
    assert status == "200 OK"
    assert dashboard_payload.find_violations(body) == []
    leaked = row(T0, reasons=["see https://example.invalid/x"])
    status, body, _ = run(q(), [leaked])
    assert status == "503 Service Unavailable" and body == {"error": "unavailable"}


# ---- state classification (gaps are missing, never filled) --------------------------------
FACTS = dict(has_row=False, row_provisional=False, pending=False, swaps=None, qualified=False,
             in_universe=True, coverage_missing=False)


@pytest.mark.parametrize("over,state", [
    ({}, "missing"),
    ({"coverage_missing": True}, "missing"),
    ({"in_universe": False}, "pre_universe"),
    ({"in_universe": False, "qualified": True}, "pre_universe"),
    ({"qualified": True}, "provisional"),            # a gap is never filled with a zero
    ({"pending": True}, "provisional"),
    ({"has_row": True, "swaps": 4, "qualified": True}, "observed"),
    ({"has_row": True, "swaps": 0, "qualified": True}, "zero"),
    ({"has_row": True, "swaps": 4}, "partial"),
    ({"has_row": True, "swaps": 4, "qualified": True, "coverage_missing": True}, "partial"),
    ({"has_row": True, "swaps": 4, "qualified": True, "row_provisional": True}, "provisional"),
    ({"has_row": True, "swaps": 4, "qualified": True, "pending": True}, "provisional"),
])
def test_coverage_state_truth_table(over, state):
    assert coverage_state(**{**FACTS, **over})[0] == state


def test_coverage_state_reasons_are_stable_strings():
    assert coverage_state(**FACTS) == ("missing", ["no_qualification"])
    assert coverage_state(**{**FACTS, "in_universe": False}) == (
        "pre_universe", ["before_first_served_row"])
    assert coverage_state(**{**FACTS, "qualified": True}) == (
        "provisional", ["qualified_without_served_row"])
    assert coverage_state(**{**FACTS, "has_row": True, "swaps": 1, "row_provisional": True,
                             "qualified": True})[1] == ["row_provisional"]
    assert coverage_state(**{**FACTS, "coverage_missing": True})[1] == [
        "no_qualification", "coverage_missing"]


def test_coverage_state_rejects_bad_facts():
    with pytest.raises(ValueError):
        coverage_state(**{**FACTS, "pending": 1})
    with pytest.raises(ValueError):
        coverage_state(**{**FACTS, "has_row": True, "swaps": None})


# ---- reader function validation ------------------------------------------------------------
@pytest.mark.parametrize("over", [
    {"network": "devnet"}, {"token": ""}, {"token": None}, {"window_from": None},
    {"window_from": T0 + 1}, {"window_to": T0}, {"window_to": T0 + 604800 + 300},
    {"limit": 0}, {"limit": 1001}, {"after": T0 + 1}, {"after": -300}, {"after": "x"},
    {"after": T0 - 300}, {"after": T0 + 3600}, {"fetch_extra": 1},
])
def test_read_coverage_validates_before_querying(over):
    args = dict(network="testnet", token="jetton:a", window_from=T0, window_to=T0 + 3600,
                limit=10, after=None, fetch_extra=False)
    args.update(over)
    with pytest.raises(ValueError):
        read_coverage(FakeConnection(), **args)


def test_read_coverage_reads_only_reader_tables():
    import re
    source = open(serving.__file__, encoding="utf-8").read()
    start = source.index("def read_coverage")
    body = source[start:source.index("\ndef ", start + 10)]
    tables = set(re.findall(r"\b(?:FROM|JOIN)\s+([a-z_]+)", body))
    tables -= {"generate_series", "jsonb_array_elements_text"}
    assert tables and tables <= serving.READER_TABLES, tables
