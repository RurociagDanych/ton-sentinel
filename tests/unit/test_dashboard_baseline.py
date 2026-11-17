"""Dashboard API unit tests: GET /v1/baseline, the pinned baseline input read (no database)."""
import json
from unittest.mock import patch

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from ton_sentinel.db import serving

PROV = dashboard_payload.build_provenance(qualification=[], scored=0)
T0 = 1_710_000_000 - (1_710_000_000 % 300)
DIGEST = "sha256:" + "a" * 64
PATH = "/v1/baseline"


class FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, *_a, **_k):
        return self


def call(query):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    body = b"".join(create_app(dsn="x")({"REQUEST_METHOD": "GET", "PATH_INFO": PATH,
                                         "QUERY_STRING": query}, start_response))
    return out["status"], json.loads(body)


def run(query, rows):
    with patch("ton_sentinel.api.psycopg.connect", return_value=FakeConnection()), \
         patch("ton_sentinel.api.read_baseline_inputs", return_value=rows) as mock, \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = call(query)
    return status, body, mock


def cell(start, state="observed", **extra):
    values = {"swaps": 4, "known_traders": 2, "unknown_count": 1, "coverage": "bounded_observed"}
    if state in ("missing", "pre_universe"):
        values = {"swaps": None, "known_traders": None, "unknown_count": None, "coverage": None}
    if state == "zero":
        values = {"swaps": 0, "known_traders": 0, "unknown_count": 0,
                  "coverage": "bounded_observed"}
    doc = {"window_start": start, "state": state, "synthetic": False, **values}
    doc.update(extra)
    return doc


def pin(digest=DIGEST, cells=None, **extra):
    cells = cells if cells is not None else [cell(T0 - 900), cell(T0 - 600, "zero"),
                                             cell(T0 - 300, "missing")]
    doc = {"input_digest": digest, "network": "testnet", "token": "jetton:a",
           "window_start": T0, "policy": {"policy_id": "sha256:" + "b" * 64,
                                          "lookback_seconds": 900, "min_prior_windows": 2},
           "universe_version": 1, "admitted_window_start": 0, "dependency_epoch": 3,
           "status": "ready", "eligible_count": 2, "certification": "bounded_observed",
           "pinned_at": "2026-11-13T00:00:00.000000Z", "current_cell": cell(T0),
           "cells": cells, "coverage_counts": counts(cells, 3)}
    doc.update(extra)
    return doc


def counts(cells, expected):
    by_state = dict.fromkeys(("observed", "zero", "partial", "provisional", "missing",
                              "pre_universe"), 0)
    for c in cells:
        by_state[c["state"]] += 1
    return {"cells_total": len(cells), "expected": expected, "by_state": by_state,
            "eligible": by_state["observed"] + by_state["zero"]}


def digest_query(**over):
    q = {"network": "testnet", "input_digest": DIGEST}
    q.update(over)
    return "&".join(f"{k}={v}" for k, v in q.items() if v is not None)


def key_query(**over):
    q = {"network": "testnet", "token": "jetton:a", "window_start": T0}
    q.update(over)
    return "&".join(f"{k}={v}" for k, v in q.items() if v is not None)


# ---- validation ------------------------------------------------------------------------
@pytest.mark.parametrize("query", [
    "",                                                       # no selector at all
    digest_query(network=None), digest_query(network="devnet"),
    digest_query(input_digest="sha256:" + "A" * 64),          # uppercase hex
    digest_query(input_digest="sha256:" + "a" * 63),
    digest_query(input_digest="md5:" + "a" * 64),
    digest_query(input_digest=""),
    digest_query(token="jetton:a"),                           # digest and key are exclusive
    digest_query(window_start=T0),
    digest_query(limit=1),                                    # a digest names exactly one pin
    key_query(token=None), key_query(token=""), key_query(window_start=None),
    key_query(window_start=T0 + 1), key_query(window_start="-300"),
    key_query(window_start="abc"), key_query(limit=0), key_query(limit=11), key_query(limit="x"),
    key_query(network=None), digest_query() + "&bogus=1", digest_query() + "&input_digest=" + DIGEST,
    key_query() + "&limit=2&limit=3",
])
def test_malformed_selector_is_400_and_never_reads(query):
    status, body, mock = run(query, [])
    assert status == "400 Bad Request" and "provenance" not in body
    mock.assert_not_called()


def test_digest_selector_reads_exactly_one_pin_with_bounded_query():
    status, body, mock = run(digest_query(), [pin()])
    kw = mock.call_args.kwargs
    assert status == "200 OK"
    assert (kw["network"], kw["input_digest"], kw.get("token"), kw.get("window_start")) == (
        "testnet", DIGEST, None, None)
    assert kw["limit"] == 1 and kw["fetch_extra"] is True


def test_key_selector_default_and_max_limit():
    status, body, mock = run(key_query(), [pin()])
    kw = mock.call_args.kwargs
    assert status == "200 OK" and body["limit"] == 5
    assert (kw["token"], kw["window_start"], kw.get("input_digest")) == ("jetton:a", T0, None)
    assert kw["limit"] == 5 and kw["fetch_extra"] is True
    assert run(key_query(limit=10), [pin()])[2].call_args.kwargs["limit"] == 10


# ---- response shape --------------------------------------------------------------------
def test_response_shape_has_provenance_digest_policy_cells_and_coverage_counts():
    status, body, _ = run(digest_query(), [pin()])
    assert status == "200 OK"
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert body["provenance"]["certification_state"] == "not_certified"
    assert body["truncated"] is False and body["network"] == "testnet"
    (item,) = body["baselines"]
    assert item["input_digest"] == DIGEST and item["token"] == "jetton:a"
    assert item["policy"] == {"policy_id": "sha256:" + "b" * 64, "lookback_seconds": 900,
                              "min_prior_windows": 2}
    assert item["status"] == "ready" and item["certification"] == "bounded_observed"
    assert [c["window_start"] for c in item["cells"]] == [T0 - 900, T0 - 600, T0 - 300]
    assert item["current_cell"]["window_start"] == T0
    counts = item["coverage_counts"]
    assert counts["cells_total"] == 3 and counts["eligible"] == 2
    assert counts["by_state"] == {"observed": 1, "zero": 1, "partial": 0, "provisional": 0,
                                  "missing": 1, "pre_universe": 0}
    assert counts["expected"] == 3  # lookback_seconds / 300


def test_every_cell_state_is_reported_verbatim_and_counted():
    states = ["observed", "zero", "partial", "provisional", "missing", "pre_universe"]
    runs = [run_cell(T0 - 300 * (6 - i), T0 - 300 * (5 - i), state, **(
        {} if state in ("observed", "zero") else {"coverage": None}))
            for i, state in enumerate(states)]
    (item,) = serving.read_baseline_inputs(
        RecordingConnection([raw_row(runs, lookback=1800)]), network="testnet",
        input_digest=DIGEST, limit=1)
    assert [c["state"] for c in item["cells"]] == states
    assert item["coverage_counts"]["by_state"] == dict.fromkeys(states, 1)
    assert item["coverage_counts"]["eligible"] == 2 and item["coverage_counts"]["expected"] == 6
    _, body, _ = run(digest_query(), [item])
    assert [c["state"] for c in body["baselines"][0]["cells"]] == states


def test_unknown_digest_is_an_explicit_404_not_an_empty_list():
    status, body, _ = run(digest_query(), [])
    assert status == "404 Not Found" and body == {"error": "baseline_not_found"}
    status, body, _ = run(key_query(), [])
    assert status == "404 Not Found" and body == {"error": "baseline_not_found"}


# ---- safety filter ---------------------------------------------------------------------
def test_lease_owner_and_lineage_keys_are_stripped_from_the_response():
    dirty = pin(lease_owner="worker-7", lease_fence=9, lineage={"offsets": [1]},
                current_cell=cell(T0, wallet="0:" + "1" * 64))
    status, body, _ = run(digest_query(), [dirty])
    text = json.dumps(body)
    assert status == "200 OK"
    for name in ("lease_owner", "lease_fence", "lineage", "wallet", "worker-7"):
        assert name not in text


def test_address_like_value_in_a_cell_is_refused_not_served():
    status, body, _ = run(digest_query(), [pin(cells=[cell(T0 - 300, revision="0:" + "a" * 64)])])
    assert status == "503 Service Unavailable" and body == {"error": "unavailable"}


def test_oversized_response_is_422_not_truncated_silently():
    cells = [cell(T0 - 300 * (n + 1)) for n in range(2000)]
    rows = [pin(digest="sha256:" + f"{i:064x}", cells=cells) for i in range(5)]
    status, body, _ = run(key_query(), rows)
    assert status == "422 Unprocessable Entity" and body == {"error": "response_too_large"}


def test_more_rows_than_limit_are_cut_and_flagged_truncated():
    rows = [pin(digest="sha256:" + f"{i:064x}") for i in range(3)]
    status, body, _ = run(key_query(limit=2), rows)
    assert status == "200 OK" and body["truncated"] is True and len(body["baselines"]) == 2


# ---- serving projection (no database) ----------------------------------------------------
class RecordingConnection:
    def __init__(self, rows):
        self.rows, self.sql, self.params = rows, None, None

    def execute(self, sql, params=None):
        self.sql, self.params = sql, params
        return self

    def fetchall(self):
        return self.rows


def raw_row(cells, current=None, lookback=1200):
    return (DIGEST, "testnet", "jetton:a", T0, "sha256:" + "b" * 64, lookback, 2, 1, 0, 3, "ready",
            2, "bounded_observed", "2026-11-13T00:00:00.000000Z",
            current or {"network": "testnet", "token": "jetton:a", "direction": "both",
                        "start": T0, "end": T0 + 300, "state": "observed", "swaps": 1,
                        "known_traders": 1, "unknown_count": 0, "revision": "r1",
                        "coverage": "bounded_observed", "synthetic": False},
            cells)


def run_cell(start, end, state, **extra):
    doc = {"network": "testnet", "token": "jetton:a", "direction": "both", "start": start,
           "end": end, "state": state, "swaps": 0, "known_traders": 0, "unknown_count": 0,
           "revision": "r0", "coverage": "bounded_observed", "synthetic": False}
    doc.update(extra)
    return doc


def test_reader_selects_only_allowlisted_columns_never_lease_columns():
    conn = RecordingConnection([])
    serving.read_baseline_inputs(conn, network="testnet", input_digest=DIGEST, limit=1)
    assert "baseline_inputs" in conn.sql
    for forbidden in ("lease_owner", "lease_fence", "active_epoch", "SELECT *"):
        assert forbidden not in conn.sql
    assert DIGEST in conn.params and "testnet" in conn.params


def test_reader_expands_run_length_cells_to_one_cell_per_window_without_scope_fields():
    runs = [run_cell(T0 - 900, T0 - 300, "zero"),
            run_cell(T0 - 300, T0, "observed", swaps=3, known_traders=2, unknown_count=1)]
    (item,) = serving.read_baseline_inputs(
        RecordingConnection([raw_row(runs)]), network="testnet", input_digest=DIGEST, limit=1)
    assert [c["window_start"] for c in item["cells"]] == [T0 - 900, T0 - 600, T0 - 300]
    assert [c["state"] for c in item["cells"]] == ["zero", "zero", "observed"]
    for c in [*item["cells"], item["current_cell"]]:
        assert set(c) == {"window_start", "state", "swaps", "known_traders", "unknown_count",
                          "coverage", "synthetic"}
    assert item["policy"] == {"policy_id": "sha256:" + "b" * 64, "lookback_seconds": 1200,
                              "min_prior_windows": 2}
    assert item["coverage_counts"]["by_state"]["zero"] == 2


def test_reader_validates_its_arguments():
    conn = RecordingConnection([])
    for kw in ({"network": "devnet", "input_digest": DIGEST}, {"network": "testnet"},
               {"network": "testnet", "input_digest": "nope"},
               {"network": "testnet", "token": "t"},
               {"network": "testnet", "token": "t", "window_start": 7}):
        with pytest.raises(ValueError):
            serving.read_baseline_inputs(conn, limit=1, **kw)
    assert conn.sql is None
