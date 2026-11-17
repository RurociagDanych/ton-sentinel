"""Dashboard API unit tests: provenance block, payload safety filter, /v1/policy shape (no database)."""
import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from ton_sentinel import dashboard_payload as dp
from ton_sentinel.api import create_app

KEYS = {"source_kind", "qualification_class", "certification_state", "coverage_state",
        "scoring_state", "honesty_banner", "generated_at", "certified_claimed",
        "synthetic", "bounded_observed"}
NOW = datetime(2026, 11, 13, 12, 0, tzinfo=timezone.utc)


def test_provenance_has_every_key():
    block = dp.build_provenance(qualification=[("bounded_observed", True, 2)], scored=1, now=NOW)
    assert KEYS <= set(block)
    assert block["generated_at"] == "2026-11-13T12:00:00Z"


@pytest.mark.parametrize("rows", [None, [], [("bounded_observed", False, 1)],
                                  [("bounded_observed", True, 1)],
                                  [("certified", False, 5)],
                                  [("certified", False, 5), ("bounded_observed", True, 1)]])
@pytest.mark.parametrize("scored", [None, 0, 3])
def test_certification_never_certified(rows, scored):
    block = dp.build_provenance(qualification=rows, scored=scored, now=NOW)
    assert block["certification_state"] == "not_certified"
    assert block["honesty_banner"] == dp.HONESTY_BANNER


def test_qualification_classes():
    f = lambda rows: dp.build_provenance(qualification=rows, scored=0, now=NOW)
    assert f(None)["qualification_class"] == "unavailable"
    assert f(None)["coverage_state"] == "unavailable"
    assert f([])["qualification_class"] == "unrecorded"
    assert f([("bounded_observed", False, 1)])["qualification_class"] == "bounded_observed"
    mixed = f([("bounded_observed", False, 1), ("bounded_observed", True, 4)])
    assert mixed["qualification_class"] == "synthetic" and mixed["synthetic"] == 4
    assert f([("certified", False, 2)])["certified_claimed"] == 2
    assert f([("certified", False, 2)])["qualification_class"] != "certified"


def test_unavailable_is_never_zero():
    block = dp.build_provenance(qualification=None, scored=None, now=NOW)
    assert block["synthetic"] is None and block["bounded_observed"] is None
    assert block["certified_claimed"] is None and block["scoring_state"] == "unavailable"


def test_banner_text_fixed():
    assert dp.HONESTY_BANNER == (
        "Local fixture data. Not live, not certified, detector effectiveness unmeasured. "
        "Row origin is not recorded; provisional rows are not final.")


@pytest.mark.parametrize("key", sorted(dp.FORBIDDEN_KEYS))
def test_filter_strips_forbidden_key_at_any_depth(key):
    payload = {"ok": 1, "a": [{"b": {key: "x", "keep": 2}}], key: "y"}
    cleaned = dp.strip_forbidden(payload)
    assert cleaned == {"ok": 1, "a": [{"b": {"keep": 2}}]}
    assert payload[key] == "y"  # input untouched
    assert dp.find_violations(cleaned) == []


def test_key_match_is_case_insensitive():
    assert dp.strip_forbidden({"Lease_Owner": 1, "x": {"DSN": 2}}) == {"x": {}}


@pytest.mark.parametrize("value", [
    "postgres://u:p@h/db", "postgresql://h/db", "x password=abc", "api_key: z", "X-Api-Key: z",
    "see https://example.org/feed", "0:" + "ab" * 32, "EQ" + "A" * 46, "UQ" + "b-_" * 15 + "A"])
def test_filter_refuses_forbidden_values(value):
    with pytest.raises(dp.UnsafePayload):
        dp.ensure_safe({"deep": [{"x": value}]})


def test_ensure_safe_returns_stripped_and_accepts_clean():
    out = dp.ensure_safe({"token": "USDT", "n": 3, "lease_owner": "w1"})
    assert out == {"token": "USDT", "n": 3}


def test_response_cap_refused():
    with pytest.raises(dp.UnsafePayload):
        dp.ensure_safe({"blob": "a" * (dp.LIMITS["response_cap_bytes"] + 1)})


def test_policy_limits_shape():
    assert set(dp.LIMITS) == {"page_size_default", "page_size_max", "time_range_seconds_max",
                              "response_cap_bytes"}
    assert all(type(v) is int and v > 0 for v in dp.LIMITS.values())


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class _Conn:
    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        s = " ".join(str(statement).split())
        if "FROM active_detector_policy" in s or "FROM detector_policies" in s:
            body = {"version": "detector-policy.v1", "lookback_seconds": 604800,
                    "min_prior_windows": 24, "score_threshold": "6", "lease_owner": "leak"}
            return _Result([("sha256:" + "a" * 64, "detector-policy.v1", body)])
        if "action_window_qualification" in s:
            return _Result([("bounded_observed", True, 2)])
        if "detector_results" in s:
            return _Result([(1,)])
        return _Result([])


def _get(path):
    out = {}
    def sr(status, headers, *e):
        out["status"] = status
    with patch("ton_sentinel.api.psycopg.connect", _Conn):
        body = b"".join(create_app(dsn="postgresql://x/x")({"REQUEST_METHOD": "GET", "PATH_INFO": path.split("?")[0],
                                                           "QUERY_STRING": path.partition("?")[2]}, sr))
    return out["status"], json.loads(body)


def test_policy_endpoint_shape():
    status, body = _get("/v1/policy")
    assert status == "200 OK"
    assert set(body) == {"policy", "limits", "honesty_banner", "provenance"}
    assert body["policy"]["policy_id"].startswith("sha256:")
    assert body["policy"]["version"] == "detector-policy.v1"
    assert body["policy"]["score_threshold"] == "6"
    assert "lease_owner" not in json.dumps(body)
    assert body["limits"] == dp.LIMITS and body["honesty_banner"] == dp.HONESTY_BANNER
    assert body["provenance"]["certification_state"] == "not_certified"


def test_policy_rejects_malformed_id():
    status, body = _get("/v1/policy?policy_id=nope")
    assert status == "400 Bad Request"


def test_policy_post_is_405():
    out = {}
    create_app(dsn="postgresql://x/x")({"REQUEST_METHOD": "POST", "PATH_INFO": "/v1/policy"},
                                       lambda s, h, *e: out.setdefault("s", s))
    assert out["s"].startswith("405")


# Row origin. Origins are (origin, served windows) rows over the scoped windows.
def test_all_live_windows_make_the_provenance_live():
    block = dp.build_provenance(qualification=[("bounded_observed", False, 3)], scored=1,
                                origins=[("live", 3)], now=NOW)
    assert block["source_kind"] == "live"
    assert block["honesty_banner"] == dp.LIVE_BANNER
    assert "Not live" not in block["honesty_banner"]
    assert block["certification_state"] == "not_certified"
    assert block["origin_windows"] == {"live": 3}


def test_live_mixed_with_other_origins_is_mixed_and_never_live():
    block = dp.build_provenance(qualification=[], scored=0,
                                origins=[("live", 3), ("unrecorded", 2)], now=NOW)
    assert block["source_kind"] == "mixed_origin"
    assert block["honesty_banner"] == dp.MIXED_BANNER
    assert block["origin_windows"] == {"live": 3, "unrecorded": 2}


@pytest.mark.parametrize("origins", [None, [], [("unrecorded", 4)], [("recorded", 1)],
                                     [("synthetic", 2)], [("live", 0)], [("bogus", 1)],
                                     [("bounded_observed", True, 2)]])
def test_without_live_origin_the_block_stays_unverified(origins):
    block = dp.build_provenance(qualification=[], scored=0, origins=origins, now=NOW)
    assert block["source_kind"] == "unverified_origin"
    assert block["honesty_banner"] == dp.HONESTY_BANNER


def test_live_banner_never_claims_certification_or_effectiveness():
    for banner in (dp.LIVE_BANNER, dp.MIXED_BANNER):
        assert "not certified" in banner and "effectiveness unmeasured" in banner
        assert dp.find_violations({"b": banner}) == []


def test_series_points_carry_their_own_window_origin():
    from ton_sentinel import api

    rows = [(1794915000, 1794915300, 1, 5, "1", "2", 3, 0, "unknown", "provisional", True,
             "bounded_observed", False, "live"),
            (1794915300, 1794915600, 1, 5, "1", "2", 3, 0, "unknown", "provisional", True,
             None, None, None)]

    class Conn(_Conn):
        def execute(self, statement, parameters=None):
            s = " ".join(str(statement).split())
            if s.startswith("SELECT f.window_start"):
                return _Result(rows)
            return super().execute(statement, parameters)

    out = {}
    def sr(status, headers, *e):
        out["status"] = status
    with patch("ton_sentinel.api.psycopg.connect", Conn):
        body = json.loads(b"".join(create_app(dsn="postgresql://x/x")(
            {"REQUEST_METHOD": "GET", "PATH_INFO": "/v1/series",
             "QUERY_STRING": "network=mainnet&token=TON&from=1794914700&to=1794915900"}, sr)))
    assert out["status"] == "200 OK", body
    first, second = body["series"]
    assert first["provenance"]["source_kind"] == "live"
    assert second["provenance"]["source_kind"] == "unverified_origin"
    assert first["provenance"]["origin_windows"] == {"live": 1}
    assert first["qualification"] == {"mode": "bounded_observed", "synthetic": False}
    assert "origin" not in first


def test_read_provenance_scopes_origin_counts_to_the_requested_windows():
    from ton_sentinel.db import serving

    seen = []

    class Conn(_Conn):
        def execute(self, statement, parameters=None):
            s = " ".join(str(statement).split())
            if "origin" in s and "served_features" in s:
                seen.append((s, parameters))
                return _Result([("live", 3)])
            return super().execute(statement, parameters)

    block = serving.read_provenance(Conn(), network="mainnet", window_from=100 * 300,
                                    window_to=103 * 300)
    assert block["source_kind"] == "live"
    statement, parameters = seen[0]
    assert "action_window_qualification" in statement
    assert parameters["n"] == "mainnet" and parameters["f"] == 30000 and parameters["t"] == 30900
    assert serving.read_provenance(Conn())["source_kind"] in ("live", "unverified_origin")


def test_dashboard_state_legend_knows_the_origin_states():
    from pathlib import Path

    legend = (Path(dp.__file__).parent / "dashboard" / "states.js").read_text()
    for state in ("\"live\":", "\"mixed_origin\":", "\"unverified_origin\":"):
        assert state in legend


def test_top_level_banner_follows_the_scoped_provenance():
    # The payload-level honesty_banner must not say "Not live" when every scoped window is
    # live, and must stay the fixture banner otherwise.
    class Conn(_Conn):
        def execute(self, statement, parameters=None):
            s = " ".join(str(statement).split())
            if s.startswith("SELECT coalesce(q.origin"):
                return _Result([("live", 3)])
            if "action_window_qualification" in s and "count(*) FROM action_window" not in s:
                return _Result([])                  # view rows: none in this fake
            return super().execute(statement, parameters)

    out = {}
    def sr(status, headers, *e):
        out["status"] = status
    query = "network=mainnet&token=TON&from=1794914700&to=1794915900"
    for path in ("/v1/series", "/v1/coverage", "/v1/detections/range"):
        with patch("ton_sentinel.api.psycopg.connect", Conn):
            body = json.loads(b"".join(create_app(dsn="postgresql://x/x")(
                {"REQUEST_METHOD": "GET", "PATH_INFO": path, "QUERY_STRING": query}, sr)))
        assert out["status"] == "200 OK", (path, body)
        assert body["provenance"]["source_kind"] == "live", path
        assert body["honesty_banner"] == dp.LIVE_BANNER, path
    status, body = _get("/v1/policy")
    assert body["honesty_banner"] == dp.HONESTY_BANNER     # fake: origin rows unreadable
