"""Dashboard API over HTTP on real PostgreSQL: the endpoints the explorer reads, as the view sends them."""
import re
from pathlib import Path

from tests.integration.test_revisions import db  # noqa: F401
from tests.integration.test_dashboard_series_pg import (  # noqa: F401
    HOSTILE, NET, T0, base, get, reader_dsn, seeded, walk)

DASH = Path(__file__).resolve().parents[2] / "src" / "ton_sentinel" / "dashboard"
SEVEN_DAYS = 7 * 86400
PROVENANCE_KEYS = {"source_kind", "certification_state", "coverage_state", "scoring_state",
                   "honesty_banner", "generated_at"}
RANGE = {"from": T0, "to": T0 + 300 * 12}


def legend_states():
    return set(re.findall(r'^  "([a-z_]+)": \{ label:', (DASH / "states.js").read_text(), re.M))


def test_token_list_pages_by_keyset_with_provenance_and_the_unicode_token(base):
    q = {"network": NET, **RANGE, "limit": 2}
    status, first = get(base, "/v1/tokens", q)
    assert status == 200 and PROVENANCE_KEYS <= set(first["provenance"])
    assert len(first["tokens"]) == 2 and "next_cursor" in first
    names = [t["token"] for t in walk(base, "/v1/tokens", "tokens", q)]
    assert names == sorted(names, key=lambda s: s.encode()) and len(names) == len(set(names))
    assert {"jetton:A", "jetton:B", HOSTILE} <= set(names) and "jetton:late" not in names


def test_series_as_the_view_requests_it_has_gaps_provenance_and_unrecorded_null(base):
    status, body = get(base, "/v1/series", {"network": NET, "token": "jetton:A", **RANGE,
                                           "limit": 1000})
    assert status == 200 and PROVENANCE_KEYS <= set(body["provenance"])
    starts = [p["window_start"] - T0 for p in body["series"]]
    assert starts == [300 * i for i in range(12) if i not in (2, 3, 7)]  # gaps stay absent
    for point in body["series"]:
        assert PROVENANCE_KEYS <= set(point["provenance"])
        assert isinstance(point["provisional"], bool)
    assert body["series"][0]["qualification"] == {"mode": "bounded_observed", "synthetic": True}
    assert body["series"][-1]["qualification"] is None


def test_range_limit_of_seven_days_is_enforced_by_the_server_too(base):
    q = {"network": NET, "token": "jetton:A"}
    assert get(base, "/v1/series", {**q, "from": T0, "to": T0 + SEVEN_DAYS})[0] == 200
    assert get(base, "/v1/series", {**q, "from": T0, "to": T0 + SEVEN_DAYS + 300})[0] == 400
    assert get(base, "/v1/tokens", {"network": NET, "from": T0, "to": T0 + SEVEN_DAYS + 300})[0] == 400
    assert get(base, "/v1/coverage", {**q, "from": T0, "to": T0 + SEVEN_DAYS + 300})[0] == 400


def test_window_detail_reads_exactly_one_window_from_coverage(base):
    q = {"network": NET, "token": "jetton:A"}
    status, row = get(base, "/v1/coverage", {**q, "from": T0, "to": T0 + 300})
    assert status == 200 and PROVENANCE_KEYS <= set(row["provenance"])
    assert len(row["coverage"]) == 1 and "next_cursor" not in row
    detail = row["coverage"][0]
    assert detail["window_start"] == T0 and detail["state"] in legend_states()
    assert isinstance(detail["reasons"], list) and isinstance(detail["lineage_records"], int)
    status, gap = get(base, "/v1/coverage", {**q, "from": T0 + 600, "to": T0 + 900})
    assert status == 200 and gap["coverage"][0]["state"] in ("missing", "provisional")
    assert gap["coverage"][0]["served"]["rows"] == 0  # a gap window is not a served zero


def test_every_coverage_state_the_server_emits_is_in_the_client_legend(base):
    states = set()
    for token in ("jetton:A", "jetton:B"):
        status, body = get(base, "/v1/coverage", {"network": NET, "token": token, **RANGE})
        assert status == 200
        states |= {w["state"] for w in body["coverage"]}
    assert states and states <= legend_states(), states - legend_states()


def test_explorer_assets_are_served_with_csp_and_match_the_files(base):
    from urllib.request import urlopen
    for name in ("explorer.js", "explorer-model.js"):
        with urlopen(base + f"/dashboard/assets/{name}", timeout=8) as response:
            assert response.status == 200 and response.read() == (DASH / name).read_bytes()
            assert response.headers["Content-Type"].startswith("text/javascript")
            assert "script-src 'self'" in response.headers["Content-Security-Policy"]
