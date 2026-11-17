"""Dashboard API on real PostgreSQL over HTTP, through the sentinel_reader login."""
import json
import os
import threading
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from tests.integration.test_revisions import db  # noqa: F401

NET = "testnet"
T0 = 1_700_000_100 - (1_700_000_100 % 300)
TOKEN = "jetton:A"
RANGE = {"from": T0, "to": T0 + 300 * 12}
EXPECTED = {0: "pre_universe", 1: "pre_universe", 2: "observed", 3: "zero", 4: "partial",
            5: "provisional", 6: "provisional", 7: "missing", 8: "missing", 9: "missing",
            10: "missing", 11: "provisional"}


def _feature(db, network, start, token, swaps, provisional):
    db.execute(
        "INSERT INTO served_features (network, window_start, window_end, token, generation, swaps,"
        " input_raw, output_raw, known_traders, unknown_count, coverage_status, status,"
        " provisional, write_fence) VALUES (%s,%s,%s,%s,'g',%s,%s,NULL,2,1,'bounded_observed',"
        "%s,%s,1)", (network, start, start + 300, token, swaps, swaps,
                     "provisional" if provisional else "final", provisional))


def _qualify(db, offset, synthetic=True):
    start = T0 + 300 * offset
    db.execute(
        "INSERT INTO action_window_qualification (network, window_start, window_end, mode,"
        " mapping_version, delta_seconds, trace_ranges, source_epoch, synthetic,"
        " dependency_epoch, lease_owner, lease_fence) VALUES (%s,%s,%s,'bounded_observed',"
        "'m.v1',60,'[[1,2]]','{}',%s,1,'o',1)", (NET, start, start + 300, synthetic))


@pytest.fixture
def seeded(db):  # noqa: F811
    _feature(db, NET, T0 + 600, TOKEN, 3, False)       # 2 observed
    _feature(db, NET, T0 + 900, TOKEN, 0, False)       # 3 zero
    _feature(db, NET, T0 + 1200, TOKEN, 2, False)      # 4 partial (no qualification)
    _feature(db, NET, T0 + 1500, TOKEN, 2, True)       # 5 provisional row
    _feature(db, NET, T0 + 1800, TOKEN, 2, False)      # 6 provisional: pending dirty row
    for offset in (2, 3, 5, 6, 11):
        _qualify(db, offset)
    db.execute(
        "INSERT INTO dirty_windows (network, token, window_start, role, window_end, covers_start,"
        " covers_end, dependency_epoch, reason, cause_event_id) VALUES (%s,%s,%s,'target',%s,%s,%s,"
        "1,'late_action','ev')", (NET, TOKEN, T0 + 1800, T0 + 2100, T0 + 1800, T0 + 2100))
    for offset, reason in ((7, "no_action_time_scan_evidence"),
                           (8, "trace_completion_mapping_unavailable")):
        start = T0 + 300 * offset
        db.execute(
            "INSERT INTO action_window_coverage (coverage_id, network, epoch, window_start,"
            " window_end, status, eligible_zero, reason, lineage) VALUES (%s,%s,'{}',%s,%s,"
            "'missing',false,%s,%s::jsonb)",
            (uuid.uuid4(), NET, start, start + 300, reason, json.dumps([{"r": 1}, {"r": 2}])))
    start = T0 + 300 * 9
    db.execute(
        "INSERT INTO window_admission_refusals (admission_digest, network, window_start,"
        " window_end, status, reasons, work_windows, decided_at, event_cutoff, repair_cutoff,"
        " dependency_epoch, usage, budget, lease_owner, lease_fence) VALUES (%s,%s,%s,%s,"
        "'missing','[\"window_open\"]','[1]',1,1,1,1,'{}','{}','o',1)",
        ("sha256:" + "a" * 64, NET, start, start + 300))
    _feature(db, NET, T0, "jetton:B", 1, False)        # other token must not leak
    _feature(db, "mainnet", T0 + 600, TOKEN, 1, False)  # other network must not leak
    db.commit()
    return db


@pytest.fixture
def reader_dsn(seeded):
    admin_dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    role, password = "rdr_" + uuid.uuid4().hex[:12], uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(password)))
        admin.execute(sql.SQL("GRANT sentinel_reader TO {}").format(sql.Identifier(role)))
    try:
        yield make_conninfo(admin_dsn, user=role, password=password,
                            options=f"-c search_path={seeded.schema_name}")
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


@pytest.fixture
def base(reader_dsn):
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get(base, query):
    url = base + "/v1/coverage?" + urlencode(query)
    try:
        with urlopen(Request(url), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def walk(base, query):
    items, cursor = [], None
    for _ in range(40):
        status, body = get(base, {**query, **({"cursor": cursor} if cursor else {})})
        assert status == 200, body
        items += body["coverage"]
        cursor = body.get("next_cursor")
        if cursor is None:
            return items
    raise AssertionError("cursor walk did not terminate")


Q = {"network": NET, "token": TOKEN, **RANGE}


def by_offset(items):
    return {(c["window_start"] - T0) // 300: c for c in items}


def test_every_window_listed_with_state_gaps_missing_never_filled(base):
    status, body = get(base, {**Q, "limit": 1000})
    assert status == 200 and len(body["coverage"]) == 12
    cells = by_offset(body["coverage"])
    assert {o: c["state"] for o, c in cells.items()} == EXPECTED
    assert [c["window_start"] for c in body["coverage"]] == [T0 + 300 * i for i in range(12)]
    assert all(c["window_end"] == c["window_start"] + 300 for c in body["coverage"])
    for offset in (7, 8, 9, 10):  # no served row: missing, no values invented
        assert cells[offset]["served"]["rows"] == 0
        assert cells[offset]["served"]["known_traders"] is None
    assert cells[11]["state"] == "provisional"  # qualified gap is not proven zero
    assert cells[11]["reasons"] == ["qualified_without_served_row"]


def test_missing_reasons_and_admission_refusals_are_verbatim(base):
    cells = by_offset(get(base, {**Q, "limit": 1000})[1]["coverage"])
    assert cells[7]["missing"] == ["no_action_time_scan_evidence"]
    assert cells[7]["lineage_records"] == 2 and cells[7]["admission_refused"] is False
    assert cells[8]["missing"] == ["trace_completion_mapping_unavailable"]
    assert cells[9]["admission_refused"] is True and cells[9]["admission_reasons"] == ["window_open"]
    assert cells[9]["missing"] == [] and cells[9]["state"] == "missing"
    assert cells[10]["missing"] == [] and cells[10]["admission_refused"] is False
    assert cells[10]["reasons"] == ["no_qualification"]
    assert "coverage_missing" in cells[7]["reasons"]


def test_qualification_served_and_synthetic_flag_propagate(base):
    status, body = get(base, {**Q, "limit": 1000})
    cells = by_offset(body["coverage"])
    assert cells[2]["qualification"] == {"mode": "bounded_observed", "synthetic": True,
                                         "mapping_version": "m.v1", "delta_seconds": 60}
    assert cells[2]["served"] == {"rows": 1, "coverage_status_counts": {"bounded_observed": 1},
                                  "known_traders": 2, "unknown_count": 1}
    assert cells[4]["qualification"] is None and cells[4]["state"] == "partial"
    assert cells[6]["reasons"] == ["pending_recompute"]
    assert cells[5]["reasons"] == ["row_provisional"]
    assert body["provenance"]["synthetic"] == 5
    assert body["provenance"]["coverage_state"] == "synthetic_bounded_observed"
    assert body["provenance"]["certified_claimed"] == 0


def test_other_token_network_and_lineage_contents_never_returned(base):
    status, body = get(base, {**Q, "limit": 1000})
    text = json.dumps(body)
    assert "jetton:B" not in text and '"r"' not in text and "lineage\"" not in text
    assert dashboard_payload.find_violations(body) == []
    assert by_offset(body["coverage"])[2]["served"]["rows"] == 1  # mainnet row not counted
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    status, body = get(base, {"network": NET, "token": "jetton:none", **RANGE, "limit": 1000})
    assert status == 200 and {c["state"] for c in body["coverage"]} == {"pre_universe"}


def test_cursor_pages_each_window_once_and_is_bound(base):
    full = walk(base, {**Q, "limit": 1000})
    paged = walk(base, {**Q, "limit": 5})
    assert paged == full and len(paged) == 12
    status, page = get(base, {**Q, "limit": 5})
    assert len(page["coverage"]) == 5 and "next_cursor" in page
    for tamper in ({"token": "jetton:B"}, {"to": T0 + 300 * 13}, {"limit": 6},
                   {"network": "mainnet"}):
        status, body = get(base, {**Q, "limit": 5, **tamper, "cursor": page["next_cursor"]})
        assert status == 400 and body == {"error": "invalid_query"}


def test_range_is_half_open_and_bounds_are_400(base):
    status, body = get(base, {**Q, "from": T0 + 600, "to": T0 + 1200})
    assert [c["window_start"] for c in body["coverage"]] == [T0 + 600, T0 + 900]
    for bad in ({"from": T0, "to": T0}, {"from": T0 + 1, "to": T0 + 300},
                {"from": T0, "to": T0 + 604800 + 300}, {"from": T0}, {}):
        assert get(base, {"network": NET, "token": TOKEN, **bad})[0] == 400
    assert get(base, {**Q, "limit": 1001})[0] == 400
    status, body = get(base, {"network": NET, "token": TOKEN, "from": T0, "to": T0 + 604800})
    assert status == 200 and len(body["coverage"]) == 288 and "next_cursor" in body


def test_reader_login_cannot_write_via_coverage_tables(seeded, reader_dsn):
    with psycopg.connect(reader_dsn) as c:
        for table in ("action_window_coverage", "window_admission_refusals", "dirty_windows"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                c.execute(f"DELETE FROM {table}")
                c.rollback()
            c.rollback()
