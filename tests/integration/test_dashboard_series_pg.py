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
HOSTILE = "jetton:<img onerror=alert(1)>é\u202e日本"
GAPS = {2, 3, 7}


def _insert(db, network, start, token, swaps=1):
    db.execute(
        "INSERT INTO served_features (network, window_start, window_end, token, generation, swaps,"
        " input_raw, output_raw, known_traders, unknown_count, coverage_status, status,"
        " provisional, write_fence) VALUES (%s,%s,%s,%s,'g',%s,%s,NULL,1,0,'unknown',"
        "'provisional',true,1)", (network, start, start + 300, token, swaps, swaps))


@pytest.fixture
def seeded(db):  # noqa: F811
    for i in range(12):
        if i not in GAPS:
            _insert(db, NET, T0 + 300 * i, "jetton:A", swaps=i + 1)
    for i in range(0, 12, 2):
        _insert(db, NET, T0 + 300 * i, "jetton:B")
        _insert(db, NET, T0 + 300 * i, HOSTILE)
    _insert(db, NET, T0 + 300 * 20, "jetton:late")
    _insert(db, "mainnet", T0, "jetton:A")
    for i in (0, 1):
        db.execute(
            "INSERT INTO action_window_qualification (network, window_start, window_end, mode,"
            " mapping_version, delta_seconds, trace_ranges, source_epoch, synthetic,"
            " dependency_epoch, lease_owner, lease_fence) VALUES (%s,%s,%s,'bounded_observed',"
            "'m.v1',60,'[[1,2]]','{}',true,1,'o',1)", (NET, T0 + 300 * i, T0 + 300 * i + 300))
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


def get(base, path, query):
    url = base + path + "?" + urlencode(query)
    try:
        with urlopen(Request(url), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def walk(base, path, name, query):
    items, cursor = [], None
    for _ in range(40):
        status, body = get(base, path, {**query, **({"cursor": cursor} if cursor else {})})
        assert status == 200, body
        items += body[name]
        cursor = body.get("next_cursor")
        if cursor is None:
            return items
    raise AssertionError("cursor walk did not terminate")


RANGE = {"from": T0, "to": T0 + 300 * 12}


def test_series_pages_every_row_exactly_once_ascending_without_gap_fill(base):
    q = {"network": NET, "token": "jetton:A", **RANGE}
    full = walk(base, "/v1/series", "series", {**q, "limit": 1000})
    paged = walk(base, "/v1/series", "series", {**q, "limit": 3})
    assert paged == full
    starts = [p["window_start"] for p in full]
    assert starts == [T0 + 300 * i for i in range(12) if i not in GAPS]
    assert starts == sorted(set(starts))


def test_series_marks_unrecorded_qualification_as_null_not_live(base):
    _, body = get(base, "/v1/series", {"network": NET, "token": "jetton:A", **RANGE})
    by_offset = {(p["window_start"] - T0) // 300: p for p in body["series"]}
    assert by_offset[0]["qualification"] == {"mode": "bounded_observed", "synthetic": True}
    assert by_offset[1]["qualification"] == {"mode": "bounded_observed", "synthetic": True}
    assert by_offset[0]["provenance"]["qualification_class"] == "synthetic"
    for offset in (4, 5, 6, 11):
        assert by_offset[offset]["qualification"] is None
        assert by_offset[offset]["provenance"]["qualification_class"] == "unrecorded"
        assert by_offset[offset]["provenance"]["certified_claimed"] == 0
    assert body["provenance"]["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert dashboard_payload.find_violations(body) == []


def test_series_range_is_half_open_and_network_scoped(base):
    _, body = get(base, "/v1/series", {"network": NET, "token": "jetton:A",
                                       "from": T0 + 300, "to": T0 + 900})
    assert [p["window_start"] for p in body["series"]] == [T0 + 300]
    _, body = get(base, "/v1/series", {"network": "mainnet", "token": "jetton:A", **RANGE})
    assert [p["window_start"] for p in body["series"]] == [T0]
    _, body = get(base, "/v1/series", {"network": NET, "token": "jetton:none", **RANGE})
    assert body["series"] == [] and body["provenance"]["source_kind"] == "unverified_origin"


def test_series_hostile_token_roundtrips_through_cursor_inert(base):
    q = {"network": NET, "token": HOSTILE, **RANGE, "limit": 2}
    status, page = get(base, "/v1/series", q)
    assert status == 200 and len(page["series"]) == 2 and page["token"] == HOSTILE
    items = walk(base, "/v1/series", "series", q)
    assert [p["window_start"] for p in items] == [T0 + 300 * i for i in range(0, 12, 2)]
    status, body = get(base, "/v1/series", {**q, "token": "jetton:B", "cursor": page["next_cursor"]})
    assert status == 400 and body == {"error": "invalid_query"}


def test_series_bounds_are_400_over_http(base):
    q = {"network": NET, "token": "jetton:A"}
    for bad in ({"from": T0, "to": T0}, {"from": T0 + 1, "to": T0 + 300},
                {"from": T0, "to": T0 + 604800 + 300}, {"from": T0}, {}):
        assert get(base, "/v1/series", {**q, **bad})[0] == 400
    assert get(base, "/v1/series", {**q, **RANGE, "limit": 1001})[0] == 400
    assert get(base, "/v1/series", {**q, "from": T0, "to": T0 + 604800})[0] == 200


def test_tokens_keyset_pagination_complete_and_disjoint(base):
    q = {"network": NET, **RANGE}
    full = walk(base, "/v1/tokens", "tokens", {**q, "limit": 1000})
    paged = walk(base, "/v1/tokens", "tokens", {**q, "limit": 1})
    names = [t["token"] for t in paged]
    assert paged == full and len(names) == len(set(names))
    assert names == sorted(names, key=lambda t: t.encode("utf-8"))
    assert set(names) == {"jetton:A", "jetton:B", HOSTILE}
    a = next(t for t in full if t["token"] == "jetton:A")
    assert a["feature_rows"] == 9 and a["window_count"] == 9
    assert a["first_window_start"] == T0 and a["latest_window_end"] == T0 + 300 * 12
    _, body = get(base, "/v1/tokens", {**q, "limit": 1})
    assert body["provenance"]["certification_state"] == "not_certified"
    status, body = get(base, "/v1/tokens", {**q, "limit": 1, "cursor": body["next_cursor"],
                                            "to": T0 + 300 * 13})
    assert status == 400


def test_windows_range_filter_with_cursor(base):
    q = {"network": NET, "from": T0 + 300 * 4, "to": T0 + 300 * 10}
    full = walk(base, "/v1/windows", "windows", {**q, "limit": 1000})
    paged = walk(base, "/v1/windows", "windows", {**q, "limit": 2})
    assert paged == full
    assert [w["window_start"] for w in full] == [T0 + 300 * i for i in (4, 5, 6, 8, 9)]
    assert get(base, "/v1/windows", {"network": NET, "from": T0, "to": T0})[0] == 400
    plain = get(base, "/v1/windows", {"network": NET, "limit": 1})[1]
    assert "provenance" not in plain and set(plain) == {"windows", "next_cursor"}


def test_series_uses_token_index(seeded, reader_dsn):
    with psycopg.connect(reader_dsn) as c:
        c.execute("SET enable_seqscan = off")
        plan = "\n".join(r[0] for r in c.execute(
            "EXPLAIN SELECT window_start FROM served_features WHERE network=%s "
            'AND token COLLATE "C" = %s COLLATE "C" AND window_start >= %s AND window_start < %s '
            "ORDER BY window_start", (NET, "jetton:A", T0, T0 + 3600)).fetchall())
    assert "served_features_token_idx" in plan, plan


def test_reader_login_cannot_write_via_new_paths(seeded, reader_dsn):
    with psycopg.connect(reader_dsn) as c:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("DELETE FROM served_features")
