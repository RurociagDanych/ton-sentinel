"""Dashboard API on real PostgreSQL over HTTP, through the sentinel_reader login (known fixture counts)."""
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
RANGE = {"from": T0, "to": T0 + 300 * 12}
Q = {"network": NET, **RANGE}
RANGE_ID = uuid.uuid4()


def _feature(db, network, offset, token, status):
    start = T0 + 300 * offset
    db.execute(
        "INSERT INTO served_features (network, window_start, window_end, token, generation, swaps,"
        " input_raw, output_raw, known_traders, unknown_count, coverage_status, status,"
        " provisional, write_fence) VALUES (%s,%s,%s,%s,'g',1,1,NULL,1,0,%s,'final',false,1)",
        (network, start, start + 300, token, status))


def _qualify(db, network, offset, mode="bounded_observed", synthetic=True):
    start = T0 + 300 * offset
    db.execute(
        "INSERT INTO action_window_qualification (network, window_start, window_end, mode,"
        " mapping_version, delta_seconds, trace_ranges, source_epoch, synthetic,"
        " dependency_epoch, lease_owner, lease_fence, certification) VALUES (%s,%s,%s,%s,"
        "'m.v1',60,'[[1,2]]','{}',%s,1,'o',1,%s::jsonb)",
        (network, start, start + 300, mode, synthetic,
         '{"claim": "fixture"}' if mode == "certified" else None))


def _dirty(db, network, offset, token, role, status):
    start = T0 + 300 * offset
    db.execute(
        "INSERT INTO dirty_windows (network, token, window_start, role, window_end, covers_start,"
        " covers_end, dependency_epoch, reason, cause_event_id, status) VALUES "
        "(%s,%s,%s,%s,%s,%s,%s,1,'late_action','ev',%s)",
        (network, token, start, role, start + 300, start, start + 300, status))


def _revision_q(db, network, subject, status):
    db.execute(
        "INSERT INTO revision_quarantine (subject, scope, network, trace_id, reason,"
        " content_digest, range_id, status, resolved_at, resolved_generation) VALUES "
        "(%s,'event',%s,'t','stale_revision','d',%s,%s,"
        "CASE WHEN %s = 'resolved' THEN clock_timestamp() END,"
        "CASE WHEN %s = 'resolved' THEN 1 END)",
        (subject, network, RANGE_ID, status, status, status))


def _delivery_q(db, network, offset, event):
    db.execute(
        "INSERT INTO delivery_quarantine (range_id, event_id, network, window_start, reasons,"
        " semantic_digest, proposed_action, payload_from, guard_retired_before, lease_owner,"
        " lease_fence) VALUES (%s,%s,%s,%s,'[\"expired\"]','d','{}',0,0,'o',1)",
        (RANGE_ID, event, network, T0 + 300 * offset))


@pytest.fixture
def seeded(db):  # noqa: F811
    db.execute("INSERT INTO source_ranges(range_id,start_inclusive,end_exclusive,epoch,status)"
               " VALUES (%s,1,2,'{}','complete')", (RANGE_ID,))
    # served rows in range: bounded_observed 3, incomplete 1, NULL status 1; windows 0,1,2,3
    _feature(db, NET, 0, "jetton:A", "bounded_observed")
    _feature(db, NET, 1, "jetton:A", "bounded_observed")
    _feature(db, NET, 2, "jetton:A", "incomplete")
    _feature(db, NET, 1, "jetton:B", "bounded_observed")
    _feature(db, NET, 3, "jetton:B", None)
    _feature(db, NET, 20, "jetton:A", "bounded_observed")       # outside range
    _feature(db, "mainnet", 0, "jetton:A", "bounded_observed")  # other network
    # qualification in range: synthetic 3, bounded (non-synthetic) 1, certified claim 1
    for offset in (0, 1, 2):
        _qualify(db, NET, offset)
    _qualify(db, NET, 3, synthetic=False)
    _qualify(db, NET, 4, mode="certified", synthetic=False)
    _qualify(db, NET, 20)                                       # outside range
    _qualify(db, "mainnet", 0)                                  # other network
    # dirty: pending target 2, pending baseline_dependents 1 (done / outside / mainnet ignored)
    _dirty(db, NET, 2, "jetton:A", "target", "pending")
    _dirty(db, NET, 3, "jetton:B", "target", "pending")
    _dirty(db, NET, 4, "jetton:A", "baseline_dependents", "pending")
    _dirty(db, NET, 5, "jetton:A", "target", "done")
    _dirty(db, NET, 20, "jetton:A", "target", "pending")
    _dirty(db, "mainnet", 2, "jetton:A", "target", "pending")
    # revision quarantine: 2 open on testnet (1 resolved, 1 mainnet open ignored)
    _revision_q(db, NET, "s1", "open")
    _revision_q(db, NET, "s2", "open")
    _revision_q(db, NET, "s3", "resolved")
    _revision_q(db, "mainnet", "s4", "open")
    # delivery quarantine: 2 in range, 1 outside, 1 mainnet
    _delivery_q(db, NET, 1, "e1")
    _delivery_q(db, NET, 6, "e2")
    _delivery_q(db, NET, 30, "e3")
    _delivery_q(db, "mainnet", 1, "e4")
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
    try:
        with urlopen(Request(base + "/v1/quality?" + urlencode(query)), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def test_known_counts_for_one_network_and_range(base):
    status, body = get(base, Q)
    assert status == 200, body
    quality = body["quality"]
    assert quality["served_rows_by_coverage_status"]["status"] == "observed"
    assert quality["served_rows_by_coverage_status"]["value"] == {
        "bounded_observed": 3, "incomplete": 1, "unrecorded": 1}
    assert quality["served_rows_by_coverage_status"]["truncated"] is False
    assert quality["served_windows"] == {"status": "observed", "value": 4}
    assert quality["dirty_windows_pending_by_role"]["value"] == {"target": 2,
                                                                 "baseline_dependents": 1}
    assert quality["dirty_windows_pending_by_role"]["truncated"] is False
    assert quality["revision_quarantine_open"] == {"status": "observed", "value": 2}
    assert quality["delivery_quarantine"] == {"status": "observed", "value": 2}
    assert quality["qualifications_by_class"]["value"] == {
        "synthetic_bounded_observed": 3, "bounded_observed": 1, "certified_claimed": 1}


def test_range_is_half_open_and_other_network_not_counted(base):
    status, body = get(base, {"network": NET, "from": T0, "to": T0 + 300})
    assert status == 200
    assert body["quality"]["served_windows"]["value"] == 1          # only window 0
    status, body = get(base, {"network": "mainnet", **RANGE})
    assert body["quality"]["served_windows"]["value"] == 1
    assert body["quality"]["revision_quarantine_open"]["value"] == 1
    assert body["quality"]["delivery_quarantine"]["value"] == 1
    status, body = get(base, {"network": NET, "from": T0 + 7200, "to": T0 + 7500})
    assert body["quality"]["served_windows"] == {"status": "observed", "value": 0}


def test_untracked_tables_are_unavailable_never_zero(base):
    _, body = get(base, Q)
    for name in ("scan_quarantine", "coverage_ranges_pending",
                 "coverage_ranges_bounded_observed"):
        assert body["quality"][name] == {"status": "unavailable", "value": None,
                                         "reason": "not_network_scoped"}


def test_unreadable_table_is_unavailable_and_others_unaffected(seeded, base):
    seeded.execute("REVOKE SELECT ON dirty_windows FROM sentinel_reader")
    seeded.commit()
    status, body = get(base, Q)
    assert status == 200
    dirty = body["quality"]["dirty_windows_pending_by_role"]
    assert dirty["status"] == "unavailable" and dirty["value"] is None and dirty["reason"]
    assert body["quality"]["delivery_quarantine"]["value"] == 2
    assert body["quality"]["revision_quarantine_open"]["value"] == 2


def test_provenance_banner_and_no_forbidden_content(base):
    status, body = get(base, Q)
    assert status == 200
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    prov = body["provenance"]
    assert prov["certification_state"] == "not_certified"
    assert prov["synthetic"] is not None and prov["coverage_state"] != "unavailable"
    assert dashboard_payload.find_violations(body) == []
    text = json.dumps(body)
    for word in ("lease_owner", "lineage", "postgres", "proposed_action", "trace_id"):
        assert word not in text


@pytest.mark.parametrize("over", [{"to": T0}, {"to": T0 + 604800 + 300}, {"from": T0 + 1},
                                  {"network": "devnet"}, {"token": "jetton:A"}])
def test_range_bounds_400(base, over):
    status, body = get(base, {**Q, **over})
    assert status == 400 and body == {"error": "invalid_query"}


def test_reader_login_cannot_write_via_quality_tables(reader_dsn):
    with psycopg.connect(reader_dsn) as conn:
        for table in ("dirty_windows", "revision_quarantine", "delivery_quarantine"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                conn.execute(sql.SQL("DELETE FROM {}").format(sql.Identifier(table)))
            conn.rollback()
