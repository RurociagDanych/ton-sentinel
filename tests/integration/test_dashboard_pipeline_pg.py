"""Dashboard API on real PostgreSQL over HTTP, through sentinel_reader (seeded known backlog counts)."""

import json
import threading
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from ton_sentinel.db.work import promote_scores, rescore_stale, score_window
from tests.integration.test_baseline_grid_pg import promote_rows, row
from tests.integration.test_detection import NET, SCORED, TOK, ready_world
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401

T0 = SCORED


@pytest.fixture
def served(db, reader_dsn):  # noqa: F811
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get(base, query):
    try:
        with urlopen(Request(base + "/v1/pipeline?" + urlencode(query)), timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


def seed(db):
    """Served windows: 0 (never scored) and SCORED (scored, then made stale by a correction)."""
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()
    return lease


def test_backlog_counts_never_scored_and_stale(served):
    db, base = served
    lease = seed(db)
    promote_rows(db, lease, SCORED, [row(SCORED, TOK, 2, 2, 0)])
    db.commit()
    status, body = get(base, {"network": NET})
    assert status == 200, body
    backlog = body["pipeline"]["scoring_backlog"]["value"]
    assert backlog["unscored_served_windows"] == {"status": "observed", "value": 1}
    assert backlog["stale_results"]["status"] == "observed"
    assert backlog["stale_results"]["value"] == 1
    assert backlog["stale_results"]["oldest_age_seconds"] >= 0
    assert get(base, {"network": "mainnet"})[1]["pipeline"]["scoring_backlog"]["value"][
        "unscored_served_windows"]["value"] == 0
    assert rescore_stale(db, lease, network=NET) == [(NET, TOK, SCORED)]
    db.commit()
    after = get(base, {"network": NET})[1]["pipeline"]["scoring_backlog"]["value"]
    assert after["stale_results"]["value"] == 0 and after["unscored_served_windows"]["value"] == 1


def test_dirty_outbox_and_stream_sections_known_counts(served):
    db, base = served
    seed(db)
    for offset, role, status in ((1, "target", "pending"), (2, "target", "pending"),
                                 (3, "baseline_dependents", "pending"), (4, "target", "done")):
        start = T0 + 300 * offset
        db.execute(
            "INSERT INTO dirty_windows (network, token, window_start, role, window_end,"
            " covers_start, covers_end, dependency_epoch, reason, cause_event_id, status)"
            " VALUES (%s,'jetton:Z',%s,%s,%s,%s,%s,1,'late_action','ev',%s)",
            (NET, start, role, start + 300, start, start + 300, status))
    db.execute(
        "INSERT INTO stream_batches (environment_id, query_id, pipeline_epoch, batch_id, topic,"
        " topic_incarnation, replay_epoch, offset_metadata, record_digest, record_count)"
        " VALUES ('env-secret','q-secret','e',1,'t-secret','i',0,'{\"p\":9}'::jsonb,%s,42)",
        ("a" * 64,))
    db.commit()
    status, body = get(base, {"network": NET})
    assert status == 200, body
    pipe = body["pipeline"]
    assert pipe["dirty_windows_pending_by_role"]["value"] == {"target": 2, "baseline_dependents": 1}
    assert pipe["stream_progress"]["value"]["record_count"] == 42
    assert set(pipe["stream_progress"]["value"]) == {"committed_at", "record_count"}
    assert pipe["outbox"]["status"] == "observed"
    text = json.dumps(body)
    for leaked in ("env-secret", "q-secret", "t-secret", "offset_metadata"):
        assert leaked not in text


def test_not_measured_provenance_and_banner(served):
    _, base = served
    status, body = get(base, {"network": NET})
    assert status == 200
    for name in ("ingestion_freshness", "kafka_lag"):
        assert body["pipeline"][name]["status"] == "not_measured"
        assert body["pipeline"][name]["value"] is None and body["pipeline"][name]["reason"]
    assert body["pipeline"]["stream_progress"]["status"] == "none_recorded"
    assert body["honesty_banner"] == dashboard_payload.HONESTY_BANNER
    assert body["provenance"]["certification_state"] == "not_certified"
    assert dashboard_payload.find_violations(body) == []
    assert get(base, {"network": "devnet"})[0] == 400


def test_unreadable_table_is_unavailable_not_zero(served):
    db, base = served
    seed(db)
    db.execute("REVOKE SELECT ON detector_current FROM sentinel_reader")
    db.execute("REVOKE SELECT ON dirty_windows FROM sentinel_reader")
    db.commit()
    status, body = get(base, {"network": NET})
    assert status == 200
    backlog = body["pipeline"]["scoring_backlog"]["value"]
    assert backlog["stale_results"]["status"] == "unavailable"
    assert backlog["stale_results"]["value"] is None and backlog["stale_results"]["reason"]
    dirty = body["pipeline"]["dirty_windows_pending_by_role"]
    assert dirty["status"] == "unavailable" and dirty["value"] is None
    assert body["pipeline"]["outbox"]["status"] == "observed"
