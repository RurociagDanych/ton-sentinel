"""Dashboard API over real HTTP + real PostgreSQL through the reader role: /v1/policy and payload safety."""
import json
import threading
from urllib.error import HTTPError
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel import dashboard_payload as dp
from ton_sentinel.api import create_app
from ton_sentinel.db.work import promote_scores, score_window
from tests.integration.test_detection import NET, SCORED, TOK, ready_world
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401

POLICY_ID = "sha256:34fc97d20f4907d727db53efd921830cdbb3ae17f877d67a1654d591db6830fc"


@pytest.fixture
def reader_served(db, reader_dsn):  # noqa: F811
    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield db, f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def get(base, path):
    try:
        with urlopen(base + path, timeout=8) as response:
            return response.status, response.read().decode("utf-8")
    except HTTPError as error:
        return error.code, error.read().decode("utf-8")


def seed(db):
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()


def test_policy_works_through_reader_role(reader_served):
    db, base = reader_served
    status, body = get(base, "/v1/policy")
    assert status == 200
    doc = json.loads(body)
    assert doc["policy"]["policy_id"] == POLICY_ID and doc["policy"]["active"] is True
    assert doc["policy"]["version"] == "detector-policy.v1"
    assert doc["limits"] == dp.LIMITS and doc["honesty_banner"] == dp.HONESTY_BANNER
    prov = doc["provenance"]
    assert prov["certification_state"] == "not_certified"
    assert prov["qualification_class"] in ("unrecorded", "synthetic", "bounded_observed")
    assert prov["qualification_class"] != "unavailable"  # reader role can read the source tables


def test_policy_pinned_malformed_and_unknown(reader_served):
    _, base = reader_served
    assert get(base, f"/v1/policy?policy_id={POLICY_ID}")[0] == 200
    assert get(base, "/v1/policy?policy_id=nope")[0] == 400
    assert get(base, "/v1/policy?x=1")[0] == 400
    assert get(base, "/v1/policy?policy_id=sha256:" + "0" * 64)[0] == 404


def test_provenance_reflects_seeded_synthetic_windows(reader_served):
    db, base = reader_served
    seed(db)
    prov = json.loads(get(base, "/v1/policy")[1])["provenance"]
    assert prov["qualification_class"] == "synthetic" and prov["synthetic"] > 0
    assert prov["certification_state"] == "not_certified"
    assert prov["scoring_state"] == "scored_effectiveness_unmeasured"


@pytest.mark.parametrize("path", ["/v1/policy", "/status", "/v1/features", "/v1/windows"])
def test_responses_never_contain_forbidden_fields(reader_served, path):
    db, base = reader_served
    seed(db)
    status, body = get(base, path)
    assert status == 200
    assert dp.find_violations(json.loads(body)) == []
    for needle in ("postgres://", "lease_owner", "lease_fence", "authority_grant",
                   "offset_metadata", "X-Api-Key"):
        assert needle not in body
