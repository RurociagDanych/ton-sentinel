"""Dashboard API on real PostgreSQL over HTTP (sentinel_reader login): the files and endpoint the pipeline view uses."""
import json
import threading
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import pytest

from ton_sentinel.api import create_app
from tests.integration.test_detection import NET
from tests.integration.test_reader_role_pg import reader_dsn  # noqa: F401
from tests.integration.test_revisions import db  # noqa: F401

DASH = Path(__file__).resolve().parents[2] / "src" / "ton_sentinel" / "dashboard"
SECTIONS = {"outbox", "dirty_windows_pending_by_role", "scoring_backlog", "stream_progress",
            "ingestion_freshness", "kafka_lag"}


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


def test_pipeline_assets_are_served_with_csp_and_match_the_files(served):
    _, base = served
    for name in ("pipeline.js", "pipeline-model.js"):
        with urlopen(base + f"/dashboard/assets/{name}", timeout=8) as response:
            assert response.status == 200 and response.read() == (DASH / name).read_bytes()
            assert response.headers["Content-Type"].startswith("text/javascript")
            assert "script-src 'self'" in response.headers["Content-Security-Policy"]


def test_pipeline_endpoint_gives_the_sections_the_view_renders(served):
    _, base = served
    with urlopen(Request(base + "/v1/pipeline?" + urlencode({"network": NET})), timeout=8) as response:
        body = json.load(response)
    pipe = body["pipeline"]
    assert set(pipe) == SECTIONS
    backlog = pipe["scoring_backlog"]["value"]
    assert set(backlog) == {"unscored_served_windows", "stale_results"}
    for name in ("ingestion_freshness", "kafka_lag"):
        assert pipe[name]["status"] == "not_measured" and pipe[name]["value"] is None and pipe[name]["reason"]
    assert body["honesty_banner"] and body["provenance"]["certification_state"] == "not_certified"
