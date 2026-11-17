"""Regression tests for defects found on the live stack.

1. measured Kafka lag rendered as "unavailable" (overview + pipeline view), label/banner stale.
2. an address-like token (wallet or raw address) answered 503 "unavailable" instead of 400.
3. quality gap list -> explorer deep link lost the window: the explorer opened on 24 h.
4. overview service-status counts are global but were not labelled as all networks.
5. Grafana storage panels rendered epoch seconds as milliseconds (1970 dates).
6. one idle TCP client blocked the single-threaded api server (false SentinelApiDown).
"""
import json
import re
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
from pathlib import Path
from unittest.mock import patch

import pytest

from ton_sentinel import dashboard_payload
from ton_sentinel.api import create_app
from ton_sentinel.db.serving import PIPELINE_LABEL

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="node not installed")
PROV = dashboard_payload.build_provenance(qualification=[], scored=0)
T0 = 1_794_559_800
REAL_TOKEN = "jetton:0:" + "0d716e395b44e9f756f3bc76b268f3bafba85fd4b9b85e5599e2b37a2f9a1f3d"
OBSERVED_LAG = {
    "semantics": "log_end_offset_minus_next_offset_after_newest_audited_record",
    "provenance": "Kafka ListOffsets(latest) on the internal PLAINTEXT listener",
    "status": "observed",
    "value": [{"query_id": "q1-recorded-features", "topic": "q1-recorded-x", "status": "observed",
               "partitions": [{"partition": 0, "log_end_offset": 475, "next_offset": 475,
                               "lag_records": 0}], "total_lag_records": 0}]}


def node_json(tmp_path, body):
    script = tmp_path / "check.mjs"
    script.write_text(
        f'import * as M from "file://{DASH}/model.js";\n'
        f'import * as P from "file://{DASH}/pipeline-model.js";\n'
        f'import * as X from "file://{DASH}/explorer-model.js";\n{body}\n')
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


# ---- 1. a measured Kafka lag is shown as measured --------------------------------------------

@needs_node
def test_overview_shows_measured_kafka_lag_not_unavailable(tmp_path):
    payload = {"pipeline": {"kafka_lag": OBSERVED_LAG}}
    out = node_json(tmp_path, f"console.log(JSON.stringify(M.pipelineRows({json.dumps(payload)})));")
    lag = {r["id"]: r for r in out}["kafka_lag"]
    assert lag["state"] == "observed"
    assert lag["text"] == "0 records"
    assert "unavailable" not in lag["text"]


@needs_node
def test_pipeline_view_shows_measured_kafka_lag_and_keeps_freshness_not_measured(tmp_path):
    pipe = {"ingestion_freshness": {"status": "not_measured", "value": None,
                                    "reason": "no_ingestion_clock_recorded"},
            "kafka_lag": OBSERVED_LAG}
    out = node_json(tmp_path, f"""
const panel = P.notMeasuredPanel({json.dumps(pipe)});
const many = P.kafkaLagCell({{status: "observed", value: [
  {{query_id: "a", status: "observed", total_lag_records: 3}},
  {{query_id: "b", status: "observed", total_lag_records: 0}}]}});
const partial = P.kafkaLagCell({{status: "observed", value: [
  {{query_id: "a", status: "observed", total_lag_records: 3}},
  {{query_id: "b", status: "unavailable", reason: "broker_unreachable"}}]}});
const malformed = P.kafkaLagCell({{status: "observed", value: 7}});
const empty = P.kafkaLagCell({{status: "observed", value: []}});
const down = P.kafkaLagCell({{status: "unavailable", value: null, reason: "broker_unreachable"}});
console.log(JSON.stringify({{panel, many, partial, malformed, empty, down, banner: P.BANNER_TEXT}}));""")
    items = {i["key"]: i for i in out["panel"]["items"]}
    assert items["kafka_lag"]["state"] == "observed" and items["kafka_lag"]["text"] == "0 records"
    assert items["ingestion_freshness"]["state"] == "not_measured"
    assert "Nothing measures these two values" not in out["panel"]["text"]
    assert out["many"]["state"] == "observed" and out["many"]["text"] == "a: 3 records, b: 0 records"
    for bad in ("partial", "malformed", "empty", "down"):  # never a number for an unreadable lag
        assert out[bad]["state"] == "unavailable", bad
        assert not re.search(r"\d", out[bad]["text"]), bad
    assert out["down"]["detail"] == "broker_unreachable"
    assert "Kafka lag are not measured" not in out["banner"]


def test_pipeline_view_styles_a_measured_lag_as_observed_not_as_not_measured():
    src = (DASH / "pipeline.js").read_text(encoding="utf-8")
    render = src[src.index("function render("):src.index("async function load(")]
    assert '(t) => tileNode(t, " pipe-not-measured")' not in render
    assert 't.state === "observed" ? "" : " pipe-not-measured"' in render
    # the section must not carry the class either: ".pipe-not-measured .pipe-tile" would restyle every tile
    assert 'el("section", "card pipe-not-measured")' not in src


def test_pipeline_label_does_not_claim_kafka_lag_is_never_measured():
    assert "Kafka lag (not_measured)" not in PIPELINE_LABEL
    assert "Kafka lag" in PIPELINE_LABEL and "broker probe" in PIPELINE_LABEL


# ---- 2. an address-like token is bad input, not a server failure -----------------------------

class _FakeConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, *_a, **_k):
        return self


def _call(path, query):
    out = {}

    def start_response(status, headers):
        out["status"] = status

    body = b"".join(create_app(dsn="x")({"REQUEST_METHOD": "GET", "PATH_INFO": path,
                                         "QUERY_STRING": query}, start_response))
    return out["status"], json.loads(body)


ADDRESS_TOKENS = ("EQ" + "A" * 46, "0:" + "a" * 64, "-1:" + "b" * 64)
ECHO_ROUTES = (
    ("/v1/series", "read_series", "network=mainnet&token={t}&from=%d&to=%d" % (T0, T0 + 900)),
    ("/v1/coverage", "read_coverage", "network=mainnet&token={t}&from=%d&to=%d" % (T0, T0 + 900)),
    ("/v1/detections/range", "read_detection_range",
     "network=mainnet&token={t}&from=%d&to=%d" % (T0, T0 + 900)),
    ("/v1/detections/revisions", "read_result_revisions",
     "network=mainnet&token={t}&window_start=%d&detector=swap_count_spike.v1" % T0),
)


@pytest.mark.parametrize("token", ADDRESS_TOKENS)
@pytest.mark.parametrize("path,reader,query", ECHO_ROUTES)
def test_address_like_token_is_a_400_before_any_connection(path, reader, query, token):
    with patch("ton_sentinel.api.psycopg.connect", return_value=_FakeConnection()) as connect, \
         patch(f"ton_sentinel.api.{reader}", return_value=[]), \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = _call(path, query.format(t=token))
    assert status.startswith("400"), (status, body)
    assert body == {"error": "invalid_query"}
    assert token not in json.dumps(body)
    connect.assert_not_called()


@pytest.mark.parametrize("token", (REAL_TOKEN, "native"))
@pytest.mark.parametrize("path,reader,query", ECHO_ROUTES)
def test_real_token_ids_still_answer_200(path, reader, query, token):
    with patch("ton_sentinel.api.psycopg.connect", return_value=_FakeConnection()), \
         patch(f"ton_sentinel.api.{reader}", return_value=[]), \
         patch("ton_sentinel.api.read_provenance", return_value=PROV):
        status, body = _call(path, query.format(t=token))
    assert status.startswith("200"), (status, body)
    assert body["token"] == token


# ---- 3. a linked window keeps its range ------------------------------------------------------

@needs_node
def test_linked_range_contains_the_linked_window(tmp_path):
    now_range = {"from": 1_794_825_600, "to": 1_794_912_000}
    out = node_json(tmp_path, f"""
const r = {json.dumps(now_range)};
console.log(JSON.stringify({{
  outside: X.linkedRange(1794560100, r), inside: X.linkedRange(1794900000, r),
  bad: X.linkedRange(1794560101, r), early: X.linkedRange(600, r)}}));""")
    outside = out["outside"]
    assert outside["from"] <= 1_794_560_100 < outside["to"]
    assert outside["from"] % 300 == 0 and outside["to"] % 300 == 0
    assert 0 < outside["to"] - outside["from"] <= 7 * 86400
    assert outside["ok"] is True
    assert out["inside"] is None  # already in range: keep the current range
    assert out["bad"] is None  # not a window start: ignore the link
    assert out["early"]["from"] == 0 and out["early"]["from"] <= 600 < out["early"]["to"]


def test_explorer_applies_the_linked_range_before_the_first_load():
    src = (DASH / "explorer.js").read_text(encoding="utf-8")
    assert "linkedRange(" in src
    mount = src[src.index("export function mountExplorer"):]
    assert mount.index("linkedRange(") < mount.rindex("loadTokens(true);")


# ---- 4. global status counts say they are global ---------------------------------------------

@needs_node
def test_overview_global_status_tiles_are_labelled_all_networks(tmp_path):
    status = {"status": "ok", "availability": {"process": "available", "database": "available"},
              "status_query": "available", "feature_rows": 108, "windows": 3,
              "serving_freshness": {"status": "observed", "scope": "global_all_scopes",
                                    "latest_window_end": 1794560700,
                                    "latest_serving_write_at": "2026-11-15T18:49:09Z",
                                    "lag": {"status": "unknown", "meaning": "not_measured"}},
              "pending_work": {"outbox": {"status": "observed", "pending": 0},
                               "repair": {"status": "observed", "pending_windows": 0}}}
    out = node_json(tmp_path, f"console.log(JSON.stringify(M.statusTiles({json.dumps(status)})));")
    by = {t["id"]: t for t in out}
    for key in ("feature_rows", "windows", "latest_window_end", "latest_write", "serving_freshness",
                "outbox_pending", "repair_pending"):
        assert "all networks" in by[key]["detail"], key
    assert by["feature_rows"]["text"] == "108"
    assert "lag not measured" in by["serving_freshness"]["detail"]


# ---- 5. Grafana time panels get milliseconds -------------------------------------------------

def test_grafana_datetime_panels_convert_epoch_seconds_to_milliseconds():
    seen = 0
    for path in sorted((ROOT / "deploy/monitoring/grafana/dashboards").glob("*.json")):
        for panel in json.loads(path.read_text())["panels"]:
            defaults = panel.get("fieldConfig", {}).get("defaults", {})
            overrides = {o["matcher"]["options"]: o for o in panel.get("fieldConfig", {})
                         .get("overrides", []) if o.get("matcher", {}).get("id") == "byFrameRefID"}
            for target in panel.get("targets", []):
                unit = defaults.get("unit", "")
                for prop in overrides.get(target["refId"], {}).get("properties", []):
                    if prop.get("id") == "unit":
                        unit = prop["value"]
                if unit.startswith("dateTime"):
                    seen += 1
                    assert re.search(r"_seconds\b.*\*\s*1000|1000\s*\*.*_seconds\b", target["expr"]), \
                        (path.name, panel["title"], target["expr"])
                    assert "build_info" not in target["expr"], (path.name, panel["title"])
    assert seen >= 2


# ---- 6. an idle client must not block the api server ----------------------------------------

def test_idle_client_does_not_block_other_requests():
    from ton_sentinel import cli

    server = cli.build_server("127.0.0.1", 0, create_app(dsn="x"))
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    idle = socket.create_connection(("127.0.0.1", port))
    try:
        time.sleep(0.2)
        started = time.monotonic()
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/favicon.ico", timeout=3) as response:
            assert response.status == 204
        assert time.monotonic() - started < 2
    finally:
        idle.close()
        server.shutdown()
        server.server_close()


def test_api_server_drops_idle_connections_after_a_bounded_timeout():
    from ton_sentinel import cli

    assert 1 <= cli.REQUEST_SOCKET_TIMEOUT_SECONDS <= 30
    server = cli.build_server("127.0.0.1", 0, create_app(dsn="x"))
    try:
        assert server.RequestHandlerClass.timeout == cli.REQUEST_SOCKET_TIMEOUT_SECONDS
        assert getattr(server, "daemon_threads", False) is True
    finally:
        server.server_close()
