import re
import base64
import json
import os
import threading
import uuid
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest
from pyspark.sql import SparkSession

from ton_sentinel.api import create_app
from ton_sentinel.contracts import Action, ActionIdentity, Endpoint, SourceEpoch
from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.migrate import migrate
from ton_sentinel.db.serving import WindowKey, replace_features
from ton_sentinel.spark.batch import compute_features


def _visible_text(html):
    """Text as a reader sees it: inline tags removed without adding spaces, whitespace collapsed."""
    return " ".join(re.sub(r"<[^>]+>", "", html).split())


def _id(number):
    return base64.b64encode(number.to_bytes(32, "big")).decode("ascii")


def _action(number, *, end, asset, incoming, outgoing, trader):
    identity = ActionIdentity("testnet", _id(100 + number), _id(number))
    epoch = SourceEpoch("testnet", "src-v1", "filter-v1", "build-7", "policy-v1")
    return Action(identity, epoch, "STON.fi", "synthetic-v1", end, "success", True, True,
                  ("TON", asset), Endpoint("TON", incoming, None, None),
                  Endpoint(asset, outgoing, None, None), trader, "explicit", True).to_json()


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("serving-vertical-slice")
               .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "1").getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def database():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "serve_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    scoped_dsn = psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={schema}")
    try:
        with psycopg.connect(scoped_dsn) as connection:
            migrate(connection)
        yield scoped_dsn
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _compute(spark, rows):
    actions = spark.createDataFrame([(value,) for value in rows], "action_json string")
    coverage = spark.createDataFrame([("testnet", 0, 300, "bounded_observed", False)],
                                     "network string, window_start long, window_end long, status string, eligible_zero boolean")
    return compute_features(spark, actions, coverage, {"schema_version": 1, "synthetic": True})


def _expected():
    return [
        {"network": "testnet", "window_start": 0, "window_end": 300, "token": "JETTON:A",
         "generation": "build-7", "swaps": 2, "input_raw": None, "output_raw": "255",
         "known_traders": 1, "unknown_count": 0, "coverage_status": "bounded_observed",
         "status": "provisional", "provisional": True},
        {"network": "testnet", "window_start": 0, "window_end": 300, "token": "TON",
         "generation": "build-7", "swaps": 2, "input_raw": "10000000000000000010",
         "output_raw": None, "known_traders": 1, "unknown_count": 0,
         "coverage_status": "bounded_observed", "status": "provisional", "provisional": True},
    ]


def test_real_spark_full_replacement_and_read_only_http(spark, database):
    canonical = [
        _action(1, end=20, asset="JETTON:A", incoming="10000000000000000001", outgoing="250", trader=_id(900)),
        _action(2, end=50, asset="JETTON:A", incoming="9", outgoing="5", trader=_id(900)),
    ]
    first = _compute(spark, canonical).collect()
    reordered = _compute(spark, list(reversed(canonical))).repartition(2).collect()
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "vertical-slice")
        assert replace_features(connection, lease, first) == 2
        assert replace_features(connection, lease, reordered) == 2
        stored = connection.execute(
            "SELECT network, window_start, window_end, token, generation, swaps, input_raw::text, output_raw::text, "
            "known_traders, unknown_count, coverage_status, status, provisional FROM served_features "
            "ORDER BY token").fetchall()
    names = ("network", "window_start", "window_end", "token", "generation", "swaps", "input_raw", "output_raw",
             "known_traders", "unknown_count", "coverage_status", "status", "provisional")
    assert [dict(zip(names, row)) for row in stored] == _expected()

    app = create_app(dsn=database)
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(base + "/health") as response:
            assert response.status == 200 and json.load(response) == {"status": "ok"}
        with urlopen(base + "/status") as response:
            status = json.load(response)
            assert status["status"] == "ok"
            assert status["feature_rows"] == 2
            assert status["windows"] == 1
            freshness = status["serving_freshness"]
            assert freshness["status"] == "observed"
            assert freshness["scope"] == "global_all_scopes"
            assert freshness["latest_window_end"] == 300
            assert freshness["latest_serving_write_at"].endswith("Z")
            assert freshness["lag"] == {"status": "unknown", "meaning": "not_measured"}
            assert "not pipeline/source freshness" in freshness["limitations"]
            assert status["status_meaning"] == "database_query_succeeded"
            assert status["readiness"] == "unknown"
            assert status["coverage"] == {"status": "unknown"}
            assert status["backup"] == "not_configured"
        with urlopen(base + "/") as response:
            demo = response.read().decode("utf-8")
            assert response.status == 200
            text = _visible_text(demo)
            assert "Observed serving freshness" in text and "observed (global_all_scopes)" in text
            assert "Latest observed served window end (UTC epoch seconds)</dt><dd><code>300" in demo
            assert freshness["latest_serving_write_at"] in demo
            assert "Source or pipeline lag</dt><dd><code>unknown</code> — not measured" in demo
        with urlopen(base + "/v1/features?network=testnet&window_start=0&window_end=300") as response:
            assert json.load(response) == {"features": _expected()}
        try:
            urlopen(Request(base + "/v1/features", data=b"{}", method="POST"))
        except HTTPError as error:
            assert error.code == 405
        else:
            pytest.fail("feature API accepted a mutation")
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_invalid_full_window_input_is_atomic(spark, database):
    canonical = [_action(3, end=40, asset="JETTON:B", incoming="19", outgoing="8", trader=_id(901))]
    output = _compute(spark, canonical).collect()
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "atomic-slice")
        replace_features(connection, lease, output)
        before = connection.execute("SELECT token, swaps FROM served_features ORDER BY token").fetchall()
        invalid = dict(output[0].asDict(), token="", swaps=99)
        with pytest.raises(ValueError):
            replace_features(connection, lease, [invalid])
        database_invalid = dict(output[0].asDict(), token="invalid\x00token", swaps=99)
        with pytest.raises(psycopg.DataError):
            replace_features(connection, lease, [database_invalid])
        assert connection.execute("SELECT token, swaps FROM served_features ORDER BY token").fetchall() == before
        acquire_transition(connection, "newer-atomic-slice")
        with pytest.raises(LeaseRejected):
            replace_features(connection, lease, output)
        after = connection.execute("SELECT token, swaps FROM served_features ORDER BY token").fetchall()
    assert after == before


def test_explicit_empty_window_replacement_http_and_rollback(spark, database, monkeypatch):
    output = _compute(spark, [
        _action(10, end=40, asset="JETTON:TARGET", incoming="11", outgoing="7", trader=_id(910)),
        _action(11, end=340, asset="JETTON:OTHER", incoming="13", outgoing="9", trader=_id(911)),
    ]).collect()
    target = WindowKey("testnet", 0, 300)
    unrelated = WindowKey("testnet", 300, 600)
    assert {WindowKey(row.network, row.window_start, row.window_end) for row in output} == {target, unrelated}

    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "empty-window-slice")
        assert replace_features(connection, lease, output) == len(output)
        before = connection.execute(
            "SELECT network, window_start, window_end, token FROM served_features ORDER BY window_start, token"
        ).fetchall()
        with pytest.raises(ValueError, match="undeclared target"):
            replace_features(connection, lease, output, windows=[target])
        assert connection.execute(
            "SELECT network, window_start, window_end, token FROM served_features ORDER BY window_start, token"
        ).fetchall() == before

    # The final fence check occurs after DELETE; failure must roll it back even
    # when the caller catches the error and commits its surrounding transaction.
    import ton_sentinel.db.serving as serving
    validate = serving.validate_transition
    calls = 0

    def fail_final_validation(connection, current_lease):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise LeaseRejected("injected final validation rejection")
        return validate(connection, current_lease)

    monkeypatch.setattr(serving, "validate_transition", fail_final_validation)
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "rollback-empty-window-slice")
        with pytest.raises(LeaseRejected, match="injected final"):
            replace_features(connection, lease, [], windows=[target])
        # Exiting normally commits caller work after replace_features rolled
        # back its nested transaction/savepoint.
        connection.execute("SELECT 1")
    monkeypatch.setattr(serving, "validate_transition", validate)
    with psycopg.connect(database) as connection:
        assert connection.execute(
            "SELECT token FROM served_features WHERE network='testnet' AND window_start=0 ORDER BY token"
        ).fetchall() == [(row.token,) for row in output if row.window_start == 0]
        other_rows = [row for row in output if row.window_start == 300]
        assert replace_features(connection, lease, other_rows, windows=[target, unrelated]) == len(other_rows)
        assert connection.execute(
            "SELECT count(*) FROM served_features WHERE network='testnet' AND window_start=0"
        ).fetchone()[0] == 0
        assert connection.execute(
            "SELECT token FROM served_features WHERE network='testnet' AND window_start=300 ORDER BY token"
        ).fetchall() == [(row.token,) for row in other_rows]

    # Lease-state fencing remains durable after deletion, so a stale writer
    # cannot recreate the now-empty window after a takeover.
    with psycopg.connect(database) as connection:
        newer = acquire_transition(connection, "newer-empty-window-slice")
        assert newer.fence > lease.fence
        with pytest.raises(LeaseRejected):
            replace_features(connection, lease, [row for row in output if row.window_start == 0],
                             windows=[target])
        assert connection.execute(
            "SELECT count(*) FROM served_features WHERE network='testnet' AND window_start=0"
        ).fetchone()[0] == 0

    server = make_server("127.0.0.1", 0, create_app(dsn=database))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        with urlopen(base + "/v1/features?network=testnet&window_start=0&window_end=300") as response:
            assert json.load(response) == {"features": []}
        with urlopen(base + "/v1/features?network=testnet&window_start=300&window_end=600") as response:
            unrelated_rows = json.load(response)["features"]
            assert unrelated_rows
            assert {row["token"] for row in unrelated_rows} == {
                row.token for row in output if row.window_start == 300
            }
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
