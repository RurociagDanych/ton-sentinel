"""Persisted replacement features served through the read-only HTTP slice."""

import json
import threading
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest

from tests.oracle import expected_economic_features
from tests.integration.test_feature_replacement import (  # noqa: F401  (fixtures + helpers)
    _audited_rows, _fixture_actions, _make_events, _run, _stored, database, spark,
)
from ton_sentinel.api import create_app
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.serving import replace_features
from ton_sentinel.outbox import configure_delivery
from tests.integration.test_feature_replacement import _spark_features


def _serve(app):
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


def _stop(server, thread):
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def test_api_serves_exact_persisted_spark_features_with_pagination(spark, database):
    source = _fixture_actions()
    expected = expected_economic_features(source)
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "task62-serve")
        assert replace_features(connection, lease, _spark_features(spark, source, (0, 300, 600))) == len(expected)
        stored = _stored(connection)
    server, thread, base = _serve(create_app(dsn=database))
    try:
        with urlopen(base + "/v1/features?network=testnet&limit=1000") as response:
            served = json.load(response)["features"]
        by_key = lambda rows: {(r["window_start"], r["token"]): r for r in rows}  # noqa: E731
        assert by_key(served) == by_key(stored) and len(served) == len(stored) == len(expected)
        assert by_key(served)[(0, "TON")]["input_raw"] == "1000000000"
        assert by_key(served)[(0, "jetton:A")]["output_raw"] == "250"
        pages, query = [], {"network": "testnet", "limit": "1"}
        while True:
            with urlopen(base + "/v1/features?" + urlencode(query)) as response:
                body = json.load(response)
            pages.extend(body["features"])
            if "next_cursor" not in body:
                break
            query["cursor"] = body["next_cursor"]
        assert len(pages) == len(stored) and by_key(pages) == by_key(stored)
        with urlopen(base + "/v1/features?network=testnet&token=jetton:A") as response:
            assert {r["token"] for r in json.load(response)["features"]} == {"jetton:A"}
    finally:
        _stop(server, thread)


def test_audit_identity_uses_pipeline_epoch_and_batch_id_and_status_states_limitation(spark, database):
    incarnation = "t62-" + uuid.uuid4().hex
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "task62-audit", ttl_seconds=300)
        configure_delivery(connection, topic="t61", topic_incarnation=incarnation)
        rows = _audited_rows(_make_events(connection, lease, incarnation), incarnation)
        _run(connection, lease, spark, incarnation, rows, 0, query="q1", epoch="pe-a")
        connection.commit()
        reference = _stored(connection)
        connection.commit()
        # Same batch_id under another pipeline epoch is a distinct audit record.
        _run(connection, lease, spark, incarnation, rows, 0, query="q1", epoch="pe-b")
        connection.commit()
        audit = connection.execute(
            "SELECT pipeline_epoch, batch_id FROM stream_batches ORDER BY pipeline_epoch").fetchall()
        assert audit == [("pe-a", 0), ("pe-b", 0)]
        assert _stored(connection) == reference
        with pytest.raises(psycopg.errors.UniqueViolation):
            connection.execute(
                "INSERT INTO stream_batches (environment_id,query_id,pipeline_epoch,batch_id,topic,"
                "topic_incarnation,replay_epoch,offset_metadata,record_digest,record_count) "
                "SELECT environment_id,query_id,pipeline_epoch,batch_id,topic,topic_incarnation,"
                "replay_epoch,offset_metadata,record_digest,record_count FROM stream_batches "
                "WHERE pipeline_epoch='pe-a'")
        connection.rollback()
    server, thread, base = _serve(create_app(dsn=database))
    try:
        with urlopen(base + "/status") as response:
            status = json.load(response)
        assert status["feature_rows"] == len(reference)
        assert status["interim_limitation"]["mode"] == "stateless_batch_replacement"
        assert status["interim_limitation"]["stateful_streaming"] == "not_qualified"
        with urlopen(base + "/") as response:
            assert "stateless_batch_replacement" in response.read().decode("utf-8")
    finally:
        _stop(server, thread)


def test_mutations_rejected_and_secrets_never_rendered(spark, database):
    source = _fixture_actions()
    with psycopg.connect(database) as connection:
        lease = acquire_transition(connection, "task62-readonly")
        replace_features(connection, lease, _spark_features(spark, source, (0, 300, 600)))
        before = _stored(connection)
    password = psycopg.conninfo.conninfo_to_dict(database).get("password") or ""
    server, thread, base = _serve(create_app(dsn=database))
    bad_server, bad_thread, bad_base = _serve(create_app(dsn="host=127.0.0.1 port=1 user=u password=zz-secret-zz dbname=d"))
    try:
        for method in ("POST", "PUT", "PATCH", "DELETE"):
            for path in ("/v1/features", "/status", "/health", "/"):
                with pytest.raises(HTTPError) as error:
                    urlopen(Request(base + path, data=b"{}", method=method))
                assert error.value.code == 405 and error.value.headers["Allow"] == "GET"
        bodies = []
        for path in ("/", "/health", "/status", "/v1/features", "/v1/windows", "/nope"):
            try:
                with urlopen(base + path) as response:
                    bodies.append(response.read().decode("utf-8"))
            except HTTPError as error:
                bodies.append(error.read().decode("utf-8"))
            try:
                with urlopen(bad_base + path) as response:
                    bodies.append(response.read().decode("utf-8"))
            except HTTPError as error:
                assert error.code in (404, 503)
                bodies.append(error.read().decode("utf-8"))
        joined = "\n".join(bodies)
        for secret in filter(None, (password, "zz-secret-zz", "127.0.0.1", "port=1")):
            assert secret not in joined
    finally:
        _stop(server, thread)
        _stop(bad_server, bad_thread)
    with psycopg.connect(database) as connection:
        assert _stored(connection) == before
