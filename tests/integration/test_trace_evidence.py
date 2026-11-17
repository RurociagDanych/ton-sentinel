"""Durable trace lookup evidence stays bound to completed scan lineage."""
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import uuid

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.migrate import migrate
from ton_sentinel.source.client import RESTPageClient
from ton_sentinel.source.scan import ScanPolicy, scan_range
from ton_sentinel.source.trace import lookup_and_persist_trace


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "trace_evidence_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def epoch(version="v1"):
    return SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                       f"synthetic:build:{version}", "synthetic:policy")


class EmptyWalk:
    def fetch_page(self, *_args):
        return []


class FailedWalk:
    def fetch_page(self, *_args):
        raise ValueError("synthetic unavailable")


class TraceServer:
    def __init__(self, payload):
        self.payload = payload
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps(owner.payload).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_args):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.origin = f"http://127.0.0.1:{self.httpd.server_port}"

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)


def trace(state="classified"):
    return {"trace_id": "trace-exact", "is_incomplete": False,
            "trace_info": {"trace_state": "complete", "classification_state": state},
            "actions": [{"action_id": "action-one"}]}


def complete_range(db, source_epoch=None):
    source_epoch = source_epoch or epoch()
    policy = ScanPolicy(1_700_000_100, 1_700_000_400, 2, 4, 8,
                        max_bytes=100_000, max_seconds=30, max_requests=20)
    range_id = scan_range(db, EmptyWalk(), source_epoch, policy)
    db.commit()
    return range_id


def test_loopback_trace_lookup_persists_and_reads_back_with_scan_lineage(db):
    range_id = complete_range(db)
    server = TraceServer({"traces": [trace()]})
    try:
        client = RESTPageClient(server.origin, "/api/v3/traces")
        result = lookup_and_persist_trace(db, client, range_id, "trace-exact")
        db.commit()
        schema = db.execute("SELECT current_schema()").fetchone()[0]
        dsn = os.environ["TON_SENTINEL_TEST_DSN"]
        with psycopg.connect(dsn) as reader:
            reader.execute(f'SET search_path TO "{schema}"')
            row = reader.execute(
                "SELECT e.range_id::text,e.trace_id,e.state,e.action_keys,e.global_coverage, "
                "r.epoch,r.start_inclusive,r.end_exclusive "
                "FROM trace_lookup_evidence e JOIN source_ranges r USING (range_id) "
                "WHERE e.range_id=%s AND e.trace_id=%s", (range_id, "trace-exact"),
            ).fetchone()
        assert result.state == "classified_complete"
        assert row[:5] == (range_id, "trace-exact", "classified_complete", ["action-one"], False)
        assert row[5] == json.loads(epoch().to_json())
        assert row[6:] == (1_700_000_100, 1_700_000_400)
    finally:
        server.close()


def test_unclassified_trace_is_durable_nonproof_and_never_global_coverage(db):
    range_id = complete_range(db)
    server = TraceServer({"traces": [trace("unclassified")]})
    try:
        result = lookup_and_persist_trace(
            db, RESTPageClient(server.origin, "/api/v3/traces"), range_id, "trace-exact")
        db.commit()
        row = db.execute(
            "SELECT state,action_keys,global_coverage FROM trace_lookup_evidence "
            "WHERE range_id=%s", (range_id,),
        ).fetchone()
        assert result.state == "unclassified"
        assert row == ("unclassified", None, False)
    finally:
        server.close()


def test_incomplete_and_nonexistent_ranges_do_not_persist_trace_evidence(db):
    policy = ScanPolicy(1_700_000_100, 1_700_000_400, 2, 1, 2,
                        max_bytes=100_000, max_seconds=30, max_requests=20)
    range_id = scan_range(db, FailedWalk(), epoch(), policy)
    db.commit()
    server = TraceServer({"traces": [trace()]})
    try:
        client = RESTPageClient(server.origin, "/api/v3/traces")
        with pytest.raises(ValueError, match="complete scan range"):
            lookup_and_persist_trace(db, client, range_id, "trace-exact")
        with pytest.raises(ValueError, match="unknown scan range"):
            lookup_and_persist_trace(db, client, str(uuid.uuid4()), "trace-exact")
        assert db.execute("SELECT count(*) FROM trace_lookup_evidence").fetchone()[0] == 0
    finally:
        server.close()


def test_lookup_refuses_lineage_epoch_mismatch_before_source_request(db):
    range_id = complete_range(db, epoch("v1"))
    server = TraceServer({"traces": [trace()]})
    try:
        client = RESTPageClient(server.origin, "/api/v3/traces")
        with pytest.raises(ValueError, match="source epoch mismatch"):
            lookup_and_persist_trace(db, client, range_id, "trace-exact", source_epoch=epoch("v2"))
        assert db.execute("SELECT count(*) FROM trace_lookup_evidence").fetchone()[0] == 0
        # The bounded HTTP endpoint is not called when supplied lineage disagrees.
        assert db.execute("SELECT count(*) FROM source_ranges WHERE range_id=%s", (range_id,)).fetchone()[0] == 1
    finally:
        server.close()


def test_repeated_lookups_preserve_each_observed_classification(db):
    range_id = complete_range(db)
    server = TraceServer({"traces": [trace()]})
    try:
        client = RESTPageClient(server.origin, "/api/v3/traces")
        lookup_and_persist_trace(db, client, range_id, "trace-exact")
        db.commit()
        lookup_and_persist_trace(db, client, range_id, "trace-exact")
        db.commit()

        server.payload = {"traces": [trace("unclassified")]}
        lookup_and_persist_trace(db, client, range_id, "trace-exact")
        db.commit()

        rows = db.execute(
            "SELECT state,action_keys,global_coverage FROM trace_lookup_evidence "
            "WHERE range_id=%s AND trace_id=%s ORDER BY observation_id",
            (range_id, "trace-exact"),
        ).fetchall()
        assert rows == [
            ("classified_complete", ["action-one"], False),
            ("classified_complete", ["action-one"], False),
            ("unclassified", None, False),
        ]
    finally:
        server.close()
