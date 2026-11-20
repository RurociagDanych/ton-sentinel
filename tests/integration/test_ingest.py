"""Real loopback HTTP to canonical acceptance/outbox integration coverage."""

import base64
import json
import os
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.ledger import migrate
from ton_sentinel.ingest import IngestionBackpressure, OutboxBudget, ingest_range
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.client import RESTPageClient
from ton_sentinel.source.scan import ScanPolicy


def epoch():
    return SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                       "synthetic:build", "synthetic:policy")


def action(marker=32):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet",
        "trace_id": base64.b64encode(bytes([31]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([marker]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": 11, "status": "success", "eligible": True,
        "route_proven": True, "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }


class Server:
    def __init__(self, behavior):
        owner = self
        self.requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append(self.path)
                status, body = owner.behavior(self)
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        self.behavior = behavior
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()
        self.origin = f"http://127.0.0.1:{self.httpd.server_port}"

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)


@pytest.fixture

def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "ingest_test_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        with psycopg.connect(dsn) as connection:
            connection.execute(f'SET search_path TO "{schema}"')
            migrate(connection)
            configure_delivery(connection, topic="test-ingest-actions",
                               topic_incarnation="test-ingest-incarnation")
            yield connection
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def policy():
    return ScanPolicy(start=10, end=20, page_size=1, max_pages=4,
                      max_actions=4, max_bytes=100_000, max_seconds=30,
                      max_requests=8)


def default_budget():
    return OutboxBudget(max_pending_bytes=1_000_000_000,
                        max_oldest_age_seconds=86_400)


def test_bounded_real_http_walks_promote_canonical_action_and_outbox_intent(db):
    expected = action()

    def behavior(request):
        query = parse_qs(urlsplit(request.path).query)
        return 200, json.dumps([expected] if query["offset"] == ["0"] else []).encode()

    server = Server(behavior)
    try:
        result = ingest_range(db, RESTPageClient(server.origin, "/v1/actions"),
                              epoch(), policy(), owner="ingest-integration", budget=default_budget())
        assert result.start == 10 and result.end == 20
        assert result.status == "accepted" and result.accepted
        assert len(result.promoted_event_ids) == 1
        assert result.publication_intents == result.promoted_event_ids
        assert len(server.requests) == 4
        canonical = db.execute("SELECT event_id,action FROM canonical_actions").fetchall()
        outbox = db.execute("SELECT event_id,payload,published_at FROM outbox").fetchall()
        assert len(canonical) == len(outbox) == 1
        assert canonical[0][0] == outbox[0][0] == result.promoted_event_ids[0]
        assert canonical[0][1] == outbox[0][1]
        assert outbox[0][2] is None
        assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    finally:
        server.close()


def test_incomplete_second_walk_accepts_and_publishes_nothing(db):
    expected = action()

    def behavior(request):
        query = parse_qs(urlsplit(request.path).query)
        if len(server.requests) == 3:
            return 503, b"unavailable"
        return 200, json.dumps([expected] if query["offset"] == ["0"] else []).encode()

    server = Server(behavior)
    try:
        result = ingest_range(db, RESTPageClient(server.origin, "/v1/actions",
                                                  max_retries=0),
                              epoch(), policy(), owner="ingest-incomplete", budget=default_budget())
        assert result.status == "pending" and not result.accepted
        assert result.promoted_event_ids == result.publication_intents == ()
        assert db.execute("SELECT count(*) FROM source_ranges WHERE status='incomplete'").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM source_ranges WHERE status='accepted'").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
        assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 0
    finally:
        server.close()


def test_outbox_byte_budget_blocks_before_http_then_acknowledgement_allows_ingest(db):
    expected = action()

    def behavior(request):
        query = parse_qs(urlsplit(request.path).query)
        return 200, json.dumps([expected] if query["offset"] == ["0"] else []).encode()

    server = Server(behavior)
    try:
        client = RESTPageClient(server.origin, "/v1/actions")
        first = ingest_range(db, client, epoch(), policy(), owner="budget-seed",
                             budget=default_budget())
        assert first.accepted
        request_count = len(server.requests)
        pending_size = db.execute(
            "SELECT pg_column_size(o) FROM outbox o WHERE event_id=%s",
            (first.promoted_event_ids[0],),
        ).fetchone()[0]
        db.commit()
        budget = OutboxBudget(max_pending_bytes=pending_size,
                              max_oldest_age_seconds=86_400)

        with pytest.raises(IngestionBackpressure):
            ingest_range(db, client, epoch(), policy(), owner="budget-blocked",
                         budget=budget)
        assert len(server.requests) == request_count
        assert db.info.transaction_status.name == "IDLE"
        assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20

        # This component-level lifecycle update models acknowledgement for the
        # admission regression; it is not evidence of Kafka acknowledgement.
        db.execute(
            "UPDATE outbox SET publication_state='acknowledged', "
            "published_at=clock_timestamp(), acknowledged_at=clock_timestamp() "
            "WHERE event_id=%s", (first.promoted_event_ids[0],),
        )
        db.commit()
        second = ingest_range(db, client, epoch(), policy(), owner="budget-released",
                              budget=budget)
        assert second.accepted
        assert len(server.requests) == request_count + 4
        assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 1
        assert db.execute(
            "SELECT publication_state FROM outbox WHERE event_id=%s",
            (first.promoted_event_ids[0],),
        ).fetchone()[0] == "acknowledged"
        assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    finally:
        server.close()


def test_outbox_age_budget_blocks_before_http_and_preserves_caller_transaction(db):
    expected = action()

    def behavior(request):
        query = parse_qs(urlsplit(request.path).query)
        return 200, json.dumps([expected] if query["offset"] == ["0"] else []).encode()

    server = Server(behavior)
    try:
        client = RESTPageClient(server.origin, "/v1/actions")
        first = ingest_range(db, client, epoch(), policy(), owner="age-seed",
                             budget=default_budget())
        assert first.accepted
        db.execute("UPDATE outbox SET created_at=clock_timestamp()-interval '10 seconds'")
        db.commit()
        request_count = len(server.requests)

        schema = db.execute("SELECT current_schema()").fetchone()[0]
        db.commit()
        # An active caller transaction must not be committed by admission.
        db.execute("UPDATE ingest_state SET cursor=19")
        with pytest.raises(IngestionBackpressure):
            ingest_range(db, client, epoch(), policy(), owner="age-blocked",
                         budget=OutboxBudget(10**9, 5))
        assert len(server.requests) == request_count
        assert db.info.transaction_status.name != "IDLE"
        with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as observer:
            observer.execute(f'SET search_path TO "{schema}"')
            assert observer.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
        db.rollback()
        assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
    finally:
        server.close()


def test_acceptance_budget_rechecks_after_source_scan_and_rolls_back_overflow(db):
    original = action()
    new_action = action(marker=33)
    seed_server = Server(lambda request: (
        200, json.dumps([original] if parse_qs(urlsplit(request.path).query)["offset"] == ["0"] else []).encode()
    ))
    try:
        seeded = ingest_range(db, RESTPageClient(seed_server.origin, "/v1/actions"),
                              epoch(), policy(), owner="budget-overshoot-seed",
                              budget=default_budget())
        assert seeded.accepted
    finally:
        seed_server.close()

    schema = db.execute("SELECT current_schema()").fetchone()[0]
    db.commit()
    injected = []

    def behavior(request):
        if not injected:
            injected.append(True)
            # The baseline admission read happened before this loopback source
            # call. Age a real pending intent past the configured bound.
            with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as concurrent:
                concurrent.execute(f'SET search_path TO "{schema}"')
                concurrent.execute(
                    "UPDATE outbox SET created_at=clock_timestamp()-interval '10 seconds' "
                    "WHERE event_id=%s",
                    (seeded.promoted_event_ids[0],),
                )
        query = parse_qs(urlsplit(request.path).query)
        return 200, json.dumps([new_action] if query["offset"] == ["0"] else []).encode()

    server = Server(behavior)
    try:
        with pytest.raises(IngestionBackpressure):
            ingest_range(db, RESTPageClient(server.origin, "/v1/actions"), epoch(), policy(),
                         owner="budget-overshoot-reject",
                         budget=OutboxBudget(1_000_000, 5))
        assert injected and server.requests
        assert db.info.transaction_status.name == "IDLE"
        assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM scan_membership").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1
        assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == 20
        assert db.execute("SELECT count(*) FROM source_ranges WHERE status='complete'").fetchone()[0] >= 1
        assert db.execute("SELECT count(*) FROM corroboration_groups WHERE status='corroborated'").fetchone()[0] >= 1
    finally:
        server.close()


@pytest.mark.parametrize("max_pending_bytes", [1, 1_000_000])
def test_acceptance_budget_accounts_for_new_intent_bytes(db, max_pending_bytes):
    expected = action()
    initial_cursor = db.execute("SELECT cursor FROM ingest_state").fetchone()[0]
    db.commit()

    def behavior(request):
        query = parse_qs(urlsplit(request.path).query)
        return 200, json.dumps([expected] if query["offset"] == ["0"] else []).encode()

    server = Server(behavior)
    try:
        if max_pending_bytes == 1:
            with pytest.raises(IngestionBackpressure):
                ingest_range(db, RESTPageClient(server.origin, "/v1/actions"), epoch(), policy(),
                             owner="budget-first-batch-rejected",
                             budget=OutboxBudget(max_pending_bytes, 86_400))
            assert server.requests
            assert db.info.transaction_status.name == "IDLE"
            for table in ("canonical_actions", "scan_membership", "outbox"):
                assert db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0
            assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == initial_cursor
            assert db.execute(
                "SELECT count(*) FROM corroboration_groups WHERE status='corroborated'"
            ).fetchone()[0] == 1
            return
        result = ingest_range(db, RESTPageClient(server.origin, "/v1/actions"), epoch(), policy(),
                              owner="budget-under-limit",
                              budget=OutboxBudget(max_pending_bytes, 86_400))
        assert result.accepted and len(result.publication_intents) == 1
        pending_bytes = db.execute(
            "SELECT COALESCE(sum(pg_column_size(outbox)),0)::bigint FROM outbox "
            "WHERE publication_state <> 'acknowledged'"
        ).fetchone()[0]
        assert 0 < pending_bytes < max_pending_bytes
    finally:
        server.close()


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.5, "10"])
def test_outbox_budget_rejects_nonpositive_or_noninteger_limits(value):
    with pytest.raises(ValueError, match="positive integer"):
        OutboxBudget(value, 10)
    with pytest.raises(ValueError, match="positive integer"):
        OutboxBudget(10, value)


def test_missing_outbox_budget_fails_closed_before_source_request_or_database_change(db):
    server = Server(lambda request: (200, b"[]"))
    try:
        before = db.execute("SELECT cursor FROM ingest_state").fetchone()[0]
        db.commit()
        with pytest.raises(ValueError, match="OutboxBudget required"):
            ingest_range(db, RESTPageClient(server.origin, "/v1/actions"),
                         epoch(), policy(), owner="missing-budget")
        assert server.requests == []
        assert db.info.transaction_status.name == "IDLE"
        assert db.execute("SELECT cursor FROM ingest_state").fetchone()[0] == before
        assert db.execute("SELECT count(*) FROM source_ranges").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM canonical_actions").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM outbox").fetchone()[0] == 0
    finally:
        server.close()
