"""Database-backed, fail-closed action-window coverage read path."""

import os
import uuid

import psycopg
import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.migrate import migrate
from ton_sentinel.source.coverage import action_window_coverage
from ton_sentinel.source.scan import ScanPolicy, corroborate_range, scan_range


@pytest.fixture
def db():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "action_window_coverage_" + uuid.uuid4().hex
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


def epoch(network="testnet", version="v1"):
    return SourceEpoch(network, "synthetic:source", "synthetic:filter",
                       f"synthetic:build:{version}", "synthetic:policy")


class EmptyWalks:
    def __init__(self, walks=1):
        self.walks = [[] for _ in range(walks)]
        self.index = -1

    def fetch_page(self, source_epoch, start, end, offset, limit):
        if offset == 0:
            self.index += 1
        return self.walks[self.index][offset:offset + limit]


def policy(start=1_700_000_100, end=1_700_000_400):
    return ScanPolicy(start=start, end=end, page_size=2, max_pages=4,
                      max_actions=8, max_bytes=100_000, max_seconds=30,
                      max_requests=20)


def test_no_applicable_scans_persists_and_independently_reads_absent_evidence(db):
    window = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    assert (window.status, window.eligible_zero, window.reason, window.lineage) == (
        "missing", False, "no_action_time_scan_evidence", (),
    )
    schema = db.execute("SELECT current_schema()").fetchone()[0]
    with psycopg.connect(os.environ["TON_SENTINEL_TEST_DSN"]) as reader:
        reader.execute(f'SET search_path TO "{schema}"')
        row = reader.execute(
            "SELECT status,eligible_zero,reason,lineage FROM action_window_coverage "
            "WHERE coverage_id=%s", (window.coverage_id,),
        ).fetchone()
    assert row == ("missing", False, "no_action_time_scan_evidence", [])


def test_complete_empty_walk_and_agreeing_corroborated_walks_never_certify_window(db):
    scan_range(db, EmptyWalks(), epoch(), policy())
    corroborate_range(db, EmptyWalks(2), epoch(), policy())
    db.commit()

    window = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    assert window.status == "missing"
    assert window.eligible_zero is False
    assert window.reason == "trace_completion_mapping_unavailable"
    assert len(window.lineage) == 3
    assert {item["status"] for item in window.lineage} == {"complete"}
    assert any(item["corroborated"] for item in window.lineage)
    assert {item["action_count"] for item in window.lineage} == {0}


def test_epoch_isolation_and_half_open_scan_boundaries(db):
    scan_range(db, EmptyWalks(), epoch(), policy(start=1_699_999_700, end=1_700_000_100))
    scan_range(db, EmptyWalks(), epoch(version="v2"), policy())
    db.commit()
    window = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    assert window.reason == "no_action_time_scan_evidence"
    assert window.lineage == ()

    scan_range(db, EmptyWalks(), epoch(), policy(start=1_700_000_100, end=1_700_000_400))
    db.commit()
    window = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    assert window.reason == "trace_completion_mapping_unavailable"
    assert len(window.lineage) == 1


def test_replay_updates_same_window_and_can_be_read_back_after_commit(db):
    first = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    scan_range(db, EmptyWalks(), epoch(), policy())
    db.commit()
    second = action_window_coverage(db, "testnet", epoch(), 1_700_000_100, 1_700_000_400)
    db.commit()
    assert second.coverage_id == first.coverage_id
    assert second.reason == "trace_completion_mapping_unavailable"
    assert len(second.lineage) == 1
    assert db.execute("SELECT count(*) FROM action_window_coverage").fetchone()[0] == 1


@pytest.mark.parametrize(("network", "source_epoch", "start", "end"), [
    ("mainnet", None, 1_700_000_100, 1_700_000_400),
    ("testnet", None, True, 1_700_000_400),
    ("testnet", None, 1_700_000_100, 1_700_000_400.0),
    ("testnet", None, 1_700_000_100, 1_700_000_299),
    ("testnet", None, 1_700_000_400, 1_700_000_600),
])
def test_rejects_malformed_network_epoch_or_action_window(db, network, source_epoch, start, end):
    source_epoch = epoch() if source_epoch is None else source_epoch
    with pytest.raises(ValueError):
        action_window_coverage(db, network, source_epoch, start, end)
