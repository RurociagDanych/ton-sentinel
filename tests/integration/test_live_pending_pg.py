"""Recorded pending live records through production ingest on real PostgreSQL."""

from __future__ import annotations

import copy
import json
import os
import uuid
from pathlib import Path

import psycopg
import pytest

from ton_sentinel.contracts import SYNTHETIC_FIXTURE_ENV, SourceEpoch
from ton_sentinel.db.ledger import migrate
from ton_sentinel.ingest import OutboxBudget, ingest_range
from ton_sentinel.normalize import normalize
from ton_sentinel.outbox import configure_delivery
from ton_sentinel.source.live import LivePages, live_swap_batch
from ton_sentinel.source.scan import ScanPolicy

FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'live' / 'g3_pending.json'
EPOCH = SourceEpoch('mainnet', 'toncenter-v3:/api/v3/actions (recorded pending)',
                    'jetton_swap actions', 'v3-pending', 'live_swap v1')
COUNT_SQL = ('SELECT (SELECT count(*) FROM canonical_actions), (SELECT count(*) FROM outbox), '
             '(SELECT count(*) FROM scan_membership), (SELECT count(*) FROM scan_quarantine), '
             '(SELECT count(*) FROM served_features)')


@pytest.fixture(autouse=True)
def production(monkeypatch):
    monkeypatch.delenv(SYNTHETIC_FIXTURE_ENV, raising=False)


@pytest.fixture(scope='module')
def fx():
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def database():
    dsn = os.environ.get('TON_SENTINEL_TEST_DSN')
    if not dsn:
        pytest.fail('TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL')
    schema = 'live_pending_' + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    scoped = psycopg.conninfo.make_conninfo(dsn, options=f'-c search_path={schema}')
    try:
        with psycopg.connect(scoped) as connection:
            migrate(connection)
        yield scoped
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _pages(fx, key):
    batch = live_swap_batch(fx[key], fx['trace_responses'], network=fx['network'],
                            native_aliases=frozenset(fx['native_aliases']))
    return LivePages(batch)


def _policy(fx):
    w0, w1 = fx['window']
    return ScanPolicy(start=w0, end=w1, page_size=2, max_pages=10, max_actions=10,
                      max_bytes=4_000_000, max_seconds=60, max_requests=20)


def _budget():
    return OutboxBudget(max_pending_bytes=1_000_000_000, max_oldest_age_seconds=86_400)


def _counts(connection):
    row = connection.execute(COUNT_SQL).fetchone()
    connection.commit()
    return tuple(row)


def test_recorded_pending_records_are_paged_and_marked_not_successful(fx):
    pages = _pages(fx, 'pending_action_responses')
    w0, w1 = fx['window']
    served, offset = [], 0
    while True:
        page = pages.fetch_page(EPOCH, w0, w1, offset, 1)
        served += page
        offset += len(page)
        if len(page) < 1:
            break
    assert len(served) == 2
    assert {p['status'] for p in served} == {'pending'}
    assert {p['eligible'] for p in served} == {False}
    for p in served:
        action = normalize(p, EPOCH)
        assert not action.eligible and action.reasons[0] == 'status_not_success'
    assert pages.excluded(EPOCH, w0, w1) == ()


def test_pending_range_is_quarantined_not_accepted_never_served_then_final_is_accepted(
        database, fx):
    with psycopg.connect(database) as connection:
        configure_delivery(connection, topic='v3-pending', topic_incarnation='v3-pending')
        result = ingest_range(connection, _pages(fx, 'pending_action_responses'), EPOCH,
                              _policy(fx), owner='v3-pending', budget=_budget())
        assert not result.accepted and result.status != 'accepted'
        assert result.promoted_event_ids == () and result.publication_intents == ()
        connection.commit()
        canonical, outbox, membership, quarantine, served = _counts(connection)
        assert (canonical, outbox, membership, served) == (0, 0, 0, 0)
        assert quarantine == 2
        reasons = connection.execute(
            "SELECT proposed_payload->>'status', proposed_payload->>'eligible', reason "
            'FROM scan_quarantine ORDER BY ordinal').fetchall()
        assert {(r[0], r[1]) for r in reasons} == {('pending', 'false')}
        rng = connection.execute(
            'SELECT status, reason FROM source_ranges').fetchall()
        assert rng == [('incomplete', 'action_quarantined')]
        accepted_ranges = connection.execute(
            "SELECT count(*) FROM source_ranges WHERE status IN ('complete','accepted')"
        ).fetchone()[0]
        assert accepted_ranges == 0
        connection.commit()
        final = ingest_range(connection, _pages(fx, 'final_action_responses'), EPOCH,
                             _policy(fx), owner='v3-final', budget=_budget())
        assert final.accepted and final.status == 'accepted'
        assert len(final.promoted_event_ids) == 2
        connection.commit()
        canonical, outbox, membership, quarantine, _ = _counts(connection)
        assert (canonical, outbox, membership) == (2, 2, 2)
        statuses = {r[0] for r in connection.execute(
            "SELECT action->>'status' FROM canonical_actions").fetchall()}
        connection.commit()
        assert statuses == {'success'}


def test_mixed_range_with_one_pending_record_is_not_accepted(database, fx):
    mixed = copy.deepcopy(fx['final_action_responses'])
    mixed[1] = copy.deepcopy(fx['pending_action_responses'][1])
    batch_fx = dict(fx, mixed=mixed)
    with psycopg.connect(database) as connection:
        configure_delivery(connection, topic='v3-mixed', topic_incarnation='v3-mixed')
        result = ingest_range(connection, _pages(batch_fx, 'mixed'), EPOCH, _policy(fx),
                              owner='v3-mixed', budget=_budget())
        assert not result.accepted
        connection.commit()
        canonical, outbox, membership, quarantine, served = _counts(connection)
        assert (canonical, outbox, membership, served) == (0, 0, 0, 0)
        assert quarantine == 1
