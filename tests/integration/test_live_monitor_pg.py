"""The live monitor cycle qualifies, closes and scores live windows (production path)."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import psycopg
import pytest
from kafka.admin import KafkaAdminClient, NewTopic

from ton_sentinel import live_monitor as lm
from ton_sentinel.contracts import SYNTHETIC_FIXTURE_ENV, SourceEpoch
from ton_sentinel.db.ledger import migrate
from ton_sentinel.db.serving import read_provenance

FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'live' / 'g3_window.json'
EPOCH = SourceEpoch('mainnet', 'toncenter-v3:/api/v3/actions+/api/v3/traces (recorded G3 W)',
                    'jetton_swap actions + complete DeDust traces', 'l2-monitor-integration',
                    'live_swap v1')


@pytest.fixture(autouse=True)
def production(monkeypatch):
    monkeypatch.delenv(SYNTHETIC_FIXTURE_ENV, raising=False)


@pytest.fixture
def database():
    dsn = os.environ.get('TON_SENTINEL_TEST_DSN')
    if not dsn:
        pytest.fail('TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL')
    schema = 'live_monitor_' + uuid.uuid4().hex
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


def _config(database, tmp_path):
    bootstrap = os.environ.get('TON_SENTINEL_TEST_KAFKA_BOOTSTRAP')
    if not bootstrap:
        pytest.fail('TON_SENTINEL_TEST_KAFKA_BOOTSTRAP must name the owned disposable Kafka')
    topic = 'l2-monitor-' + uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15_000)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    finally:
        admin.close()
    return lm.CycleConfig(
        dsn=database, bootstrap_servers=bootstrap, topic=topic,
        topic_incarnation='l2-incarnation-' + uuid.uuid4().hex, replay_epoch=0,
        checkpoint=str((tmp_path / 'checkpoint').resolve()), environment_id='l2-local',
        query_id='l2-monitor-features', pipeline_epoch='l2-epoch', epoch=EPOCH)


def _batch(window, as_of):
    return {'network': window['network'], 'window': window['window'], 'as_of': as_of,
            'native_aliases': window['native_aliases'],
            'action_responses': window['action_responses'],
            'trace_responses': window['trace_responses']}


def test_live_cycle_qualifies_closes_and_scores_every_window_of_the_accepted_range(
        database, tmp_path):
    window = json.loads(FIXTURE.read_text())
    w0, w1 = window['window']
    config = _config(database, tmp_path)
    summary = lm.ingest_batch(config, _batch(window, as_of=w1 + 600), now=w1 + 3600)
    assert summary['status'] == 'ok', summary
    assert summary['ingest']['accepted'] and summary['ingest']['promoted'] == 475
    windows = list(range(w0, w1, 300))
    assert summary['qualified'] == windows
    with psycopg.connect(database) as connection:
        qualified = connection.execute(
            'SELECT window_start, mode, synthetic, origin, delta_seconds FROM '
            'action_window_qualification ORDER BY window_start').fetchall()
        closed = [r[0] for r in connection.execute(
            'SELECT window_start FROM window_sweep_closures ORDER BY window_start').fetchall()]
        statuses = dict(connection.execute(
            'SELECT r.status, count(*) FROM detector_current c JOIN detector_results r '
            'USING (network,token,direction,window_start,detector_id,result_revision) '
            'WHERE NOT c.stale GROUP BY 1').fetchall())
        stale = connection.execute(
            'SELECT count(*) FROM detector_current WHERE stale').fetchone()[0]
        served = connection.execute(
            'SELECT count(DISTINCT (network, window_start, token)) FROM served_features'
        ).fetchone()[0]
        connection.commit()
        provenance = read_provenance(connection, network='mainnet', window_from=w0,
                                     window_to=w1)
        connection.commit()
    assert qualified == [(w, 'bounded_observed', False, 'live', 120) for w in windows]
    assert closed == windows
    assert stale == 0
    assert sum(statuses.values()) == 2 * served > 0
    assert statuses.get('missing', 0) == 0, statuses
    assert provenance['source_kind'] == 'live'
    assert 'Not live' not in provenance['honesty_banner']


def test_a_second_cycle_on_the_same_window_writes_no_conflicting_qualification(
        database, tmp_path):
    window = json.loads(FIXTURE.read_text())
    w0, w1 = window['window']
    config = _config(database, tmp_path)
    first = lm.ingest_batch(config, _batch(window, as_of=w1 + 600), now=w1 + 3600)
    again = lm.ingest_batch(config, _batch(window, as_of=w1 + 900), now=w1 + 4500)
    assert first['status'] == 'ok', first
    assert again['status'] in ('ok', 'not_accepted'), again
    assert again['qualified'] == []
    with psycopg.connect(database) as connection:
        count = connection.execute(
            'SELECT count(*) FROM action_window_qualification').fetchone()[0]
        connection.commit()
    assert count == 3
