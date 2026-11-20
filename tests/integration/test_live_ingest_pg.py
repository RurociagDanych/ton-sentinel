"""Recorded real live data through production ingest, real Kafka and Spark to served rows."""

from __future__ import annotations

import base64
import json
import os
import uuid
from pathlib import Path

import psycopg
import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession

from ton_sentinel.contracts import SYNTHETIC_FIXTURE_ENV, SourceEpoch, SyntheticInputRefused
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.ledger import migrate
from ton_sentinel.ingest import OutboxBudget, ingest_range
from ton_sentinel.normalize import normalize
from ton_sentinel.outbox import configure_delivery, publish_pending
from ton_sentinel.source.live import LivePages, live_swap_batch
from ton_sentinel.source.scan import ScanPolicy
from ton_sentinel.spark.batch import compute_features
from ton_sentinel.stream_cli import ConsumerConfig, run_consumer

FIXTURE = Path(__file__).resolve().parents[1] / 'fixtures' / 'live' / 'g3_window.json'
EPOCH = SourceEpoch('mainnet', 'toncenter-v3:/api/v3/actions+/api/v3/traces (recorded G3 W)',
                    'jetton_swap actions + complete DeDust traces', 'task-16-integration',
                    'live_swap v1')
COVERAGE = 'network string, window_start long, window_end long, status string, eligible_zero boolean'


@pytest.fixture(autouse=True)
def production(monkeypatch):
    monkeypatch.delenv(SYNTHETIC_FIXTURE_ENV, raising=False)


@pytest.fixture(scope='module')
def window():
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope='module')
def spark():
    session = (SparkSession.builder.master('local[2]').appName('task16-live-batch')
               .config('spark.ui.enabled', 'false').config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', '2').getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def database():
    dsn = os.environ.get('TON_SENTINEL_TEST_DSN')
    if not dsn:
        pytest.fail('TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL')
    schema = 'live_ingest_' + uuid.uuid4().hex
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


def _pages(fx):
    return LivePages(live_swap_batch(fx['action_responses'], fx['trace_responses'],
                                     network=fx['network'],
                                     native_aliases=frozenset(fx['native_aliases'])))


def _synthetic():
    return {
        'schema_version': 1, 'kind': 'synthetic_swap', 'synthetic': True, 'network': 'testnet',
        'trace_id': base64.b64encode(bytes([31]) * 32).decode('ascii'),
        'action_id': base64.b64encode(bytes([32]) * 32).decode('ascii'),
        'protocol': 'STON.fi', 'protocol_version': 'synthetic-v1', 'action_end': 11,
        'status': 'success', 'eligible': True, 'route_proven': True, 'route': ['TON', 'jetton:A'],
        'input': {'asset': 'TON', 'raw_amount': '100'},
        'output': {'asset': 'jetton:A', 'raw_amount': '25'},
    }


class _FixedPages:
    def __init__(self, payloads):
        self.payloads = list(payloads)

    def fetch_page(self, epoch, start, end, offset, limit):
        return self.payloads[offset:offset + limit]


def _budget():
    return OutboxBudget(max_pending_bytes=1_000_000_000, max_oldest_age_seconds=86_400)


def test_production_ingest_refuses_synthetic_input_with_an_explicit_error(database):
    epoch = SourceEpoch('testnet', 'synthetic:source', 'synthetic:filter', 'synthetic:build',
                        'synthetic:policy')
    policy = ScanPolicy(start=10, end=20, page_size=2, max_pages=4, max_actions=8,
                        max_bytes=100_000, max_seconds=30, max_requests=8)
    with psycopg.connect(database) as connection:
        configure_delivery(connection, topic='task16-refuse', topic_incarnation='task16-refuse')
        with pytest.raises(SyntheticInputRefused, match='production path accepts only live_swap'):
            ingest_range(connection, _FixedPages([_synthetic()]), epoch, policy,
                         owner='task16-refuse', budget=_budget())
        connection.rollback()
        counts = connection.execute(
            'SELECT (SELECT count(*) FROM canonical_actions), (SELECT count(*) FROM outbox), '
            '(SELECT count(*) FROM action_observations), (SELECT count(*) FROM scan_quarantine)'
        ).fetchone()
        reasons = [r[0] for r in connection.execute('SELECT reason FROM source_ranges').fetchall()]
        connection.commit()
    assert counts == (0, 0, 0, 0)
    assert reasons == ['SyntheticInputRefused']


def test_spark_batch_refuses_synthetic_rows_and_computes_live_rows(spark, monkeypatch, window):
    monkeypatch.setenv(SYNTHETIC_FIXTURE_ENV, 'test-only')
    synthetic = normalize(_synthetic(), SourceEpoch('testnet', 's', 'f', 'b', 'p')).to_json()
    monkeypatch.delenv(SYNTHETIC_FIXTURE_ENV)
    coverage = spark.createDataFrame([('testnet', 0, 300, 'bounded_observed', False)], COVERAGE)
    with pytest.raises(SyntheticInputRefused):
        compute_features(spark, spark.createDataFrame([(synthetic,)], 'action_json string'),
                         coverage, {'schema_version': 1})
    w0, w1 = window['window']
    served = []
    pages, offset = _pages(window), 0
    while True:
        page = pages.fetch_page(EPOCH, w0, w1, offset, 1000)
        served += page
        offset += len(page)
        if len(page) < 1000:
            break
    rows = [(normalize(p, EPOCH).to_json(),) for p in served]
    cov = spark.createDataFrame([('mainnet', s, s + 300, 'bounded_observed', False)
                                 for s in range(w0, w1, 300)], COVERAGE)
    out = compute_features(spark, spark.createDataFrame(rows, 'action_json string'), cov,
                           {'schema_version': 1}).collect()
    assert sum(1 for p in served if p['protocol_version'].startswith('toncenter-v3-trace/')) == 4
    truth = {(x['token'], x['window_start']): x['swaps']
             for x in window['truth']['window_aggregates_all_dex']}
    assert {(r.token, r.window_start): r.swaps for r in out} == truth


def test_recorded_window_through_ingest_kafka_and_spark_matches_the_closure_truth(
        database, window, tmp_path):
    bootstrap = os.environ.get('TON_SENTINEL_TEST_KAFKA_BOOTSTRAP')
    if not bootstrap:
        pytest.fail('TON_SENTINEL_TEST_KAFKA_BOOTSTRAP must name the owned disposable Kafka')
    w0, w1 = window['window']
    topic = 'task16-live-' + uuid.uuid4().hex
    incarnation = 'task16-incarnation-' + uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15_000)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    finally:
        admin.close()
    pages = _pages(window)
    policy = ScanPolicy(start=w0, end=w1, page_size=100, max_pages=10, max_actions=1000,
                        max_bytes=4_000_000, max_seconds=120, max_requests=100)
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks='all', retries=3,
                             max_block_ms=15_000, max_request_size=4_000_000)
    try:
        with psycopg.connect(database) as connection:
            configure_delivery(connection, topic=topic, topic_incarnation=incarnation)
            result = ingest_range(connection, pages, EPOCH, policy, owner='task16-live-ingest',
                                  budget=_budget())
            assert result.status == 'accepted' and result.accepted
            assert len(result.promoted_event_ids) == 475
            versions = dict(connection.execute(
                "SELECT action->>'protocol_version', count(*) FROM canonical_actions GROUP BY 1"
            ).fetchall())
            connection.commit()
            assert sum(versions.values()) == 475
            assert versions['toncenter-v3-trace/dedust'] == 4
            assert 'synthetic-v1' not in versions
            lease = acquire_transition(connection, 'task16-publish', ttl_seconds=300)
            sent = publish_pending(connection, producer, lease, limit=1000)
            connection.commit()
            assert len(sent) == 475
        producer.flush(timeout=30)
    finally:
        producer.close()

    run_consumer(ConsumerConfig(
        dsn=database, bootstrap_servers=bootstrap, topic=topic,
        checkpoint=str((tmp_path / 'checkpoint').resolve()), topic_incarnation=incarnation,
        replay_epoch=0, max_offsets_per_trigger=500, environment_id='task16-local',
        query_id='task16-live-features', pipeline_epoch='task16-epoch'))

    with psycopg.connect(database) as connection:
        served = connection.execute(
            'SELECT network, window_start, window_end, token, generation, swaps, input_raw::text, '
            'output_raw::text, known_traders, unknown_count, coverage_status, status '
            'FROM served_features ORDER BY window_start, token COLLATE "C"').fetchall()
        connection.commit()
    truth = window['truth']
    want = {(x['token'], x['window_start']): x for x in truth['window_aggregates_all_dex']}
    got = {(r[3], r[1]): r for r in served}
    keys = set(want) | set(got)
    agreement = {
        'keys': len(keys),
        'equal': sum(1 for k in keys if k in want and k in got
                     and (got[k][5], got[k][8], got[k][9])
                     == (want[k]['swaps'], want[k]['known_traders'], want[k]['unknown_count'])),
        'swaps_equal': sum(1 for k in keys if k in want and k in got
                           and got[k][5] == want[k]['swaps'])}
    assert agreement == truth['closure_agreement'] == {'keys': 108, 'equal': 106, 'swaps_equal': 108}
    assert all((got[k][6], got[k][7]) == (want[k]['input_raw'], want[k]['output_raw'])
               for k in keys)
    assert {r[0] for r in served} == {'mainnet'}
    assert {r[4] for r in served} == {EPOCH.build_fingerprint}
    assert {(r[1], r[2]) for r in served} == {(s, s + 300) for s in range(w0, w1, 300)}
    assert {r[11] for r in served} == {'provisional'}
