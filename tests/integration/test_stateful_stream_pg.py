"""Real Kafka + Spark Structured Streaming + PostgreSQL: bounded dedup state and durable checkpoint."""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import psycopg
import pytest
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from pyspark.sql import SparkSession

from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.db.ledger import migrate
from ton_sentinel.db.revisions import decide_revision
from ton_sentinel.spark import stream
from ton_sentinel.stream_cli import ConsumerConfig, kafka_reader_options

W1 = 1_800_000_000 // 300 * 300
POLICY = {"watermark_seconds": 60, "max_state_rows": 50, "max_partitions": 1}


def _session(name="stateful-stream"):
    return (SparkSession.builder.master("local[2]").appName(name)
            .config("spark.ui.enabled", "false").config("spark.sql.session.timeZone", "UTC")
            .config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1").getOrCreate())


@pytest.fixture
def spark():
    session = _session()
    yield session
    session.stop()


@pytest.fixture
def database():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    schema = "stateful_stream_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(f'CREATE SCHEMA "{schema}"')
    scoped = psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={schema}")
    try:
        with psycopg.connect(scoped) as connection:
            migrate(connection)
        yield scoped
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
def kafka():
    bootstrap = os.environ.get("TON_SENTINEL_TEST_KAFKA_BOOTSTRAP")
    if not bootstrap:
        pytest.fail("TON_SENTINEL_TEST_KAFKA_BOOTSTRAP must name the owned disposable Kafka")
    topic = "stateful-" + uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15_000)
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    finally:
        admin.close()
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3, max_block_ms=15_000)

    def send(*events):
        for event_id, action_end in events:
            body = {"event_id": event_id, "action": {"network": "testnet", "action_end": action_end,
                                                     "input": {"raw_amount": "10"}}}
            producer.send(topic, key=event_id.encode(), value=json.dumps(body).encode())
        producer.flush(timeout=15)

    yield bootstrap, topic, send
    producer.close()


def _config(database, bootstrap, topic, checkpoint, *, max_offsets=500, epoch="epoch-a"):
    return ConsumerConfig(dsn=database, bootstrap_servers=bootstrap, topic=topic,
                          checkpoint=str(checkpoint), topic_incarnation="inc-1", replay_epoch=0,
                          environment_id="local-test", query_id="fast-path", pipeline_epoch=epoch,
                          max_offsets_per_trigger=max_offsets)


def _rows(database):
    with psycopg.connect(database) as c:
        return c.execute("SELECT network,window_start,swaps,diagnostic_only FROM stream_candidate_windows "
                         "ORDER BY window_start").fetchall()


def test_actual_state_store_and_checkpoint(spark, database, kafka, tmp_path):
    bootstrap, topic, send = kafka
    send(("a", W1 + 10), ("b", W1 + 20))
    config = _config(database, bootstrap, topic, tmp_path / "cp")
    source = spark.readStream.format("kafka").options(**kafka_reader_options(config)).load()
    plan = stream.explain_plan(stream.build_fast_path(source, POLICY))
    assert "EventTimeWatermark" in plan and "DeduplicateWithinWatermark" in plan
    result = stream.run_fast_path(spark, config, POLICY)
    assert "StreamingDeduplicateWithinWatermark" in result.executed_plan
    assert "StateStoreSave" in result.executed_plan
    assert result.progress and all(p["stateOperators"] for p in result.progress if p["numInputRows"])
    assert result.max_state_rows >= 2 and result.max_state_rows <= POLICY["max_state_rows"]
    assert (tmp_path / "cp" / "state").is_dir() and (tmp_path / "cp" / "commits").is_dir()
    assert kafka_reader_options(config)["failOnDataLoss"] == "true"


def test_duplicates_and_equal_trades(spark, database, kafka, tmp_path):
    bootstrap, topic, send = kafka
    send(("a", W1 + 10), ("a", W1 + 10), ("b", W1 + 20), ("c", W1 + 20), ("close", W1 + 900))
    stream.run_fast_path(spark, _config(database, bootstrap, topic, tmp_path / "cp"), POLICY)
    assert _rows(database)[0] == ("testnet", _rows(database)[0][1], 3, True)
    assert _rows(database)[0][1].timestamp() == W1


def test_state_bound_enforced_and_offsets_bounded(spark, database, kafka, tmp_path):
    bootstrap, topic, send = kafka
    send(*[(f"e{i}", W1 + i) for i in range(6)])
    tight = {**POLICY, "max_state_rows": 3}
    config = _config(database, bootstrap, topic, tmp_path / "cp", max_offsets=2)
    with pytest.raises(stream.StateBoundExceeded):
        stream.run_fast_path(spark, config, tight)
    with psycopg.connect(database) as c:
        assert c.execute("SELECT coalesce(max(input_rows),0) FROM stream_state_metrics").fetchone()[0] <= 2
    with pytest.raises(ValueError):
        spark.conf.set("spark.sql.shuffle.partitions", "4")
        try:
            stream.run_fast_path(spark, _config(database, bootstrap, topic, tmp_path / "cp2"), POLICY)
        finally:
            spark.conf.set("spark.sql.shuffle.partitions", "1")


def test_restart_resumes_state_without_double_counting(database, kafka, tmp_path):
    bootstrap, topic, send = kafka
    checkpoint = tmp_path / "cp"
    config = _config(database, bootstrap, topic, checkpoint, max_offsets=2)
    send(("a", W1 + 10), ("b", W1 + 20))
    first = _session("run-1")
    try:
        r1 = stream.run_fast_path(first, config, POLICY)
    finally:
        first.stop()
    assert _rows(database) == []
    assert r1.max_state_rows >= 2
    committed = sorted(p.name for p in (checkpoint / "commits").iterdir() if p.name.isdigit())

    send(("a", W1 + 10), ("b", W1 + 20), ("c", W1 + 30), ("close", W1 + 900))
    second = _session("run-2")
    try:
        r2 = stream.run_fast_path(second, config, POLICY)
    finally:
        second.stop()
    assert [(row[0], row[1].timestamp(), row[2]) for row in _rows(database)] == [("testnet", W1, 3)]
    assert sorted(p.name for p in (checkpoint / "commits").iterdir() if p.name.isdigit()) != committed
    assert min(p["batchId"] for p in r2.progress) > int(committed[-1])
    replay = min((p for p in r2.progress if p["numInputRows"]), key=lambda p: p["batchId"])
    assert replay["numInputRows"] == 2
    assert sum(op["numRowsUpdated"] for op in replay["stateOperators"]) == 0
    assert sum(op["numRowsTotal"] for op in replay["stateOperators"]) >= 2

    third = _session("run-3")
    try:
        stream.run_fast_path(third, config, POLICY)
    finally:
        third.stop()
    assert [(row[1].timestamp(), row[2]) for row in _rows(database)] == [(W1, 3)]
    with psycopg.connect(database) as c:
        assert c.execute("SELECT count(*) FROM stream_candidate_windows").fetchone()[0] == 1


def test_changed_pipeline_epoch_refuses_existing_checkpoint(spark, database, kafka, tmp_path):
    bootstrap, topic, send = kafka
    send(("a", W1 + 10))
    checkpoint = tmp_path / "cp"
    stream.run_fast_path(spark, _config(database, bootstrap, topic, checkpoint), POLICY)
    with pytest.raises(ValueError, match="checkpoint identity mismatch"):
        stream.run_fast_path(spark, _config(database, bootstrap, topic, checkpoint, epoch="epoch-b"), POLICY)


from ton_sentinel.db import work as work_module  # noqa: E402
from tests.integration.test_repair_fencing import world_with_correction  # noqa: E402,F401
from tests.integration.test_revisions import A, X, Y, Z, World, db, eid, val  # noqa: E402,F401

SW_NOW = 200_000
SW_ADMISSION = {"event_retention_seconds": 4_000_000_000, "repair_margin_seconds": 0,
                "baseline_lookback_seconds": 86400, "derived_retention_seconds": 4_000_000_000,
                "max_windows": 1000, "max_actions": 1_000_000, "max_input_bytes": 1_000_000_000,
                "max_baseline_rows": 1_000_000}
SW_POLICY = stream.SweepPolicy(idle_sweep_seconds=600, watermark_seconds=60, max_windows=20,
                               admission=SW_ADMISSION)


@pytest.fixture(scope="module")
def repair_spark():
    session = _session("sweeper")
    yield session
    session.stop()


def _cand(db, start, swaps):
    db.execute("INSERT INTO stream_candidate_windows (environment_id,query_id,pipeline_epoch,network,"
               "window_start,window_end,swaps,batch_id) VALUES ('e','q','p','testnet',"
               "to_timestamp(%s),to_timestamp(%s),%s,0)", (start, start + 300, swaps))
    db.commit()


def _served(db):
    db.commit()
    return db.execute("SELECT window_start,token,swaps FROM served_features "
                      "ORDER BY window_start,token COLLATE \"C\"").fetchall()


def _promotions(db):
    db.commit()
    return db.execute("SELECT count(*) FROM repair_promotions").fetchone()[0]


def _closures(db):
    db.commit()
    return db.execute("SELECT window_start,reason,w.work_id IS NOT NULL FROM window_sweep_closures w "
                      "ORDER BY window_start").fetchall()


def _sweep(db, spark, now=SW_NOW, policy=SW_POLICY):
    db.commit()
    lease = acquire_transition(db, "sweeper")
    return stream.sweep_windows(db, lease, spark, now, policy)


def _wrap_repair(monkeypatch):
    """Spy on the canonical path; every closure must pass freeze -> compute -> promote."""
    seen = {"admit": [], "freeze": [], "promote": [], "compute": []}
    for name, key in (("admit_work", "admit"), ("freeze_work", "freeze"), ("promote", "promote"),
                      ("compute_replacement", "compute")):
        real = getattr(work_module, name)

        def spy(*a, _real=real, _key=key, **kw):
            seen[_key].append(a)
            return _real(*a, **kw)
        monkeypatch.setattr(work_module, name, spy)
        if hasattr(stream, name):
            monkeypatch.setattr(stream, name, spy)
    return seen


def _seed_at(db, end):
    World(db).seed({X: val(end=end)})
    db.commit()


def test_idle_window_closes_only_after_the_configured_sweep_through_canonical_repair(
        db, repair_spark, monkeypatch):
    _seed_at(db, 90_010)
    seen = _wrap_repair(monkeypatch)
    early = _sweep(db, repair_spark, now=90_300 + 599)
    assert early.repaired == () and _served(db) == [] and _promotions(db) == 0
    report = _sweep(db, repair_spark, now=90_300 + 600)
    assert [(k.window_start, r) for k, r in report.repaired] == [(90_000, "idle")]
    assert _served(db) == [(90_000, "TON", 1), (90_000, "jetton:A", 1)]
    assert len(seen["admit"]) == len(seen["freeze"]) == len(seen["compute"]) == 1
    assert len(seen["promote"]) == 1 and _promotions(db) == 1
    assert _closures(db) == [(90_000, "idle", True)]
    assert db.execute("SELECT count(*) FROM stream_state_metrics").fetchone()[0] == 0


def test_evicted_window_is_repaired_once_and_candidate_never_becomes_current(
        db, repair_spark, monkeypatch):
    _seed_at(db, 90_010)
    _cand(db, 90_000, 99)
    seen = _wrap_repair(monkeypatch)
    first = _sweep(db, repair_spark, now=90_300 + 60)
    assert [(k.window_start, r) for k, r in first.repaired] == [(90_000, "evicted")]
    assert _served(db) == [(90_000, "TON", 1), (90_000, "jetton:A", 1)]
    again = _sweep(db, repair_spark)
    assert again.repaired == () and _promotions(db) == 1 and len(seen["promote"]) == 1
    db.commit()
    assert db.execute("SELECT swaps,diagnostic_only FROM stream_candidate_windows").fetchall() == [
        (99, True)]


def test_late_revision_beyond_candidate_is_repaired_exactly_once(db, repair_spark, monkeypatch):
    world = World(db)
    world.seed({X: val(end=90_010)})
    db.commit()
    _cand(db, 90_000, 1)
    first = _sweep(db, repair_spark, now=90_300 + 60)
    assert [r for _, r in first.repaired] == ["evicted"]
    assert _promotions(db) == 1
    world.seed({Z: val(end=90_020, o_asset="jetton:B")})
    db.commit()
    seen = _wrap_repair(monkeypatch)
    late = _sweep(db, repair_spark)
    assert [(k.window_start, r) for k, r in late.repaired] == [(90_000, "late")]
    assert dict(((s, t), n) for s, t, n in _served(db)) == {
        (90_000, "TON"): 2, (90_000, "jetton:A"): 1, (90_000, "jetton:B"): 1}
    assert len(seen["promote"]) == 1 and _promotions(db) == 2
    assert _sweep(db, repair_spark).repaired == () and _promotions(db) == 2
    assert _closures(db) == [(90_000, "late", True)]


def test_dirty_windows_are_closed_by_repair_and_rows_match_the_policy_oracle(
        db, repair_spark, monkeypatch):
    world, evidence, auth = world_with_correction(db)
    lease = acquire_transition(db, "revisor")
    decide_revision(db, lease, evidence, auth)
    db.commit()
    pending = db.execute("SELECT window_start FROM dirty_windows WHERE role='target' "
                         "AND status='pending' ORDER BY 1").fetchall()
    db.commit()
    assert {p[0] for p in pending} == {0, 600}
    seen = _wrap_repair(monkeypatch)
    report = _sweep(db, repair_spark, now=SW_NOW)
    assert sorted((k.window_start, r) for k, r in report.repaired) == [(0, "dirty"), (600, "dirty")]
    assert len(seen["promote"]) == 2 and _promotions(db) == 2
    assert _served(db) == [(600, "TON", 1), (600, "jetton:A", 1)]
    assert db.execute("SELECT status FROM dirty_windows WHERE role='target' "
                      "AND status<>'done'").fetchall() == []
    assert _sweep(db, repair_spark).repaired == ()


def test_refused_admission_is_reported_and_nothing_is_written(db, repair_spark):
    _seed_at(db, 10)
    short = stream.SweepPolicy(600, 60, 20, {**SW_ADMISSION, "event_retention_seconds": 1000})
    report = _sweep(db, repair_spark, now=200_000, policy=short)
    assert report.repaired == () and report.refused == (
        (stream.WindowKey("testnet", 0, 300), "unrepairable"),)
    assert _served(db) == [] and _promotions(db) == 0 and _closures(db) == []


SW1 = 90_000


def test_real_stream_candidates_are_closed_only_by_canonical_repair(
        db, repair_spark, kafka, tmp_path, monkeypatch):
    bootstrap, topic, send = kafka
    send(("a", SW1 + 10), ("b", SW1 + 20), ("c", SW1 + 30), ("close", SW1 + 900))
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    conninfo = psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={db.schema_name}")
    config = _config(conninfo, bootstrap, topic, tmp_path / "cp")
    stream.run_fast_path(repair_spark, config, POLICY)
    db.commit()
    assert db.execute("SELECT swaps FROM stream_candidate_windows").fetchall() == [(3,)]
    _seed_at(db, SW1 + 10)
    before = db.execute("SELECT * FROM stream_candidate_windows").fetchall()
    seen = _wrap_repair(monkeypatch)
    report = _sweep(db, repair_spark, now=SW1 + 3000)
    assert [(k.window_start, r) for k, r in report.repaired] == [(SW1, "evicted")]
    assert _served(db) == [(SW1, "TON", 1), (SW1, "jetton:A", 1)]
    assert len(seen["promote"]) == 1
    db.commit()
    assert db.execute("SELECT * FROM stream_candidate_windows").fetchall() == before


def test_candidate_cap_on_a_real_spark_dataframe(spark):
    frame = spark.range(0, 5).toDF("n")
    assert [r["n"] for r in stream.collect_candidate_rows(frame, cap=5)] == [0, 1, 2, 3, 4]
    with pytest.raises(ValueError, match="window cap"):
        stream.collect_candidate_rows(frame, cap=4)


def test_fail_on_data_loss_stops_the_stream_when_unread_records_are_deleted(
        spark, database, kafka, tmp_path):
    from kafka import KafkaAdminClient, TopicPartition
    bootstrap, topic, send = kafka
    config = _config(database, bootstrap, topic, tmp_path / "cp")
    send(("a", W1 + 10), ("b", W1 + 20), ("close", W1 + 900))
    stream.run_fast_path(spark, config, POLICY)
    send(("c", W1 + 1200), ("d", W1 + 1210), ("e", W1 + 1220))
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15_000)
    try:
        admin.delete_records({TopicPartition(topic, 0): 5})
    finally:
        admin.close()
    with pytest.raises(Exception, match=r"(?i)lost|out of range|no longer available|deleted"):
        stream.run_fast_path(spark, config, POLICY)


@pytest.fixture
def sweep_spark():
    # Function scoped: the earlier real-stream test stops the shared session.
    session = _session("sweeper-interface")
    yield session
    session.stop()


def _lease(db):
    db.commit()
    return acquire_transition(db, "sweeper")


def test_sweep_due_returns_the_repaired_count_and_is_bounded(db, sweep_spark, monkeypatch):
    _seed_at(db, 90_010)
    seen = _wrap_repair(monkeypatch)
    with pytest.raises(ValueError):
        stream.sweep_due(db, sweep_spark, 90_900, 0, lease=_lease(db), policy=SW_POLICY)
    assert _served(db) == [] and _promotions(db) == 0
    assert stream.sweep_due(db, sweep_spark, 90_900, 5, lease=_lease(db), policy=SW_POLICY) == 1
    assert _served(db) == [(90_000, "TON", 1), (90_000, "jetton:A", 1)]
    assert len(seen["promote"]) == 1 and _closures(db) == [(90_000, "idle", True)]
    assert stream.sweep_due(db, sweep_spark, 90_900, 5, lease=_lease(db), policy=SW_POLICY) == 0
    assert _promotions(db) == 1


def test_close_candidate_windows_uses_canonical_repair_and_never_the_candidate(
        db, sweep_spark, monkeypatch):
    _seed_at(db, 90_010)
    _cand(db, 90_000, 99)
    seen = _wrap_repair(monkeypatch)
    key = stream.WindowKey("testnet", 90_000, 90_300)
    assert stream.close_candidate_windows(db, sweep_spark, (key,), lease=_lease(db),
                                          now=90_300, policy=SW_POLICY) is None
    assert _served(db) == [(90_000, "TON", 1), (90_000, "jetton:A", 1)]
    assert len(seen["admit"]) == len(seen["freeze"]) == len(seen["compute"]) == len(seen["promote"]) == 1
    assert _closures(db) == [(90_000, "evicted", True)]
    assert db.execute("SELECT swaps FROM stream_candidate_windows").fetchall() == [(99,)]
    with pytest.raises(ValueError):
        stream.close_candidate_windows(db, sweep_spark, (), lease=_lease(db), now=90_300,
                                       policy=SW_POLICY)


def test_close_candidate_windows_reports_a_refused_admission(db, sweep_spark):
    _seed_at(db, 10)
    short = stream.SweepPolicy(600, 60, 20, {**SW_ADMISSION, "event_retention_seconds": 1000})
    with pytest.raises(RuntimeError, match="unrepairable"):
        stream.close_candidate_windows(db, sweep_spark, (stream.WindowKey("testnet", 0, 300),),
                                       lease=_lease(db), now=200_000, policy=short)
    assert _served(db) == [] and _promotions(db) == 0 and _closures(db) == []
