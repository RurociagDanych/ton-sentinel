"""Recovery drills on real Spark, Kafka and PostgreSQL (app container).

* checkpoint loss with pending and ambiguous sends: pause, cut, resume, and a new Spark lineage
  that starts exactly at the cut's end offsets E;
* Kafka expiry: the normal query stops first, a new cut recovers from canonical truth;
* real process kills: Spark driver (SIGKILL of the whole driver process group), Spark executor
  (SIGKILL of the executor JVM in ``local-cluster`` mode) and the maintenance sweeper;
* PostgreSQL outage mid-batch and mid-sweep: the server backend serving the writer is
  terminated (``pg_terminate_backend``) inside the open promotion.

Every scenario ends on the same oracle: per window and token, served ``swaps`` equal the
number of active canonical actions, and a full replay of the whole retained log in an
independent lineage reproduces the served rows byte for byte.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("pyspark")
from kafka import KafkaProducer, TopicPartition  # noqa: E402
from kafka.admin import KafkaAdminClient, NewTopic  # noqa: E402

import ton_sentinel.spark.streaming as streaming  # noqa: E402
from ton_sentinel.db import work as work_module  # noqa: E402
from ton_sentinel.db.fencing import acquire_transition  # noqa: E402
from ton_sentinel.db.ledger import migrate  # noqa: E402
from ton_sentinel.outbox import configure_delivery, publish_pending  # noqa: E402
from ton_sentinel.recovery import (KafkaOffsetReader, KafkaRecordReader,  # noqa: E402
                                   capture_recovery_cut, claim_recovery_point,
                                   pause_for_recovery, resume_from_recovery_cut,
                                   resumed_consumer_config)
from ton_sentinel.spark import stream  # noqa: E402
from ton_sentinel.stream_cli import ConsumerConfig, run_consumer  # noqa: E402
from tests.integration.test_canonical_kafka import _accept  # noqa: E402
from tests.integration.test_recovery import FailingProducer, wire_value  # noqa: E402
from tests.integration.test_stateful_stream_pg import SW_ADMISSION, _session  # noqa: E402

TOKENS = ("JETTON:CANONICAL", "TON")
SWEEP = stream.SweepPolicy(idle_sweep_seconds=600, watermark_seconds=60, max_windows=20,
                           admission=SW_ADMISSION)
SWEEP_NOW = 100_000
DATA_LOSS = r"(?i)lost|out of range|no longer available|deleted"
# A sweep here starts a fresh Spark session and computes each window in turn: about 26 s on
# an idle app container, 39 s with one competing CPU-bound process. The default 30 s lease
# then expires mid-sweep. Lease expiry is not what these drills test, so the sweeper takes
# the longest lease the fencing contract allows.
SWEEP_LEASE_SECONDS = 300
PYTHONPATH = os.environ.get("PYTHONPATH", "")


def window(k: int) -> int:
    return k * 300


class Drill:
    def __init__(self, dsn, bootstrap, topic, incarnation, admin, producer):
        self.dsn, self.bootstrap, self.topic = dsn, bootstrap, topic
        self.incarnation, self.admin, self.producer = incarnation, admin, producer
        self.reader = KafkaOffsetReader(bootstrap)
        self.events = {}

    def connect(self):
        return psycopg.connect(self.dsn)

    def accept(self, k: int) -> str:
        """Event k: its own ingest range and five-minute window k*300."""
        with self.connect() as c:
            before = {r[0] for r in c.execute("SELECT event_id FROM outbox").fetchall()}
            _accept(c, window(k), window(k + 1), k * 10 + 1, window(k) + 10)
            after = {r[0] for r in c.execute("SELECT event_id FROM outbox").fetchall()}
        (event,) = after - before
        self.events[k] = event
        return event

    def publish(self) -> tuple:
        with self.connect() as c:
            sent = publish_pending(c, self.producer, acquire_transition(c, "publisher"), limit=50)
        self.producer.flush(timeout=15)
        return sent

    def make_ambiguous(self) -> None:
        with self.connect() as c:
            with pytest.raises(Exception):
                publish_pending(c, FailingProducer(), acquire_transition(c, "publisher"),
                                limit=1, max_retries=0)
            c.rollback()
            assert c.execute("SELECT count(*) FROM outbox WHERE publication_state="
                             "'ambiguous'").fetchone()[0] == 1

    def config(self, checkpoint: Path, epoch: str, max_offsets: int = 20) -> ConsumerConfig:
        return ConsumerConfig(dsn=self.dsn, bootstrap_servers=self.bootstrap, topic=self.topic,
                              checkpoint=str(checkpoint.resolve()),
                              topic_incarnation=self.incarnation, replay_epoch=0,
                              environment_id="local-test", query_id="canonical-features",
                              pipeline_epoch=epoch, max_offsets_per_trigger=max_offsets)

    def query(self, sql, args=()):
        with self.connect() as c:
            return c.execute(sql, args).fetchall()

    def served_rows(self):
        return self.query("SELECT network, window_start, token, swaps, input_raw::text, "
                          "output_raw::text FROM served_features "
                          "ORDER BY network, window_start, token COLLATE \"C\"")

    def served(self) -> dict:
        return {(w, t): s for _n, w, t, s, _i, _o in self.served_rows()}

    def oracle(self) -> dict:
        """Policy oracle from canonical truth: one swap per token per active action."""
        expected = {}
        for (end,) in self.query("SELECT (action->>'action_end')::bigint FROM canonical_actions"):
            for token in TOKENS:
                key = (end // 300 * 300, token)
                expected[key] = expected.get(key, 0) + 1
        return expected

    def batches(self, epoch=None):
        sql = "SELECT pipeline_epoch, batch_id, offset_metadata FROM stream_batches"
        rows = self.query(sql + " ORDER BY pipeline_epoch, batch_id")
        return [r for r in rows if epoch is None or r[0] == epoch]

    def sweep(self, spark, compute=None):
        with self.connect() as c:
            lease = acquire_transition(c, "sweeper", ttl_seconds=SWEEP_LEASE_SECONDS)
            return stream.sweep_windows(c, lease, spark, SWEEP_NOW, SWEEP, compute=compute)

    def cut(self, owner="recovery"):
        with self.connect() as c:
            pause = pause_for_recovery(c, owner)
            cut = capture_recovery_cut(c, pause.lease, pause.pause_id, self.reader)
            return pause, cut, claim_recovery_point(c, cut.cut_id, self.reader)

    def resume(self, point, epoch):
        with self.connect() as c:
            handoff = acquire_transition(c, "resumer", ttl_seconds=300)
            resumed = resume_from_recovery_cut(c, handoff, point, self.reader,
                                               KafkaRecordReader(self.bootstrap),
                                               pipeline_epoch=epoch)
            sent = publish_pending(c, self.producer, handoff, limit=50)
        self.producer.flush(timeout=15)
        return resumed, sent

    def full_replay_matches(self, checkpoint: Path) -> None:
        """Oracle 2: an independent lineage over the whole retained log changes no row."""
        rows = self.served_rows()
        run_consumer(self.config(checkpoint, "epoch-oracle-" + uuid.uuid4().hex[:8]))
        assert self.served_rows() == rows


@pytest.fixture
def drill():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    bootstrap = os.environ.get("TON_SENTINEL_TEST_KAFKA_BOOTSTRAP")
    if not dsn or not bootstrap:
        pytest.fail("TON_SENTINEL_TEST_DSN and TON_SENTINEL_TEST_KAFKA_BOOTSTRAP are required")
    suffix = uuid.uuid4().hex
    schema, topic = "rd_" + suffix, "recovery-drill-" + suffix
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=15_000)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
    with psycopg.connect(dsn, autocommit=True) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
    scoped = psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={schema}")
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3,
                             max_block_ms=15_000)
    try:
        with psycopg.connect(scoped) as c:
            migrate(c)
            configure_delivery(c, topic=topic, topic_incarnation="inc-" + suffix)
        yield Drill(scoped, bootstrap, topic, "inc-" + suffix, admin, producer)
    finally:
        producer.close(timeout=10)
        try:
            admin.delete_topics([topic])
        except Exception:
            pass
        admin.close()
        with psycopg.connect(dsn, autocommit=True) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def record_batches(monkeypatch) -> list:
    seen = []
    original = streaming.process_canonical_microbatch

    def recording(*args, **kwargs):
        rows = args[3].select("partition", "offset", "key").collect()
        seen.append([(r.partition, r.offset, r.key.decode()) for r in rows])
        return original(*args, **kwargs)

    monkeypatch.setattr(streaming, "process_canonical_microbatch", recording)
    return seen


def commits(checkpoint: Path) -> set:
    path = checkpoint / "commits"
    return {p.name for p in path.iterdir()} if path.is_dir() else set()


def sweep_once(drill, compute=None):
    spark = _session("recovery-drill-sweeper-" + uuid.uuid4().hex[:6])
    try:
        return drill.sweep(spark, compute=compute)
    finally:
        spark.stop()


def offsets_of(batches) -> list:
    return sorted((o["partition"], o["offset"]) for *_, meta in batches for o in meta)


def log_offsets(drill) -> list:
    sample = drill.reader(drill.topic)["partitions"]
    return sorted((int(p), o) for p, v in sample.items() for o in range(v["start"], v["end"]))


@pytest.mark.integration
def test_checkpoint_loss_pending_and_ambiguous(drill, tmp_path, monkeypatch):
    seen = record_batches(monkeypatch)
    cp_a = tmp_path / "cp-a"
    e1 = drill.accept(1)
    assert drill.publish() == (e1,)
    run_consumer(drill.config(cp_a, "epoch-a"))
    assert drill.served() == {(300, t): 1 for t in TOKENS}
    e2 = drill.accept(2)
    assert drill.publish() == (e2,)            # acknowledged below E; the consumer lags
    e3 = drill.accept(3)
    drill.make_ambiguous()                     # e3: the broker outcome is unknown
    e4 = drill.accept(4)                       # e4: a pending intent
    with drill.connect() as c:
        pause = pause_for_recovery(c, "recovery")
    served, batches, done = drill.served_rows(), drill.batches(), commits(cp_a)
    # Consumer callbacks and maintenance are fenced by the durable pause.
    with pytest.raises(Exception, match="paused for a recovery cut"):
        run_consumer(drill.config(cp_a, "epoch-a"))
    with pytest.raises(Exception, match="paused for a recovery cut"):
        sweep_once(drill)
    assert (drill.served_rows(), drill.batches(), commits(cp_a)) == (served, batches, done)
    # Those callbacks took the slot; the durable pause is retaken with its original epoch.
    with drill.connect() as c:
        again = pause_for_recovery(c, "recovery")
        assert (again.pause_id, again.recovery_epoch, again.resumed) == (
            pause.pause_id, pause.recovery_epoch, True)
        cut = capture_recovery_cut(c, again.lease, again.pause_id, drill.reader)
        point = claim_recovery_point(c, cut.cut_id, drill.reader)
    end = cut.manifest["kafka"]["end_offsets"]
    assert sum(end.values()) == 2                                   # e1, e2 below E
    members = {m[0]: m[3] for m in cut.manifest["outbox"]["members"]}
    assert members == {e1: "acknowledged", e2: "acknowledged", e3: "ambiguous", e4: "pending"}
    assert len(cut.manifest["snapshot"]["contributions"]) == 4      # one per accepted event
    # The superseded producer's in-flight send of e3 lands after the cut.
    with drill.connect() as c:
        envelope = c.execute("SELECT delivery_envelope FROM outbox WHERE event_id=%s",
                             (e3,)).fetchone()[0]
    landed = drill.producer.send(drill.topic, key=e3.encode(), value=wire_value(envelope)).get(15)
    assert landed.offset >= end[str(landed.partition)]
    shutil.rmtree(cp_a)                                             # Spark checkpoint lost
    resumed, sent = drill.resume(point, "pe-resumed")
    assert resumed.resolutions == {e3: (landed.partition, landed.offset)}
    assert sent == (e4,)                                            # e3 deduplicated, not resent
    cp_r = tmp_path / "cp-resumed"
    config = resumed_consumer_config(drill.config(cp_r, "epoch-a"), resumed)
    # Explicit coordinates are refused unless they are the durable resumption's exact E.
    with pytest.raises(ValueError, match="durable recovery resumption"):
        run_consumer(replace(config, pipeline_epoch="pe-guessed"))
    with pytest.raises(ValueError, match="differ"):
        run_consumer(replace(config, starting_offsets=json.dumps({drill.topic: {"0": 0, "1": 0}})))
    assert not cp_r.exists()                                       # no lineage was bound
    # Batches handed to the callback so far: e1 (committed) and e2 (refused while paused).
    assert [k for batch in seen for *_, k in batch] == [e1, e2]
    seen.clear()
    run_consumer(config)
    applied = [item for batch in seen for item in batch]
    assert sorted(k for *_, k in applied) == sorted([e3, e4])       # each once, nothing below E
    assert all(o >= end[str(p)] for p, o, _ in applied)
    assert {b[0] for b in drill.batches()} == {"epoch-a", "pe-resumed"}
    assert all(o["offset"] >= end[str(o["partition"])]
               for b in drill.batches("pe-resumed") for o in b[2])
    # e2 lies below E and was never consumed: the canonical sweeper closes its window.
    assert (600, "TON") not in drill.served()
    report = sweep_once(drill)
    assert [(k.window_start, r) for k, r in report.repaired] == [(600, "idle")]
    assert drill.served() == drill.oracle() == {(window(k), t): 1 for k in (1, 2, 3, 4)
                                                for t in TOKENS}
    assert drill.query("SELECT count(*), count(*) FILTER (WHERE publication_state="
                       "'acknowledged') FROM outbox") == [(4, 4)]
    drill.full_replay_matches(tmp_path / "cp-oracle")


@pytest.mark.integration
def test_expired_offsets_stop_the_query_and_a_new_cut_recovers_from_canonical_truth(
        drill, tmp_path, monkeypatch):
    seen = record_batches(monkeypatch)
    cp_a = tmp_path / "cp-a"
    e1 = drill.accept(1)
    drill.publish()
    run_consumer(drill.config(cp_a, "epoch-a"))
    drill.accept(2)
    drill.accept(3)
    drill.publish()
    ends = {int(p): v["end"] for p, v in drill.reader(drill.topic)["partitions"].items()}
    # Kafka expiry of the isolated test topic: e2/e3 are deleted before the consumer read them.
    drill.admin.delete_records({TopicPartition(drill.topic, p): e for p, e in ends.items() if e})
    served = drill.served_rows()
    with pytest.raises(Exception, match=DATA_LOSS):                # the normal query stops first
        run_consumer(drill.config(cp_a, "epoch-a"))
    assert drill.served_rows() == served == drill.served_rows()
    assert drill.served() == {(300, t): 1 for t in TOKENS}
    _pause, cut, point = drill.cut()
    end = cut.manifest["kafka"]["end_offsets"]
    assert {int(p): e for p, e in end.items()} == ends
    assert cut.manifest["kafka"]["log_start_offsets"] == end           # nothing retained below E
    resumed, sent = drill.resume(point, "pe-resumed")
    assert resumed.resolutions == {} and sent == ()
    seen.clear()
    cp_r = tmp_path / "cp-resumed"
    config = resumed_consumer_config(drill.config(cp_r, "epoch-a"), resumed)
    run_consumer(config)
    assert [item for batch in seen for item in batch] == []
    # The expired windows are recovered from canonical truth, not from the lost records.
    report = sweep_once(drill)
    assert sorted((k.window_start, r) for k, r in report.repaired) == [(600, "idle"),
                                                                        (900, "idle")]
    assert drill.served() == drill.oracle()
    # The resumed lineage then continues normally from its own checkpoint.
    e4 = drill.accept(4)
    assert drill.publish() == (e4,)
    seen.clear()
    run_consumer(config)
    assert [k for batch in seen for *_, k in batch] == [e4]
    assert drill.served() == drill.oracle() == {(window(k), t): 1 for k in (1, 2, 3, 4)
                                                for t in TOKENS}
    assert e1 in {m[0] for m in cut.manifest["outbox"]["members"]}


def _cli(drill, checkpoint: Path, epoch: str, *, max_offsets=2) -> list:
    # Spark's per-trigger rate limit still reads at least one record per partition (2 here),
    # so 2 is the smallest cap that the batch row bound (max_rows = cap) accepts.
    return [sys.executable, "-m", "ton_sentinel.stream_cli", "--bootstrap-servers",
            drill.bootstrap, "--topic", drill.topic, "--checkpoint", str(checkpoint.resolve()),
            "--topic-incarnation", drill.incarnation, "--replay-epoch", "0",
            "--environment-id", "local-test", "--query-id", "canonical-features",
            "--pipeline-epoch", epoch, "--max-offsets-per-trigger", str(max_offsets)]


def _env(drill, **extra) -> dict:
    return {**os.environ, "TON_SENTINEL_DSN": drill.dsn, **extra}   # DSN via env, not argv


def _live_members(pgid: int) -> list:
    """Non-zombie processes of the group (PID 1 in the app container does not reap zombies)."""
    live = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            stat = (proc / "stat").read_text()
        except OSError:
            continue
        fields = stat[stat.rindex(")") + 2:].split()       # state, ppid, pgrp, ...
        if int(fields[2]) == pgid and fields[0] != "Z":
            live.append(int(proc.name))
    return live


def _wait_gone(pgid: int) -> None:
    for _ in range(100):
        if not _live_members(pgid):
            return
        time.sleep(0.1)
    raise AssertionError(f"killed process group still has live members {_live_members(pgid)}")


def _published(drill, n: int) -> list:
    events = [drill.accept(k) for k in range(1, n + 1)]
    assert sorted(drill.publish()) == sorted(events)
    return events


@pytest.mark.integration
def test_spark_driver_kill_mid_stream_restarts_from_checkpoint_to_the_oracle(drill, tmp_path):
    _published(drill, 10)
    checkpoint, log = tmp_path / "cp-driver", (tmp_path / "driver.log").open("w")
    proc = subprocess.Popen(_cli(drill, checkpoint, "epoch-driver"), env=_env(drill),
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.time() + 300
    while len(drill.batches("epoch-driver")) < 2:
        assert proc.poll() is None, "driver ended before the kill"
        assert time.time() < deadline
        time.sleep(0.2)
    os.killpg(proc.pid, signal.SIGKILL)                    # driver Python + its Spark JVM
    assert proc.wait(timeout=60) == -signal.SIGKILL
    _wait_gone(proc.pid)
    at_kill = len(drill.batches("epoch-driver"))
    assert at_kill >= 2
    restarted = subprocess.run(_cli(drill, checkpoint, "epoch-driver"), env=_env(drill),
                               stdout=log, stderr=subprocess.STDOUT, timeout=600)
    assert restarted.returncode == 0
    assert len(drill.batches("epoch-driver")) > at_kill          # the kill was mid-stream
    # Every retained record is applied by exactly one committed batch; nothing doubles.
    assert offsets_of(drill.batches("epoch-driver")) == log_offsets(drill)
    assert drill.served() == drill.oracle() == {(window(k), t): 1 for k in range(1, 11)
                                                for t in TOKENS}
    drill.full_replay_matches(tmp_path / "cp-oracle")
    print(f"driver killed after {at_kill} committed batches")


def _spark_home(tmp_path: Path) -> Path:
    """A writable SPARK_HOME view (local-cluster workers need SPARK_HOME/work)."""
    home = tmp_path / "spark-home"
    (home / "work").mkdir(parents=True)
    for entry in Path("/opt/spark").iterdir():
        if entry.name != "work":
            (home / entry.name).symlink_to(entry)
    return home


def _executors(session: int) -> list:
    found = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            cmdline = (proc / "cmdline").read_bytes()
            if b"CoarseGrainedExecutorBackend" in cmdline and os.getsid(int(proc.name)) == session:
                found.append(int(proc.name))
        except (OSError, ProcessLookupError):
            continue
    return found


@pytest.mark.integration
def test_spark_executor_kill_is_retried_without_a_driver_restart(drill, tmp_path):
    if not Path("/opt/spark/bin/spark-class").exists():
        pytest.fail("the Spark distribution of the app container is required")
    _published(drill, 10)
    checkpoint, log = tmp_path / "cp-executor", (tmp_path / "executor.log").open("w")
    env = _env(drill, SPARK_HOME=str(_spark_home(tmp_path)),
               PYSPARK_SUBMIT_ARGS="--master local-cluster[1,1,512] --conf "
                                   "spark.executor.memory=512m --driver-memory 1g pyspark-shell")
    proc = subprocess.Popen(_cli(drill, checkpoint, "epoch-executor"), env=env, stdout=log,
                            stderr=subprocess.STDOUT, start_new_session=True)
    try:
        deadline = time.time() + 300
        while not (_executors(proc.pid) and len(drill.batches("epoch-executor")) >= 1):
            assert proc.poll() is None, "driver ended before the executor kill"
            assert time.time() < deadline
            time.sleep(0.2)
        (victim,) = _executors(proc.pid)
        os.kill(victim, signal.SIGKILL)                      # the executor JVM only
        replacement = []
        while proc.poll() is None and time.time() < deadline:
            replacement = [p for p in _executors(proc.pid) if p != victim] or replacement
            time.sleep(0.2)
        assert proc.wait(timeout=600) == 0                   # the same driver finished the run
    finally:
        if proc.poll() is None:
            os.killpg(proc.pid, signal.SIGKILL)
    assert replacement, "no replacement executor was launched"
    assert offsets_of(drill.batches("epoch-executor")) == log_offsets(drill)
    assert drill.served() == drill.oracle() == {(window(k), t): 1 for k in range(1, 11)
                                                for t in TOKENS}
    drill.full_replay_matches(tmp_path / "cp-oracle")


def _terminate(drill, connection) -> None:
    with psycopg.connect(drill.dsn, autocommit=True) as admin:
        assert admin.execute("SELECT pg_terminate_backend(%s)",
                             (connection.info.backend_pid,)).fetchone()[0] is True


@pytest.mark.integration
def test_postgres_backend_lost_mid_batch_retries_to_the_oracle(drill, tmp_path, monkeypatch):
    _published(drill, 3)
    checkpoint = tmp_path / "cp-pg"
    config = drill.config(checkpoint, "epoch-pg")
    original, killed = streaming.replace_features, []

    def terminate_then_replace(connection, *args, **kwargs):
        if not killed:                       # inside the open promotion transaction
            killed.append(connection.info.backend_pid)
            _terminate(drill, connection)
        return original(connection, *args, **kwargs)

    monkeypatch.setattr(streaming, "replace_features", terminate_then_replace)
    with pytest.raises(Exception, match=r"(?i)terminat|connection|closed|server"):
        run_consumer(config)
    assert len(killed) == 1
    assert drill.served_rows() == [] and drill.batches() == [] and commits(checkpoint) == set()
    monkeypatch.setattr(streaming, "replace_features", original)
    run_consumer(config)                     # same lineage, same checkpoint
    assert offsets_of(drill.batches("epoch-pg")) == log_offsets(drill)
    assert drill.served() == drill.oracle()
    drill.full_replay_matches(tmp_path / "cp-oracle")


@pytest.mark.integration
def test_postgres_backend_lost_mid_sweep_leaves_work_open_and_sweeps_once(drill):
    for k in (1, 2):
        drill.accept(k)                      # canonical truth only; no stream ever ran
    killed, sweeper = [], {}

    def terminate_then_compute(spark, frozen):
        if not killed:                       # frozen work is durable; promotion not yet run
            killed.append(frozen.work_id)
            _terminate(drill, sweeper["connection"])
        return work_module.compute_replacement(spark, frozen)

    spark = _session("recovery-drill-pg-sweep")
    try:
        with drill.connect() as c:
            sweeper["connection"] = c
            lease = acquire_transition(c, "sweeper", ttl_seconds=SWEEP_LEASE_SECONDS)
            with pytest.raises(Exception, match=r"(?i)terminat|connection|closed|server"):
                stream.sweep_windows(c, lease, spark, SWEEP_NOW, SWEEP,
                                     compute=terminate_then_compute)
        assert len(killed) == 1
        assert drill.served_rows() == []
        assert drill.query("SELECT count(*) FROM repair_promotions") == [(0,)]
        assert drill.query("SELECT count(*) FROM window_sweep_closures") == [(0,)]
        report = drill.sweep(spark)
        assert sorted((k.window_start, r) for k, r in report.repaired) == [(300, "idle"),
                                                                            (600, "idle")]
        assert drill.served() == drill.oracle()
        assert drill.sweep(spark).repaired == ()             # closed once
        assert drill.query("SELECT count(*) FROM repair_promotions") == [(2,)]
    finally:
        spark.stop()


_KILLED_SWEEPER = r"""
import json, os, signal
import psycopg
from ton_sentinel.db.fencing import acquire_transition
from ton_sentinel.spark import stream

policy = stream.SweepPolicy(600, 60, 20, json.loads(os.environ["RD_ADMISSION"]))

def dying(spark, frozen):            # after the work is frozen, before any promotion
    print("frozen", frozen.work_id, flush=True)
    os.kill(os.getpid(), signal.SIGKILL)

with psycopg.connect(os.environ["TON_SENTINEL_DSN"]) as c:
    stream.sweep_windows(c, acquire_transition(c, "sweeper-child"), None,
                         int(os.environ["RD_NOW"]), policy, compute=dying)
print("UNREACHABLE", flush=True)
"""


@pytest.mark.integration
def test_sweeper_process_kill_mid_sweep_then_restart_sweeps_once(drill):
    for k in (1, 2):
        drill.accept(k)
    child = subprocess.run([sys.executable, "-c", _KILLED_SWEEPER], capture_output=True,
                           text=True, timeout=120,
                           env=_env(drill, RD_ADMISSION=json.dumps(SW_ADMISSION),
                                    RD_NOW=str(SWEEP_NOW)))
    assert child.returncode == -signal.SIGKILL, child.stderr[-2000:]
    assert "frozen" in child.stdout and "UNREACHABLE" not in child.stdout
    # Durable work is not falsely complete: nothing promoted, served or closed.
    assert drill.served_rows() == []
    assert drill.query("SELECT count(*) FROM repair_promotions") == [(0,)]
    assert drill.query("SELECT count(*) FROM window_sweep_closures") == [(0,)]
    spark = _session("recovery-drill-sweeper-restart")
    try:
        report = drill.sweep(spark)
        assert sorted((k.window_start, r) for k, r in report.repaired) == [(300, "idle"),
                                                                            (600, "idle")]
        assert drill.served() == drill.oracle()
        assert drill.sweep(spark).repaired == ()
        assert drill.query("SELECT count(*) FROM repair_promotions") == [(2,)]
    finally:
        spark.stop()
