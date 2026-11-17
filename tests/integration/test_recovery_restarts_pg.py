"""Broker restart and PostgreSQL restart drills (host-driven, real docker).

The host restarts real containers between and during phases that run inside the app
container (``docker exec``): the project's Kafka broker (``docker restart``, data on its
volume) and a disposable PostgreSQL 18.6 container created for this test on the fixture
network. The shared compose PostgreSQL is never stopped.

* broker + PG restart between cut and resume: the durable pause and cut survive, the broker
  keeps its cluster id, topic id and offsets, the claim and resume succeed, the resumed
  consumer starts at E, and served rows equal the canonical oracle;
* PG restart in the middle of a running resumed consumer: the consumer fails, a rerun of the
  same lineage applies every record above E exactly once and reaches the same oracle.

Skipped where no docker CLI exists (inside the app container); run it on the host.
"""
from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import time
import uuid

import pytest

pytestmark = pytest.mark.skipif(shutil.which("docker") is None,
                                reason="host-only drill: needs the docker CLI")

APP = os.environ.get("TON_SENTINEL_APP_CONTAINER", "ton-sentinel-local-app-1")
KAFKA = os.environ.get("TON_SENTINEL_KAFKA_CONTAINER", "ton-sentinel-local-kafka-1")
NETWORK = os.environ.get("TON_SENTINEL_FIXTURE_NETWORK", "ton-sentinel-local_fixture")
PG_IMAGE = "postgres:18.6"
PYTHONPATH = ("/workspace/src:/opt/probe/deps:/opt/spark/python:"
              "/opt/spark/python/lib/py4j-0.10.9.9-src.zip:/workspace")


# ------------------------------------------------------------------ phases (app container)

def _drill(create: bool = False):
    import psycopg
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic

    from ton_sentinel.db.ledger import migrate
    from ton_sentinel.outbox import configure_delivery
    from tests.integration.test_recovery_drills_pg import Drill

    dsn, topic = os.environ["RS_DSN"], os.environ["RS_TOPIC"]
    bootstrap = os.environ["TON_SENTINEL_TEST_KAFKA_BOOTSTRAP"]
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=30_000)
    if create:
        admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
        with psycopg.connect(dsn) as c:
            migrate(c)
            configure_delivery(c, topic=topic, topic_incarnation=os.environ["RS_INC"])
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=5,
                             max_block_ms=30_000)
    return Drill(dsn, bootstrap, topic, os.environ["RS_INC"], admin, producer)


def _base():
    from pathlib import Path
    return Path(os.environ["RS_DIR"])


def _resumed_config(drill, max_offsets: int = 20):
    from dataclasses import replace
    return replace(drill.config(_base() / "cp-resumed", "pe-restart", max_offsets=max_offsets),
                   starting_offsets=os.environ["RS_START"])


def _phase_a(drill) -> dict:
    from ton_sentinel.stream_cli import run_consumer

    events = [drill.accept(1)]
    drill.publish()
    run_consumer(drill.config(_base() / "cp-a", "epoch-a"))
    events.append(drill.accept(2))
    drill.publish()
    events.append(drill.accept(3))
    drill.make_ambiguous()
    events.append(drill.accept(4))
    pause, cut, _point = drill.cut()
    return {"events": events, "cut_id": cut.cut_id, "pause_id": pause.pause_id,
            "end": cut.manifest["kafka"]["end_offsets"],
            "topic_id": cut.manifest["kafka"]["topic_id"],
            "cluster_id": cut.manifest["kafka"]["cluster_id"]}


def _phase_b(drill) -> dict:
    from ton_sentinel.recovery import claim_recovery_point, resumed_consumer_config, \
        spark_starting_offsets
    from ton_sentinel.stream_cli import run_consumer
    from tests.integration.test_recovery_drills_pg import TOKENS, sweep_once, window

    before = json.loads(os.environ["RS_BEFORE"])
    e1, e2, e3, e4 = before["events"]
    live = drill.reader(drill.topic)
    assert (live["cluster_id"], live["topic_id"]) == (before["cluster_id"], before["topic_id"])
    with drill.connect() as c:                            # after the PG restart
        assert c.execute("SELECT released_at FROM recovery_pauses WHERE pause_id=%s",
                         (before["pause_id"],)).fetchone() == (None,)
        c.commit()
        point = claim_recovery_point(c, before["cut_id"], drill.reader)
    assert {str(p): e for p, e in point.end_offsets.items()} == before["end"]
    resumed, sent = drill.resume(point, "pe-restart")
    assert resumed.resolutions == {e3: None}              # never landed: redelivered once
    assert sorted(sent) == sorted([e3, e4])
    run_consumer(resumed_consumer_config(drill.config(_base() / "cp-resumed", "epoch-a"),
                                         resumed))
    report = sweep_once(drill)
    assert [(k.window_start, r) for k, r in report.repaired] == [(600, "idle")]
    assert drill.served() == drill.oracle() == {(window(k), t): 1 for k in (1, 2, 3, 4)
                                                for t in TOKENS}
    for k in range(5, 11):                                # backlog for the mid-run restart
        drill.accept(k)
    assert len(drill.publish()) == 6
    return {"start": spark_starting_offsets(resumed), "batches": len(drill.batches("pe-restart")),
            "e2": e2, "e1": e1}


def _phase_c(drill) -> dict:
    from ton_sentinel.stream_cli import run_consumer

    run_consumer(_resumed_config(drill, max_offsets=2))  # PostgreSQL is restarted meanwhile
    return {"completed": True}


def _phase_d(drill) -> dict:
    from ton_sentinel.stream_cli import run_consumer
    from tests.integration.test_recovery_drills_pg import TOKENS, offsets_of, window

    run_consumer(_resumed_config(drill))
    end = {int(p): e for p, e in json.loads(os.environ["RS_START"])[drill.topic].items()}
    sample = drill.reader(drill.topic)["partitions"]
    above_e = sorted((int(p), o) for p, v in sample.items() for o in range(end[int(p)], v["end"]))
    assert offsets_of(drill.batches("pe-restart")) == above_e  # each record above E once
    assert drill.served() == drill.oracle() == {(window(k), t): 1 for k in range(1, 11)
                                                for t in TOKENS}
    drill.full_replay_matches(_base() / "cp-oracle")
    return {"batches": len(drill.batches("pe-restart")), "records_above_e": len(above_e)}


def _cleanup(drill) -> dict:
    try:
        drill.admin.delete_topics([drill.topic])
    finally:
        drill.admin.close()
    shutil.rmtree(_base(), ignore_errors=True)
    return {}


def phase() -> None:
    name = os.environ["RS_PHASE"]
    drill = _drill(create=name == "a")
    try:
        result = {"a": _phase_a, "b": _phase_b, "c": _phase_c, "d": _phase_d,
                  "cleanup": _cleanup}[name](drill)
    finally:
        drill.producer.close(timeout=10)
    print("RESULT " + json.dumps(result), flush=True)


# ------------------------------------------------------------------------- host side

def _docker(*args, check=True, timeout=180, **kwargs):
    return subprocess.run(["docker", *args], check=check, timeout=timeout, text=True,
                          capture_output=True, **kwargs)


def _exec_args(env: dict) -> list:
    pairs = [x for k, v in env.items() for x in ("-e", f"{k}={v}")]
    code = "from tests.integration.test_recovery_restarts_pg import phase; phase()"
    return ["docker", "exec", *pairs, APP, "sh", "-c",
            f"cd /workspace && PYTHONPATH={PYTHONPATH} python3 -c '{code}'"]


def _run_phase(env: dict, name: str, log, timeout=900) -> dict:
    done = subprocess.run(_exec_args({**env, "RS_PHASE": name}), text=True, timeout=timeout,
                          capture_output=True)
    log.write(f"--- phase {name} exit {done.returncode}\n{done.stdout[-4000:]}\n"
              f"{done.stderr[-6000:]}\n")
    log.flush()
    assert done.returncode == 0, f"phase {name} failed: {done.stderr[-3000:]}"
    (line,) = [x for x in done.stdout.splitlines() if x.startswith("RESULT ")]
    return json.loads(line[len("RESULT "):])


def _wait(predicate, what: str, timeout=180) -> float:
    start = time.time()
    while not predicate():
        assert time.time() - start < timeout, f"timed out waiting for {what}"
        time.sleep(1)
    return time.time() - start


def _kafka_healthy() -> bool:
    out = _docker("inspect", "-f", "{{.State.Health.Status}}", KAFKA, check=False)
    return out.stdout.strip() == "healthy"


def _pg_ready(name: str) -> bool:
    return _docker("exec", name, "pg_isready", "-U", "drill", "-d", "drill",
                   check=False).returncode == 0


def _pg_count(name: str, sql: str) -> int:
    out = _docker("exec", name, "psql", "-U", "drill", "-d", "drill", "-tAc", sql, check=False)
    return int(out.stdout.strip()) if out.returncode == 0 and out.stdout.strip() else -1


@pytest.mark.integration
def test_broker_and_postgres_restarts_preserve_the_cut_and_converge_to_the_oracle(tmp_path):
    if _docker("inspect", APP, check=False).returncode != 0:
        pytest.fail(f"app container {APP} is not running")
    suffix = uuid.uuid4().hex[:12]
    pg, password = "ts-t12-drill-pg-" + suffix, secrets.token_hex(16)
    env = {"RS_DSN": f"postgresql://drill:{password}@{pg}:5432/drill",
           "RS_TOPIC": "recovery-restart-" + suffix, "RS_INC": "inc-" + suffix,
           "RS_DIR": f"/runtime/tmp/t12-restart-{suffix}",
           "TON_SENTINEL_TEST_KAFKA_BOOTSTRAP": "kafka:9092", "TON_SENTINEL_LOCAL_FIXTURE": "1"}
    log = (tmp_path / "restart-drill.log").open("w")
    timings = {}
    _docker("run", "-d", "--name", pg, "--network", NETWORK, "--memory", "384m",
            "-e", "POSTGRES_USER=drill", "-e", f"POSTGRES_PASSWORD={password}",
            "-e", "POSTGRES_DB=drill", PG_IMAGE, "-c", "fsync=on", "-c", "synchronous_commit=on")
    try:
        _wait(lambda: _pg_ready(pg), "disposable PostgreSQL")
        before = _run_phase(env, "a", log)
        # Restart the real broker and the disposable PostgreSQL between cut and resume.
        _docker("restart", KAFKA, timeout=300)
        timings["kafka_restart_to_healthy_s"] = round(_wait(_kafka_healthy, "Kafka healthy", 300), 1)
        _docker("restart", pg, timeout=120)
        timings["pg_restart_to_ready_s"] = round(_wait(lambda: _pg_ready(pg), "PG ready"), 1)
        resumed = _run_phase({**env, "RS_BEFORE": json.dumps(before)}, "b", log)
        env["RS_START"] = resumed["start"]
        # Restart PostgreSQL while the resumed consumer is running batch by batch.
        sql = "SELECT count(*) FROM stream_batches WHERE pipeline_epoch='pe-restart'"
        consumer = subprocess.Popen(_exec_args({**env, "RS_PHASE": "c"}), text=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        _wait(lambda: _pg_count(pg, sql) > resumed["batches"] or consumer.poll() is not None,
              "a committed resumed batch", 300)
        at_restart = _pg_count(pg, sql)
        _docker("restart", pg, timeout=120)
        output, _ = consumer.communicate(timeout=600)
        log.write(f"--- phase c exit {consumer.returncode}\n{output[-6000:]}\n")
        timings["pg_mid_run_restart_ready_s"] = round(_wait(lambda: _pg_ready(pg), "PG ready"), 1)
        assert consumer.returncode != 0, "the consumer finished before the PG restart hit it"
        final = _run_phase(env, "d", log)
        assert final["batches"] >= 1 and final["records_above_e"] == 8
        summary = {"before": before, "at_restart_batches": at_restart,
                   "consumer_exit_during_pg_restart": consumer.returncode,
                   "final": final, "timings": timings}
        log.write("SUMMARY " + json.dumps(summary) + "\n")
        print("SUMMARY " + json.dumps(summary))
    finally:
        try:
            _run_phase(env, "cleanup", log, timeout=120)
        except Exception as exc:  # cleanup must not hide the drill result
            log.write(f"cleanup failed: {exc}\n")
        _docker("rm", "-f", pg, check=False)
        log.close()
        evidence = os.environ.get("TON_SENTINEL_DRILL_LOG")
        if evidence:
            shutil.copy(tmp_path / "restart-drill.log", evidence)
