"""Tasks 12.1/12.2: paused/fenced recovery cut and validated resume against real PostgreSQL and Kafka (opt-in via env)."""

import base64
import json
import os
import uuid

import pytest

from ton_sentinel.contracts import SourceEpoch
from ton_sentinel.db.fencing import LeaseRejected, acquire_transition
from ton_sentinel.db.ledger import IngestionPaused, accept_corroborated_range, migrate
from ton_sentinel.outbox import configure_delivery, publish_pending
from ton_sentinel.recovery import (KafkaOffsetReader, RecoveryCutRejected,
                                   capture_recovery_cut, claim_recovery_point,
                                   load_recovery_cut, manifest_digest, pause_for_recovery,
                                   release_recovery_pause)
from ton_sentinel.source.scan import ScanPolicy, corroborate_range

EPOCH = SourceEpoch("testnet", "synthetic:source", "synthetic:filter",
                    "synthetic:build", "synthetic:policy")


def synthetic_action(index, action_end):
    return {
        "schema_version": 1, "kind": "synthetic_swap", "synthetic": True,
        "network": "testnet",
        "trace_id": base64.b64encode(bytes([index]) * 32).decode("ascii"),
        "action_id": base64.b64encode(bytes([index + 1]) * 32).decode("ascii"),
        "protocol": "STON.fi", "protocol_version": "synthetic-v1",
        "action_end": action_end, "status": "success", "eligible": True,
        "route_proven": True, "route": ["TON", "jetton:A"],
        "input": {"asset": "TON", "raw_amount": "100"},
        "output": {"asset": "jetton:A", "raw_amount": "25"},
    }


class Pages:
    def __init__(self, values):
        self.values = values

    def fetch_page(self, source_epoch, start, end, offset, limit):
        return self.values[offset:offset + limit]


def corroborated(db, start, end, indexes):
    policy = ScanPolicy(start=start, end=end, page_size=4, max_pages=8, max_actions=8,
                        max_bytes=100_000, max_seconds=30, max_requests=10)
    actions = [synthetic_action(i, start + 1) for i in indexes]
    group = corroborate_range(db, Pages(actions), EPOCH, policy)
    db.commit()
    return group


class FailingProducer:
    config = {"acks": -1}

    def send(self, *_args, **_kwargs):
        class Failed:
            def get(self, timeout):
                raise TimeoutError("simulated lost Kafka acknowledgement")
        return Failed()


@pytest.fixture
def world():
    """Own schema + 2-partition topic: one acknowledged, one ambiguous, one pending intent."""
    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    bootstrap = os.environ["TON_SENTINEL_KAFKA_BOOTSTRAP"]
    pytest.importorskip("kafka")
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from psycopg import connect

    suffix = uuid.uuid4().hex
    topic, schema = "recovery-" + suffix, "rc_" + suffix
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="recovery-" + suffix)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
    db = connect(dsn, options=options)
    try:
        migrate(db)
        configure_delivery(db, topic=topic, topic_incarnation="inc-" + suffix)
        group = corroborated(db, 10, 20, [11, 21, 31])
        writer = acquire_transition(db, "ingest-" + suffix)
        assert accept_corroborated_range(db, writer, group)
        assert len(publish_pending(db, producer, writer, limit=1)) == 1
        producer.flush(timeout=10)
        with pytest.raises(Exception):
            publish_pending(db, FailingProducer(), writer, limit=1, max_retries=0)
        db.rollback()
        states = dict(db.execute("SELECT publication_state, count(*) FROM outbox "
                                 "GROUP BY 1").fetchall())
        db.commit()
        assert states == {"acknowledged": 1, "ambiguous": 1, "pending": 1}
        yield {"dsn": dsn, "options": options, "db": db, "topic": topic, "admin": admin,
               "producer": producer, "bootstrap": bootstrap, "writer": writer,
               "reader": KafkaOffsetReader(bootstrap), "suffix": suffix,
               "incarnation": "inc-" + suffix, "connect": connect}
    finally:
        producer.close()
        db.close()
        try:
            admin.delete_topics([topic])
        except Exception:
            pass
        admin.close()
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


def one(db, sql, args=()):
    row = db.execute(sql, args).fetchone()
    db.commit()
    return row


def cut_count(world):
    with world["connect"](world["dsn"], options=world["options"]) as fresh:
        return fresh.execute("SELECT count(*) FROM recovery_cuts").fetchone()[0]


def kafka_records(world, end_offsets):
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(bootstrap_servers=world["bootstrap"], enable_auto_commit=False,
                             consumer_timeout_ms=5000)
    try:
        parts = [TopicPartition(world["topic"], int(p)) for p in end_offsets]
        consumer.assign(parts)
        consumer.seek_to_beginning(*parts)
        records = []
        for record in consumer:
            if record.offset < end_offsets[str(record.partition)]:
                records.append(record)
            if all(consumer.position(p) >= end_offsets[str(p.partition)] for p in parts):
                break
        return records
    finally:
        consumer.close()


@pytest.mark.integration
def test_pause_fences_writers_and_release_resumes_ingestion(world):
    db, connect = world["db"], world["connect"]
    pending_group = corroborated(db, 20, 30, [41])
    pause = pause_for_recovery(db, "recovery-" + world["suffix"])
    assert pause.lease.fence > world["writer"].fence
    assert one(db, "SELECT recovery_epoch, lease_fence, released_at FROM recovery_pauses "
               "WHERE pause_id=%s", (pause.pause_id,)) == (pause.recovery_epoch,
                                                           pause.lease.fence, None)
    with connect(world["dsn"], options=world["options"]) as other:
        with pytest.raises(LeaseRejected):
            publish_pending(other, world["producer"], world["writer"])
        other.rollback()
        fresh = acquire_transition(other, "late-writer")
        with pytest.raises(IngestionPaused, match="recovery cut"):
            accept_corroborated_range(other, fresh, pending_group)
        other.rollback()
        with pytest.raises(Exception, match="paused for a recovery cut"):
            publish_pending(other, world["producer"], fresh)
        other.rollback()
        for statement in ("UPDATE outbox SET publication_attempts=publication_attempts",
                          "UPDATE ingest_state SET cursor=cursor+1",
                          "DELETE FROM scan_membership"):
            with pytest.raises(Exception, match="paused for a recovery cut"):
                other.execute(statement)
            other.rollback()
    assert one(db, "SELECT count(*) FROM canonical_actions")[0] == 3
    with pytest.raises(LeaseRejected):
        release_recovery_pause(db, pause.lease, pause.pause_id)
    db.rollback()
    operator = acquire_transition(db, "recovery-" + world["suffix"])
    release_recovery_pause(db, operator, pause.pause_id)
    assert one(db, "SELECT released_at IS NOT NULL, release_fence FROM recovery_pauses "
               "WHERE pause_id=%s", (pause.pause_id,)) == (True, operator.fence)
    resumed = acquire_transition(db, "ingest-again")
    assert accept_corroborated_range(db, resumed, pending_group)
    assert one(db, "SELECT count(*) FROM canonical_actions")[0] == 4
    assert one(db, "SELECT count(*) FROM outbox WHERE publication_state='pending'")[0] == 2
    again = pause_for_recovery(db, "recovery-" + world["suffix"])
    assert again.recovery_epoch == pause.recovery_epoch + 1 and not again.resumed


@pytest.mark.integration
def test_cut_is_consistent_across_snapshot_outbox_and_offsets(world):
    db = world["db"]
    pause = pause_for_recovery(db, "recovery-" + world["suffix"])
    cut = capture_recovery_cut(db, pause.lease, pause.pause_id, world["reader"])
    m = cut.manifest
    generation = one(db, "SELECT generation FROM canonical_generation")[0]
    canon = db.execute("SELECT event_id, accepted_generation, semantic_digest "
                       "FROM canonical_actions ORDER BY event_id COLLATE \"C\"").fetchall()
    db.commit()
    assert m["snapshot"]["canonical_generation"] == generation == 3
    assert [(e, g, d) for e, _n, g, d in m["snapshot"]["contributions"]] == [tuple(r) for r in canon]
    assert m["snapshot"]["tables"]["canonical_actions"]["rows"] == 3
    rows = db.execute("SELECT event_id, accepted_generation, publication_state FROM outbox "
                      "ORDER BY event_id COLLATE \"C\"").fetchall()
    db.commit()
    assert [tuple(x[:3]) for x in m["outbox"]["members"]] == [tuple(r) for r in rows]
    assert m["outbox"]["counts"] == {"acknowledged": 1, "ambiguous": 1, "pending": 1}
    described = world["admin"].describe_topics([world["topic"]])[0]
    cluster = world["admin"].describe_cluster()["cluster_id"]
    k = m["kafka"]
    assert (k["topic"], k["topic_incarnation"], k["replay_epoch"], k["cluster_id"],
            k["topic_id"]) == (world["topic"], world["incarnation"], 0, cluster,
                               described["topic_id"])
    assert sorted(k["end_offsets"]) == ["0", "1"]
    below = kafka_records(world, k["end_offsets"])
    assert sum(k["end_offsets"].values()) == len(below) == 1
    acknowledged = [x[0] for x in m["outbox"]["members"] if x[3] == "acknowledged"]
    assert [r.key.decode() for r in below] == acknowledged
    row = one(db, "SELECT lease_owner, lease_fence, recovery_epoch, canonical_generation, "
              "end_offsets, manifest_digest FROM recovery_cuts WHERE cut_id=%s", (cut.cut_id,))
    assert row == (pause.lease.owner, pause.lease.fence, pause.recovery_epoch, 3,
                   k["end_offsets"], manifest_digest(m))
    assert m["fence"] == {"owner": pause.lease.owner, "fence": pause.lease.fence}


@pytest.mark.integration
def test_concurrent_writer_is_fenced_out_during_capture(world):
    import psycopg

    db, connect = world["db"], world["connect"]
    pause = pause_for_recovery(db, "recovery-" + world["suffix"])
    other = connect(world["dsn"], options=world["options"])
    observed = []
    calls = 0

    def reader(topic):
        nonlocal calls
        calls += 1
        if calls == 2:
            other.execute("SET lock_timeout = '1s'")
            other.commit()
            try:
                acquire_transition(other, "concurrent-writer")
            except psycopg.errors.LockNotAvailable:
                observed.append("slot blocked")
            other.rollback()
            try:
                publish_pending(other, world["producer"], world["writer"])
            except psycopg.errors.LockNotAvailable:
                observed.append("stale writer blocked")
            other.rollback()
            try:
                other.execute("UPDATE outbox SET publication_attempts=publication_attempts+1")
            except psycopg.Error as exc:
                if "paused for a recovery cut" in str(exc):
                    observed.append("write refused")
            other.rollback()
        return world["reader"](topic)

    try:
        cut = capture_recovery_cut(db, pause.lease, pause.pause_id, reader)
        assert observed == ["slot blocked", "stale writer blocked", "write refused"]
        assert one(db, "SELECT owner, fence FROM lease_state")[0:2] == (
            pause.lease.owner, pause.lease.fence)
        assert cut.manifest["fence"]["fence"] == pause.lease.fence
        assert one(db, "SELECT sum(publication_attempts) FROM outbox")[0] == 2
        with pytest.raises(LeaseRejected):
            publish_pending(other, world["producer"], world["writer"])
        other.rollback()
        late = acquire_transition(other, "concurrent-writer")
        assert late.fence == pause.lease.fence + 1
        with pytest.raises(Exception, match="paused for a recovery cut"):
            publish_pending(other, world["producer"], late)
        other.rollback()
    finally:
        other.close()


@pytest.mark.integration
def test_cut_is_durable_reproducible_and_refused_when_stale_or_mismatched(world):
    import psycopg

    db, connect = world["db"], world["connect"]
    pause = pause_for_recovery(db, "recovery-" + world["suffix"])
    cut = capture_recovery_cut(db, pause.lease, pause.pause_id, world["reader"])
    db.close()
    with connect(world["dsn"], options=world["options"]) as reopened:
        loaded = load_recovery_cut(reopened, cut.cut_id)
        assert loaded == cut
        first = claim_recovery_point(reopened, cut.cut_id, world["reader"])
        second = claim_recovery_point(reopened, cut.cut_id, world["reader"])
        assert first == second
        assert (first.digest, first.canonical_generation, first.end_offsets) == (
            cut.digest, 3, {int(p): e for p, e in cut.manifest["kafka"]["end_offsets"].items()})
        lease = acquire_transition(reopened, "recovery-" + world["suffix"], ttl_seconds=300)
        with pytest.raises(RecoveryCutRejected, match="already"):
            capture_recovery_cut(reopened, lease, pause.pause_id, world["reader"])
        reopened.rollback()
        for statement in ("UPDATE recovery_cuts SET canonical_generation=0",
                          "DELETE FROM recovery_cuts"):
            with pytest.raises(psycopg.Error, match="immutable"):
                reopened.execute(statement)
            reopened.rollback()
        base = ("INSERT INTO recovery_cuts(cut_id,pause_id,recovery_epoch,lease_owner,"
                "lease_fence,canonical_generation,topic,topic_incarnation,replay_epoch,"
                "kafka_cluster_id,kafka_topic_id,end_offsets,outbox_members,manifest,"
                "manifest_digest) VALUES (gen_random_uuid(),%s,%s,%s,%s,%s,%s,%s,0,'c','t',"
                "%s::jsonb,%s::jsonb,%s::jsonb,%s)")
        members = reopened.execute(
            "SELECT coalesce(jsonb_agg(jsonb_build_array(event_id,accepted_generation,"
            "publication_state) ORDER BY event_id COLLATE \"C\"),'[]')::text FROM outbox"
        ).fetchone()[0]
        reopened.rollback()
        for offsets, outbox_members, words in [
                ("{}", members, "offsets incomplete"),
                ('{"1": 0}', members, "offsets incomplete"),
                ('{"0": -1}', members, "offsets incomplete"),
                ('{"0": 0}', "[]", "outbox membership mismatch")]:
            with pytest.raises(psycopg.Error, match=words):
                reopened.execute(base, (pause.pause_id, pause.recovery_epoch, lease.owner,
                                        lease.fence, 3, world["topic"], world["incarnation"],
                                        offsets, outbox_members,
                                        '{"snapshot":{},"outbox":{},"kafka":{},"fence":{}}',
                                        "0" * 64))
            reopened.rollback()
        from kafka.admin import NewTopic
        world["admin"].delete_topics([world["topic"]])
        import time
        for _ in range(30):
            try:
                world["admin"].create_topics([NewTopic(world["topic"], num_partitions=2,
                                                       replication_factor=1)])
                break
            except Exception:
                time.sleep(1)
        with pytest.raises(RecoveryCutRejected, match="mismatched: .*topic id"):
            claim_recovery_point(reopened, cut.cut_id, world["reader"])
        release_recovery_pause(reopened, lease, pause.pause_id)
        with pytest.raises(RecoveryCutRejected, match="stale: .*released"):
            claim_recovery_point(reopened, cut.cut_id, world["reader"])
        assert load_recovery_cut(reopened, cut.cut_id) == cut


@pytest.mark.integration
def test_failed_capture_leaves_no_partial_cut(world):
    from kafka import KafkaProducer

    db = world["db"]
    pause = pause_for_recovery(db, "recovery-" + world["suffix"])

    def broken(topic):
        raise ConnectionError("simulated Kafka metadata failure")

    with pytest.raises(ConnectionError):
        capture_recovery_cut(db, pause.lease, pause.pause_id, broken)
    assert cut_count(world) == 0

    calls = 0

    def second_fails(topic):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise TimeoutError("simulated offset read timeout after snapshot")
        return world["reader"](topic)

    with pytest.raises(TimeoutError):
        capture_recovery_cut(db, pause.lease, pause.pause_id, second_fails)
    assert cut_count(world) == 0

    stray = KafkaProducer(bootstrap_servers=world["bootstrap"], acks="all")
    moved = 0

    def moving(topic):
        nonlocal moved
        moved += 1
        value = world["reader"](topic)
        if moved == 1:
            stray.send(topic, key=b"stray", value=b"late", partition=1).get(timeout=10)
        return value

    try:
        with pytest.raises(RecoveryCutRejected, match="offsets moved"):
            capture_recovery_cut(db, pause.lease, pause.pause_id, moving)
    finally:
        stray.close()
    assert cut_count(world) == 0

    with world["connect"](world["dsn"], options=world["options"]) as other:
        acquire_transition(other, "takeover")
    with pytest.raises(LeaseRejected):
        capture_recovery_cut(db, pause.lease, pause.pause_id, world["reader"])
    db.rollback()
    assert cut_count(world) == 0
    assert one(db, "SELECT released_at FROM recovery_pauses WHERE pause_id=%s",
               (pause.pause_id,)) == (None,)

    restarted = pause_for_recovery(db, "recovery-" + world["suffix"])
    assert (restarted.pause_id, restarted.recovery_epoch, restarted.resumed) == (
        pause.pause_id, pause.recovery_epoch, True)
    cut = capture_recovery_cut(db, restarted.lease, restarted.pause_id, world["reader"])
    assert cut_count(world) == 1
    assert cut.manifest["fence"]["fence"] == restarted.lease.fence
    offsets = cut.manifest["kafka"]["end_offsets"]
    assert sum(offsets.values()) == 2 and offsets["1"] >= 1



def resume_state(world):
    """Everything a refused resume must leave untouched."""
    with world["connect"](world["dsn"], options=world["options"]) as fresh:
        return (fresh.execute("SELECT count(*) FROM recovery_resumptions").fetchone()[0],
                fresh.execute("SELECT pause_id FROM recovery_pauses "
                              "WHERE released_at IS NULL").fetchall(),
                fresh.execute("SELECT event_id, publication_state, publication_attempts "
                              "FROM outbox ORDER BY event_id COLLATE \"C\"").fetchall(),
                fresh.execute("SELECT count(*) FROM canonical_actions").fetchone()[0])


def wire_value(envelope):
    """Exactly the bytes publish_pending sends for a persisted delivery envelope."""
    import json

    return json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")


def all_records(world):
    from ton_sentinel.recovery import KafkaRecordReader

    sample = world["reader"](world["topic"])["partitions"]
    return KafkaRecordReader(world["bootstrap"])(
        world["topic"], {p: 0 for p in sample}, {p: o["end"] for p, o in sample.items()})


def paused_cut(world):
    db = world["db"]
    pause = pause_for_recovery(db, "recovery-" + world["suffix"])
    cut = capture_recovery_cut(db, pause.lease, pause.pause_id, world["reader"])
    return pause, cut, claim_recovery_point(db, cut.cut_id, world["reader"])


@pytest.mark.integration
def test_resume_refused_on_stale_incomplete_or_mismatched_cut(world):
    import dataclasses

    from kafka import KafkaProducer

    from ton_sentinel.recovery import KafkaRecordReader, resume_from_recovery_cut

    db, records = world["db"], KafkaRecordReader(world["bootstrap"])
    db.execute("INSERT INTO stream_batches(environment_id,query_id,pipeline_epoch,batch_id,"
               "topic,topic_incarnation,replay_epoch,offset_metadata,record_digest,"
               "record_count) VALUES ('env','query','pe-used',0,%s,%s,0,'[]'::jsonb,%s,0)",
               (world["topic"], world["incarnation"], "0" * 64))
    db.commit()
    pause, cut, point = paused_cut(world)

    def refused(lease, presented, words, epoch="pe-new"):
        before = resume_state(world)
        with pytest.raises(RecoveryCutRejected, match=words) as caught:
            resume_from_recovery_cut(db, lease, presented, world["reader"], records,
                                     pipeline_epoch=epoch)
        db.rollback()
        assert resume_state(world) == before
        return caught.value.reasons

    refused(pause.lease, point, "stale: .*handoff")
    handoff = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    refused(handoff, dataclasses.replace(point, end_offsets={0: 0, 1: 0}),
            "mismatched: presented recovery point")
    refused(handoff, dataclasses.replace(point, topic_incarnation="other"),
            "mismatched: presented recovery point")
    refused(handoff, point, "stale: .*pipeline epoch", epoch="pe-used")
    refused(handoff, dataclasses.replace(point, cut_id=str(uuid.uuid4())), "incomplete: unknown")
    stray = KafkaProducer(bootstrap_servers=world["bootstrap"], acks="all")
    try:
        stray.send(world["topic"], key=b"stray", value=b"late", partition=0).get(timeout=10)
    finally:
        stray.close()
    refused(handoff, point, "mismatched: post-cut record")
    release_recovery_pause(db, handoff, pause.pause_id)
    again = pause_for_recovery(db, "recovery-" + world["suffix"])
    later = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    reasons = refused(later, point, "stale: .*released")
    assert any("newer recovery epoch" in r for r in reasons)
    cut2 = capture_recovery_cut(db, later, again.pause_id, world["reader"])
    point2 = claim_recovery_point(db, cut2.cut_id, world["reader"])
    db.execute("ALTER TABLE recovery_cuts DISABLE TRIGGER recovery_cuts_immutable")
    db.execute("UPDATE recovery_cuts SET manifest = manifest #- '{snapshot,tables,outbox}' "
               "WHERE cut_id=%s", (cut2.cut_id,))
    db.execute("ALTER TABLE recovery_cuts ENABLE TRIGGER recovery_cuts_immutable")
    db.commit()
    final = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    refused(final, point2, "incomplete: manifest digest")
    assert resume_state(world)[0] == 0


@pytest.mark.integration
def test_resume_on_exact_fenced_match_deduplicates_a_late_ambiguous_send(world):
    from kafka import KafkaProducer

    from ton_sentinel.recovery import (KafkaRecordReader, resume_from_recovery_cut,
                                       resumed_records, spark_starting_offsets)

    db = world["db"]
    event, envelope = one(db, "SELECT event_id, delivery_envelope FROM outbox "
                              "WHERE publication_state='ambiguous'")
    pending = one(db, "SELECT event_id FROM outbox WHERE publication_state='pending'")[0]
    pause, cut, point = paused_cut(world)
    cut_end = cut.manifest["kafka"]["end_offsets"]
    old = KafkaProducer(bootstrap_servers=world["bootstrap"], acks="all")
    try:
        first = old.send(world["topic"], key=event.encode(), value=wire_value(envelope)).get(10)
        second = old.send(world["topic"], key=event.encode(), value=wire_value(envelope)).get(10)
    finally:
        old.close()
    assert first.offset >= cut_end[str(first.partition)]
    handoff = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    resumed = resume_from_recovery_cut(db, handoff, point, world["reader"],
                                       KafkaRecordReader(world["bootstrap"]),
                                       pipeline_epoch="pe-" + world["suffix"])
    assert resumed.resolutions == {event: (first.partition, first.offset)}
    assert resumed.duplicates == ((second.partition, second.offset),)
    assert resumed.start_offsets == {int(p): e for p, e in cut_end.items()}
    assert json.loads(spark_starting_offsets(resumed)) == {world["topic"]: cut_end}
    row = one(db, "SELECT lease_fence, pipeline_epoch, start_offsets, manifest_digest, "
                  "resolutions FROM recovery_resumptions WHERE cut_id=%s", (cut.cut_id,))
    assert row == (handoff.fence, "pe-" + world["suffix"], cut_end, cut.digest,
                   [[event, first.partition, first.offset]])
    assert one(db, "SELECT released_at IS NOT NULL, release_fence FROM recovery_pauses "
                   "WHERE pause_id=%s", (pause.pause_id,)) == (True, handoff.fence)
    assert one(db, "SELECT publication_state, publication_attempts FROM outbox "
                   "WHERE event_id=%s", (event,)) == ("acknowledged", 1)
    assert publish_pending(db, world["producer"], handoff, limit=10) == (pending,)
    world["producer"].flush(timeout=10)
    assert publish_pending(db, world["producer"], handoff, limit=10) == ()
    log = all_records(world)
    above = [r for r in log if r["offset"] >= cut_end[str(r["partition"])]]
    assert sorted(r["key"].decode() for r in above) == sorted([event, event, pending])
    applied = resumed_records(resumed, log)
    assert sorted(r["key"].decode() for r in applied) == sorted([event, pending])
    assert all(r["offset"] >= cut_end[str(r["partition"])] for r in applied)
    assert one(db, "SELECT count(*), count(*) FILTER (WHERE publication_state='acknowledged') "
                   "FROM outbox")[0:2] == (3, 3)
    assert one(db, "SELECT count(*) FROM canonical_actions")[0] == 3
    other = acquire_transition(db, "resumer-again", ttl_seconds=300)
    with pytest.raises(RecoveryCutRejected, match="stale: .*released"):
        resume_from_recovery_cut(db, other, point, world["reader"],
                                 KafkaRecordReader(world["bootstrap"]), pipeline_epoch="pe-x")
    db.rollback()
    assert one(db, "SELECT count(*) FROM recovery_resumptions")[0] == 1
    group = corroborated(db, 20, 30, [41])
    assert accept_corroborated_range(db, acquire_transition(db, "ingest-after"), group)
    assert one(db, "SELECT count(*) FROM canonical_actions")[0] == 4


@pytest.mark.integration
def test_resume_redelivers_an_ambiguous_send_that_never_landed_exactly_once(world):
    import psycopg

    from ton_sentinel.recovery import (KafkaRecordReader, resume_from_recovery_cut,
                                       resumed_records)

    db = world["db"]
    event = one(db, "SELECT event_id FROM outbox WHERE publication_state='ambiguous'")[0]
    pending = one(db, "SELECT event_id FROM outbox WHERE publication_state='pending'")[0]
    _pause, cut, point = paused_cut(world)
    cut_end = cut.manifest["kafka"]["end_offsets"]
    handoff = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    resumed = resume_from_recovery_cut(db, handoff, point, world["reader"],
                                       KafkaRecordReader(world["bootstrap"]),
                                       pipeline_epoch="pe-" + world["suffix"])
    assert resumed.resolutions == {event: None} and resumed.duplicates == ()
    assert one(db, "SELECT publication_state FROM outbox WHERE event_id=%s",
               (event,))[0] == "ambiguous"
    assert sorted(publish_pending(db, world["producer"], handoff, limit=10)) == sorted(
        [event, pending])
    world["producer"].flush(timeout=10)
    assert publish_pending(db, world["producer"], handoff, limit=10) == ()
    log = all_records(world)
    above = [r["key"].decode() for r in log if r["offset"] >= cut_end[str(r["partition"])]]
    assert sorted(above) == sorted([event, pending])
    assert sorted(r["key"].decode() for r in resumed_records(resumed, log)) == sorted(
        [event, pending])
    for statement in ("UPDATE recovery_resumptions SET pipeline_epoch='x'",
                      "DELETE FROM recovery_resumptions"):
        with pytest.raises(psycopg.Error, match="immutable"):
            db.execute(statement)
        db.rollback()
    with pytest.raises(psycopg.Error, match="stale cut|does not match its cut"):
        db.execute("INSERT INTO recovery_resumptions(cut_id,pause_id,recovery_epoch,"
                   "lease_owner,lease_fence,pipeline_epoch,start_offsets,manifest_digest,"
                   "resolutions) SELECT cut_id,pause_id,recovery_epoch,%s,%s,'pe-other',"
                   "end_offsets,manifest_digest,'[]'::jsonb FROM recovery_cuts",
                   (handoff.owner, handoff.fence))
    db.rollback()
    assert one(db, "SELECT count(*) FROM recovery_resumptions")[0] == 1


@pytest.mark.integration
def test_failure_injection_around_resume_leaves_an_explicit_blocked_state(world):
    import time

    from kafka import KafkaProducer

    from ton_sentinel.recovery import KafkaRecordReader, resume_from_recovery_cut

    db = world["db"]
    event, envelope = one(db, "SELECT event_id, delivery_envelope FROM outbox "
                              "WHERE publication_state='ambiguous'")
    pause, cut, point = paused_cut(world)
    records = KafkaRecordReader(world["bootstrap"])
    blocked = resume_state(world)
    assert blocked[0] == 0 and blocked[1] == [(pause.pause_id,)]

    def attempt(lease, kafka, reader, error, words=None):
        with pytest.raises(error, match=words):
            resume_from_recovery_cut(db, lease, point, kafka, reader,
                                     pipeline_epoch="pe-" + world["suffix"])
        db.rollback()
        assert resume_state(world) == blocked

    handoff = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)

    def broken(topic):
        raise ConnectionError("simulated Kafka metadata failure during resume")

    attempt(handoff, broken, records, ConnectionError)

    def unreadable(topic, start, end):
        raise TimeoutError("simulated record read timeout during resume")

    attempt(handoff, world["reader"], unreadable, TimeoutError)
    old = KafkaProducer(bootstrap_servers=world["bootstrap"], acks="all")

    def landing(topic, start, end):
        value = records(topic, start, end)
        old.send(topic, key=event.encode(), value=wire_value(envelope)).get(timeout=10)
        return value

    try:
        attempt(handoff, world["reader"], landing, RecoveryCutRejected, "offsets moved")
    finally:
        old.close()
    with world["connect"](world["dsn"], options=world["options"]) as other:
        acquire_transition(other, "takeover")
    attempt(handoff, world["reader"], records, LeaseRejected)
    short = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=1)

    def slow(topic, start, end):
        value = records(topic, start, end)
        time.sleep(1.5)
        return value

    import psycopg
    attempt(short, world["reader"], slow, (LeaseRejected, psycopg.Error), "expired")
    final = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    resumed = resume_from_recovery_cut(db, final, point, world["reader"], records,
                                       pipeline_epoch="pe-" + world["suffix"])
    landed = resumed.resolutions[event]
    assert landed is not None and landed[1] >= cut.manifest["kafka"]["end_offsets"][str(landed[0])]
    assert resume_state(world)[0:2] == (1, [])



@pytest.mark.integration
def test_expired_offsets_new_cut(world):
    from kafka import KafkaProducer, TopicPartition

    from ton_sentinel.recovery import KafkaRecordReader, resume_from_recovery_cut

    db, records = world["db"], KafkaRecordReader(world["bootstrap"])
    event, envelope = one(db, "SELECT event_id, delivery_envelope FROM outbox "
                              "WHERE publication_state='ambiguous'")
    pending = one(db, "SELECT event_id FROM outbox WHERE publication_state='pending'")[0]
    pause, cut, point = paused_cut(world)
    cut_end = cut.manifest["kafka"]["end_offsets"]
    old = KafkaProducer(bootstrap_servers=world["bootstrap"], acks="all")
    try:
        landed = old.send(world["topic"], key=event.encode(), value=wire_value(envelope)).get(10)
    finally:
        old.close()
    part = str(landed.partition)
    assert landed.offset >= cut_end[part]
    handoff = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    before = resume_state(world)
    with pytest.raises(RecoveryCutRejected, match="exceeds max_records"):
        resume_from_recovery_cut(db, handoff, point, world["reader"], records,
                                 pipeline_epoch="pe-" + world["suffix"], max_records=1)
    db.rollback()
    assert resume_state(world) == before
    world["admin"].delete_records({TopicPartition(world["topic"], landed.partition):
                                   landed.offset + 1})
    expired = f"partition {part} offset {cut_end[part]} outside the retained log"
    with pytest.raises(RecoveryCutRejected, match=expired):
        claim_recovery_point(db, cut.cut_id, world["reader"])
    db.rollback()
    with pytest.raises(RecoveryCutRejected, match=expired):
        resume_from_recovery_cut(db, handoff, point, world["reader"], records,
                                 pipeline_epoch="pe-" + world["suffix"])
    db.rollback()
    assert resume_state(world) == before
    operator = acquire_transition(db, "recovery-" + world["suffix"], ttl_seconds=300)
    release_recovery_pause(db, operator, pause.pause_id)
    again = pause_for_recovery(db, "recovery-" + world["suffix"])
    assert again.recovery_epoch == pause.recovery_epoch + 1 and not again.resumed
    cut2 = capture_recovery_cut(db, again.lease, again.pause_id, world["reader"])
    end2, start2 = (cut2.manifest["kafka"]["end_offsets"],
                    cut2.manifest["kafka"]["log_start_offsets"])
    assert end2[part] == start2[part] == landed.offset + 1
    point2 = claim_recovery_point(db, cut2.cut_id, world["reader"])
    handoff2 = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    resumed = resume_from_recovery_cut(db, handoff2, point2, world["reader"], records,
                                       pipeline_epoch="pe2-" + world["suffix"])
    assert resumed.resolutions == {event: None} and resumed.duplicates == ()
    assert sorted(publish_pending(db, world["producer"], handoff2, limit=10)) == sorted(
        [event, pending])
    world["producer"].flush(timeout=10)
    assert publish_pending(db, world["producer"], handoff2, limit=10) == ()
    sample = world["reader"](world["topic"])["partitions"]
    retained = records(world["topic"], {p: o["start"] for p, o in sample.items()},
                       {p: o["end"] for p, o in sample.items()})
    above = [r["key"].decode() for r in retained if r["offset"] >= end2[str(r["partition"])]]
    assert sorted(above) == sorted([event, pending])
    with pytest.raises(RecoveryCutRejected, match="stale: .*released"):
        claim_recovery_point(db, cut.cut_id, world["reader"])
    db.rollback()
    assert one(db, "SELECT count(*) FROM recovery_resumptions")[0] == 1


@pytest.fixture
def revision_world(world):
    """The world schema plus seeded X=A and an authorized candidate revising X to B."""
    from tests.integration.test_revisions import A, B, X, World, mono, revise

    db = world["db"]
    revisions = World(db)
    revisions.seed({X: A})
    db.commit()
    lease = acquire_transition(db, "publisher-" + world["suffix"])
    publish_pending(db, world["producer"], lease, limit=10)
    world["producer"].flush(timeout=10)
    evidence = revisions.candidate({X: B}, {1: {2}})
    auth = revisions.authorization(mono(revise(X, B, src=2, prev=1)))
    db.commit()
    return {**world, "evidence": evidence, "auth": auth, "x": X, "revisions": revisions}


@pytest.mark.integration
def test_revision_during_cut_deferred(revision_world):
    from ton_sentinel.db.revisions import decide_revision
    from ton_sentinel.recovery import KafkaRecordReader, resume_from_recovery_cut
    from tests.integration.test_revisions import eid

    world = revision_world
    db, event = world["db"], eid(world["x"])
    pause, cut, point = paused_cut(world)
    g = cut.manifest["snapshot"]["canonical_generation"]
    current_before = one(db, "SELECT accepted_generation FROM canonical_current "
                             "WHERE event_id=%s", (event,))
    revisor = acquire_transition(db, "revisor-" + world["suffix"], ttl_seconds=300)
    with pytest.raises(Exception, match="paused for a recovery cut"):
        decide_revision(db, revisor, world["evidence"], world["auth"])
    db.rollback()
    assert one(db, "SELECT accepted_generation FROM canonical_current WHERE event_id=%s",
               (event,)) == current_before
    assert claim_recovery_point(db, cut.cut_id, world["reader"]) == point
    handoff = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    resume_from_recovery_cut(db, handoff, point, world["reader"],
                             KafkaRecordReader(world["bootstrap"]),
                             pipeline_epoch="pe-" + world["suffix"])
    changes = decide_revision(db, acquire_transition(db, "revisor-again"), world["evidence"],
                              world["auth"])
    assert changes
    generation = one(db, "SELECT accepted_generation FROM canonical_current "
                         "WHERE event_id=%s", (event,))[0]
    assert generation > g
    dirty = db.execute("SELECT window_start, status FROM dirty_windows WHERE role='target' "
                       "ORDER BY window_start").fetchall()
    db.commit()
    assert {(w, s) for w, s in dirty} == {(0, "pending"), (600, "pending")}
    pause2 = pause_for_recovery(db, "recovery-" + world["suffix"])
    cut2 = capture_recovery_cut(db, pause2.lease, pause2.pause_id, world["reader"])
    mine = [c for c in cut2.manifest["snapshot"]["contributions"] if c[0] == event]
    assert len(mine) == 1 and mine[0][2] == generation
    revisions = one(db, "SELECT count(*) FROM canonical_revisions")[0]
    assert cut2.manifest["snapshot"]["tables"]["canonical_revisions"]["rows"] == revisions >= 1
    release_recovery_pause(db, acquire_transition(db, "operator-" + world["suffix"]),
                           pause2.pause_id)
    from tests.integration.test_revisions import C
    unordered = world["revisions"].candidate({world["x"]: C}, {1: {2}})
    decide_revision(db, acquire_transition(db, "revisor-unordered"), unordered, None)
    quarantined = one(db, "SELECT count(*) FROM revision_quarantine")[0]
    assert quarantined >= 1
    assert one(db, "SELECT eligibility_suppressed FROM canonical_current WHERE event_id=%s",
               (event,)) == (True,)
    pause3 = pause_for_recovery(db, "recovery-" + world["suffix"])
    cut3 = capture_recovery_cut(db, pause3.lease, pause3.pause_id, world["reader"])
    assert [c for c in cut3.manifest["snapshot"]["contributions"] if c[0] == event] == []
    assert cut3.manifest["snapshot"]["tables"]["revision_quarantine"]["rows"] == quarantined


_KILLED_CAPTURE = r"""
import os, signal
from psycopg import connect
from ton_sentinel.recovery import KafkaOffsetReader, capture_recovery_cut, pause_for_recovery

db = connect(os.environ["TON_SENTINEL_TEST_DSN"], options=os.environ["RC_OPTIONS"])
pause = pause_for_recovery(db, os.environ["RC_OWNER"])
print(pause.pause_id, pause.recovery_epoch, flush=True)
reader, calls = KafkaOffsetReader(os.environ["TON_SENTINEL_KAFKA_BOOTSTRAP"]), []

def dying(topic):
    sample = reader(topic)
    calls.append(topic)
    if len(calls) == 2:          # PG snapshot read, manifest not yet durable: real SIGKILL
        os.kill(os.getpid(), signal.SIGKILL)
    return sample

capture_recovery_cut(db, pause.lease, pause.pause_id, dying)
print("UNREACHABLE", flush=True)
"""


@pytest.mark.integration
def test_cut_interrupted_stays_paused(world):
    import signal
    import subprocess
    import sys

    db = world["db"]
    owner = "recovery-" + world["suffix"]
    pending_group = corroborated(db, 20, 30, [41])
    child = subprocess.run([sys.executable, "-c", _KILLED_CAPTURE], capture_output=True,
                           text=True, timeout=120,
                           env={**os.environ, "RC_OPTIONS": world["options"], "RC_OWNER": owner})
    assert child.returncode == -signal.SIGKILL, child.stderr[-2000:]
    assert "UNREACHABLE" not in child.stdout
    pause_id, epoch = (int(v) for v in child.stdout.split()[:2])
    assert cut_count(world) == 0
    assert one(db, "SELECT released_at FROM recovery_pauses WHERE pause_id=%s",
               (pause_id,)) == (None,)
    with pytest.raises(IngestionPaused, match="recovery cut"):
        accept_corroborated_range(db, acquire_transition(db, "late-writer"), pending_group)
    db.rollback()
    assert one(db, "SELECT count(*) FROM canonical_actions")[0] == 3
    restarted = pause_for_recovery(db, owner)
    assert (restarted.pause_id, restarted.recovery_epoch, restarted.resumed) == (
        pause_id, epoch, True)
    sample = world["reader"](world["topic"])["partitions"]
    cut = capture_recovery_cut(db, restarted.lease, restarted.pause_id, world["reader"])
    assert cut_count(world) == 1
    assert cut.manifest["kafka"]["end_offsets"] == {str(p): o["end"] for p, o in sample.items()}
    claimed = claim_recovery_point(db, cut.cut_id, world["reader"])
    assert {str(p): e for p, e in claimed.end_offsets.items()} == \
        cut.manifest["kafka"]["end_offsets"]


@pytest.fixture
def detector_world():
    """Own schema + real topic with the scored window (jetton:A, 24 prior windows)."""
    from kafka import KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    from psycopg import connect

    dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    bootstrap = os.environ["TON_SENTINEL_KAFKA_BOOTSTRAP"]
    suffix = uuid.uuid4().hex
    topic, schema = "recovery-det-" + suffix, "rcd_" + suffix
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="recovery-det-" + suffix)
    admin.create_topics([NewTopic(topic, num_partitions=2, replication_factor=1)])
    with connect(dsn) as root:
        root.execute(f'CREATE SCHEMA "{schema}"')
        root.commit()
    options = f"-c search_path={schema}"
    producer = KafkaProducer(bootstrap_servers=bootstrap, acks="all", retries=3)
    db = connect(dsn, options=options)
    db.schema_name = schema
    try:
        migrate(db)
        configure_delivery(db, topic=topic, topic_incarnation="inc-" + suffix)
        db.commit()
        yield {"dsn": dsn, "options": options, "db": db, "topic": topic, "admin": admin,
               "producer": producer, "bootstrap": bootstrap,
               "reader": KafkaOffsetReader(bootstrap), "suffix": suffix,
               "incarnation": "inc-" + suffix, "connect": connect}
    finally:
        producer.close()
        db.close()
        try:
            admin.delete_topics([topic])
        except Exception:
            pass
        admin.close()
        with connect(dsn) as root:
            root.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            root.commit()


@pytest.mark.integration
def test_detector_results_and_alerts_are_unchanged_by_a_cut_and_resume(detector_world):
    from ton_sentinel.db.work import rescore_stale, rescore_window
    from ton_sentinel.recovery import KafkaRecordReader, resume_from_recovery_cut
    from tests.integration.test_detection import (COUNT, NET, SCORED, TOK, WALLETS, alerts,
                                                  current_results, oracle_for, ready_world,
                                                  served)

    world, db = detector_world, detector_world["db"]
    _seeded, lease = ready_world(db)
    rescore_window(db, lease, NET, TOK, SCORED)
    before = current_results(db)
    digest = before[COUNT][0]["input_digest"]
    assert [before[COUNT][0], before[WALLETS][0]] == oracle_for(db, digest)
    assert before[COUNT][0]["triggered"] is True
    alerts_before, served_before = alerts(db), served(db)
    assert alerts_before
    _pause, cut, point = paused_cut(world)
    handoff = acquire_transition(db, "resumer-" + world["suffix"], ttl_seconds=300)
    resume_from_recovery_cut(db, handoff, point, world["reader"],
                             KafkaRecordReader(world["bootstrap"]),
                             pipeline_epoch="pe-" + world["suffix"])
    publish_pending(db, world["producer"], handoff, limit=50)
    world["producer"].flush(timeout=10)
    assert (current_results(db), alerts(db), served(db)) == (before, alerts_before,
                                                             served_before)
    rescorer = acquire_transition(db, "rescorer-" + world["suffix"], ttl_seconds=300)
    assert rescore_stale(db, rescorer, network=NET) == []
    rescore_window(db, rescorer, NET, TOK, SCORED)
    after = current_results(db)
    assert after[COUNT][0]["input_digest"] == digest
    assert [after[COUNT][0], after[WALLETS][0]] == oracle_for(db, digest)
    assert alerts(db) == alerts_before
    assert cut.manifest["snapshot"]["canonical_generation"] >= 1
