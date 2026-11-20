"""Unit tests: ordering of persisted state around Kafka acknowledgements."""

from datetime import datetime, timedelta, timezone

import pytest

from ton_sentinel.contracts import Lease
from ton_sentinel.db.fencing import LeaseRejected
from ton_sentinel.envelope import build_delivery_envelope
from ton_sentinel.outbox import PublicationError, publish_pending

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


class Cursor:
    def __init__(self, one=None, many=()):
        self._one, self._many = one, many

    def fetchone(self):
        return self._one

    def fetchall(self):
        return list(self._many)


class FakeDb:
    def __init__(self, log, lease_valid=True, events=("e1",)):
        self.log, self.lease_valid = log, lease_valid
        self.info = type("I", (), {"transaction_status": type("S", (), {"name": "IDLE"})()})()
        self.lease = Lease("owner", 1, NOW + timedelta(seconds=30))
        self.events = events

    def commit(self):
        pass

    def transaction(self):
        db = self

        class Tx:
            def __enter__(self):
                return db

            def __exit__(self, *exc):
                return False
        return Tx()

    def execute(self, sql, params=()):
        if "FROM outbox_delivery_config" in sql or "FROM outbox_delivery_authority" in sql:
            return Cursor(("t", "inc", 0))
        if "FROM lease_state" in sql:
            return Cursor(("owner", 1 if self.lease_valid else 2, self.lease.expires_at))
        if "clock_timestamp()" in sql and sql.startswith("SELECT clock"):
            return Cursor((NOW,))
        if sql.startswith("SELECT event_id,payload"):
            rows = []
            for e in self.events:
                action = {"x": e}
                env = build_delivery_envelope(event_id=e, topic_incarnation="inc",
                                              replay_epoch=0, action=action)
                rows.append((e, action, "t", "inc", 0, env, None))
            return Cursor(many=rows)
        if "SET publication_state=" in sql:
            state = sql.split("publication_state='")[1].split("'")[0]
            self.log.append(("sql", state, params[0]))
        return Cursor()


class Future:
    def __init__(self, log, event, fail):
        self.log, self.event, self.fail = log, event, fail

    def get(self, timeout):
        self.log.append(("get", self.event))
        if self.fail:
            raise self.fail
        return object()


class Producer:
    config = {"acks": -1}

    def __init__(self, log, fail=None, send_fail=None):
        self.log, self.fail, self.send_fail = log, fail, send_fail

    def send(self, topic, key, value):
        event = key.decode()
        self.log.append(("send", event))
        if self.send_fail:
            raise self.send_fail
        return Future(self.log, event, self.fail)


def test_attempt_is_persisted_before_send_and_ack_only_after_future_succeeds():
    log = []
    sent = publish_pending(FakeDb(log), Producer(log), FakeDb(log).lease)
    assert sent == ("e1",)
    assert log == [("sql", "attempted", "e1"), ("send", "e1"), ("get", "e1"),
                   ("sql", "acknowledged", "e1")]


def test_unacknowledged_send_is_never_marked_acknowledged_and_retries_are_bounded():
    log = []
    db = FakeDb(log)
    with pytest.raises(PublicationError, match="after 3 attempt"):
        publish_pending(db, Producer(log, fail=TimeoutError("no ack")), db.lease, max_retries=2)
    assert [e for e in log if e[0] == "send"] == [("send", "e1")] * 3
    assert not [e for e in log if e[:2] == ("sql", "acknowledged")]
    assert [e[1] for e in log if e[0] == "sql"].count("attempted") == 3
    assert log[-1] == ("sql", "ambiguous", "e1")


def test_synchronous_backpressure_or_outage_error_is_ambiguous_not_acknowledged():
    log = []
    db = FakeDb(log, events=("e1", "e2"))
    with pytest.raises(PublicationError):
        publish_pending(db, Producer(log, send_fail=BufferError("buffer full")),
                        db.lease, max_retries=0)
    assert log == [("sql", "attempted", "e1"), ("send", "e1"), ("sql", "ambiguous", "e1")]


def test_superseded_or_expired_lease_rejects_before_any_kafka_send():
    log = []
    db = FakeDb(log, lease_valid=False)
    with pytest.raises(LeaseRejected):
        publish_pending(db, Producer(log), db.lease)
    assert not [e for e in log if e[0] == "send"]
