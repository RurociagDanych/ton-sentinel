from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json

import pytest

from ton_sentinel.contracts import Lease
from ton_sentinel.db.fencing import LeaseRejected
from ton_sentinel.spark.streaming import process_canonical_microbatch


class _Batch:
    columns = ["topic", "partition", "offset", "key", "value"]

    def select(self, *columns):
        return self

    def limit(self, _count):
        return self

    def collect(self):
        return []


class _Connection:
    def __init__(self, *, replay, expire):
        self.info = type("Info", (), {"transaction_status": type("Status", (), {"name": "IDLE"})()})()
        self.now = datetime(2030, 1, 1, tzinfo=timezone.utc)
        self.lease = Lease("owner", 7, self.now + timedelta(seconds=1))
        digest = hashlib.sha256(json.dumps([], separators=(",", ":")).encode()).hexdigest()
        self.audit = ("topic", "incarnation", 0, [], digest, 0) if replay else None
        self.expire = expire
        self.rows = []
        self.committed_rows = []

    @contextmanager
    def transaction(self):
        before = list(self.rows)
        try:
            yield
        except Exception:
            self.rows[:] = before
            raise
        else:
            self.committed_rows[:] = self.rows

    def execute(self, sql, params=None):
        if sql.startswith("SELECT owner, fence, expires_at"):
            return _Result((self.lease.owner, self.lease.fence, self.lease.expires_at))
        if sql == "SELECT clock_timestamp()":
            return _Result((self.now,))
        if sql.startswith("SELECT topic_incarnation,replay_epoch"):
            return _Result(("incarnation", 0))
        if sql.startswith("SELECT topic,topic_incarnation,replay_epoch,offset_metadata"):
            # Model audit lookup consuming enough time for the lease to expire.
            if self.expire:
                self.now = self.lease.expires_at
            return _Result(self.audit)
        if sql.startswith("INSERT INTO stream_batches"):
            self.rows.append(params)
            # Model the audit insert completing only after lease expiry.
            if self.expire:
                self.now = self.lease.expires_at
            return _Result(None)
        raise AssertionError(f"unexpected SQL in adapter test: {sql}")


class _Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


def test_audited_empty_batches_revalidate_lease_after_new_or_replayed_audit():
    for replay in (False, True):
        connection = _Connection(replay=replay, expire=True)
        with pytest.raises(LeaseRejected, match="expired or fencing token"):
            process_canonical_microbatch(
                connection, connection.lease, None, _Batch(),
                topic_incarnation="incarnation", replay_epoch=0,
                batch_id=0, environment_id="env", query_id="query",
                pipeline_epoch="epoch", topic="topic",
            )
        assert connection.rows == []
        assert connection.committed_rows == []


def test_audited_empty_batches_accept_valid_new_and_replayed_audits():
    for replay in (False, True):
        connection = _Connection(replay=replay, expire=False)
        assert process_canonical_microbatch(
            connection, connection.lease, None, _Batch(),
            topic_incarnation="incarnation", replay_epoch=0,
            batch_id=0, environment_id="env", query_id="query",
            pipeline_epoch="epoch", topic="topic",
        ) == 0
        assert len(connection.committed_rows) == (0 if replay else 1)
