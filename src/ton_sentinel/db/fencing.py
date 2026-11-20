"""PostgreSQL fencing operations for range acceptance."""
from datetime import timedelta

from ton_sentinel.contracts import Lease


class LeaseRejected(RuntimeError):
    """The database says the presented owner/fence is no longer current."""
    pass


def acquire_transition(connection, owner: str, ttl_seconds: int = 30) -> Lease:
    if type(owner) is not str or not owner.strip():
        raise ValueError("lease owner must be nonempty text")
    if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 300:
        raise ValueError("lease duration must be 1..300 integer seconds")
    row = connection.execute(
        "INSERT INTO lease_state(singleton, owner, fence, expires_at) "
        "VALUES (true, %s, 1, clock_timestamp() + %s * interval '1 second') "
        "ON CONFLICT (singleton) DO UPDATE SET owner=EXCLUDED.owner, "
        "fence=lease_state.fence + 1, expires_at=EXCLUDED.expires_at "
        "RETURNING owner, fence, expires_at",
        (owner, ttl_seconds),
    ).fetchone()
    connection.commit()
    return Lease(*row)


def validate_transition(connection, lease: Lease) -> None:
    row = connection.execute(
        "SELECT owner, fence, expires_at FROM lease_state "
        "WHERE singleton=true FOR UPDATE"
    ).fetchone()
    if row is None or type(lease) is not Lease:
        raise LeaseRejected("lease expired or fencing token superseded")
    now = connection.execute("SELECT clock_timestamp()").fetchone()[0]
    if (row[2] <= now or row[0] != lease.owner or row[1] != lease.fence
            or row[2] != lease.expires_at):
        raise LeaseRejected("lease expired or fencing token superseded")
