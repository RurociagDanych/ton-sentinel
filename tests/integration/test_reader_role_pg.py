"""Dashboard API on real PostgreSQL: the reader login can read what the API reads and nothing else."""
import os
import uuid

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from ton_sentinel import api
from ton_sentinel.db import serving
from ton_sentinel.db.migrate import migrate
from ton_sentinel.db.serving import READER_TABLES
from tests.integration.test_revisions import db  # noqa: F401


@pytest.fixture
def reader_dsn(db):  # noqa: F811
    admin_dsn = os.environ["TON_SENTINEL_TEST_DSN"]
    role = "rdr_" + uuid.uuid4().hex[:12]
    password = uuid.uuid4().hex
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(password)))
        admin.execute(sql.SQL("GRANT sentinel_reader TO {}").format(sql.Identifier(role)))
    try:
        yield make_conninfo(admin_dsn, user=role, password=password,
                            options=f"-c search_path={db.schema_name}")
    finally:
        with psycopg.connect(admin_dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


def test_reader_login_can_run_every_api_reader(db, reader_dsn):  # noqa: F811
    with psycopg.connect(reader_dsn) as c:
        serving.read_features(c, limit=5)
        serving.read_windows(c, limit=5)
        serving.read_detector_results(c, network="testnet")
        serving.read_alerts(c, network="testnet")
        serving.read_alert_history(c, network="testnet", token="t", window_start=0,
                                   detector="d")
        for table in sorted(READER_TABLES):
            c.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table))).fetchone()
    status, payload = api._read_status(reader_dsn)
    assert status == "200 OK"
    assert payload["pending_work"]["outbox"]["status"] == "observed"
    assert payload["pending_work"]["repair"]["status"] == "observed"


@pytest.mark.parametrize("statement", [
    "INSERT INTO served_features (network) VALUES ('testnet')",
    "UPDATE served_features SET status = 'x'",
    "DELETE FROM served_features",
    "TRUNCATE served_features",
    "INSERT INTO outbox DEFAULT VALUES",
    "DROP TABLE served_features",
])
def test_reader_insert_update_delete_truncate_fail_with_permission_denied(
        db, reader_dsn, statement):  # noqa: F811
    with psycopg.connect(reader_dsn) as c:  # read-write transaction: privilege is what blocks
        with pytest.raises((psycopg.errors.InsufficientPrivilege, psycopg.errors.UndefinedTable,
                            psycopg.errors.SyntaxError)) as info:
            c.execute(statement)
        assert isinstance(info.value, psycopg.errors.InsufficientPrivilege) or "DROP" in statement
        assert "permission denied" in str(info.value) or "must be owner" in str(info.value)


def test_reader_cannot_see_ungranted_table(db, reader_dsn):  # noqa: F811
    assert "lease_state" not in READER_TABLES
    with psycopg.connect(reader_dsn) as c:
        with pytest.raises(psycopg.errors.InsufficientPrivilege, match="permission denied"):
            c.execute("SELECT * FROM lease_state")


def test_reader_cannot_create_in_schema_or_read_future_tables(db, reader_dsn):  # noqa: F811
    db.execute("CREATE TABLE later_table (x int)")
    db.commit()
    with psycopg.connect(reader_dsn) as c:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("SELECT * FROM later_table")
    with psycopg.connect(reader_dsn) as c:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            c.execute("CREATE TABLE mine (x int)")


def test_migration_replay_is_idempotent(db, reader_dsn):  # noqa: F811
    before = _acl(db)
    migrate(db)
    migrate(db)
    assert _acl(db) == before
    with psycopg.connect(reader_dsn) as c:
        c.execute("SELECT count(*) FROM served_features").fetchone()
    granted = {r[0] for r in db.execute(
        "SELECT table_name FROM information_schema.role_table_grants "
        "WHERE grantee = 'sentinel_reader' AND table_schema = current_schema() "
        "AND privilege_type = 'SELECT'").fetchall()}
    assert granted == READER_TABLES
    others = db.execute(
        "SELECT privilege_type FROM information_schema.role_table_grants "
        "WHERE grantee = 'sentinel_reader' AND table_schema = current_schema() "
        "AND privilege_type <> 'SELECT'").fetchall()
    assert others == []


def _acl(connection):
    return connection.execute(
        "SELECT relname, relacl::text FROM pg_class WHERE relnamespace = current_schema()::regnamespace "
        "AND relkind = 'r' ORDER BY 1").fetchall()
