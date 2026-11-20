"""Least-privilege reader behavior on disposable real PostgreSQL."""

import json
import os
import secrets
import threading
import uuid
from urllib.error import HTTPError
from urllib.request import urlopen
from wsgiref.simple_server import make_server

import psycopg
import pytest
from psycopg import sql

from ton_sentinel.api import create_app
from ton_sentinel.db.migrate import migrate
from ton_sentinel.db.reader import provision_api_reader


class _ReaderFixture(tuple):
    def __repr__(self):
        return "<provisioned reader fixture>"


@pytest.fixture
def provisioned_reader():
    dsn = os.environ.get("TON_SENTINEL_TEST_DSN")
    if not dsn:
        pytest.fail("TON_SENTINEL_TEST_DSN must point to the owned disposable PostgreSQL")
    suffix = uuid.uuid4().hex
    schema = "api_reader_" + suffix
    role = "api_reader_" + suffix
    password = secrets.token_urlsafe(32)
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        admin.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(role), sql.Literal(password)
        ))
    scoped_dsn = psycopg.conninfo.make_conninfo(
        dsn, options=f"-c search_path={schema}"
    )
    try:
        with psycopg.connect(dsn) as admin:
            assert admin.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_hba_file_rules "
                "WHERE type = 'host' AND auth_method <> 'trust' "
                "AND (database @> ARRAY['all']::text[] OR database @> ARRAY[current_database()]::text[]) "
                "AND (user_name @> ARRAY['all']::text[] OR user_name @> ARRAY[current_user]::text[]))"
            ).fetchone()[0]
        with psycopg.connect(scoped_dsn) as connection:
            migrate(connection)
            connection.execute(
                "INSERT INTO served_features "
                "(network, window_start, window_end, token, generation, swaps, known_traders, "
                "unknown_count, status, provisional, write_fence) "
                "VALUES ('testnet', 0, 300, 'token', 'generation', 2, 1, 0, 'ready', false, 1)"
            )
            provision_api_reader(connection, role)
            provision_api_reader(connection, role)
        reader_dsn = psycopg.conninfo.make_conninfo(
            dsn, user=role, password=password, options=f"-c search_path={schema}"
        )
        yield _ReaderFixture((reader_dsn, schema, role, dsn))
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("REVOKE {} FROM {}").format(
                sql.Identifier(role), sql.Identifier(_admin_name(dsn))
            ))
            admin.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(role)))


def _admin_name(dsn):
    with psycopg.connect(dsn) as connection:
        return connection.execute("SELECT current_user").fetchone()[0]


def _get(base, path):
    try:
        with urlopen(base + path, timeout=8) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


@pytest.mark.integration
def test_dedicated_reader_serves_api_and_cannot_write_or_read_private_tables(provisioned_reader):
    reader_dsn, schema, role, _ = provisioned_reader
    with psycopg.connect(reader_dsn) as reader:
        assert reader.execute("SELECT current_user").fetchone()[0] == role
        assert reader.execute("SELECT session_user").fetchone()[0] == role
        # This is the actual session_user LOGIN identity; the rejected password
        # below verifies that authentication is not bypassed by a trust rule.
    bad_password_dsn = psycopg.conninfo.make_conninfo(reader_dsn, password="wrong-password")
    with pytest.raises(psycopg.OperationalError):
        psycopg.connect(bad_password_dsn)

    server = make_server("127.0.0.1", 0, create_app(dsn=reader_dsn))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{server.server_port}"
    try:
        status, payload = _get(base, "/v1/features")
        assert status == 200
        assert len(payload["features"]) == 1
        assert payload["features"][0]["token"] == "token"
        status, health = _get(base, "/health")
        assert status == 200 and health == {"status": "ok"}
        status, report = _get(base, "/status")
        assert status == 200
        assert report["feature_rows"] == 1 and report["windows"] == 1
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()

    denied = []
    commands = (
        ("INSERT", "INSERT INTO served_features "
         "(network,window_start,window_end,token,generation,swaps,known_traders,unknown_count,status,provisional,write_fence) "
         "VALUES ('testnet',0,300,'x','g',0,0,0,'ready',false,1)"),
        ("UPDATE", "UPDATE served_features SET swaps=swaps"),
        ("DELETE", "DELETE FROM served_features"),
        ("DDL", "CREATE TABLE reader_must_not_create (id integer)"),
    )
    with psycopg.connect(reader_dsn) as reader:
        assert reader.execute("SHOW transaction_read_only").fetchone()[0] == "off"
        before = reader.execute("SELECT count(*) FROM served_features").fetchone()[0]
        reader.execute("RESET ROLE")
        assert reader.execute("SELECT current_user, session_user").fetchone() == (role, role)
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as set_role_error:
            reader.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(_admin_name(_))))
        assert set_role_error.value.sqlstate == "42501"
        reader.rollback()
        for label, statement in commands:
            with pytest.raises(psycopg.errors.InsufficientPrivilege) as error:
                with reader.transaction():
                    assert reader.execute("SHOW transaction_read_only").fetchone()[0] == "off"
                    reader.execute(statement)
            assert error.value.sqlstate == "42501"
            denied.append(label)
        with pytest.raises(psycopg.errors.InsufficientPrivilege) as private_error:
            with reader.transaction():
                reader.execute("SELECT * FROM outbox LIMIT 1")
        assert private_error.value.sqlstate == "42501"
        assert reader.execute("SELECT count(*) FROM served_features").fetchone()[0] == before
    assert denied == ["INSERT", "UPDATE", "DELETE", "DDL"]


@pytest.mark.integration
def test_provisioning_refuses_privileged_or_inherited_reader_roles(provisioned_reader):
    _, schema, _, dsn = provisioned_reader
    unsafe = "unsafe_reader_" + uuid.uuid4().hex
    inherited = "inherited_reader_" + uuid.uuid4().hex
    writer = "reader_parent_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN BYPASSRLS").format(sql.Identifier(unsafe)))
        admin.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(inherited)))
        admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(writer)))
        admin.execute(sql.SQL("GRANT {} TO {}").format(
            sql.Identifier(writer), sql.Identifier(inherited)
        ))
    try:
        with psycopg.connect(dsn, options=f"-c search_path={schema}") as connection:
            with connection.transaction():
                with pytest.raises(ValueError, match="privileged role attributes"):
                    provision_api_reader(connection, unsafe)
                with pytest.raises(ValueError, match="inherits privileges"):
                    provision_api_reader(connection, inherited)
            assert not connection.execute(
                "SELECT has_table_privilege(%s, 'served_features', 'SELECT')", (inherited,)
            ).fetchone()[0]
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(inherited)))
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(writer)))
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(unsafe)))


def _assert_no_reader_grants(dsn, schema, role):
    with psycopg.connect(dsn) as connection:
        assert not connection.execute(
            "SELECT has_schema_privilege(%s, %s, 'USAGE')", (role, schema)
        ).fetchone()[0]
        assert not connection.execute(
            "SELECT has_table_privilege(%s, %s, 'SELECT')",
            (role, f"{schema}.served_features"),
        ).fetchone()[0]


@pytest.mark.integration
@pytest.mark.parametrize("privilege", ["USAGE", "UPDATE"])
def test_provisioning_rejects_sequence_mutation_privileges_without_grant_leaks(
    provisioned_reader, privilege
):
    _, schema, role, dsn = provisioned_reader
    sequence = "reader_sequence_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("REVOKE USAGE ON SCHEMA {} FROM {}").format(
            sql.Identifier(schema), sql.Identifier(role)
        ))
        admin.execute(sql.SQL("REVOKE SELECT ON TABLE {}.served_features FROM {}").format(
            sql.Identifier(schema), sql.Identifier(role)
        ))
        admin.execute(
            sql.SQL("CREATE SEQUENCE {}.{}").format(
                sql.Identifier(schema), sql.Identifier(sequence)
            )
        )
        admin.execute(
            sql.SQL("GRANT {} ON SEQUENCE {}.{} TO {}").format(
                sql.SQL(privilege), sql.Identifier(schema), sql.Identifier(sequence),
                sql.Identifier(role),
            )
        )
    try:
        with psycopg.connect(dsn, options=f"-c search_path={schema}") as connection:
            with connection.transaction():
                with pytest.raises(ValueError, match="application sequence"):
                    provision_api_reader(connection, role)
        _assert_no_reader_grants(dsn, schema, role)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP SEQUENCE IF EXISTS {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(sequence)
                )
            )


@pytest.mark.integration
def test_provisioning_rejects_public_execute_security_definer_without_grant_leaks(
    provisioned_reader,
):
    _, schema, role, dsn = provisioned_reader
    routine = "reader_definer_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("REVOKE USAGE ON SCHEMA {} FROM {}").format(
            sql.Identifier(schema), sql.Identifier(role)
        ))
        admin.execute(sql.SQL("REVOKE SELECT ON TABLE {}.served_features FROM {}").format(
            sql.Identifier(schema), sql.Identifier(role)
        ))
        admin.execute(
            sql.SQL(
                "CREATE FUNCTION {}.{}() RETURNS integer LANGUAGE sql SECURITY DEFINER "
                "AS 'SELECT 1'"
            ).format(sql.Identifier(schema), sql.Identifier(routine))
        )
    try:
        with psycopg.connect(dsn, options=f"-c search_path={schema}") as connection:
            with connection.transaction():
                with pytest.raises(ValueError, match="SECURITY DEFINER routine"):
                    provision_api_reader(connection, role)
        _assert_no_reader_grants(dsn, schema, role)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("DROP FUNCTION IF EXISTS {}.{}()").format(
                    sql.Identifier(schema), sql.Identifier(routine)
                )
            )


@pytest.mark.integration
@pytest.mark.parametrize("object_kind", ["sequence", "function"])
def test_provisioning_rejects_hidden_object_ownership_without_grant_leaks(
    provisioned_reader, object_kind
):
    _, schema, _, dsn = provisioned_reader
    owner = "reader_object_owner_" + uuid.uuid4().hex
    object_name = "reader_owned_" + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE ROLE {}").format(sql.Identifier(owner)))
        admin.execute(sql.SQL("GRANT CREATE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema), sql.Identifier(owner)
        ))
        if object_kind == "sequence":
            admin.execute(
                sql.SQL("CREATE SEQUENCE {}.{}").format(
                    sql.Identifier(schema), sql.Identifier(object_name)
                )
            )
            admin.execute(
                sql.SQL("ALTER SEQUENCE {}.{} OWNER TO {}").format(
                    sql.Identifier(schema), sql.Identifier(object_name), sql.Identifier(owner)
                )
            )
            admin.execute(
                sql.SQL("REVOKE ALL ON SEQUENCE {}.{} FROM PUBLIC").format(
                    sql.Identifier(schema), sql.Identifier(object_name)
                )
            )
        else:
            admin.execute(
                sql.SQL("CREATE FUNCTION {}.{}() RETURNS integer LANGUAGE sql AS 'SELECT 1'").format(
                    sql.Identifier(schema), sql.Identifier(object_name)
                )
            )
            admin.execute(
                sql.SQL("ALTER FUNCTION {}.{}() OWNER TO {}").format(
                    sql.Identifier(schema), sql.Identifier(object_name), sql.Identifier(owner)
                )
            )
            admin.execute(
                sql.SQL("REVOKE ALL ON FUNCTION {}.{}() FROM PUBLIC").format(
                    sql.Identifier(schema), sql.Identifier(object_name)
                )
            )
        admin.execute(sql.SQL("REVOKE CREATE ON SCHEMA {} FROM {}").format(
            sql.Identifier(schema), sql.Identifier(owner)
        ))
    try:
        with psycopg.connect(dsn, options=f"-c search_path={schema}") as connection:
            with connection.transaction():
                with pytest.raises(ValueError, match="owns an application"):
                    provision_api_reader(connection, owner)
        _assert_no_reader_grants(dsn, schema, owner)
    finally:
        with psycopg.connect(dsn, autocommit=True) as admin:
            if object_kind == "sequence":
                admin.execute(
                    sql.SQL("DROP SEQUENCE IF EXISTS {}.{}").format(
                        sql.Identifier(schema), sql.Identifier(object_name)
                    )
                )
            else:
                admin.execute(
                    sql.SQL("DROP FUNCTION IF EXISTS {}.{}()").format(
                        sql.Identifier(schema), sql.Identifier(object_name)
                    )
                )
            admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(owner)))


@pytest.mark.integration
def test_provisioning_requires_caller_transaction_and_caller_controls_rollback(provisioned_reader):
    _, schema, role, dsn = provisioned_reader
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("REVOKE USAGE ON SCHEMA {} FROM {}").format(
            sql.Identifier(schema), sql.Identifier(role)
        ))
        admin.execute(sql.SQL("REVOKE SELECT ON TABLE {}.served_features FROM {}").format(
            sql.Identifier(schema), sql.Identifier(role)
        ))

    with psycopg.connect(dsn, options=f"-c search_path={schema}") as connection:
        assert connection.info.transaction_status.name == "IDLE"
        with pytest.raises(RuntimeError, match="active caller-owned transaction"):
            provision_api_reader(connection, role)
        assert connection.info.transaction_status.name == "IDLE"
        with pytest.raises(ArithmeticError, match="force caller rollback"):
            with connection.transaction():
                provision_api_reader(connection, role)
                assert connection.execute(
                    "SELECT has_schema_privilege(%s, current_schema(), 'USAGE')", (role,)
                ).fetchone()[0]
                assert connection.execute(
                    "SELECT has_table_privilege(%s, 'served_features', 'SELECT')", (role,)
                ).fetchone()[0]
                raise ArithmeticError("force caller rollback")
    _assert_no_reader_grants(dsn, schema, role)
