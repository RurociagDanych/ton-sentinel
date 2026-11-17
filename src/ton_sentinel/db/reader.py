"""Explicit least-privilege provisioning for the feature API reader."""
from __future__ import annotations

from psycopg import sql
from psycopg.pq import TransactionStatus


_TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")
_COLUMN_PRIVILEGES = {"SELECT", "INSERT", "UPDATE", "REFERENCES"}


def provision_api_reader(connection, role_name: str) -> None:
    """Grant one existing role read access to this schema's served_features table.

    The caller owns the connection, transaction, role lifecycle, and credentials.
    This function creates no identities and never revokes or widens global access.
    It fails closed when the target role is privileged, owns serving objects, or
    already has access beyond SELECT on the one served table.
    """
    if type(role_name) is not str or not role_name or len(role_name.encode("utf-8")) > 63:
        raise ValueError("reader role name must be nonempty PostgreSQL identifier text")

    if connection.info.transaction_status != TransactionStatus.INTRANS:
        raise RuntimeError("provisioning requires an active caller-owned transaction")

    with connection.transaction():
        role = connection.execute(
            "SELECT oid, rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls "
            "FROM pg_roles WHERE rolname = %s",
            (role_name,),
        ).fetchone()
        if role is None:
            raise ValueError("reader role must already exist")
        role_oid, superuser, create_role, create_db, replication, bypass_rls = role
        if any((superuser, create_role, create_db, replication, bypass_rls)):
            raise ValueError("reader role has privileged role attributes")
        if connection.execute(
            "SELECT has_database_privilege(%s, current_database(), 'CREATE')", (role_oid,)
        ).fetchone()[0]:
            raise ValueError("reader role can create schemas in this database")
        if connection.execute("SELECT current_user = %s", (role_name,)).fetchone()[0]:
            raise ValueError("caller cannot provision itself as the reader")
        if connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_auth_members WHERE member = %s)", (role_oid,)
        ).fetchone()[0]:
            raise ValueError("reader role inherits privileges from another role")

        if connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = current_database() "
            "AND datdba = %s)", (role_oid,)
        ).fetchone()[0]:
            raise ValueError("reader role owns the current database")

        # Ownership remains a mutation capability even when ordinary ACLs have
        # been revoked. Restrict this check to application schemas, not catalogs.
        if connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace n WHERE "
            "n.nspname NOT IN ('pg_catalog','information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%%' AND n.nspowner = %s)", (role_oid,)
        ).fetchone()[0]:
            raise ValueError("reader role owns an application schema")
        if connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n "
            "ON n.oid = c.relnamespace WHERE c.relowner = %s "
            "AND n.nspname NOT IN ('pg_catalog','information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%%')", (role_oid,)
        ).fetchone()[0]:
            raise ValueError("reader role owns an application relation")
        if connection.execute(
            "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n "
            "ON n.oid = p.pronamespace WHERE p.proowner = %s "
            "AND n.nspname NOT IN ('pg_catalog','information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%%')", (role_oid,)
        ).fetchone()[0]:
            raise ValueError("reader role owns an application routine")

        target = connection.execute(
            "SELECT n.oid, n.nspname, c.oid, c.relkind, pg_get_userbyid(n.nspowner), "
            "pg_get_userbyid(c.relowner) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = current_schema() AND c.relname = 'served_features'"
        ).fetchone()
        if target is None or target[3] not in ("r", "p"):
            raise ValueError("current schema must contain a served_features table")
        schema_oid, schema_name, table_oid, _, schema_owner, table_owner = target
        if table_owner == role_name or schema_owner == role_name:
            raise ValueError("reader role must not own the serving table or schema")

        # Existing grants, including PUBLIC grants and column grants, are observed
        # before mutation. Only target-table SELECT may already be present.
        objects = connection.execute(
            "SELECT c.oid, n.nspname, c.relname FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('r','p','v','m','f') "
            "AND n.nspname NOT IN ('pg_catalog','information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%'"
        ).fetchall()
        for object_oid, object_schema, object_name in objects:
            allowed_target = object_oid == table_oid
            for privilege in _TABLE_PRIVILEGES:
                if not allowed_target or privilege != "SELECT":
                    table_access = connection.execute(
                        "SELECT has_table_privilege(%s, %s, %s)",
                        (role_oid, object_oid, privilege),
                    ).fetchone()[0]
                    column_access = privilege in _COLUMN_PRIVILEGES and connection.execute(
                        "SELECT has_any_column_privilege(%s, %s, %s)",
                        (role_oid, object_oid, privilege),
                    ).fetchone()[0]
                    if table_access or column_access:
                        raise ValueError("reader role has access beyond served_features SELECT")

        sequences = connection.execute(
            "SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind = 'S' AND n.nspname NOT IN ('pg_catalog','information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%'"
        ).fetchall()
        for (sequence_oid,) in sequences:
            if any(
                connection.execute(
                    "SELECT has_sequence_privilege(%s, %s, %s)",
                    (role_oid, sequence_oid, privilege),
                ).fetchone()[0]
                for privilege in ("USAGE", "SELECT", "UPDATE")
            ):
                raise ValueError("reader role has access to an application sequence")

        # A SECURITY DEFINER routine can mutate data without table write ACLs.
        # Include the current schema because it will receive USAGE below, making
        # PUBLIC EXECUTE grants callable after provisioning.
        routines = connection.execute(
            "SELECT p.oid, n.nspname FROM pg_proc p JOIN pg_namespace n "
            "ON n.oid = p.pronamespace WHERE p.prosecdef "
            "AND n.nspname NOT IN ('pg_catalog','information_schema') "
            "AND n.nspname NOT LIKE 'pg_toast%'"
        ).fetchall()
        for routine_oid, routine_schema in routines:
            if routine_schema == schema_name or connection.execute(
                "SELECT has_schema_privilege(%s, %s, 'USAGE')", (role_oid, routine_schema)
            ).fetchone()[0]:
                if connection.execute(
                    "SELECT has_function_privilege(%s, %s, 'EXECUTE')",
                    (role_oid, routine_oid),
                ).fetchone()[0]:
                    raise ValueError("reader role can execute an application SECURITY DEFINER routine")

        schemas = connection.execute(
            "SELECT oid, nspname FROM pg_namespace "
            "WHERE nspname NOT IN ('pg_catalog','information_schema') "
            "AND nspname NOT LIKE 'pg_toast%'"
        ).fetchall()
        for object_schema_oid, _ in schemas:
            if connection.execute(
                "SELECT has_schema_privilege(%s, %s, 'CREATE')", (role_oid, object_schema_oid)
            ).fetchone()[0]:
                raise ValueError("reader role can create objects in a schema")

        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier(role_name)
            )
        )
        connection.execute(
            sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                sql.Identifier(schema_name), sql.Identifier("served_features"),
                sql.Identifier(role_name),
            )
        )
