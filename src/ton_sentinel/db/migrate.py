"""Apply the packaged PostgreSQL ledger schema."""
from pathlib import Path


def _superseded(connection) -> set[str]:
    """Migration files a later migration replaced; replaying them is unsafe."""
    present = connection.execute(
        "SELECT to_regclass('schema_migration_supersessions')"
    ).fetchone()
    if present is None or present[0] is None:
        return set()
    return {row[0] for row in connection.execute(
        "SELECT migration FROM schema_migration_supersessions").fetchall()}


def migrate(connection) -> None:
    migrations = Path(__file__).with_name("migrations")
    superseded = _superseded(connection)
    connection.commit()
    for path in sorted(migrations.glob("[0-9][0-9][0-9]_*.sql")):
        if path.name in superseded:
            continue
        connection.execute(path.read_text())
        connection.commit()
