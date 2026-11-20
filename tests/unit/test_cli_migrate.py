"""The ``migrate`` CLI applies the packaged schema to a deployment database."""

import importlib
import json
from contextlib import contextmanager

import pytest

import ton_sentinel.cli as cli

SECRET_DSN = "postgresql://owner:s3cr3t-pw@db:5432/feasibility"


class FakeConnection:
    def __init__(self):
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.closed = True
        return False


@pytest.fixture
def wiring(monkeypatch):
    calls = {"connect": [], "migrate": []}
    connection = FakeConnection()

    def connect(dsn, **kwargs):
        calls["connect"].append(dsn)
        return connection

    import psycopg
    migrate_module = importlib.import_module("ton_sentinel.db.migrate")

    monkeypatch.setattr(psycopg, "connect", connect)
    monkeypatch.setattr(migrate_module, "migrate", lambda c: calls["migrate"].append(c))
    monkeypatch.delenv("SENTINEL_DATABASE_DSN", raising=False)
    return calls, connection


def test_migrate_applies_the_packaged_schema_with_the_given_dsn(wiring, capsys):
    calls, connection = wiring
    assert cli.main(["migrate", "--dsn", SECRET_DSN]) == 0
    assert calls["connect"] == [SECRET_DSN]
    assert calls["migrate"] == [connection]
    out = capsys.readouterr()
    assert json.loads(out.out) == {"status": "migrated"}
    assert "s3cr3t" not in out.out + out.err


def test_migrate_reads_the_dsn_from_the_environment(wiring, monkeypatch, capsys):
    calls, _ = wiring
    monkeypatch.setenv("SENTINEL_DATABASE_DSN", SECRET_DSN)
    assert cli.main(["migrate"]) == 0
    assert calls["connect"] == [SECRET_DSN]
    assert "s3cr3t" not in capsys.readouterr().out


def test_migrate_without_a_dsn_exits_2_and_touches_nothing(wiring, capsys):
    calls, _ = wiring
    assert cli.main(["migrate"]) == 2
    assert calls == {"connect": [], "migrate": []}
    assert "required" in capsys.readouterr().err


def test_migrate_failure_reports_the_class_only(monkeypatch, capsys):
    import psycopg
    migrate_module = importlib.import_module("ton_sentinel.db.migrate")

    @contextmanager
    def connect(dsn, **kwargs):
        yield object()

    def broken(connection):
        raise RuntimeError(f"cannot apply to {SECRET_DSN}")

    monkeypatch.setattr(psycopg, "connect", connect)
    monkeypatch.setattr(migrate_module, "migrate", broken)
    assert cli.main(["migrate", "--dsn", SECRET_DSN]) == 1
    out = capsys.readouterr()
    assert json.loads(out.out) == {"status": "failed", "error": "RuntimeError"}
    assert "s3cr3t" not in out.out + out.err
