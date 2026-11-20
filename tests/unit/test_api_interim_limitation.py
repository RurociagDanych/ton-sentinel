"""The read-only slice must state the interim stateless limitation explicitly."""

import json
import re
from datetime import datetime, timezone
from unittest.mock import patch

from ton_sentinel.api import create_app


class _Result:
    def fetchone(self):
        return (2, 1, 300, datetime(2024, 3, 9, 16, 5, tzinfo=timezone.utc))


class _Connection:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def transaction(self):
        return self

    def execute(self, statement, parameters=None):
        return _Result()


def _get(app, path):
    out = {}
    body = b"".join(app({"REQUEST_METHOD": "GET", "PATH_INFO": path, "QUERY_STRING": ""},
                        lambda status, headers: out.update(status=status)))
    return out["status"], body.decode("utf-8")


def test_status_declares_interim_stateless_limitation_until_stateful_path_is_qualified():
    app = create_app(dsn="postgresql://u:s3cret-pass@h/db")
    with patch("ton_sentinel.api.psycopg.connect", return_value=_Connection()):
        status, body = _get(app, "/status")
    limit = json.loads(body)["interim_limitation"]
    assert status == "200 OK"
    assert limit["mode"] == "stateless_batch_replacement"
    assert limit["stateful_streaming"] == "not_qualified"
    assert "own qualification" in limit["meaning"] and "stateless" in limit["meaning"]
    assert "s3cret-pass" not in body


def test_demo_page_renders_the_limitation_and_no_secret():
    app = create_app(dsn="postgresql://u:s3cret-pass@h/db")
    with patch("ton_sentinel.api.psycopg.connect", return_value=_Connection()):
        status, html = _get(app, "/")
    text = " ".join(re.sub(r"<[^>]+>", "", html).split())
    assert status == "200 OK"
    assert "Interim limitation" in text and "stateless_batch_replacement" in text
    assert "not_qualified" in text and "own qualification" in text
    assert "s3cret-pass" not in html


def test_limitation_is_present_even_when_database_is_unavailable():
    app = create_app(dsn="explicit-test-dsn")
    with patch("ton_sentinel.api.psycopg.connect", side_effect=RuntimeError("postgres://x:pw@h/db")):
        status, body = _get(app, "/status")
    assert status == "503 Service Unavailable"
    assert json.loads(body)["interim_limitation"]["mode"] == "stateless_batch_replacement"
    assert "pw@" not in body
