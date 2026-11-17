"""The API entrypoint emits only the structured request log.

The default wsgiref handler writes the raw request line (path and query) for every request;
the entrypoint must replace it with the single structured, redacted record from api.py.
"""
import json
import logging
from unittest.mock import patch

import ton_sentinel.cli as cli


class FakeServer:
    def __init__(self, *args, **kwargs):
        self.args, self.kwargs = args, kwargs

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def serve_forever(self):
        return None


def _serve(argv):
    seen = {}

    def fake_make_server(*args, **kwargs):
        seen["server"] = FakeServer(*args, **kwargs)
        return seen["server"]

    with patch.object(cli, "make_server", fake_make_server):
        cli.main(argv)
    return seen["server"]


def test_entrypoint_silences_raw_request_lines_and_emits_structured_records(capsys):
    logger = logging.getLogger("ton_sentinel.api")
    before = list(logger.handlers)
    try:
        server = _serve(["--dsn", "postgresql://u:cli-pw-12@127.0.0.1:1/none", "--port", "8081"])
        handler_class = server.kwargs["handler_class"]
        handler = handler_class.__new__(handler_class)
        handler.log_message("%s", "GET /v1/alerts?token=private-cli-token HTTP/1.1")
        app = server.args[2]
        app({"REQUEST_METHOD": "GET", "PATH_INFO": "/nope", "QUERY_STRING": "q=private-q"},
            lambda status, headers: None)
        err = capsys.readouterr().err
        assert "private-cli-token" not in err and "private-q" not in err and "cli-pw-12" not in err
        [line] = [line for line in err.splitlines() if line.strip()]
        assert json.loads(line) == {**json.loads(line), "event": "http_request",
                                    "route": "unmatched", "status": 404}
        # idempotent: a second start does not duplicate handlers
        _serve(["--dsn", "postgresql://u:cli-pw-12@127.0.0.1:1/none"])
        assert len(logger.handlers) == len(before) + 1
    finally:
        for handler in list(logger.handlers):
            if handler not in before:
                logger.removeHandler(handler)
