"""Serve the read-only feature API with explicit command-line settings."""
import argparse
import json
import logging
import os
import sys
import time
from socketserver import ThreadingMixIn
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from ton_sentinel.api import create_app
from ton_sentinel.telemetry_probes import probes_from_environ

_STRUCTURED_HANDLER = "ton_sentinel.api.structured_stderr"
# An idle or slow client is dropped after this many seconds of socket inactivity.
REQUEST_SOCKET_TIMEOUT_SECONDS = 15


class _QuietRequestHandler(WSGIRequestHandler):
    """Drop wsgiref's raw request line (path and query); api.py logs a structured record.

    ``timeout`` bounds every socket read, so a client that connects and sends nothing cannot
    hold a worker forever.
    """
    timeout = REQUEST_SOCKET_TIMEOUT_SECONDS

    def log_message(self, format, *args):  # noqa: A002 - wsgiref signature
        return None


class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    """One thread per connection: an idle client must not block /health or /metrics."""
    daemon_threads = True


def build_server(host, port, app):
    """The api HTTP server: threaded, idle connections time out, quiet request log."""
    return make_server(host, port, app, server_class=_ThreadingWSGIServer,
                       handler_class=_QuietRequestHandler)


def _configure_request_log():
    logger = logging.getLogger("ton_sentinel.api")
    logger.setLevel(logging.INFO)
    if not any(getattr(h, "name", None) == _STRUCTURED_HANDLER for h in logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.name = _STRUCTURED_HANDLER
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)


def _maintenance(argv) -> int:
    from ton_sentinel.maintenance import cycle_policy, run_retention_cycle

    parser = argparse.ArgumentParser(prog="ton-sentinel maintenance",
                                     description="Run one fenced retention cycle.")
    parser.add_argument("--dsn", help="PostgreSQL DSN (default: SENTINEL_DATABASE_DSN)")
    parser.add_argument("--policy-file", required=True,
                        help="JSON object naming every cycle bound explicitly")
    parser.add_argument("--owner", required=True, help="transition lease owner")
    parser.add_argument("--now", type=int, help="maintenance time T (UTC epoch seconds)")
    args = parser.parse_args(argv)
    try:
        with open(args.policy_file, encoding="utf-8") as handle:
            policy = json.load(handle)
        cycle_policy(policy)
    except (OSError, ValueError) as exc:
        print(f"maintenance: {exc}", file=sys.stderr)
        return 2
    dsn = args.dsn or os.environ.get("SENTINEL_DATABASE_DSN")
    if not dsn:
        print("maintenance: --dsn or SENTINEL_DATABASE_DSN is required", file=sys.stderr)
        return 2
    now = int(time.time()) if args.now is None else args.now
    try:
        import psycopg

        with psycopg.connect(dsn) as connection:
            report = run_retention_cycle(connection, args.owner, now, policy)
    except Exception as exc:  # noqa: BLE001 - report the class only: messages can carry DSNs
        print(json.dumps({"status": "failed", "error": type(exc).__name__, "now": now}))
        return 1
    print(json.dumps(report, sort_keys=True, default=str))
    return 0


def _score(argv) -> int:
    parser = argparse.ArgumentParser(prog="ton-sentinel score",
                                     description="Score unscored windows, then rescore stale ones.")
    parser.add_argument("--dsn", help="PostgreSQL DSN (default: SENTINEL_DATABASE_DSN)")
    parser.add_argument("--owner", required=True, help="transition lease owner")
    parser.add_argument("--network", required=True, choices=("mainnet", "testnet"))
    parser.add_argument("--limit", required=True, type=int, help="windows per run, 1..10000")
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= 10000:
        parser.error("--limit must be between 1 and 10000")
    dsn = args.dsn or os.environ.get("SENTINEL_DATABASE_DSN")
    if not dsn:
        print("score: --dsn or SENTINEL_DATABASE_DSN is required", file=sys.stderr)
        return 2
    from ton_sentinel.db.fencing import LeaseRejected

    try:
        import psycopg
        from ton_sentinel.db.fencing import acquire_transition
        from ton_sentinel.db.work import score_pending

        with psycopg.connect(dsn) as connection:
            lease = acquire_transition(connection, args.owner, 300)
            connection.commit()
            report = score_pending(connection, lease, network=args.network, limit=args.limit)
    except LeaseRejected as exc:
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 2
    except Exception as exc:  # noqa: BLE001 - report the class only: messages can carry DSNs
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 1
    print(json.dumps(report, sort_keys=True))
    return 0


def _migrate(argv) -> int:
    parser = argparse.ArgumentParser(prog="ton-sentinel migrate",
                                     description="Apply the packaged PostgreSQL schema.")
    parser.add_argument("--dsn", help="PostgreSQL DSN (default: SENTINEL_DATABASE_DSN)")
    args = parser.parse_args(argv)
    dsn = args.dsn or os.environ.get("SENTINEL_DATABASE_DSN")
    if not dsn:
        print("migrate: --dsn or SENTINEL_DATABASE_DSN is required", file=sys.stderr)
        return 2
    try:
        import importlib

        import psycopg

        apply = importlib.import_module("ton_sentinel.db.migrate").migrate
        with psycopg.connect(dsn) as connection:
            apply(connection)
    except Exception as exc:  # noqa: BLE001 - report the class only: messages can carry DSNs
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 1
    print(json.dumps({"status": "migrated"}))
    return 0


def main(argv=None):
    argv = sys.argv[1:] if argv is None else list(argv)
    if argv[:1] == ["migrate"]:
        return _migrate(argv[1:])
    if argv[:1] == ["score"]:
        return _score(argv[1:])
    if argv[:1] == ["maintenance"]:
        return _maintenance(argv[1:])
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--dsn", help="PostgreSQL DSN (never logged)")
    source.add_argument("--dsn-file", help="file holding the PostgreSQL DSN (never logged)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    dsn = args.dsn
    if args.dsn_file:
        try:
            with open(args.dsn_file, encoding="utf-8") as handle:
                dsn = handle.read().strip()
        except OSError as exc:
            parser.error(f"--dsn-file unreadable ({type(exc).__name__})")
        if not dsn:
            parser.error("--dsn-file is empty")
    try:
        probes = probes_from_environ(os.environ)
    except ValueError as exc:
        parser.error(f"telemetry probe configuration invalid ({exc})")
    _configure_request_log()
    with build_server(args.host, args.port, create_app(dsn=dsn, telemetry_probes=probes)) as server:
        server.serve_forever()


if __name__ == "__main__":
    sys.exit(main())
