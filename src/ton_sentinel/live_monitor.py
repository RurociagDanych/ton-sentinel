"""Bounded live monitor: every 15 minutes, ingest the newest closed window and score it."""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlencode, urlsplit

from ton_sentinel.db.work import qualifiable_windows

ENABLE_ENV = "TON_SENTINEL_LIVE_MONITOR"
WINDOW_SECONDS = 900
DEFAULT_LAG_SECONDS = 180
DEFAULT_DELTA_SECONDS = 120
READ_ROUTES = frozenset({"/masterchainInfo", "/actions", "/transactions", "/traces"})
PAGE = 1000
MAX_PAGES = 5
TRACE_BATCH = 20
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
DEDUST_LABELS = ("dedust", "dedust_v2")


class MonitorDisabled(RuntimeError):
    """The monitor flag is off (default); nothing is scheduled."""
    pass


class SourceUnreachable(RuntimeError):
    """The source did not answer usefully after the bounded retries; the monitor stops."""
    pass


class RequestBudgetExceeded(RuntimeError):
    """The client's hard request budget is spent; no further request is sent."""
    pass


class WindowNotClosed(RuntimeError):
    """The indexer frontier is not yet ``delta`` past the window end; do not ingest it."""
    pass


class IncompleteFetch(RuntimeError):
    """A paged query needed more pages than allowed; the window is not ingested."""
    pass


def monitor_enabled(environ) -> bool:
    """True only for ``TON_SENTINEL_LIVE_MONITOR=1`` exactly."""
    return environ.get(ENABLE_ENV) == "1"


def newest_closed_window(now: int, *, lag_seconds: int = DEFAULT_LAG_SECONDS,
                         width: int = WINDOW_SECONDS) -> tuple:
    """The newest whole ``width`` window that ended at least ``lag_seconds`` before ``now``."""
    end = (int(now) - lag_seconds) // width * width
    return end - width, end


def _urllib_transport(method, url, headers, timeout):
    import urllib.error
    import urllib.request

    request = urllib.request.Request(url, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return (response.status, {k.lower(): v for k, v in response.headers.items()},
                    response.read(MAX_RESPONSE_BYTES + 1))
    except urllib.error.HTTPError as exc:
        return exc.code, {k.lower(): v for k, v in exc.headers.items()}, exc.read(65536)


class ReadOnlyClient:
    """Serial, polite, read-only GET client for the reviewed indexer routes."""
    def __init__(self, base_url: str, api_key: str | None, *, min_interval_seconds: float = 1.0,
                 max_requests: int = 300, max_retries_429: int = 5, max_retries_5xx: int = 3,
                 max_backoff_seconds: float = 120.0, timeout_seconds: float = 60.0,
                 transport=None, clock=time.monotonic, sleep=time.sleep,
                 log: Callable | None = None):
        parts = urlsplit(base_url or "")
        if (parts.scheme != "https" or not parts.hostname or parts.username or parts.password
                or parts.query or parts.fragment):
            raise ValueError("base URL must be a plain https URL")
        if type(min_interval_seconds) not in (int, float) or min_interval_seconds < 1.0:
            raise ValueError("at most one request per second: min_interval_seconds >= 1")
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError("max_requests must be a positive integer")
        self._base = base_url.rstrip("/")
        self._host = parts.netloc
        self.route_prefix = parts.path.rstrip("/")
        self._key = api_key or None
        self._interval = float(min_interval_seconds)
        self._max_requests = max_requests
        self._retries = (max_retries_429, max_retries_5xx)
        self._max_backoff = float(max_backoff_seconds)
        self._timeout = timeout_seconds
        self._transport = transport or _urllib_transport
        self._clock, self._sleep, self._log = clock, sleep, log
        self._last_start = None
        self.requests_made = 0
        self.bytes_read = 0
        self.status_counts: dict = {}

    def _redact(self, text: str) -> str:
        return text.replace(self._key, "[REDACTED]") if self._key else text

    def _wait_turn(self):
        if self._last_start is not None:
            wait = self._last_start + self._interval - self._clock()
            if wait > 0:
                self._sleep(wait)

    def _backoff(self, headers, attempt: int) -> float:
        value = (headers or {}).get("retry-after")
        try:
            delay = float(value) if value is not None else 5.0 * (2 ** attempt)
        except ValueError:
            delay = 5.0 * (2 ** attempt)
        return max(0.0, min(delay, self._max_backoff))

    def get_json(self, path: str, params) -> dict:
        if path not in READ_ROUTES:
            raise ValueError("only the reviewed read routes are allowed")
        pairs = list(params.items()) if isinstance(params, dict) else list(params)
        if any("key" in str(name).lower() for name, _ in pairs):
            raise ValueError("credentials are never sent as query parameters")
        query = urlencode(pairs, doseq=True)
        url = self._base + path + ("?" + query if query else "")
        if urlsplit(url).netloc != self._host:
            raise ValueError("request host differs from the configured host")
        if self._key and self._key in url:
            raise ValueError("refusing: the key must never be placed in a URL")
        headers = {"Accept": "application/json",
                   "User-Agent": "ton-sentinel-live-monitor/1 (read-only, <=1 req/s)"}
        if self._key:
            headers["X-Api-Key"] = self._key
        n429 = n5xx = 0
        while True:
            if self.requests_made >= self._max_requests:
                raise RequestBudgetExceeded(f"request budget of {self._max_requests} spent")
            self._wait_turn()
            self._last_start = self._clock()
            self.requests_made += 1
            started = self._last_start
            try:
                status, rheaders, body = self._transport("GET", url, headers, self._timeout)
                error = None
            except Exception as exc:  # noqa: BLE001 - network errors are retried like 5xx
                status, rheaders, body, error = None, {}, b"", type(exc).__name__
            self.status_counts[status] = self.status_counts.get(status, 0) + 1
            self.bytes_read += len(body or b"")
            if self._log is not None:
                self._log({"route": path, "status": status, "bytes": len(body or b""),
                           "elapsed_s": round(self._clock() - started, 3), "error": error,
                           "sha256": hashlib.sha256(body or b"").hexdigest(),
                           "retry": n429 + n5xx})
            if status == 200:
                if len(body) > MAX_RESPONSE_BYTES:
                    raise SourceUnreachable(f"response over the byte cap on {path}")
                try:
                    return json.loads(body)
                except ValueError:
                    raise SourceUnreachable(f"undecodable JSON on {path}") from None
            if status == 429 and n429 < self._retries[0]:
                self._sleep(self._backoff(rheaders, n429))
                n429 += 1
                continue
            if (status is None or 500 <= status < 600) and n5xx < self._retries[1]:
                self._sleep(self._backoff(rheaders, n5xx))
                n5xx += 1
                continue
            raise SourceUnreachable(self._redact(
                f"status {status} on {path} after {n429 + n5xx} retries"
                + (f" ({error})" if error else "")))


def _paged(client, route, base_params, rows_key, groups, label):
    for page in range(MAX_PAGES):
        body = client.get_json(route, base_params + [("limit", PAGE), ("offset", page * PAGE),
                                                     ("sort", "asc")])
        rows = body.get(rows_key)
        if type(rows) is not list:
            raise SourceUnreachable(f"{route} answer has no {rows_key} array")
        groups.append({"url": client.route_prefix + route, rows_key: rows,
                       "metadata": body.get("metadata") or {}, "label": label})
        if len(rows) < PAGE:
            return
    raise IncompleteFetch(f"{label}: more than {MAX_PAGES} pages")


def fetch_window(client: ReadOnlyClient, registry: dict, start: int, end: int, *,
                 delta_seconds: int = DEFAULT_DELTA_SECONDS) -> dict:
    """Read one closed window ``[start, end)`` from the account registry; returns a batch.

    Same route set as the collector: account-scoped ``jetton_swap`` actions for every
    registered DeDust vault and STON.fi router, the vaults' transactions, the indexer actions
    of vault traces not seen in account actions, and the complete DeDust candidate traces.
    """
    if (type(start) is not int or type(end) is not int or start % 300 or end % 300
            or start >= end):
        raise ValueError("window bounds must be five-minute boundaries with start < end")
    info = client.get_json("/masterchainInfo", {})
    last = info.get("last") or {}
    as_of = last.get("gen_utime")
    if type(as_of) is str and as_of.isdigit() and as_of.isascii():
        as_of = int(as_of)  # TON Center v3 sends decimal text
    if type(as_of) is not int or as_of < 0:
        raise SourceUnreachable("masterchainInfo has no last.gen_utime")
    if as_of < end + delta_seconds:
        raise WindowNotClosed(f"indexer frontier {as_of} is not {delta_seconds}s past {end}")
    span = [("start_utime", start), ("end_utime", end - 1)]
    vaults, routers = list(registry["dedust_vaults"]), list(registry["stonfi_routers"])
    accounts, vault_txs, trace_actions, traces = [], [], [], []
    for account in vaults + routers:
        _paged(client, "/actions", [("account", account), ("action_type", "jetton_swap")] + span,
               "actions", accounts, "actions:" + account[:10])
    for vault in vaults:
        _paged(client, "/transactions", [("account", vault)] + span, "transactions", vault_txs,
               "transactions:" + vault[:10])
    seen, dedust, vault_traces = set(), set(), set()
    for group in accounts:
        for action in group["actions"]:
            seen.add(action.get("trace_id"))
            if (action.get("details") or {}).get("dex") in DEDUST_LABELS:
                dedust.add(action.get("trace_id"))
    for group in vault_txs:
        for tx in group["transactions"]:
            if tx.get("trace_id"):
                vault_traces.add(tx["trace_id"])
    trace_ids = sorted(t for t in dedust | vault_traces if t)
    uncovered = sorted(t for t in vault_traces - seen if t)
    for i in range(0, len(uncovered), TRACE_BATCH):
        _paged(client, "/actions", [("trace_id", t) for t in uncovered[i:i + TRACE_BATCH]],
               "actions", trace_actions, f"trace-actions:{i // TRACE_BATCH}")
    for i in range(0, len(trace_ids), TRACE_BATCH):
        chunk = trace_ids[i:i + TRACE_BATCH]
        body = client.get_json("/traces", [("trace_id", t) for t in chunk]
                               + [("limit", TRACE_BATCH)])
        if type(body.get("traces")) is not list:
            raise SourceUnreachable("/traces answer has no traces array")
        traces.append({"url": client.route_prefix + "/traces", "traces": body["traces"],
                       "metadata": body.get("metadata") or {}})
    return {"network": registry["network"], "window": [start, end], "as_of": as_of,
            "masterchain_seqno": last.get("seqno"),
            "native_aliases": list(registry.get("native_aliases", [])),
            "action_responses": [{k: g[k] for k in ("url", "actions", "metadata")}
                                 for g in accounts + trace_actions],
            "trace_responses": traces, "requests": client.requests_made,
            "status_counts": {str(k): v for k, v in client.status_counts.items()},
            "bytes": client.bytes_read}


def _default_sweep_policy():
    from ton_sentinel.spark.stream import SweepPolicy

    return SweepPolicy(idle_sweep_seconds=600, watermark_seconds=600, max_windows=12, admission={
        "event_retention_seconds": 7 * 86400, "repair_margin_seconds": 3600,
        "baseline_lookback_seconds": 86400, "derived_retention_seconds": 30 * 86400,
        "max_windows": 1, "max_actions": 20_000, "max_input_bytes": 200_000_000,
        "max_baseline_rows": 2_000_000})


@dataclass(frozen=True)
class CycleConfig:
    """Explicit settings of one ingest cycle (no secrets except the DSN, never printed)."""
    dsn: str
    bootstrap_servers: str
    topic: str
    topic_incarnation: str
    replay_epoch: int
    checkpoint: str
    environment_id: str
    query_id: str
    pipeline_epoch: str
    epoch: object  # contracts.SourceEpoch; must equal the active epoch
    delta_seconds: int = DEFAULT_DELTA_SECONDS
    mapping_version: str = "toncenter-v3 account-scan live_swap v1 (delta=lag at fetch)"
    score_limit: int = 200  # windows per score round (one lease each)
    score_rounds: int = 20
    owner: str = "live-monitor"
    sweep: object = field(default_factory=_default_sweep_policy)


def ingest_cycle(batch: dict, *, now: int, ops: dict) -> dict:
    """Run the production steps for one fetched batch in order; pure orchestration."""
    network = batch["network"]
    start, end = batch["window"]
    summary = {"window": [start, end], "as_of": batch["as_of"], "qualified": []}
    ingest = ops["ingest"](batch)
    summary["ingest"] = ingest
    if not ingest.get("accepted"):
        summary["status"] = "not_accepted"
        return summary
    summary["published"] = ops["publish"]()
    ops["consume"]()
    summary["sweep"] = ops["sweep"](now=now)
    summary["qualified"] = list(ops["qualify"](network=network, start=start, end=end,
                                               as_of=batch["as_of"], origin="live"))
    summary["score"] = ops["score"](network=network)
    summary["status"] = "ok"
    return summary


def production_ops(config: CycleConfig) -> dict:
    """The real steps, each on its own connection and lease (bounded by the 300 s ceiling)."""
    import psycopg

    from ton_sentinel.db.fencing import acquire_transition

    def ingest(batch):
        from ton_sentinel.ingest import OutboxBudget, ingest_range
        from ton_sentinel.outbox import configure_delivery
        from ton_sentinel.source.live import LivePages, live_swap_batch
        from ton_sentinel.source.scan import ScanPolicy

        live = live_swap_batch(batch["action_responses"], batch["trace_responses"],
                               network=batch["network"],
                               native_aliases=frozenset(batch["native_aliases"]))
        pages = LivePages(live)
        start, end = batch["window"]
        policy = ScanPolicy(start=start, end=end, page_size=100, max_pages=200,
                            max_actions=20_000, max_bytes=48_000_000, max_seconds=300,
                            max_requests=1000)
        with psycopg.connect(config.dsn) as connection:
            configure_delivery(connection, topic=config.topic,
                               topic_incarnation=config.topic_incarnation)
            result = ingest_range(connection, pages, config.epoch, policy,
                                  owner=config.owner + "-ingest",
                                  budget=OutboxBudget(max_pending_bytes=1_000_000_000,
                                                      max_oldest_age_seconds=86_400))
        return {"status": result.status, "accepted": bool(result.accepted),
                "promoted": len(result.promoted_event_ids), "start": result.start,
                "end": result.end, "payloads": len(live.payloads),
                "unsupported": len(live.unsupported), "unresolved": len(live.unresolved),
                "excluded": len(pages.excluded(config.epoch, start, end))}

    def publish():
        from kafka import KafkaProducer

        from ton_sentinel.outbox import publish_pending

        producer = KafkaProducer(bootstrap_servers=config.bootstrap_servers, acks="all",
                                 retries=3, max_block_ms=15_000, max_request_size=4_000_000)
        total = 0
        try:
            with psycopg.connect(config.dsn) as connection:
                for _ in range(50):
                    lease = acquire_transition(connection, config.owner + "-publish", 300)
                    sent = publish_pending(connection, producer, lease, limit=1000)
                    connection.commit()
                    total += len(sent)
                    if len(sent) < 1000:
                        break
            producer.flush(timeout=30)
        finally:
            producer.close()
        return total

    def consume():
        from ton_sentinel.stream_cli import ConsumerConfig, run_consumer

        run_consumer(ConsumerConfig(
            dsn=config.dsn, bootstrap_servers=config.bootstrap_servers, topic=config.topic,
            checkpoint=config.checkpoint, topic_incarnation=config.topic_incarnation,
            replay_epoch=config.replay_epoch, max_offsets_per_trigger=500,
            environment_id=config.environment_id, query_id=config.query_id,
            pipeline_epoch=config.pipeline_epoch))

    def sweep(*, now):
        from pyspark.sql import SparkSession

        from ton_sentinel.spark.stream import sweep_each

        spark = (SparkSession.builder.appName("ton-sentinel-live-sweep")
                 .config("spark.ui.enabled", "false")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        try:
            with psycopg.connect(config.dsn) as connection:
                report = sweep_each(
                    connection,
                    lambda: acquire_transition(connection, config.owner + "-sweep", 300),
                    spark, now, config.sweep)
        finally:
            spark.stop()
        return {"repaired": [[k.window_start, r] for k, r in report.repaired],
                "refused": [[k.window_start, r] for k, r in report.refused]}

    def qualify(*, network, start, end, as_of, origin):
        from ton_sentinel.db.work import qualify_live_windows

        with psycopg.connect(config.dsn) as connection:
            lease = acquire_transition(connection, config.owner + "-qualify", 300)
            return qualify_live_windows(
                connection, lease, network=network, start=start, end=end, as_of=as_of,
                delta_seconds=config.delta_seconds, mapping_version=config.mapping_version,
                origin=origin)

    def score(*, network):
        from ton_sentinel.db.work import score_pending

        totals = {"first_scored": 0, "rescored": 0, "refused": 0, "rounds": 0}
        report = {}
        with psycopg.connect(config.dsn) as connection:
            for _ in range(config.score_rounds):
                lease = acquire_transition(connection, config.owner + "-score", 300)
                report = score_pending(connection, lease, network=network,
                                       limit=config.score_limit)
                connection.commit()
                totals["rounds"] += 1
                for name in ("first_scored", "rescored", "refused"):
                    totals[name] += report[name]
                if not report["remaining_unscored"] and not report["remaining_stale"]:
                    break
        totals.update(remaining_unscored=report.get("remaining_unscored"),
                      remaining_stale=report.get("remaining_stale"))
        return totals

    return {"ingest": ingest, "publish": publish, "consume": consume, "sweep": sweep,
            "qualify": qualify, "score": score}


def ingest_batch(config: CycleConfig, batch: dict, *, now: int | None = None) -> dict:
    """One production ingest cycle for a fetched batch (``now`` defaults to the clock)."""
    return ingest_cycle(batch, now=int(time.time()) if now is None else now,
                        ops=production_ops(config))


def run_loop(environ, *, cycle: Callable, clock=time.time, sleep=time.sleep,
             max_cycles: int | None = None, lag_seconds: int = DEFAULT_LAG_SECONDS,
             on_result: Callable | None = None, should_stop: Callable | None = None,
             max_catch_up: int = 4) -> int:
    """Run ``cycle(window)`` once per closed 15-minute window, in order, serially.

    Refuses to start unless the flag is on. A ``SourceUnreachable`` stops the loop (it is
    re-raised after ``on_result``); any other error is reported and the next window runs.
    After a pause, at most ``max_catch_up`` missed windows are taken in order.
    """
    if not monitor_enabled(environ):
        raise MonitorDisabled(f"{ENABLE_ENV}=1 is required; the monitor is off by default")
    last_end, cycles = None, 0
    while max_cycles is None or cycles < max_cycles:
        if should_stop is not None and should_stop():
            break
        now = int(clock())
        newest = newest_closed_window(now, lag_seconds=lag_seconds)
        if last_end is None or newest[1] - last_end > max_catch_up * WINDOW_SECONDS:
            window = newest
        elif last_end + WINDOW_SECONDS <= newest[1]:
            window = (last_end, last_end + WINDOW_SECONDS)
        else:
            sleep(max(1, newest[1] + WINDOW_SECONDS + lag_seconds - now))
            continue
        try:
            result = cycle(window)
        except SourceUnreachable as exc:
            if on_result is not None:
                on_result(window, {"status": "source_unreachable", "error": str(exc)})
            raise
        except Exception as exc:  # noqa: BLE001 - reported by class; next window still runs
            result = {"status": "error", "error": type(exc).__name__}
        last_end, cycles = window[1], cycles + 1
        if on_result is not None:
            on_result(window, result)
    return cycles


def status_summary(connection, network: str) -> dict:
    """Read-only counters for the monitor log: live windows, rows, scores and alerts."""
    def one(sql, params=()):
        return connection.execute(sql, params).fetchone()[0]

    live = ("SELECT window_start FROM action_window_qualification "
            "WHERE network=%s AND origin='live'")
    out = {
        "live_windows_qualified": one(f"SELECT count(*) FROM ({live}) q", (network,)),
        "live_served_rows": one(
            f"SELECT count(*) FROM served_features WHERE network=%s AND window_start IN ({live})",
            (network, network)),
        "scores_live": dict(connection.execute(
            "SELECT CASE WHEN c.stale THEN 'stale' ELSE r.status END, count(*) "
            "FROM detector_current c JOIN detector_results r USING "
            "(network,token,direction,window_start,detector_id,result_revision) "
            f"WHERE c.network=%s AND c.window_start IN ({live}) GROUP BY 1",
            (network, network)).fetchall()),
        "scores_all": dict(connection.execute(
            "SELECT CASE WHEN c.stale THEN 'stale' ELSE r.status END, count(*) "
            "FROM detector_current c JOIN detector_results r USING "
            "(network,token,direction,window_start,detector_id,result_revision) "
            "WHERE c.network=%s GROUP BY 1", (network,)).fetchall()),
        "alert_revisions": one("SELECT count(*) FROM alert_revisions WHERE network=%s",
                               (network,)),
        "alerts_active": one(
            "SELECT count(*) FROM (SELECT DISTINCT ON (network,token,direction,window_start,"
            "detector_id) state FROM alert_revisions WHERE network=%s ORDER BY network,token,"
            "direction,window_start,detector_id,alert_revision DESC) a WHERE state='active'",
            (network,)),
        "triggered_live": one(
            "SELECT count(*) FROM detector_current c JOIN detector_results r USING "
            "(network,token,direction,window_start,detector_id,result_revision) "
            f"WHERE c.network=%s AND NOT c.stale AND r.triggered AND c.window_start IN ({live})",
            (network, network)),
        "sweep_closures": one("SELECT count(*) FROM window_sweep_closures WHERE network=%s",
                              (network,)),
    }
    connection.commit()
    return out


def _config_from_file(path: str) -> CycleConfig:
    from ton_sentinel.contracts import SourceEpoch

    with open(path, encoding="utf-8") as handle:
        raw = json.load(handle)
    dsn = os.environ.get("SENTINEL_DATABASE_DSN")
    bootstrap = os.environ.get("TON_SENTINEL_KAFKA_BOOTSTRAP")
    if not dsn or not bootstrap:
        raise ValueError("SENTINEL_DATABASE_DSN and TON_SENTINEL_KAFKA_BOOTSTRAP are required")
    epoch = SourceEpoch.from_json(json.dumps(raw.pop("epoch"), sort_keys=True,
                                             separators=(",", ":")))
    allowed = {f.name for f in dataclasses.fields(CycleConfig)} - {"dsn", "bootstrap_servers",
                                                                     "epoch", "sweep"}
    unknown = set(raw) - allowed
    if unknown:
        raise ValueError("unknown config keys: " + ", ".join(sorted(unknown)))
    return CycleConfig(dsn=dsn, bootstrap_servers=bootstrap, epoch=epoch, **raw)


def _client_from_env(max_requests: int, log=None) -> ReadOnlyClient:
    base, key = os.environ.get("TONCENTER_BASE_URL"), os.environ.get("TONCENTER_API_KEY")
    if not base:
        raise ValueError("TONCENTER_BASE_URL is required")
    return ReadOnlyClient(base, key, max_requests=max_requests, log=log)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="ton-sentinel live-monitor",
        description="Bounded live monitor (off unless TON_SENTINEL_LIVE_MONITOR=1).")
    sub = parser.add_subparsers(dest="command", required=True)
    fetch = sub.add_parser("fetch", help="fetch one closed window into a batch file")
    fetch.add_argument("--registry", required=True)
    fetch.add_argument("--out", required=True)
    fetch.add_argument("--window-start", type=int)
    fetch.add_argument("--max-requests", type=int, default=300)
    fetch.add_argument("--delta-seconds", type=int, default=DEFAULT_DELTA_SECONDS)
    ingest = sub.add_parser("ingest", help="ingest one batch file through the production path")
    ingest.add_argument("--batch", required=True)
    ingest.add_argument("--config", required=True)
    ingest.add_argument("--now", type=int)
    status = sub.add_parser("status", help="read-only monitor counters")
    status.add_argument("--network", required=True, choices=("mainnet", "testnet"))
    run = sub.add_parser("run", help="in-process loop (requires TON_SENTINEL_LIVE_MONITOR=1)")
    run.add_argument("--registry", required=True)
    run.add_argument("--config", required=True)
    run.add_argument("--max-cycles", type=int)
    run.add_argument("--max-requests-per-cycle", type=int, default=300)
    args = parser.parse_args(argv)
    try:
        if args.command == "fetch":
            with open(args.registry, encoding="utf-8") as handle:
                registry = json.load(handle)
            client = _client_from_env(args.max_requests)
            if args.window_start is None:
                start, end = newest_closed_window(int(time.time()))
            else:
                start, end = args.window_start, args.window_start + WINDOW_SECONDS
            batch = fetch_window(client, registry, start, end, delta_seconds=args.delta_seconds)
            with open(args.out, "w", encoding="utf-8") as handle:
                json.dump(batch, handle)
            print(json.dumps({k: batch[k] for k in ("window", "as_of", "requests",
                                                    "status_counts", "bytes")}))
            return 0
        if args.command == "ingest":
            config = _config_from_file(args.config)
            with open(args.batch, encoding="utf-8") as handle:
                batch = json.load(handle)
            print(json.dumps(ingest_batch(config, batch, now=args.now), sort_keys=True,
                             default=str))
            return 0
        if args.command == "status":
            import psycopg

            dsn = os.environ.get("SENTINEL_DATABASE_DSN")
            if not dsn:
                raise ValueError("SENTINEL_DATABASE_DSN is required")
            with psycopg.connect(dsn) as connection:
                print(json.dumps(status_summary(connection, args.network), sort_keys=True))
            return 0
        with open(args.registry, encoding="utf-8") as handle:
            registry = json.load(handle)
        config = _config_from_file(args.config)

        def cycle(window):
            client = _client_from_env(args.max_requests_per_cycle)
            batch = fetch_window(client, registry, *window, delta_seconds=config.delta_seconds)
            return ingest_batch(config, batch)

        run_loop(os.environ, cycle=cycle, max_cycles=args.max_cycles,
                 on_result=lambda w, r: print(json.dumps({"window": list(w), **r},
                                                         default=str), flush=True))
        return 0
    except MonitorDisabled as exc:
        print(json.dumps({"status": "disabled", "error": str(exc)}), file=sys.stderr)
        return 3
    except SourceUnreachable as exc:
        print(json.dumps({"status": "source_unreachable", "error": str(exc)}))
        return 4
    except Exception as exc:  # noqa: BLE001 - class only: messages can carry DSNs
        print(json.dumps({"status": "failed", "error": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
