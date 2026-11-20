"""Unit contract: the bounded live monitor (flag, schedule, polite read-only client, cycle)."""

from __future__ import annotations

import json

import pytest

from ton_sentinel import live_monitor as lm

BASE = "https://indexer.example/api/v3"
KEY = "k" * 24


class Clock:
    def __init__(self, start=1000.0):
        self.now = start
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class Transport:
    """Scripted (status, headers, body) answers; records every request."""
    def __init__(self, clock, answers):
        self.clock, self.answers, self.calls = clock, list(answers), []

    def __call__(self, method, url, headers, timeout):
        self.calls.append({"method": method, "url": url, "headers": dict(headers),
                           "at": self.clock.now})
        status, hdrs, body = self.answers.pop(0) if self.answers else (200, {}, {"ok": 1})
        return status, hdrs, json.dumps(body).encode()


def client(clock, answers, **kw):
    transport = Transport(clock, answers)
    return lm.ReadOnlyClient(BASE, KEY, transport=transport, clock=clock.time,
                             sleep=clock.sleep, **kw), transport


@pytest.mark.parametrize("env,expected", [({}, False), ({"TON_SENTINEL_LIVE_MONITOR": "0"}, False),
                                          ({"TON_SENTINEL_LIVE_MONITOR": "true"}, False),
                                          ({"TON_SENTINEL_LIVE_MONITOR": "1"}, True)])
def test_monitor_is_off_unless_the_flag_is_exactly_one(env, expected):
    assert lm.monitor_enabled(env) is expected


def test_run_loop_refuses_to_start_when_the_flag_is_off():
    with pytest.raises(lm.MonitorDisabled):
        lm.run_loop({}, cycle=lambda window: {"status": "ok"}, clock=Clock().time,
                    sleep=Clock().sleep, max_cycles=1)


def test_newest_closed_window_is_the_last_whole_quarter_hour_behind_the_lag():
    assert lm.newest_closed_window(1800 + 179) == (0, 900)
    assert lm.newest_closed_window(1800 + 180) == (900, 1800)
    start, end = lm.newest_closed_window(1794915900 + 200)
    assert (start, end) == (1794915000, 1794915900) and end - start == 900


def test_run_loop_runs_one_cycle_per_closed_window_every_fifteen_minutes():
    clock, windows = Clock(start=900 * 10 + 181), []
    lm.run_loop({"TON_SENTINEL_LIVE_MONITOR": "1"},
                cycle=lambda window: windows.append(window) or {"status": "ok"},
                clock=clock.time, sleep=clock.sleep, max_cycles=3)
    assert windows == [(8100, 9000), (9000, 9900), (9900, 10800)]
    assert all(s <= 900 for s in clock.sleeps)


def test_run_loop_stops_when_the_provider_is_unreachable():
    clock, seen = Clock(start=900 * 10 + 181), []

    def cycle(window):
        seen.append(window)
        raise lm.SourceUnreachable("status 503 after retries")

    with pytest.raises(lm.SourceUnreachable):
        lm.run_loop({"TON_SENTINEL_LIVE_MONITOR": "1"}, cycle=cycle, clock=clock.time,
                    sleep=clock.sleep, max_cycles=5)
    assert len(seen) == 1


def test_requests_are_serial_and_at_most_one_per_second():
    clock = Clock()
    c, transport = client(clock, [])
    for _ in range(4):
        c.get_json("/actions", {"account": "A"})
    starts = [call["at"] for call in transport.calls]
    assert all(b - a >= 1.0 for a, b in zip(starts, starts[1:]))
    assert c.requests_made == 4


def test_minimum_interval_below_one_second_is_refused():
    with pytest.raises(ValueError):
        lm.ReadOnlyClient(BASE, KEY, min_interval_seconds=0.5)


def test_key_goes_only_into_the_header_and_never_into_the_url_or_log():
    clock, log = Clock(), []
    c, transport = client(clock, [], log=log.append)
    c.get_json("/masterchainInfo", {})
    call = transport.calls[0]
    assert call["method"] == "GET"
    assert call["headers"]["X-Api-Key"] == KEY
    assert KEY not in call["url"] and call["url"].startswith(BASE + "/masterchainInfo")
    assert KEY not in json.dumps(log) and log[0]["status"] == 200


def test_only_reviewed_read_routes_on_the_configured_host_are_allowed():
    c, _ = client(Clock(), [])
    for path in ("/sendBoc", "/runGetMethod", "https://other.example/api/v3/actions",
                 "//other.example/actions", "/actions/../sendBoc"):
        with pytest.raises(ValueError):
            c.get_json(path, {})
    with pytest.raises(ValueError):
        c.get_json("/actions", {"api_key": "x"})


@pytest.mark.parametrize("base", ["http://indexer.example/api/v3", "https://u:p@indexer.example",
                                  "https://indexer.example/api/v3?x=1", ""])
def test_base_url_must_be_plain_https(base):
    with pytest.raises(ValueError):
        lm.ReadOnlyClient(base, KEY)


def test_429_honours_retry_after_then_succeeds():
    clock = Clock()
    c, transport = client(clock, [(429, {"retry-after": "7"}, {}), (200, {}, {"ok": 2})])
    assert c.get_json("/actions", {"account": "A"}) == {"ok": 2}
    assert 7 in clock.sleeps
    assert transport.calls[1]["at"] - transport.calls[0]["at"] >= 7
    assert c.requests_made == 2 and c.status_counts == {429: 1, 200: 1}


def test_5xx_backs_off_exponentially_and_gives_up_as_unreachable():
    clock = Clock()
    c, transport = client(clock, [(503, {}, {})] * 10, max_retries_5xx=3)
    with pytest.raises(lm.SourceUnreachable) as raised:
        c.get_json("/traces", {"trace_id": "t"})
    assert len(transport.calls) == 4
    gaps = [b["at"] - a["at"] for a, b in zip(transport.calls, transport.calls[1:])]
    assert gaps[0] >= 5 and gaps[1] >= 10 and gaps[2] >= 20
    assert KEY not in str(raised.value)


def test_request_budget_is_a_hard_stop():
    c, transport = client(Clock(), [], max_requests=2)
    c.get_json("/actions", {})
    c.get_json("/actions", {})
    with pytest.raises(lm.RequestBudgetExceeded):
        c.get_json("/actions", {})
    assert len(transport.calls) == 2


def test_fetch_window_queries_the_registry_then_the_dedust_traces():
    clock = Clock()
    answers = [(200, {}, {"last": {"seqno": 5, "gen_utime": 2000}})]
    answers += [(200, {}, {"actions": [{"trace_id": "T1", "details": {"dex": "dedust"}}]})]
    answers += [(200, {}, {"actions": []})]
    answers += [(200, {}, {"transactions": [{"trace_id": "T2"}]})]
    answers += [(200, {}, {"actions": [{"trace_id": "T2"}]})]
    answers += [(200, {}, {"traces": [{"trace_id": "T1"}, {"trace_id": "T2"}]})]
    c, transport = client(clock, answers)
    registry = {"network": "mainnet", "dedust_vaults": ["VAULT"], "stonfi_routers": ["ROUTER"],
                "native_aliases": ["0:AB"]}
    batch = lm.fetch_window(c, registry, 900, 1800)
    paths = [call["url"].split("?")[0].rsplit("/", 1)[1] for call in transport.calls]
    assert paths == ["masterchainInfo", "actions", "actions", "transactions", "actions", "traces"]
    assert "start_utime=900" in transport.calls[1]["url"]
    assert "end_utime=1799" in transport.calls[1]["url"]
    assert batch["window"] == [900, 1800] and batch["as_of"] == 2000
    assert batch["native_aliases"] == ["0:AB"] and batch["network"] == "mainnet"
    assert [g["url"] for g in batch["action_responses"]] == ["/api/v3/actions"] * 3
    assert [g["url"] for g in batch["trace_responses"]] == ["/api/v3/traces"]
    assert batch["requests"] == 6
    assert KEY not in json.dumps(batch)


def test_fetch_window_refuses_a_window_that_is_not_yet_behind_the_indexer_frontier():
    c, _ = client(Clock(), [(200, {}, {"last": {"seqno": 5, "gen_utime": 1800}})])
    registry = {"network": "mainnet", "dedust_vaults": [], "stonfi_routers": [],
                "native_aliases": []}
    with pytest.raises(lm.WindowNotClosed):
        lm.fetch_window(c, registry, 900, 1800, delta_seconds=120)


def test_ingest_cycle_runs_the_production_steps_in_order_and_qualifies_the_accepted_range():
    calls = []

    def step(name, result):
        def run(*args, **kw):
            calls.append((name, args, kw))
            return result
        return run

    ops = {
        "ingest": step("ingest", {"status": "accepted", "accepted": True, "promoted": 12,
                                  "start": 900, "end": 1800}),
        "publish": step("publish", 12),
        "consume": step("consume", None),
        "sweep": step("sweep", {"repaired": 3, "refused": 0}),
        "qualify": step("qualify", [900, 1200, 1500]),
        "score": step("score", {"first_scored": 4, "rescored": 2}),
    }
    batch = {"network": "mainnet", "window": [900, 1800], "as_of": 2000}
    summary = lm.ingest_cycle(batch, now=3000, ops=ops)
    assert [c[0] for c in calls] == ["ingest", "publish", "consume", "sweep", "qualify", "score"]
    qualify = calls[4][2]
    assert (qualify["start"], qualify["end"], qualify["as_of"]) == (900, 1800, 2000)
    assert qualify["origin"] == "live"
    assert summary["qualified"] == [900, 1200, 1500] and summary["status"] == "ok"


def test_ingest_cycle_qualifies_nothing_when_the_range_was_not_accepted():
    calls = []
    ops = {name: (lambda name: lambda *a, **k: calls.append(name) or {})(name)
           for name in ("publish", "consume", "sweep", "qualify", "score")}
    ops["ingest"] = lambda *a, **k: calls.append("ingest") or {
        "status": "pending", "accepted": False, "promoted": 0, "start": 900, "end": 1800}
    summary = lm.ingest_cycle({"network": "mainnet", "window": [900, 1800], "as_of": 2000},
                              now=3000, ops=ops)
    assert "qualify" not in calls and summary["status"] == "not_accepted"
    assert summary["qualified"] == []


def test_qualification_windows_need_the_frontier_past_each_window_end_plus_delta():
    assert lm.qualifiable_windows(900, 1800, as_of=2000, delta_seconds=120) == [900, 1200, 1500]
    assert lm.qualifiable_windows(900, 1800, as_of=1850, delta_seconds=120) == [900, 1200]
    assert lm.qualifiable_windows(900, 1800, as_of=1000, delta_seconds=120) == []
    with pytest.raises(ValueError):
        lm.qualifiable_windows(901, 1800, as_of=2000, delta_seconds=120)


def test_masterchain_gen_utime_given_as_decimal_text_is_the_frontier():
    # TON Center v3 answers gen_utime as a decimal string (raw record 643); first live cycle failed.
    c, _ = client(Clock(), [(200, {}, {"last": {"seqno": 5, "gen_utime": "1700"}})])
    registry = {"network": "mainnet", "dedust_vaults": [], "stonfi_routers": [],
                "native_aliases": []}
    with pytest.raises(lm.WindowNotClosed):
        lm.fetch_window(c, registry, 900, 1800, delta_seconds=120)
    c, _ = client(Clock(), [(200, {}, {"last": {"seqno": 5, "gen_utime": "2000"}})])
    assert lm.fetch_window(c, registry, 900, 1800, delta_seconds=120)["as_of"] == 2000
    for bad in ("", "12a", "-5", None, 1.5):
        c, _ = client(Clock(), [(200, {}, {"last": {"seqno": 5, "gen_utime": bad}})])
        with pytest.raises(lm.SourceUnreachable):
            lm.fetch_window(c, registry, 900, 1800, delta_seconds=120)
