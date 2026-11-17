#!/usr/bin/env python3
"""Verify the dashboard in real headless Chrome against a LIVE api (no stubs).

Drives Chrome over CDP (stdlib websocket client), per view: screenshot, non-empty body,
server banner, CSP/console counts, unavailable/not_measured never rendered as zero, and
WCAG contrast of every text element from computed colours. Reuses harness helpers.
Usage: dashboard_live_check.py [--base http://127.0.0.1:8080] [--out DIR] [--json PATH]
"""
import argparse
import base64
import json
import os
import re
import socket
import struct
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dashboard_browser_smoke as harness  # noqa: E402

NOT_VALUES = ("unavailable", "not_measured", "none_recorded", "unknown", "not_configured", "not_qualified")


def parse_color(text):
    nums = [float(x) for x in re.findall(r"[\d.]+", text)]
    if len(nums) < 3:
        return None
    return tuple(nums[:3]) + ((nums[3],) if len(nums) > 3 else (1.0,))


def _lin(c):
    c /= 255
    return c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4


def luminance(rgb):
    r, g, b = (_lin(c) for c in rgb[:3])
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast_ratio(fg, bg):
    a, b = luminance(fg), luminance(bg)
    hi, lo = max(a, b), min(a, b)
    return (hi + 0.05) / (lo + 0.05)


class WS:
    def __init__(self, url):
        m = re.match(r"ws://([^:/]+):(\d+)(/.*)", url)
        self.sock = socket.create_connection((m[1], int(m[2])), timeout=30)
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.sendall((f"GET {m[3]} HTTP/1.1\r\nHost: {m[1]}:{m[2]}\r\nUpgrade: websocket\r\n"
                           f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n").encode())
        buf = b""
        while b"\r\n\r\n" not in buf:
            buf += self.sock.recv(1)
        assert b" 101 " in buf.split(b"\r\n")[0], buf
        self.n = 0

    def send(self, obj):
        data = json.dumps(obj).encode()
        hdr = b"\x81"
        ln = len(data)
        hdr += bytes([0x80 | ln]) if ln < 126 else (bytes([0x80 | 126]) + struct.pack(">H", ln) if ln < 65536
                                                   else bytes([0x80 | 127]) + struct.pack(">Q", ln))
        mask = os.urandom(4)
        self.sock.sendall(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

    def _read(self, n):
        out = b""
        while len(out) < n:
            chunk = self.sock.recv(n - len(out))
            if not chunk:
                raise EOFError
            out += chunk
        return out

    def recv(self):
        msg = b""
        while True:
            b1, b2 = self._read(2)
            ln = b2 & 0x7F
            if ln == 126:
                ln = struct.unpack(">H", self._read(2))[0]
            elif ln == 127:
                ln = struct.unpack(">Q", self._read(8))[0]
            payload = self._read(ln)
            if b1 & 0x0F in (1, 0, 2):
                msg += payload
                if b1 & 0x80:
                    return json.loads(msg)

    def settimeout(self, t):
        self.sock.settimeout(t)


class Page:
    def __init__(self, port):
        req = urllib.request.Request(f"http://127.0.0.1:{port}/json/new?about:blank", method="PUT")
        self.ws = WS(json.load(urllib.request.urlopen(req))["webSocketDebuggerUrl"])
        self.events = []
        self.call("Page.enable")
        self.call("Runtime.enable")
        self.call("Log.enable")
        self.call("Network.enable")
        self.call("Emulation.setDeviceMetricsOverride", width=1280, height=900, deviceScaleFactor=1, mobile=False)

    def call(self, method, **params):
        self.ws.n += 1
        mid = self.ws.n
        self.ws.send({"id": mid, "method": method, "params": params})
        while True:
            m = self.ws.recv()
            if m.get("id") == mid:
                if "error" in m:
                    raise RuntimeError(m["error"])
                return m.get("result", {})
            self.events.append(m)

    def pump(self, seconds):
        end = time.time() + seconds
        self.ws.settimeout(0.3)
        while time.time() < end:
            try:
                self.events.append(self.ws.recv())
            except (socket.timeout, TimeoutError):
                pass
        self.ws.settimeout(30)

    def eval(self, expr):
        r = self.call("Runtime.evaluate", expression=expr, returnByValue=True, awaitPromise=True)
        if "exceptionDetails" in r:
            raise RuntimeError(r["exceptionDetails"])
        return r["result"].get("value")


JS_PROBE = r"""(() => {
  const body = document.body;
  const out = {state: body.dataset.state, text: body.innerText.trim().length,
    banner: (document.getElementById('honesty-banner-text')||{}).textContent || '',
    view: (document.querySelector('[data-view]')||{dataset:{}}).dataset.view || null,
    links: [...document.querySelectorAll('a[href^="#/"]')].map(a => a.getAttribute('href')),
    zero: [], colors: []};
  const states = %s;
  for (const el of document.querySelectorAll('[data-state]')) {
    if (el === body) continue;
    if (states.includes(el.dataset.state)) {
      const t = el.innerText.replace(/\s+/g,' ');
      if (/(^|\s)0(\.0+)?(\s|$)/.test(t.replace(el.dataset.state.replace(/_/g,' '),'')))
        out.zero.push({id: el.dataset.rowId||el.className, state: el.dataset.state, text: t.slice(0,120)});
    }
  }
  const bgOf = (el) => { // first non-transparent ancestor background
    for (let e = el; e; e = e.parentElement) {
      const c = getComputedStyle(e).backgroundColor;
      const m = c.match(/[\d.]+/g).map(Number);
      if (m.length < 4 || m[3] > 0) return c;
    }
    return 'rgb(0, 0, 0)';
  };
  const seen = new Set();
  for (const el of document.querySelectorAll('body *')) {
    const own = [...el.childNodes].filter(n => n.nodeType === 3 && n.textContent.trim()).length;
    if (!own || el.offsetParent === null && getComputedStyle(el).position !== 'fixed') continue;
    const cs = getComputedStyle(el);
    const key = [el.tagName, el.className, cs.color, bgOf(el), cs.fontSize, cs.fontWeight].join('|');
    if (seen.has(key)) continue; seen.add(key);
    out.colors.push({tag: el.tagName.toLowerCase(), cls: String(el.className).slice(0,60), fg: cs.color, bg: bgOf(el),
      size: parseFloat(cs.fontSize), weight: cs.fontWeight, text: el.textContent.trim().slice(0,40)});
  }
  return out;
})()"""


def contrast_findings(colors):
    bad = []
    for c in colors:
        fg, bg = parse_color(c["fg"]), parse_color(c["bg"])
        if not fg or not bg:
            continue
        ratio = contrast_ratio(fg, bg)
        large = c["size"] >= 24 or (c["size"] >= 18.66 and int(c["weight"]) >= 700)
        if ratio < (3.0 if large else 4.5):
            bad.append({**c, "ratio": round(ratio, 2)})
    return bad


def visit(page, base, route, wait=4.0):
    page.events.clear()
    page.call("Page.navigate", url=f"{base}/dashboard/{route}")
    page.pump(1.0)
    deadline = time.time() + 20
    while time.time() < deadline:
        page.pump(0.5)
        if page.eval("document.body.dataset.state") not in (None, "loading"):
            break
    page.pump(wait)


def fetch_link_sets(base):
    """Pick live drill-down targets from the rendered DOM only (no API-side knowledge)."""
    return {}


def console_counts(events):
    errors, csp = [], []
    for m in events:
        meth, p = m.get("method"), m.get("params", {})
        text = None
        if meth == "Network.responseReceived" and p["response"]["status"] >= 400:
            text = f'HTTP {p["response"]["status"]} {p["response"]["url"]}'
            errors.append(text)
            continue
        if meth == "Log.entryAdded" and p["entry"]["level"] in ("error", "warning"):
            text = f'{p["entry"]["source"]}: {p["entry"]["text"]}'
        elif meth == "Runtime.consoleAPICalled" and p["type"] in ("error", "warning"):
            text = " ".join(str(a.get("value", a.get("description", ""))) for a in p["args"])
        elif meth == "Runtime.exceptionThrown":
            text = p["exceptionDetails"]["text"]
        if text is not None:
            errors.append(text)
            if re.search(r"Content Security Policy|Content-Security-Policy|Refused to", text):
                csp.append(text)
    return errors, csp


def start_overlay(live_base):
    """Serve /dashboard/* from this working tree; proxy every other path to the live API (data stays live)."""
    import threading
    import urllib.error
    from wsgiref.simple_server import make_server
    local = harness.create_app(dsn="postgresql://unused.invalid/none")

    def app(environ, start_response):
        path = environ.get("PATH_INFO", "")
        if path.startswith("/dashboard") or path == "/favicon.ico":
            return local(environ, start_response)
        url = live_base + path + ("?" + environ["QUERY_STRING"] if environ.get("QUERY_STRING") else "")
        try:
            resp = urllib.request.urlopen(url, timeout=30)
            code, hdrs, body = resp.status, resp.headers, resp.read()
        except urllib.error.HTTPError as exc:
            code, hdrs, body = exc.code, exc.headers, exc.read()
        start_response(f"{code} X", [("Content-Type", hdrs.get("Content-Type", "application/json")),
                                     ("Content-Length", str(len(body)))])
        return [body]

    server = make_server("127.0.0.1", 0, app, handler_class=harness._Quiet)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_port}"


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8080")
    ap.add_argument("--out", default=str(harness.ROOT / ".cache/dashboard-screenshots"))
    ap.add_argument("--json")
    ap.add_argument("--tree-assets", action="store_true",
                    help="serve /dashboard from this working tree, proxy data paths to --base")
    args = ap.parse_args(argv)
    chrome = harness.find_chrome()
    if not chrome:
        print(json.dumps({"status": "skipped", "reason": "no chrome"}))
        return 0
    try:
        urllib.request.urlopen(args.base + "/status", timeout=5).read()
    except Exception as exc:  # provider unreachable: report, do not fake
        print(json.dumps({"status": "unreachable", "base": args.base, "error": str(exc)}))
        return 2
    # The banner follows the recorded row origin, so the expected text is the one the
    # server publishes in /v1/policy, and it must be one of the three known banners.
    known = (harness.dashboard_payload.HONESTY_BANNER, harness.dashboard_payload.LIVE_BANNER,
             harness.dashboard_payload.MIXED_BANNER)
    expected_banner = json.load(urllib.request.urlopen(args.base + "/v1/policy",
                                                       timeout=10))["honesty_banner"]
    if expected_banner not in known:
        print(json.dumps({"status": "unknown_banner", "base": args.base}))
        return 2
    if args.tree_assets:
        _server, args.base = start_overlay(args.base)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    port = 9400 + os.getpid() % 500
    results = []
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as profile:
        proc = subprocess.Popen([chrome, "--headless=new", "--no-sandbox", "--disable-gpu", "--no-first-run",
                                 "--disable-extensions", f"--user-data-dir={profile}",
                                 f"--remote-debugging-port={port}", "about:blank"],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(50):
                try:
                    urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1)
                    break
                except Exception:
                    time.sleep(0.2)
            page = Page(port)
            plan = [("overview", "#/overview"), ("explorer", "#/explorer"), ("detections", "#/detections"),
                    ("alerts", "#/alerts"), ("quality", "#/quality"), ("pipeline", "#/pipeline")]
            drill = {"explorer": r"^#/explorer/.+", "detections": r"^#/detections/.+", "alerts": r"^#/alerts/.+"}
            probe = JS_PROBE % json.dumps(list(NOT_VALUES))
            for name, route in plan:
                steps = [(name, route)]
                visit(page, args.base, route)
                info = page.eval(probe)
                targets = [h for h in info["links"] if name in drill and re.match(drill[name], h)]
                if targets:
                    steps.append((name + "-detail", targets[0]))
                for label, r in steps:
                    if label != name:
                        page.events.clear()
                        page.eval(f"location.hash = {json.dumps(r)}")
                        page.pump(5)
                        info = page.eval(probe)
                    else:
                        pass
                    errors, csp = console_counts(page.events)
                    shot = out / f"{label}.png"
                    shot.write_bytes(base64.b64decode(page.call("Page.captureScreenshot", format="png",
                                                                captureBeyondViewport=True)["data"]))
                    bad = contrast_findings(info["colors"])
                    problems = []
                    if info["state"] != "ready":
                        problems.append(f"body state {info['state']!r}")
                    if info["text"] <= 0:
                        problems.append("empty body text")
                    if info["banner"] != expected_banner:
                        problems.append("server banner missing or differs")
                    if csp:
                        problems.append("CSP violations")
                    if errors:
                        problems.append("console errors")
                    if info["zero"]:
                        problems.append("unavailable/not_measured rendered as zero")
                    if bad:
                        problems.append("contrast < 4.5:1")
                    results.append({"view": label, "route": r, "screenshot": str(shot), "state": info["state"],
                                    "body_text_chars": info["text"], "banner_present": bool(info["banner"]),
                                    "console_errors": len(errors), "csp_violations": len(csp),
                                    "console_samples": errors[:3], "zero_misrender": info["zero"],
                                    "contrast_checked": len(info["colors"]), "contrast_failures": bad,
                                    "problems": problems})
        finally:
            proc.terminate()
            try:
                proc.wait(10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
    report = {"base": args.base, "results": results, "ok": all(not r["problems"] for r in results)}
    if args.json:
        Path(args.json).write_text(json.dumps(report, indent=2))
    for r in results:
        print(f"{r['view']:18} {'OK ' if not r['problems'] else 'BAD'} csp={r['csp_violations']} "
              f"console={r['console_errors']} text={r['body_text_chars']} contrast_bad={len(r['contrast_failures'])} "
              f"{r['problems']}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
