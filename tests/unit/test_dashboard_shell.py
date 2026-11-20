"""SPA shell, fetch client, design system and browser-harness helpers."""

import importlib.util
import io
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from ton_sentinel import api
from ton_sentinel.api import create_app

ROOT = Path(__file__).resolve().parents[2]
DASH = ROOT / "src" / "ton_sentinel" / "dashboard"
JS_FILES = ["app.js", "client.js", "router.js", "views.js", "states.js", "model.js", "overview.js",
            "explorer-model.js", "explorer.js", "detections-model.js", "detections.js",
            "history-model.js", "history.js", "quality-model.js", "quality.js",
            "pipeline-model.js", "pipeline.js"]
VIEWS = ["overview", "explorer", "detections", "alerts", "quality", "pipeline"]
SVG_NS = "http://www.w3.org/2000/svg"


def call(path, method="GET"):
    seen = {}

    def start_response(status, headers, *exc):
        seen["status"], seen["headers"] = status, dict(headers)

    environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": "",
               "wsgi.input": io.BytesIO()}
    body = b"".join(create_app(dsn="postgresql://unused/none")(environ, start_response))
    return seen["status"], seen["headers"], body


def text(name):
    return (DASH / name).read_text(encoding="utf-8")


@pytest.mark.parametrize("name", JS_FILES)
def test_js_served_with_security_headers(name):
    status, headers, body = call(f"/dashboard/assets/{name}")
    assert status.startswith("200")
    assert headers["Content-Type"].startswith("text/javascript")
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert "script-src 'self'" in headers["Content-Security-Policy"]
    assert body == (DASH / name).read_bytes()


def test_every_packaged_dashboard_file_is_allowlisted_and_packaged():
    packaged = {p.name for p in DASH.iterdir() if p.is_file()}
    assert packaged == {"index.html", "app.css", *JS_FILES}
    assert set(api._DASHBOARD_ASSETS) == {"app.css", *JS_FILES}
    pyproject = (ROOT / "pyproject.toml").read_text()
    assert "dashboard/*.js" in pyproject


def test_index_has_no_inline_script_style_or_handlers():
    html = text("index.html")
    scripts = re.findall(r"<script\b[^>]*>(.*?)</script>", html, re.S | re.I)
    assert scripts == [""], "exactly one external module script, empty body"
    assert re.search(r'<script[^>]+type="module"[^>]+src="/dashboard/assets/app\.js"', html)
    assert not re.search(r"<style\b", html, re.I)
    assert not re.search(r"\sstyle\s*=", html, re.I)
    assert not re.search(r"\son[a-z]+\s*=", html, re.I)
    assert not re.search(r"<form\b", html, re.I)


def test_index_references_only_same_origin_dashboard_paths():
    html = text("index.html")
    refs = re.findall(r'(?:src|href|action)\s*=\s*"([^"]*)"', html)
    assert refs, "shell must reference its assets"
    for ref in refs:
        assert ref.startswith("/dashboard/assets/") or ref.startswith("#"), ref
    assert "http://" not in html and "https://" not in html and "//cdn" not in html


@pytest.mark.parametrize("name", JS_FILES)
def test_js_has_no_unsafe_apis_or_remote_origins(name):
    src = text(name)
    for forbidden in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write",
                      "eval(", "new Function", "XMLHttpRequest", "WebSocket", "sendBeacon",
                      "importScripts", "localStorage", "sessionStorage", "document.cookie"):
        assert forbidden not in src, (name, forbidden)
    assert not re.search(r"set(?:Timeout|Interval)\(\s*[\"'`]", src)
    assert not re.search(r"\.style\s*[.=\[]|setAttribute\(\s*[\"']style", src)
    for url in re.findall(r"(?:https?:)?//[A-Za-z0-9.-]+\.[A-Za-z]{2,}[^\s\"'`)]*", src):
        assert url == SVG_NS, (name, url)
    assert "https://" not in src.replace(SVG_NS, "") and "http://" not in src.replace(SVG_NS, "")
    for spec in re.findall(r"""(?:from|import)\s*\(?\s*["']([^"']+)["']""", src):
        assert spec.startswith("./"), (name, spec)


def test_client_only_issues_get_to_same_origin_and_fails_closed():
    src = text("client.js")
    assert "fetch(" in src
    assert 'method: "GET"' in src and 'credentials: "omit"' in src
    for verb in ("POST", "PUT", "PATCH", "DELETE"):
        assert verb not in src
    assert "AbortController" in src and "10000" in src
    assert 'startsWith("/")' in src and 'startsWith("//")' in src
    for key in ("provenance", "source_kind", "certification_state", "coverage_state",
                "scoring_state", "honesty_banner", "generated_at"):
        assert key in src, key
    assert "ProvenanceError" in src
    # fetch only appears in client.js; views and shell go through it
    for other in ("app.js", "router.js", "views.js", "states.js", "model.js", "overview.js",
                  "explorer-model.js", "explorer.js", "detections-model.js", "detections.js",
                  "history-model.js", "history.js", "quality-model.js", "quality.js",
                  "pipeline-model.js", "pipeline.js"):
        assert "fetch(" not in text(other)


def test_router_declares_six_hash_views_and_stubs():
    router, views = text("router.js"), text("views.js")
    for name in VIEWS:
        assert f'"{name}"' in router and f'"{name}"' in views, name
    assert "hashchange" in router
    assert "#/overview" in router


def test_shell_wires_policy_banner_and_blocking_state():
    html, app = text("index.html"), text("app.js")
    for element_id in ("honesty-banner", "provenance-bar", "blocking-state", "view-root", "network-select"):
        assert f'id="{element_id}"' in html, element_id
    assert "/v1/policy" in app
    assert "honesty_banner" in app and "textContent" in app
    assert "dismiss" not in html.lower() + app.lower()
    assert "hidden" in app
    assert "network-select" in app


def test_css_is_single_file_without_remote_or_import():
    css = text("app.css")
    assert len(css.splitlines()) > 40
    assert "@import" not in css and "http://" not in css and "https://" not in css
    assert not re.search(r"url\(\s*[\"']?(?!data:)", css)
    for token in ("--color-", "--space-", "#honesty-banner", "#blocking-state", "prefers-reduced-motion"):
        assert token in css, token
    assert "[hidden]" in css


NODE = shutil.which("node")


@pytest.mark.skipif(NODE is None, reason="node not installed")
def test_client_logic_validates_provenance_under_node(tmp_path):
    script = tmp_path / "check.mjs"
    script.write_text(f"""
import {{ validateProvenance, ProvenanceError, checkedPath }} from "file://{DASH}/client.js";
const good = {{source_kind:"unverified_origin", certification_state:"not_certified", coverage_state:"unrecorded",
  scoring_state:"none_recorded", honesty_banner:"x", generated_at:"2026-01-01T00:00:00Z"}};
const out = [];
validateProvenance({{provenance: good}}); out.push("ok");
for (const bad of [{{}}, {{provenance:null}}, {{provenance:[]}}, {{provenance:{{...good, source_kind:""}}}},
    {{provenance:{{...good, certification_state:undefined}}}}, null, "x"]) {{
  try {{ validateProvenance(bad); out.push("accepted"); }} catch (e) {{ out.push(e instanceof ProvenanceError ? "rejected" : "other"); }}
}}
for (const p of ["/v1/policy", "//evil.example/x", "https://evil.example/", "v1/policy"]) {{
  try {{ checkedPath(p); out.push("path-ok"); }} catch (e) {{ out.push("path-rejected"); }}
}}
console.log(JSON.stringify(out));
""")
    result = subprocess.run([NODE, str(script)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == ["ok"] + ["rejected"] * 7 + [
        "path-ok", "path-rejected", "path-rejected", "path-rejected"]


def load_harness():
    spec = importlib.util.spec_from_file_location("dashboard_browser_smoke",
                                                  ROOT / "scripts" / "dashboard_browser_smoke.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_harness_classifies_console_output():
    h = load_harness()
    log = "\n".join([
        '[1:2:INFO:CONSOLE(5)] "ordinary info", source: http://127.0.0.1:1/dashboard/assets/app.js (5)',
        '[1:2:ERROR:CONSOLE(1)] "Refused to execute inline script because it violates the following '
        "Content Security Policy directive: script-src 'self'\", source: http://127.0.0.1:1/ (1)",
        '[1:2:ERROR:CONSOLE(9)] "Uncaught TypeError: x", source: http://127.0.0.1:1/ (9)',
        "[1:2:ERROR:gpu_process_host.cc(1)] GPU noise",
    ])
    result = h.classify_console(log)
    assert len(result["csp_violations"]) == 1
    assert len(result["console_errors"]) == 2
    assert h.classify_console("")["console_errors"] == []


def test_harness_is_skippable_without_chrome(monkeypatch):
    h = load_harness()
    monkeypatch.setattr(h.shutil, "which", lambda name: None)
    assert h.find_chrome() is None
    assert h.run_scenarios(chrome=None)["status"] == "skipped"


def test_harness_skipped_by_env(monkeypatch):
    h = load_harness()
    monkeypatch.setenv("SENTINEL_SKIP_BROWSER", "1")
    assert h.run_scenarios(chrome="/bin/true")["status"] == "skipped"
