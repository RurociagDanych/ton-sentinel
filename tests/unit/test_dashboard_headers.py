"""Security headers on every response and the /dashboard/ static route."""

import hashlib
import base64
import io
import re

import pytest

from ton_sentinel.api import create_app

CSP = ("default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
       "connect-src 'self'; font-src 'none'; base-uri 'none'; form-action 'none'; "
       "frame-ancestors 'none'")


def call(path, method="GET", headers=None, dsn="postgresql://unused/none"):
    app = create_app(dsn=dsn)
    seen = {}

    def start_response(status, response_headers, *exc):
        seen["status"], seen["headers"] = status, response_headers

    environ = {"REQUEST_METHOD": method, "PATH_INFO": path, "QUERY_STRING": "",
               "wsgi.input": io.BytesIO()}
    environ.update(headers or {})
    body = b"".join(app(environ, start_response))
    return seen["status"], {k: v for k, v in seen["headers"]}, body


def assert_secure(headers, csp=CSP):
    assert headers["Content-Security-Policy"] == csp
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["X-Frame-Options"] == "DENY"
    assert headers["Referrer-Policy"] == "no-referrer"
    assert "unsafe-inline" not in csp and "unsafe-eval" not in csp
    assert "http" not in csp


def test_json_404_and_405_send_security_headers_and_no_store():
    for path, method, code in [("/nope", "GET", "404"), ("/health", "POST", "405")]:
        status, headers, _ = call(path, method)
        assert status.startswith(code)
        assert_secure(headers)
        assert headers["Cache-Control"] == "no-store"


def test_unavailable_503_sends_security_headers():
    status, headers, body = call("/health", dsn="postgresql://127.0.0.1:1/none?connect_timeout=1")
    assert status.startswith("503")
    assert_secure(headers)
    assert headers["Cache-Control"] == "no-store"


def test_demo_page_csp_hash_matches_inline_style_of_template():
    status, headers, body = call("/")
    csp = headers["Content-Security-Policy"]
    style = re.search(rb"<style>(.*?)</style>", body, re.S).group(1)
    digest = base64.b64encode(hashlib.sha256(style).digest()).decode()
    assert f"style-src 'self' 'sha256-{digest}'" in csp
    assert "unsafe-inline" not in csp and "unsafe-eval" not in csp
    assert "<script" not in body.decode()
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["X-Frame-Options"] == "DENY"
    assert headers["Referrer-Policy"] == "no-referrer"
    assert headers["Cache-Control"] == "no-store"


def test_dashboard_index_served_with_headers_and_type():
    status, headers, body = call("/dashboard/")
    assert status.startswith("200")
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    assert_secure(headers)
    # Exactly one external module script; inline script/style stay forbidden.
    assert body.count(b"<script") == 1 and b'src="/dashboard/assets/app.js"' in body
    assert b"style=" not in body and b"<style" not in body
    assert b"http://" not in body and b"https://" not in body
    assert headers["Content-Length"] == str(len(body))


def test_dashboard_css_content_type_etag_and_conditional():
    status, headers, body = call("/dashboard/assets/app.css")
    assert status.startswith("200") and body
    assert headers["Content-Type"] == "text/css; charset=utf-8"
    assert headers["Cache-Control"] == "no-cache"
    assert headers["ETag"].startswith('"') and headers["ETag"].endswith('"')
    assert_secure(headers)
    status2, headers2, body2 = call("/dashboard/assets/app.css",
                                    headers={"HTTP_IF_NONE_MATCH": headers["ETag"]})
    assert status2.startswith("304") and body2 == b""


def test_head_returns_headers_without_body():
    status, headers, body = call("/dashboard/assets/app.css", method="HEAD")
    _, getheaders, getbody = call("/dashboard/assets/app.css")
    assert status.startswith("200") and body == b""
    assert headers["Content-Length"] == str(len(getbody))
    assert_secure(headers)


@pytest.mark.parametrize("path", [
    "/dashboard/assets/../api.py", "/dashboard/assets/%2e%2e/api.py", "/dashboard/assets/..%2fapi.py",
    "/dashboard/assets/../../api.py", "/dashboard/assets//etc/passwd", "/dashboard/assets/app.css/",
    "/dashboard/assets/unknown.css", "/dashboard/assets/", "/dashboard/assets",
    "/dashboard/assets/index.html", "/dashboard/assets/app.css\x00.png", "/dashboard/../api.py",
    "/dashboard/api.py", "/dashboard/%2e%2e/api.py", "/dashboard/assets/..\\api.py"])
def test_traversal_and_unknown_assets_are_refused(path):
    status, headers, body = call(path)
    assert status.startswith("404")
    assert b"ton_sentinel" not in body and b"import " not in body
    assert_secure(headers)


@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE", "OPTIONS", "TRACE"])
@pytest.mark.parametrize("path", ["/dashboard/", "/dashboard/assets/app.css", "/dashboard/assets/../x"])
def test_only_get_and_head_allowed_on_dashboard(method, path):
    status, headers, _ = call(path, method)
    assert status.startswith("405")
    assert headers["Allow"] == "GET, HEAD"
    assert_secure(headers)


def test_existing_api_routes_keep_get_only_contract():
    status, headers, body = call("/v1/features", "HEAD")
    assert status.startswith("405") and headers["Allow"] == "GET"
    assert body == b'{"error":"read_only"}'


def test_static_files_are_packaged_data():
    import tomllib
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    data = tomllib.loads((root / "pyproject.toml").read_text())
    patterns = data["tool"]["setuptools"]["package-data"]["ton_sentinel"]
    assert "dashboard/*.html" in patterns and "dashboard/*.css" in patterns
