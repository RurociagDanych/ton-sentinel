"""Dashboard A1 over real HTTP + real PostgreSQL: bodies unchanged, headers added."""
import base64
import hashlib
import json
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from ton_sentinel.api import _read_status, _render_demo
from ton_sentinel.db.work import promote_scores, score_window
from ton_sentinel.redaction import redact
from tests.integration.test_api_truthful_status_pg import _dsn, served  # noqa: F401
from tests.integration.test_detection import NET, SCORED, TOK, ready_world
from tests.integration.test_revisions import db  # noqa: F401

CSP_API = ("default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
           "connect-src 'self'; font-src 'none'; base-uri 'none'; form-action 'none'; "
           "frame-ancestors 'none'")


def mask_clock(raw):
    """Only the wall-clock age fields legitimately differ between two reads."""
    return re.sub(rb'(_age_seconds":)[0-9.eE+-]+', rb"\1<t>", raw)


def request(base, path, method="GET"):
    try:
        with urlopen(Request(base + path, method=method), timeout=8) as response:
            return response.status, dict(response.headers), response.read()
    except HTTPError as error:
        return error.code, dict(error.headers), error.read()


def assert_common(headers):
    assert headers["X-Content-Type-Options"] == "nosniff"
    assert headers["X-Frame-Options"] == "DENY"
    assert headers["Referrer-Policy"] == "no-referrer"
    csp = headers["Content-Security-Policy"]
    assert "unsafe-inline" not in csp and "unsafe-eval" not in csp and "http" not in csp


def seed(db):
    world, lease = ready_world(db)
    inp, results = score_window(db, NET, TOK, SCORED)
    db.commit()
    assert promote_scores(db, lease, inp, results) is True
    db.commit()


def test_status_body_is_unchanged_and_gets_security_headers(served):
    db, base = served
    seed(db)
    status, headers, body = request(base, "/status")
    expected_status, payload = _read_status(_dsn(db))
    assert status == 200 == int(expected_status.split()[0])
    expected = json.dumps(redact(payload), separators=(",", ":"), ensure_ascii=False).encode()
    assert mask_clock(body) == mask_clock(expected)
    assert headers["Content-Type"] == "application/json; charset=utf-8"
    assert headers["Cache-Control"] == "no-store"
    assert headers["Content-Security-Policy"] == CSP_API
    assert_common(headers)
    assert payload["feature_rows"] > 0


def test_demo_page_body_is_unchanged_and_csp_hashes_its_inline_style(served):
    db, base = served
    seed(db)
    status, headers, body = request(base, "/")
    _, payload = _read_status(_dsn(db))
    assert status == 200
    assert re.sub(rb"\d+\.\d+", b"<t>", body) == re.sub(
        rb"\d+\.\d+", b"<t>", _render_demo(payload).encode("utf-8"))
    assert headers["Content-Type"] == "text/html; charset=utf-8"
    assert headers["Cache-Control"] == "no-store"
    assert_common(headers)
    style = re.search(rb"<style>(.*?)</style>", body, re.S).group(1)
    digest = base64.b64encode(hashlib.sha256(style).digest()).decode()
    assert f"style-src 'self' 'sha256-{digest}'" in headers["Content-Security-Policy"]


def test_health_404_and_features_keep_bodies_and_get_headers(served):
    db, base = served
    seed(db)
    status, headers, body = request(base, "/health")
    assert (status, body) == (200, b'{"status":"ok"}')
    assert_common(headers) and headers["Content-Security-Policy"] == CSP_API
    status, headers, body = request(base, "/nope")
    assert (status, body) == (404, b'{"error":"not_found"}')
    assert headers["Content-Security-Policy"] == CSP_API
    status, headers, body = request(base, "/v1/features?limit=1")
    assert status == 200 and len(json.loads(body)["features"]) == 1
    assert headers["Cache-Control"] == "no-store" and headers["Content-Security-Policy"] == CSP_API
    status, headers, body = request(base, "/v1/features", "POST")
    assert (status, body) == (405, b'{"error":"read_only"}') and headers["Allow"] == "GET"
    assert_common(headers)


def test_dashboard_static_route_over_http(served):
    _, base = served
    status, headers, body = request(base, "/dashboard/")
    assert status == 200 and headers["Content-Type"] == "text/html; charset=utf-8"
    assert headers["Content-Security-Policy"] == CSP_API
    assert_common(headers)
    status, headers, css = request(base, "/dashboard/assets/app.css")
    assert status == 200 and headers["Content-Type"] == "text/css; charset=utf-8"
    assert headers["Cache-Control"] == "no-cache" and css
    status, headers, empty = request(base, "/dashboard/assets/app.css", "HEAD")
    assert status == 200 and empty == b"" and headers["Content-Length"] == str(len(css))
    status, headers, _ = request(base, "/dashboard/assets/app.css", "POST")
    assert status == 405 and headers["Allow"] == "GET, HEAD"
    for path in ("/dashboard/assets/%2e%2e/api.py", "/dashboard/assets/../api.py",
                 "/dashboard/assets/..%2fapi.py", "/dashboard/assets/nope.css"):
        status, headers, body = request(base, path)
        assert status == 404 and b"def " not in body
        assert_common(headers)
