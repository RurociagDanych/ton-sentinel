"""Finite standard-library REST transport for explicitly configured source pages."""
from datetime import timezone
from email.utils import parsedate_to_datetime
from math import ceil, isfinite
import json
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from ton_sentinel.contracts import SourceEpoch


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def _reject_constant(value):
    raise ValueError("nonstandard JSON constant")


def _finite_float(value):
    parsed = float(value)
    if not isfinite(parsed):
        raise ValueError("non-finite JSON number")
    return parsed


def _read_response_body(response, size, deadline, request_timeout):
    """Read a Content-Length-delimited body without resetting its deadline."""
    body = bytearray()
    while len(body) < size:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("source request time budget exhausted")
        try:
            response.fp.raw._sock.settimeout(min(request_timeout, remaining))
            chunk = response.read1(min(64_000, size - len(body)))
        except AttributeError as exc:
            raise ValueError("source response has no bounded socket reader") from exc
        if not chunk:
            raise ValueError("source response ended before Content-Length")
        body.extend(chunk)
    return bytes(body)


class RESTPageClient:
    """Read one fixed JSON array route with finite request and response budgets.

    The configured route and origin are the complete authority boundary. Response
    fields are data only; they cannot select another URL or influence a redirect.
    The profile is offline/configured and does not assert a deployed TON API.
    """
    supports_durable_accounting = True

    def __init__(self, origin, route, *, max_body_bytes=4_000_000,
                 max_page_items=1000, max_requests=4, max_retries=2,
                 max_seconds=15, request_timeout=5, max_retry_after=2):
        if type(origin) is not str or type(route) is not str:
            raise ValueError("explicit text origin and route required")
        parsed = urlsplit(origin)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment):
            raise ValueError("origin must be an explicit HTTP(S) origin without credentials or path")
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("invalid origin port") from exc
        if parsed.scheme == "http" and parsed.hostname not in ("localhost", "127.0.0.1", "::1"):
            raise ValueError("cleartext HTTP is permitted only for loopback origins")
        if port is not None and not 1 <= port <= 65535:
            raise ValueError("invalid origin port")
        if (not route.startswith("/") or route.startswith("//") or "?" in route
                or "#" in route or "%" in route or "\\" in route
                or any(part in (".", "..") for part in route.split("/"))
                or not re.fullmatch(r"/[A-Za-z0-9._/-]+", route)):
            raise ValueError("route must be one fixed absolute path without query or traversal")
        bounds = {"max_body_bytes": (max_body_bytes, 1, 64_000_000),
                  "max_page_items": (max_page_items, 1, 1000),
                  "max_requests": (max_requests, 1, 10),
                  "max_retries": (max_retries, 0, 5),
                  "max_seconds": (max_seconds, 1, 300),
                  "request_timeout": (request_timeout, 1, 30),
                  "max_retry_after": (max_retry_after, 0, 30)}
        for name, (value, low, high) in bounds.items():
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"{name} must be an integer in {low}..{high}")
        if max_requests < max_retries + 1:
            raise ValueError("request budget must include all configured retries")
        self._origin = origin.rstrip("/")
        self._route = route
        self._max_body_bytes = max_body_bytes
        self._max_page_items = max_page_items
        self._max_requests = max_requests
        self._max_retries = max_retries
        self._max_seconds = max_seconds
        self._request_timeout = request_timeout
        self._max_retry_after = max_retry_after
        self._opener = build_opener(ProxyHandler({}), _NoRedirect())

    def fetch_page(self, epoch, start, end, offset, limit, *, before_request=None,
                   reserve_bytes=None, remaining_seconds=None, remaining_bytes=None):
        """Fetch an array page; `[start,end)` is sent as inclusive `end - 1`."""
        if type(epoch) is not SourceEpoch:
            raise ValueError("typed SourceEpoch required")
        for name, value in (("start", start), ("end", end),
                            ("offset", offset), ("limit", limit)):
            if type(value) is not int:
                raise ValueError(f"{name} must be an integer")
        if not 0 <= start < end <= 2**63 - 1 or offset < 0:
            raise ValueError("invalid half-open source bounds or offset")
        if not 1 <= limit <= self._max_page_items:
            raise ValueError("page limit exceeds configured bound")
        query = urlencode((("start", start), ("end", end - 1),
                           ("offset", offset), ("limit", limit)))
        url = f"{self._origin}{self._route}?{query}"
        if remaining_seconds is None:
            budget_seconds = self._max_seconds
        else:
            if (type(remaining_seconds) not in (int, float)
                    or remaining_seconds <= 0
                    or (type(remaining_seconds) is float and not isfinite(remaining_seconds))):
                raise ValueError("remaining_seconds must be a finite positive number")
            budget_seconds = min(self._max_seconds, remaining_seconds)
        if remaining_bytes is None:
            body_limit = self._max_body_bytes
        else:
            if type(remaining_bytes) is not int or remaining_bytes <= 0:
                raise ValueError("remaining_bytes must be a positive integer")
            body_limit = min(self._max_body_bytes, remaining_bytes)
        deadline = time.monotonic() + budget_seconds
        requests = 0
        retries = 0
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or requests >= self._max_requests:
                raise ValueError("source request or time budget exhausted")
            request = Request(url, headers={"Accept": "application/json",
                                            "Accept-Encoding": "identity"}, method="GET")
            if before_request is not None:
                before_request()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError("source request time budget exhausted")
            requests += 1
            try:
                with self._opener.open(request, timeout=min(self._request_timeout, remaining)) as response:
                    status = response.status
                    headers = response.headers
                    if status == 200:
                        if headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
                            raise ValueError("compressed source responses are not accepted")
                        if headers.get("Transfer-Encoding") is not None:
                            raise ValueError("transfer-encoded source responses are not accepted")
                        lengths = headers.get_all("Content-Length", [])
                        if len(lengths) != 1 or not lengths[0].isdecimal():
                            raise ValueError("one valid Content-Length is required")
                        body_size = int(lengths[0])
                        if body_size > body_limit:
                            raise ValueError("source response body exceeds configured byte limit")
                        if reserve_bytes is not None:
                            reserve_bytes(body_size)
                        body = _read_response_body(response, body_size, deadline,
                                                   self._request_timeout)
                    else:
                        body = b""
            except HTTPError as exc:
                status = exc.code
                headers = exc.headers
                body = b""
                exc.close()
            except (URLError, TimeoutError, OSError) as exc:
                raise ValueError("source transport failed") from exc
            if time.monotonic() > deadline:
                raise ValueError("source request time budget exhausted")
            if status in (429, 500, 502, 503, 504):
                if retries >= self._max_retries or requests >= self._max_requests:
                    raise ValueError("transient source response retry budget exhausted")
                delay = self._retry_delay(headers.get("Retry-After"), retries)
                remaining = deadline - time.monotonic()
                if delay > self._max_retry_after or delay > remaining:
                    raise ValueError("Retry-After exceeds configured time budget")
                if delay:
                    time.sleep(delay)
                retries += 1
                continue
            if status != 200:
                raise ValueError(f"source rejected request with HTTP {status}")
            if headers.get_content_type() != "application/json":
                raise ValueError("source response is not application/json")
            try:
                payload = json.loads(body.decode("utf-8", errors="strict"),
                                     object_pairs_hook=_unique_object,
                                     parse_constant=_reject_constant,
                                     parse_float=_finite_float)
            except (UnicodeError, ValueError, RecursionError) as exc:
                raise ValueError("invalid source JSON") from exc
            if (type(payload) is not list or len(payload) > min(limit, self._max_page_items)
                    or any(type(item) is not dict for item in payload)):
                raise ValueError("source response must be a bounded JSON object array")
            if time.monotonic() >= deadline:
                raise ValueError("source request time budget exhausted")
            return payload

    def fetch_trace(self, epoch, trace_id, *, include_actions=True):
        """Look up one known trace ID; this does not prove absence or window coverage."""
        if type(epoch) is not SourceEpoch:
            raise ValueError("typed SourceEpoch required")
        if self._route != "/api/v3/traces":
            raise ValueError("trace lookup requires the fixed /api/v3/traces route")
        if type(trace_id) is not str or not trace_id or len(trace_id) > 256:
            raise ValueError("trace_id must be non-empty text of at most 256 characters")
        if type(include_actions) is not bool:
            raise ValueError("include_actions must be boolean")
        query = urlencode((("trace_id", trace_id), ("include_actions", str(include_actions).lower()),
                           ("limit", 1), ("offset", 0)))
        url = f"{self._origin}{self._route}?{query}"
        deadline = time.monotonic() + self._max_seconds
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("source request time budget exhausted")
        request = Request(url, headers={"Accept": "application/json",
                                        "Accept-Encoding": "identity"}, method="GET")
        try:
            with self._opener.open(request, timeout=min(self._request_timeout, remaining)) as response:
                if response.status != 200:
                    raise ValueError(f"source rejected request with HTTP {response.status}")
                headers = response.headers
                if headers.get_content_type() != "application/json":
                    raise ValueError("source response is not application/json")
                if headers.get("Content-Encoding", "identity").lower() not in ("", "identity"):
                    raise ValueError("compressed source responses are not accepted")
                if headers.get("Transfer-Encoding") is not None:
                    raise ValueError("transfer-encoded source responses are not accepted")
                lengths = headers.get_all("Content-Length", [])
                if len(lengths) != 1 or not lengths[0].isdecimal():
                    raise ValueError("one valid Content-Length is required")
                size = int(lengths[0])
                if size > self._max_body_bytes:
                    raise ValueError("source response body exceeds configured byte limit")
                body = _read_response_body(response, size, deadline, self._request_timeout)
        except HTTPError as exc:
            status = exc.code
            exc.close()
            raise ValueError(f"source rejected request with HTTP {status}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            raise ValueError("source transport failed") from exc
        if time.monotonic() >= deadline:
            raise ValueError("source request time budget exhausted")
        try:
            payload = json.loads(body.decode("utf-8", errors="strict"),
                                 object_pairs_hook=_unique_object,
                                 parse_constant=_reject_constant,
                                 parse_float=_finite_float)
        except (UnicodeError, ValueError, RecursionError) as exc:
            raise ValueError("invalid source JSON") from exc
        if type(payload) is not dict or type(payload.get("traces")) is not list:
            raise ValueError("trace response must contain a traces array")
        traces = payload["traces"]
        if len(traces) > 1 or any(type(trace) is not dict for trace in traces):
            raise ValueError("trace response exceeds the one-trace lookup bound")
        if any(trace.get("trace_id") != trace_id for trace in traces):
            raise ValueError("trace response does not match requested trace_id")
        if time.monotonic() >= deadline:
            raise ValueError("source request time budget exhausted")
        return payload

    def _retry_delay(self, retry_after, retry_number):
        if retry_after is None:
            return min(2 ** retry_number, self._max_retry_after)
        value = retry_after.strip()
        if value.isdecimal():
            return int(value)
        try:
            when = parsedate_to_datetime(value)
            if when.tzinfo is None:
                when = when.replace(tzinfo=timezone.utc)
            return max(0, ceil(when.timestamp() - time.time()))
        except (TypeError, ValueError, OverflowError):
            raise ValueError("invalid Retry-After header")
