"""Redaction of API payloads: secrets never leave the process; business identifiers stay exact."""
from __future__ import annotations

import re

REDACTED = "[REDACTED]"

# Keys whose values are never rendered. ``token`` is a business identifier and is NOT listed.
_SECRET_KEY = re.compile(
    r"(pass(word|wd)?|pwd|secret|api[_-]?key|auth(orization)?|credential|private[_-]?key|"
    r"dsn|cookie|session|lease[_-]?(owner|fence)|(access|auth|bearer|refresh)[_-]?token)",
    re.IGNORECASE)
_URL_USERINFO = re.compile(r"([A-Za-z][A-Za-z0-9+.-]*://[^\s/:@]*:)[^\s/@]+(@)")
_BEARER = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]+")
_ASSIGNMENT = re.compile(
    r"(?i)\b(pass(?:word|wd)?|pwd|secret|api[_-]?key|(?:access|auth|bearer|refresh)[_-]?token"
    r"|private[_-]?key)(\s*[=:]\s*)[^\s&;,]+")


def _scrub(text: str) -> str:
    text = _URL_USERINFO.sub(lambda m: m.group(1) + REDACTED + m.group(2), text)
    text = _BEARER.sub(lambda m: m.group(1) + " " + REDACTED, text)
    return _ASSIGNMENT.sub(lambda m: m.group(1) + m.group(2) + REDACTED, text)


def redact(value):
    """Return a copy with secret-keyed values masked and secret patterns removed from text."""
    if isinstance(value, dict):
        return {key: REDACTED if isinstance(key, str) and _SECRET_KEY.search(key) else redact(item)
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return _scrub(value)
    return value
