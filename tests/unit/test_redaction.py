"""Redaction of nested/error/status payloads. Business identifiers stay exact."""
import json

import pytest

from ton_sentinel.redaction import REDACTED, redact

SECRETS = ("hunter2-pw", "sk-live-ABCDEF123", "tok-s3cret-value", "owner-secret-id")


def test_secret_keys_are_masked_at_any_depth_but_business_token_is_not():
    payload = {"token": "jetton:A+B", "nested": [{"password": SECRETS[0], "ok": 1,
                                                   "deep": {"api_key": SECRETS[1]}}],
               "lease_owner": SECRETS[3], "Authorization": "Bearer " + SECRETS[2]}
    out = redact(payload)
    assert out["token"] == "jetton:A+B"
    assert out["nested"][0]["ok"] == 1
    assert out["nested"][0]["password"] == REDACTED
    assert out["nested"][0]["deep"]["api_key"] == REDACTED
    assert out["lease_owner"] == REDACTED and out["Authorization"] == REDACTED
    assert not any(s in json.dumps(out) for s in SECRETS)


@pytest.mark.parametrize("text", [
    "connect failed postgresql://sentinel:hunter2-pw@db:5432/x",
    "password=hunter2-pw failed",
    "PASSWORD: hunter2-pw",
    "header Authorization: Bearer hunter2-pw",
    "api_key=hunter2-pw&x=1",
])
def test_secret_values_inside_strings_are_masked(text):
    out = redact({"error": [text], "status": {"detail": text}})
    assert "hunter2-pw" not in json.dumps(out)
    assert REDACTED in json.dumps(out)


def test_exact_business_identifiers_and_numbers_survive_unchanged():
    payload = {"token": "JETTON:A+B", "network": "testnet", "window_start": 0, "input_raw": "123",
               "explanation": "swaps 12 vs median 3 (input sha256:abc)", "flag": True, "none": None,
               "list": ["token:x", "a=b"]}
    assert redact(payload) == payload


def test_redaction_does_not_mutate_input_and_handles_tuples():
    payload = {"password": "x", "t": ("a", {"secret": "y"})}
    out = redact(payload)
    assert payload["password"] == "x" and out["t"][1]["secret"] == REDACTED
