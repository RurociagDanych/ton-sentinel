"""The dashboard local route is loopback-only, digest-pinned, non-root and secret-free."""

import re
from pathlib import Path

import pytest
import yaml

import ton_sentinel.cli as cli

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
OVERRIDE = DEPLOY / "compose.dashboard.yaml"


def _load(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _api():
    return _load(OVERRIDE)["services"]["api"]


def test_override_publishes_only_loopback():
    doc = _load(OVERRIDE)
    published = [(n, p) for n, s in doc["services"].items() for p in s.get("ports", [])]
    assert len(published) == 1
    assert published[0][0] == "api"
    assert re.fullmatch(r"127\.0\.0\.1:\d+:8080", str(published[0][1]))
    assert not any(str(p).startswith(("0.0.0.0:", ":", "[::]")) for _, p in published)


def test_base_compose_still_has_no_ports():
    base = _load(DEPLOY / "compose.yaml")
    assert all("ports" not in s for s in base["services"].values())


def test_postgres_and_kafka_stay_only_on_internal_network():
    doc = _load(OVERRIDE)
    assert set(doc["services"]) == {"api"}
    assert set(_api()["networks"]) == {"fixture", "edge"}
    assert doc["networks"]["edge"].get("internal") in (None, False)
    assert _load(DEPLOY / "compose.yaml")["networks"]["fixture"]["internal"] is True


def test_api_service_dsn_not_in_environment_literal():
    text = OVERRIDE.read_text(encoding="utf-8")
    api = _api()
    env = api.get("environment") or {}
    assert not any("DSN" in k or "PASSWORD" in k for k in (env if isinstance(env, dict) else []))
    assert not re.search(r"postgres(ql)?://", text)
    assert "TON_SENTINEL_PG_PASSWORD" not in text
    assert "password" not in text.lower().replace("no password", "")
    assert "--dsn-file" in api["command"]
    assert "--dsn" not in api["command"]
    assert "api_dsn" in _load(OVERRIDE)["secrets"]
    assert _load(OVERRIDE)["secrets"]["api_dsn"]["file"].startswith("../.cache/")


def test_api_image_pinned_by_digest_and_same_as_base():
    image = _api()["image"]
    assert re.fullmatch(r"[\w./-]+@sha256:[0-9a-f]{64}", image)
    assert image == _load(DEPLOY / "compose.yaml")["services"]["app"]["image"]


def test_api_runs_non_root_hardened_and_binds_all_inside_container():
    api = _api()
    uid = str(api["user"]).split(":")[0]
    assert uid not in ("0", "root")
    assert api["cap_drop"] == ["ALL"]
    assert "no-new-privileges:true" in api["security_opt"]
    assert api["read_only"] is True
    cmd = api["command"]
    assert cmd[cmd.index("--host") + 1] == "0.0.0.0"
    assert cmd[cmd.index("--port") + 1] == "8080"


def test_override_has_no_public_exposure_constructs():
    text = OVERRIDE.read_text(encoding="utf-8")
    for bad in ("expose:", "network_mode", "privileged", "NodePort", "LoadBalancer"):
        assert bad not in text


def test_serve_dsn_file_is_read_and_not_echoed(tmp_path, capsys):
    from unittest.mock import patch
    secret = tmp_path / "dsn"
    secret.write_text("postgresql://u:s3cret-value@h/db\n", encoding="utf-8")
    seen = {}

    class Srv:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def serve_forever(self): return None

    def fake_app(dsn, telemetry_probes=None):
        seen["dsn"] = dsn
        return lambda *a: []

    with patch.object(cli, "make_server", lambda *a, **k: Srv()), patch.object(cli, "create_app", fake_app):
        cli.main(["--dsn-file", str(secret), "--host", "127.0.0.1", "--port", "8080"])
    assert seen["dsn"] == "postgresql://u:s3cret-value@h/db"
    out = capsys.readouterr()
    assert "s3cret-value" not in out.out + out.err


def test_serve_requires_exactly_one_dsn_source(tmp_path):
    with pytest.raises(SystemExit):
        cli.main(["--port", "8080"])
    with pytest.raises(SystemExit):
        cli.main(["--dsn", "x", "--dsn-file", str(tmp_path / "f")])
    with pytest.raises(SystemExit):
        cli.main(["--dsn-file", str(tmp_path / "missing")])
