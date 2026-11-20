"""The app image packages the source + dashboard assets and the pins match the tagged image."""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
DOCKERFILE = (DEPLOY / "app.Dockerfile").read_text(encoding="utf-8")
LOCK = json.loads((DEPLOY / "images.lock.json").read_text(encoding="utf-8"))
APP = LOCK["images"]["app"]
DIGEST_RE = re.compile(r"ton-sentinel@(sha256:[0-9a-f]{64})")


def test_dockerfile_copies_package_source():
    assert re.search(r"^COPY\s+src/ton_sentinel/?\s+/workspace/src/ton_sentinel/?\s*$", DOCKERFILE, re.M)


def test_packaged_dashboard_assets_exist_under_copied_path():
    pkg = ROOT / "src" / "ton_sentinel"
    assert (pkg / "demo.html").is_file()
    assert (pkg / "dashboard" / "index.html").is_file()
    assert list((pkg / "dashboard").glob("*.js")) and list((pkg / "dashboard").glob("*.css"))


def test_dockerfile_stays_non_root_and_pythonpath_has_src():
    users = re.findall(r"^USER\s+(\S+)", DOCKERFILE, re.M)
    assert users[-1] == "1000:1000"
    assert "/workspace/src" in re.search(r"^ENV PYTHONPATH=(\S+)", DOCKERFILE, re.M).group(1).split(":")


def test_src_copy_is_after_dependency_layer():
    assert DOCKERFILE.index("pip install") < DOCKERFILE.index("COPY src/ton_sentinel")


def test_lock_app_entry_is_consistent_and_not_the_old_image():
    digest = APP["repo_digests"][0].split("@")[1]
    assert APP["id"] == digest
    assert APP["tag_used_for_setup"] != "ton-sentinel:local"
    assert digest != "sha256:8b1759daf094bd768b25108ae78e3e3bbc96a3706d542ea092c6620c5e68e7b2"


@pytest.mark.parametrize("name", ["compose.yaml", "compose.dashboard.yaml"])
def test_compose_app_images_pinned_to_lock_digest(name):
    text = (DEPLOY / name).read_text(encoding="utf-8")
    found = set(DIGEST_RE.findall(text))
    assert found == {APP["id"]}


def test_api_override_does_not_bind_mount_src():
    api = yaml.safe_load((DEPLOY / "compose.dashboard.yaml").read_text())["services"]["api"]
    assert not any("src" in str(v) for v in api.get("volumes", []))


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker unavailable")
def test_lock_digest_matches_local_tagged_image():
    out = subprocess.run(["docker", "image", "inspect", APP["tag_used_for_setup"], "--format", "{{.Id}}"],
                         capture_output=True, text=True)
    if out.returncode != 0:
        pytest.skip("tagged image not present locally")
    assert out.stdout.strip() == APP["id"]


def test_m3_api_image_pin_matches_lock_in_all_compose_files():
    digest = APP["repo_digests"][0].split("@")[1]
    for name in ("compose.yaml", "compose.dashboard.yaml"):
        text = (DEPLOY / name).read_text(encoding="utf-8")
        assert digest in text
        assert set(DIGEST_RE.findall(text)) == {digest}
    assert APP["tag_used_for_setup"] == "ton-sentinel:l2-app"


def test_l2_app_image_rebuilt_from_current_src_and_pinned_everywhere():
    new = "sha256:92fddf750cd564e58286c995b7910d4b1ebf1764cd52359a55aab94ceeaa0134"
    assert APP["tag_used_for_setup"] == "ton-sentinel:l2-app"
    assert APP["id"] == new and APP["repo_digests"] == [f"ton-sentinel@{new}"]
    for name in ("compose.yaml", "compose.dashboard.yaml"):
        assert set(DIGEST_RE.findall((DEPLOY / name).read_text(encoding="utf-8"))) == {new}
