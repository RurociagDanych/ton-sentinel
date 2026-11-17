import os
from pathlib import Path
import subprocess
import sys


def test_source_scan_imports_before_db_package_without_cycle():
    repository = Path(__file__).resolve().parents[2]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(repository / "src")
    result = subprocess.run(
        [sys.executable, "-B", "-c", "from ton_sentinel.source import scan_range"],
        cwd=repository,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr