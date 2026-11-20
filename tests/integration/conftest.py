"""Every test under tests/integration needs real services (PostgreSQL, Kafka, Spark, K3s)."""

from pathlib import Path

import pytest

INTEGRATION_DIR = Path(__file__).resolve().parent


def pytest_collection_modifyitems(items):
    for item in items:
        if INTEGRATION_DIR in Path(str(item.fspath)).resolve().parents:
            item.add_marker(pytest.mark.integration)
