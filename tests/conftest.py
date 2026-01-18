"""Test configuration.

These tests are designed to run both when the project is installed (e.g.
`pip install -e .`) and when running directly from a source checkout.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest


def _ensure_src_on_path() -> None:
    root = Path(__file__).resolve().parents[1]
    # Ensure the project root is importable so `import tests...` works
    # even when running pytest from a nested working directory (e.g. IDE sub-runs).
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    src = root / "src"
    if src.is_dir():
        src_str = str(src)
        if src_str not in sys.path:
            # Prepend so local sources win over any globally installed version.
            sys.path.insert(0, src_str)


_ensure_src_on_path()


@pytest.fixture(scope="session")
def test_resources_manager(tmp_path_factory: pytest.TempPathFactory):
    """Session-scoped resource manager with a persistent template cache."""

    from tests.support.test_resources_manager import TestResourcesManager

    cache_dir = tmp_path_factory.getbasetemp() / "liuxin_test_resources"
    prebuilt = os.environ.get("LIUXIN_TEST_DATABASES_DIR")
    prebuilt_dir = Path(prebuilt).expanduser() if prebuilt else None
    return TestResourcesManager(cache_dir=cache_dir, prebuilt_dir=prebuilt_dir)


@pytest.fixture
def provision_test_database(tmp_path: Path, test_resources_manager):
    """Factory fixture: provision a named test database into this test's tmp_path."""

    def _provision(name: str = "test_db_0"):
        return test_resources_manager.provision_test_database(name=name, dst_dir=tmp_path)

    return _provision


@pytest.fixture
def provision_test_books(tmp_path: Path, test_resources_manager):
    """Factory fixture: provision test book files into this test's tmp_path."""

    def _provision(names: list[str] | None = None):
        return test_resources_manager.provision_test_books(dst_dir=tmp_path, names=names)

    return _provision


@pytest.fixture
def provision_test_covers(tmp_path: Path, test_resources_manager):
    """Factory fixture: provision test cover image files into this test's tmp_path."""

    def _provision(names: list[str] | None = None):
        return test_resources_manager.provision_test_covers(dst_dir=tmp_path, names=names)

    return _provision

