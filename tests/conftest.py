"""Test configuration.

These tests are designed to run both when the project is installed (e.g.
`pip install -e .`) and when running directly from a source checkout.
"""
from __future__ import annotations

# Load shared fixture plugins (kept here so they are available to the entire suite).
pytest_plugins = (
    "tests.fixtures.liuxin_alpha_data_fixtures",
)

import os
import sys
import sqlite3
from pathlib import Path

from typing import Optional


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


def _install_clint_stub() -> None:
    """Install a minimal `clint.textui` stub.

    Some legacy modules (including a few test-support helpers) import clint for
    pretty terminal output. Clint is optional for LiuXin_alpha, so the test
    suite should remain runnable when it isn't installed.
    """

    if "clint" in sys.modules and "clint.textui" in sys.modules:
        return

    try:
        import clint.textui  # noqa: F401

        return
    except Exception:
        pass

    import types

    clint = types.ModuleType("clint")
    textui = types.ModuleType("clint.textui")

    def puts(*_args, **_kwargs):  # pragma: no cover
        return None

    class _Colored:  # pragma: no cover
        def green(self, s):
            return s

        def red(self, s):
            return s

        def yellow(self, s):
            return s

        def blue(self, s):
            return s

        def magenta(self, s):
            return s

        def cyan(self, s):
            return s

        def white(self, s):
            return s

    textui.puts = puts  # type: ignore[attr-defined]
    textui.colored = _Colored()  # type: ignore[attr-defined]

    clint.textui = textui  # type: ignore[attr-defined]
    sys.modules["clint"] = clint
    sys.modules["clint.textui"] = textui


_install_clint_stub()


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
        return test_resources_manager.provision_named_test_database(name=name, dst_dir=tmp_path)

    return _provision


@pytest.fixture
def provision_named_test_database(tmp_path: Path, test_resources_manager):
    """
    Factory fixture: call provision_named_test_database("test_db_0", dst_dir=...)
    """
    def _provision(name: str, dst_dir: Path | None = None):
        dst = dst_dir or tmp_path
        # adapt this line to whatever your manager API is
        return test_resources_manager.provision_named_test_database(name=name, dst_dir=dst)

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



def _sqlite_has_fts5(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute("CREATE VIRTUAL TABLE temp._fts5_probe USING fts5(x)")
        conn.execute("DROP TABLE temp._fts5_probe")
        return True
    except sqlite3.OperationalError:
        return False


@pytest.fixture(scope="session")
def calibre_library_template_manager(tmp_path_factory: pytest.TempPathFactory):
    from tests.support.calibre_library_templates import CalibreLibraryTemplateManager

    cache_dir = tmp_path_factory.getbasetemp() / "liuxin_calibre_library_templates"
    return CalibreLibraryTemplateManager(cache_dir=cache_dir)


@pytest.fixture
def provision_calibre_library(tmp_path: Path, calibre_library_template_manager):
    # Calibre's current metadata schema requires FTS5.
    probe = sqlite3.connect(":memory:")
    try:
        if not _sqlite_has_fts5(probe):
            pytest.skip("SQLite build lacks FTS5; Calibre metadata schema requires it")
    finally:
        probe.close()

    def _provision(name: str = "calibre_library", **kwargs):
        return calibre_library_template_manager.provision_blank_library(
            dst_dir=tmp_path, name=name, **kwargs
        )

    return _provision


@pytest.fixture
def provision_populated_calibre_library(provision_calibre_library):
    """Factory fixture: provision a Calibre library and get a builder for it."""

    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
        CalibreLibraryBuilder,
    )

    def _provision(*, name: str = "calibre_library", **kwargs):
        lib = provision_calibre_library(name=name, **kwargs)
        builder = CalibreLibraryBuilder(lib.root)
        return lib, builder

    return _provision
