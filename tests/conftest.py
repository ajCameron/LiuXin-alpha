"""Test configuration.

These tests are designed to run both when the project is installed (e.g.
`pip install -e .`) and when running directly from a source checkout.
"""
from __future__ import annotations

# Load shared fixture plugins (kept here so they are available to the entire suite).
pytest_plugins = (
    "tests.fixtures.liuxin_alpha_data_fixtures",
    # Shared DB driver / contract fixtures.
    "tests.databases.database_driver_plugins.database_driver_contract.fixture_plugin",
)

import os
import shutil
import sys
import sqlite3
import tempfile
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


_TEST_PREFS_ROOT = Path(tempfile.mkdtemp(prefix="liuxin_alpha_test_runtime_"))


def _install_test_runtime_env() -> None:
    """
    Keep prefs/config writes out of the checkout by default.

    Tests that need different paths can still override these env vars and reload
    the relevant modules explicitly.
    """

    prefs_dir = _TEST_PREFS_ROOT / "LiuXin_prefs"
    calibre_prefs_dir = prefs_dir / "calibre_prefs"
    config_dir = prefs_dir / "calibre_config"
    caches_dir = calibre_prefs_dir / "caches"

    for path in (prefs_dir, calibre_prefs_dir, config_dir, caches_dir):
        path.mkdir(parents=True, exist_ok=True)

    os.environ["LIUXIN_PREFS_DIR"] = str(prefs_dir)
    os.environ["LIUXIN_CONFIG_DIR"] = str(config_dir)


_install_test_runtime_env()


def _top_level_entries(root: Path) -> set[str]:
    return {entry.name for entry in root.iterdir()}


def _remove_path(path: Path) -> None:
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
        return
    if path.is_dir():
        shutil.rmtree(path)


def _redirect_liuxin_runtime_dirs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Point LiuXin's own scratch/prefs/debug helpers at per-test temp folders.

    Some legacy helpers bypass cwd and write under imported path constants such
    as `LiuXin_scratch_folder`. Patch those modules in-place before test code
    imports higher-level file-format components.
    """

    runtime_root = tmp_path / "liuxin_runtime"
    prefs_dir = runtime_root / "LiuXin_prefs"
    calibre_prefs_dir = prefs_dir / "calibre_prefs"
    config_dir = prefs_dir / "calibre_config"
    caches_dir = calibre_prefs_dir / "caches"
    scratch_dir = runtime_root / "LiuXin_scratch"
    debug_dir = runtime_root / "LiuXin_debug"
    program_dir = runtime_root / "LiuXin_programs"
    plugin_store = runtime_root / "LiuXin_plugins" / "plugins from calibre"

    for path in (
        prefs_dir,
        calibre_prefs_dir,
        config_dir,
        caches_dir,
        scratch_dir,
        debug_dir,
        program_dir,
        plugin_store,
    ):
        path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("LIUXIN_PREFS_DIR", str(prefs_dir))
    monkeypatch.setenv("LIUXIN_CONFIG_DIR", str(config_dir))

    import LiuXin_alpha.constants.paths as paths_mod
    import LiuXin_alpha.startup_scripts.prefs_folder_manager as prefs_folder_manager_mod
    import LiuXin_alpha.utils.paths as utils_paths_mod
    import LiuXin_alpha.utils.ptempfiles as ptempfiles_mod

    path_updates = {
        "LiuXin_prefs_folder": str(prefs_dir),
        "LiuXin_calibre_prefs_folder": str(calibre_prefs_dir),
        "LiuXin_calibre_caches": str(caches_dir),
        "LiuXin_calibre_config_folder": str(config_dir),
        "config_dir": str(config_dir),
        "LiuXin_scratch_folder": str(scratch_dir),
        "LiuXin_debug_folder": str(debug_dir),
        "LiuXin_program_folder": str(program_dir),
        "LiuXin_calibre_plugins_store": str(plugin_store),
    }
    for name, value in path_updates.items():
        monkeypatch.setattr(paths_mod, name, value, raising=False)

    prefs_folder_updates = {
        "LiuXin_prefs_folder": str(prefs_dir),
        "LiuXin_calibre_prefs_folder": str(calibre_prefs_dir),
        "LiuXin_debug_folder": str(debug_dir),
        "LiuXin_scratch_folder": str(scratch_dir),
        "LiuXin_program_folder": str(program_dir),
    }
    for mod in (prefs_folder_manager_mod, utils_paths_mod):
        for name, value in prefs_folder_updates.items():
            monkeypatch.setattr(mod, name, value, raising=False)

    monkeypatch.setattr(ptempfiles_mod, "LiuXin_scratch_folder", str(scratch_dir), raising=False)
    monkeypatch.setattr(ptempfiles_mod, "_base_dir", str(scratch_dir), raising=False)

    constants_mod = sys.modules.get("LiuXin_alpha.constants")
    if constants_mod is not None:
        for name, value in (
            ("LiuXin_calibre_caches", str(caches_dir)),
            ("LiuXin_calibre_config_folder", str(config_dir)),
            ("config_dir", str(config_dir)),
        ):
            monkeypatch.setattr(constants_mod, name, value, raising=False)

    preferences_mod = sys.modules.get("LiuXin_alpha.preferences")
    if preferences_mod is not None:
        monkeypatch.setattr(preferences_mod, "LiuXin_prefs_folder", str(prefs_dir), raising=False)

    config_base_mod = sys.modules.get("LiuXin_alpha.utils.config.config_base")
    if config_base_mod is not None:
        monkeypatch.setattr(config_base_mod, "config_dir", str(config_dir), raising=False)
        monkeypatch.setattr(config_base_mod, "plugin_dir", str(plugin_store), raising=False)

    config_tools_mod = sys.modules.get("LiuXin_alpha.utils.config.config_tools")
    if config_tools_mod is not None:
        monkeypatch.setattr(config_tools_mod, "config_dir", str(config_dir), raising=False)


@pytest.fixture(autouse=True)
def _sandbox_test_cwd_and_guard_project_root(
    monkeypatch: pytest.MonkeyPatch,
    project_root: Path,
    tmp_path: Path,
):
    """
    Run tests from an isolated temp cwd and fail on new repo-root artifacts.

    A number of file-format readers/write paths still emit relative intermediate
    files by design. Defaulting the test cwd to tmp_path keeps those artifacts
    out of the checkout, and the repo-root snapshot makes future regressions
    obvious.
    """

    before = _top_level_entries(project_root)
    _redirect_liuxin_runtime_dirs(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    yield

    leaked = sorted(_top_level_entries(project_root) - before)
    if not leaked:
        return

    leaked_paths = [project_root / name for name in leaked]
    for path in leaked_paths:
        _remove_path(path)

    pytest.fail(
        "Test leaked new repo-root artifacts: "
        + ", ".join(name for name in leaked)
        + ". Use tmp_path/tmp_path_factory or LiuXin_alpha_data for persistent fixtures."
    )


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


@pytest.fixture(scope="session")
def html_ingest_fixtures_dir(project_root: Path) -> Path:
    from tests.support.html_ingest_fixture_access import resolve_html_ingest_fixture_dir

    return resolve_html_ingest_fixture_dir(project_root)


@pytest.fixture
def html_ingest_fixture(html_ingest_fixtures_dir: Path):
    from tests.support.html_ingest_fixture_access import get_verified_html_ingest_fixture_path

    def _get(*, filename: str, verify_hash: bool = True) -> Path:
        return get_verified_html_ingest_fixture_path(
            fixture_dir=html_ingest_fixtures_dir,
            filename=filename,
            verify_hash=verify_hash,
        )

    return _get


@pytest.fixture
def html_ingest_fixtures(html_ingest_fixtures_dir: Path):
    from tests.support.html_ingest_fixture_access import iter_verified_html_ingest_fixtures

    def _get(*, verify_hash: bool = True) -> list[Path]:
        return list(iter_verified_html_ingest_fixtures(html_ingest_fixtures_dir, verify_hash=verify_hash))

    return _get
