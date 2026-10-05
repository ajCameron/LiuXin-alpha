"""
Configure process-wide and per-test isolation for the LiuXin test suite.

The module makes checkout sources importable, provides an optional ``clint`` shim,
redirects LiuXin runtime state into temporary directories, rejects repository-root
leaks, and exposes shared database, asset, Calibre-library and HTML fixtures.

Example:
    Exercise conftest through a regression that loads the root fixture configuration::

        python -m pytest -q tests/scripts/test_docstring_ownership.py
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
    """
    Prepend the checkout root and source tree to the import search path when needed.

    Example:
        Exercise  ensure src on path through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :return: None; ``sys.path`` is updated only when the checkout paths are absent.
    """
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
    """
    Install a minimal ``clint.textui`` compatibility module when Clint is unavailable.

    An installed Clint package is left intact. The fallback preserves only the colour
    and output surface imported by legacy test-support modules.

    Example:
        Exercise  install clint stub through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :return: None; existing Clint modules are retained or fallback modules are
        registered.
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
        """
        Accept legacy terminal-output arguments without emitting output.

        Example:
            Exercise  install clint stub.puts through a regression that loads the root fixture configuration::

                python -m pytest -q tests/scripts/test_docstring_ownership.py


        :param _args: Positional output arguments accepted for Clint compatibility.
        :param _kwargs: Keyword output arguments accepted for Clint compatibility.
        :return: None.
        """
        return None

    class _Colored:  # pragma: no cover
        """
        Provide identity colour functions for the optional Clint compatibility module.

        Example:
            Exercise  install clint stub. Colored through a regression that loads the root fixture configuration::

                python -m pytest -q tests/scripts/test_docstring_ownership.py
        """
        def green(self, s):
            """
            Return text unchanged for the fallback ``green`` colour operation.

            Example:
                Exercise  install clint stub. Colored.green through a regression that loads the root fixture configuration::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :param s: Text that the compatibility colour method returns unchanged.
            :return: The original text unchanged.
            """
            return s

        def red(self, s):
            """
            Return text unchanged for the fallback ``red`` colour operation.

            Example:
                Exercise  install clint stub. Colored.red through a regression that loads the root fixture configuration::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :param s: Text that the compatibility colour method returns unchanged.
            :return: The original text unchanged.
            """
            return s

        def yellow(self, s):
            """
            Return text unchanged for the fallback ``yellow`` colour operation.

            Example:
                Exercise  install clint stub. Colored.yellow through a regression that loads the root fixture configuration::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :param s: Text that the compatibility colour method returns unchanged.
            :return: The original text unchanged.
            """
            return s

        def blue(self, s):
            """
            Return text unchanged for the fallback ``blue`` colour operation.

            Example:
                Exercise  install clint stub. Colored.blue through a regression that loads the root fixture configuration::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :param s: Text that the compatibility colour method returns unchanged.
            :return: The original text unchanged.
            """
            return s

        def magenta(self, s):
            """
            Return text unchanged for the fallback ``magenta`` colour operation.

            Example:
                Exercise  install clint stub. Colored.magenta through a regression that loads the root fixture configuration::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :param s: Text that the compatibility colour method returns unchanged.
            :return: The original text unchanged.
            """
            return s

        def cyan(self, s):
            """
            Return text unchanged for the fallback ``cyan`` colour operation.

            Example:
                Exercise  install clint stub. Colored.cyan through a regression that loads the root fixture configuration::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :param s: Text that the compatibility colour method returns unchanged.
            :return: The original text unchanged.
            """
            return s

        def white(self, s):
            """
            Return text unchanged for the fallback ``white`` colour operation.

            Example:
                Exercise  install clint stub. Colored.white through a regression that loads the root fixture configuration::

                    python -m pytest -q tests/scripts/test_docstring_ownership.py


            :param s: Text that the compatibility colour method returns unchanged.
            :return: The original text unchanged.
            """
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
    Create process-scoped preference and configuration directories outside the checkout.

    The environment defaults may still be overridden by tests that explicitly reload the
    affected configuration modules.

    Example:
        Exercise  install test runtime env through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :return: None; runtime directories and environment defaults are created in place.
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
    """
    Return the immediate entry names currently present below a directory.

    Example:
        Exercise  top level entries through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param root: Directory whose immediate entries are inspected.
    :return: The set of immediate entry names.
    """
    return {entry.name for entry in root.iterdir()}


def _is_expected_repo_root_tool_artifact(name: str) -> bool:
    """
    Return whether a root entry is an allowed coverage-data artifact.

    Example:
        Exercise  is expected repo root tool artifact through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param name: Entry, database or library name addressed by the operation.
    :return: True for recognized coverage files; otherwise False.
    """
    return name == ".coverage" or name.startswith(".coverage.")


def _remove_path(path: Path) -> None:
    """
    Remove one leaked file, link or directory from the repository root.

    Example:
        Exercise  remove path through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param path: File, symbolic link or directory to remove.
    :return: None; the target is absent on return when removal succeeds.
    """
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
        return
    if path.is_dir():
        shutil.rmtree(path)


def _redirect_liuxin_runtime_dirs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """
    Redirect LiuXin preference, cache, scratch, debug and plugin paths into a test directory.

    Both environment variables and already-imported module constants are patched so
    legacy helpers cannot escape the per-test runtime root.

    Example:
        Exercise  redirect liuxin runtime dirs through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param monkeypatch: Pytest fixture used to isolate environment variables, modules
        and working state.
    :param tmp_path: Pytest-managed directory that owns per-test runtime or provisioned
        data.
    :return: None; environment variables and imported path constants are patched in
        place.
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
    Run each test from a temporary working directory and reject new repository-root entries.

    Unexpected entries are removed before the fixture fails, while coverage data is
    retained as an explicitly allowed tool artifact.

    Example:
        Exercise  sandbox test cwd and guard project root through a regression that loads the root fixture configuration::

            python -m pytest -q tests/scripts/test_docstring_ownership.py


    :param monkeypatch: Pytest fixture used to isolate environment variables, modules
        and working state.
    :param project_root: Repository root guarded against leaked test artifacts.
    :param tmp_path: Pytest-managed directory that owns per-test runtime or provisioned
        data.
    :return: A fixture iterator that yields after isolation and validates the repository
        root during teardown.
    """

    before = _top_level_entries(project_root)
    _redirect_liuxin_runtime_dirs(monkeypatch, tmp_path)
    monkeypatch.chdir(tmp_path)
    yield

    leaked = sorted(
        name
        for name in _top_level_entries(project_root) - before
        if not _is_expected_repo_root_tool_artifact(name)
    )
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
    """
    Create the session resource manager and its reusable template cache.

    Example:
        Exercise test resources manager through a regression that loads the root fixture configuration::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param tmp_path_factory: Session-scoped pytest factory used to locate a persistent
        temporary cache.
    :return: The configured session resource manager.
    """

    from tests.support.test_resources_manager import TestResourcesManager

    cache_dir = tmp_path_factory.getbasetemp() / "liuxin_test_resources"
    prebuilt = os.environ.get("LIUXIN_TEST_DATABASES_DIR")
    prebuilt_dir = Path(prebuilt).expanduser() if prebuilt else None
    return TestResourcesManager(cache_dir=cache_dir, prebuilt_dir=prebuilt_dir)


@pytest.fixture
def provision_test_database(tmp_path: Path, test_resources_manager):
    """
    Provide a factory that copies a named test database into the current test directory.

    Example:
        Exercise provision test database through a regression that loads the root fixture configuration::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param tmp_path: Pytest-managed directory that owns per-test runtime or provisioned
        data.
    :param test_resources_manager: Session resource manager that resolves and provisions
        fixture assets.
    :return: A callable accepting an optional database name.
    """

    def _provision(name: str = "test_db_0"):
        """
        Provision one named database beneath the fixture's temporary directory.

        Example:
            Exercise provision test database. provision through a regression that loads the root fixture configuration::

                python -m pytest -q tests/support/test_resources_manager_assets_test.py


        :param name: Entry, database or library name addressed by the operation.
        :return: The provisioned database descriptor.
        """
        return test_resources_manager.provision_named_test_database(name=name, dst_dir=tmp_path)

    return _provision


@pytest.fixture
def provision_named_test_database(tmp_path: Path, test_resources_manager):
    """
    Provide a factory that provisions named test databases into caller-selected directories.

    Example:
        Exercise provision named test database through a regression that loads the root fixture configuration::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param tmp_path: Pytest-managed directory that owns per-test runtime or provisioned
        data.
    :param test_resources_manager: Session resource manager that resolves and provisions
        fixture assets.
    :return: A callable accepting a database name and optional destination.
    """
    def _provision(name: str, dst_dir: Path | None = None):
        """
        Provision one named database into the requested directory or the test default.

        Example:
            Exercise provision named test database. provision through a regression that loads the root fixture configuration::

                python -m pytest -q tests/support/test_resources_manager_assets_test.py


        :param name: Entry, database or library name addressed by the operation.
        :param dst_dir: Optional destination directory; the current test directory is used
            when omitted.
        :return: The provisioned database descriptor.
        """
        dst = dst_dir or tmp_path
        # adapt this line to whatever your manager API is
        return test_resources_manager.provision_named_test_database(name=name, dst_dir=dst)

    return _provision


@pytest.fixture
def provision_test_books(tmp_path: Path, test_resources_manager):
    """
    Provide a factory that copies all or selected book fixtures into the test directory.

    Example:
        Exercise provision test books through a regression that loads the root fixture configuration::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param tmp_path: Pytest-managed directory that owns per-test runtime or provisioned
        data.
    :param test_resources_manager: Session resource manager that resolves and provisions
        fixture assets.
    :return: A callable accepting optional book fixture names.
    """

    def _provision(names: list[str] | None = None):
        """
        Provision the requested book fixtures beneath the fixture's temporary directory.

        Example:
            Exercise provision test books. provision through a regression that loads the root fixture configuration::

                python -m pytest -q tests/support/test_resources_manager_assets_test.py


        :param names: Optional fixture names to copy; omission selects the complete asset
            group.
        :return: The provisioned book-asset descriptor.
        """
        return test_resources_manager.provision_test_books(dst_dir=tmp_path, names=names)

    return _provision


@pytest.fixture
def provision_test_covers(tmp_path: Path, test_resources_manager):
    """
    Provide a factory that copies all or selected cover fixtures into the test directory.

    Example:
        Exercise provision test covers through a regression that loads the root fixture configuration::

            python -m pytest -q tests/support/test_resources_manager_assets_test.py


    :param tmp_path: Pytest-managed directory that owns per-test runtime or provisioned
        data.
    :param test_resources_manager: Session resource manager that resolves and provisions
        fixture assets.
    :return: A callable accepting optional cover fixture names.
    """

    def _provision(names: list[str] | None = None):
        """
        Provision the requested cover fixtures beneath the fixture's temporary directory.

        Example:
            Exercise provision test covers. provision through a regression that loads the root fixture configuration::

                python -m pytest -q tests/support/test_resources_manager_assets_test.py


        :param names: Optional fixture names to copy; omission selects the complete asset
            group.
        :return: The provisioned cover-asset descriptor.
        """
        return test_resources_manager.provision_test_covers(dst_dir=tmp_path, names=names)

    return _provision



def _sqlite_has_fts5(conn: sqlite3.Connection) -> bool:
    """
    Probe an SQLite connection for FTS5 virtual-table support without retaining probe state.

    Example:
        Exercise  sqlite has fts5 through a regression that loads the root fixture configuration::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


    :param conn: SQLite connection used for the temporary FTS5 capability probe.
    :return: True when creating an FTS5 virtual table succeeds; otherwise False.
    """
    try:
        conn.execute("CREATE VIRTUAL TABLE temp._fts5_probe USING fts5(x)")
        conn.execute("DROP TABLE temp._fts5_probe")
        return True
    except sqlite3.OperationalError:
        return False


@pytest.fixture(scope="session")
def calibre_library_template_manager(tmp_path_factory: pytest.TempPathFactory):
    """
    Create the session manager for reusable blank Calibre-library templates.

    Example:
        Exercise calibre library template manager through a regression that loads the root fixture configuration::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


    :param tmp_path_factory: Session-scoped pytest factory used to locate a persistent
        temporary cache.
    :return: The configured Calibre-library template manager.
    """
    from tests.support.calibre_library_templates import CalibreLibraryTemplateManager

    cache_dir = tmp_path_factory.getbasetemp() / "liuxin_calibre_library_templates"
    return CalibreLibraryTemplateManager(cache_dir=cache_dir)


@pytest.fixture
def provision_calibre_library(tmp_path: Path, calibre_library_template_manager):
    # Calibre's current metadata schema requires FTS5.
    """
    Provide a factory for isolated blank Calibre libraries.

    Tests are skipped when the active SQLite build lacks the FTS5 support required by
    the current Calibre metadata schema.

    Example:
        Exercise provision calibre library through a regression that loads the root fixture configuration::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


    :param tmp_path: Pytest-managed directory that owns per-test runtime or provisioned
        data.
    :param calibre_library_template_manager: Session template manager used to clone
        blank libraries.
    :return: A callable accepting a library name and template options.
    """
    probe = sqlite3.connect(":memory:")
    try:
        if not _sqlite_has_fts5(probe):
            pytest.skip("SQLite build lacks FTS5; Calibre metadata schema requires it")
    finally:
        probe.close()

    def _provision(name: str = "calibre_library", **kwargs):
        """
        Provision an isolated blank Calibre library with the requested options.

        Example:
            Exercise provision calibre library. provision through a regression that loads the root fixture configuration::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


        :param name: Entry, database or library name addressed by the operation.
        :param kwargs: Keyword arguments or library options forwarded by the compatibility
            or provisioning call.
        :return: The provisioned Calibre-library descriptor.
        """
        return calibre_library_template_manager.provision_blank_library(
            dst_dir=tmp_path, name=name, **kwargs
        )

    return _provision


@pytest.fixture
def provision_populated_calibre_library(provision_calibre_library):
    """
    Provide a factory returning a blank Calibre library and its population builder.

    Example:
        Exercise provision populated calibre library through a regression that loads the root fixture configuration::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


    :param provision_calibre_library: Fixture factory that creates an isolated blank
        Calibre library.
    :return: A callable accepting a library name and template options.
    """

    from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import (
        CalibreLibraryBuilder,
    )

    def _provision(*, name: str = "calibre_library", **kwargs):
        """
        Provision a Calibre library and bind a builder to its root directory.

        Example:
            Exercise provision populated calibre library. provision through a regression that loads the root fixture configuration::

                python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_templates.py


        :param name: Entry, database or library name addressed by the operation.
        :param kwargs: Keyword arguments or library options forwarded by the compatibility
            or provisioning call.
        :return: A pair containing the provisioned library and its population builder.
        """
        lib = provision_calibre_library(name=name, **kwargs)
        builder = CalibreLibraryBuilder(lib.root)
        return lib, builder

    return _provision


@pytest.fixture(scope="session")
def html_ingest_fixtures_dir(project_root: Path) -> Path:
    """
    Resolve the checked-in HTML ingest fixture directory after corpus validation.

    Example:
        Exercise html ingest fixtures dir through a regression that loads the root fixture configuration::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param project_root: Repository root guarded against leaked test artifacts.
    :return: The validated HTML fixture directory.
    """
    from tests.support.html_ingest_fixture_access import resolve_html_ingest_fixture_dir

    return resolve_html_ingest_fixture_dir(project_root)


@pytest.fixture
def html_ingest_fixture(html_ingest_fixtures_dir: Path):
    """
    Provide hash-verified lookup of one named HTML ingest fixture.

    Example:
        Exercise html ingest fixture through a regression that loads the root fixture configuration::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param html_ingest_fixtures_dir: Validated directory containing the checked-in HTML
        fixture corpus.
    :return: A callable accepting a fixture filename and hash-verification flag.
    """
    from tests.support.html_ingest_fixture_access import get_verified_html_ingest_fixture_path

    def _get(*, filename: str, verify_hash: bool = True) -> Path:
        """
        Return one HTML fixture path, optionally verifying its recorded digest.

        Example:
            Exercise html ingest fixture. get through a regression that loads the root fixture configuration::

                python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


        :param filename: Recorded HTML fixture filename to resolve.
        :param verify_hash: Whether to compare fixture contents with the recorded digest
            before returning.
        :return: The verified fixture path.
        """
        return get_verified_html_ingest_fixture_path(
            fixture_dir=html_ingest_fixtures_dir,
            filename=filename,
            verify_hash=verify_hash,
        )

    return _get


@pytest.fixture
def html_ingest_fixtures(html_ingest_fixtures_dir: Path):
    """
    Provide iteration over the complete verified HTML ingest fixture corpus.

    Example:
        Exercise html ingest fixtures through a regression that loads the root fixture configuration::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param html_ingest_fixtures_dir: Validated directory containing the checked-in HTML
        fixture corpus.
    :return: A callable accepting a hash-verification flag.
    """
    from tests.support.html_ingest_fixture_access import iter_verified_html_ingest_fixtures

    def _get(*, verify_hash: bool = True) -> list[Path]:
        """
        Return all HTML fixture paths in stable order with optional digest verification.

        Example:
            Exercise html ingest fixtures. get through a regression that loads the root fixture configuration::

                python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


        :param verify_hash: Whether to compare fixture contents with the recorded digest
            before returning.
        :return: All verified fixture paths in stable order.
        """
        return list(iter_verified_html_ingest_fixtures(html_ingest_fixtures_dir, verify_hash=verify_hash))

    return _get
