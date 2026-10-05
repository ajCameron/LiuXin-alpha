"""
Configure compatibility import shims for the pure sqlite3 driver tests.

The pytest hook mutates process-wide sys.modules entries without teardown
restoration. It installs a no-output clint fallback after any ordinary import
failure and attempts a historical LiuXin preferences alias.

Example:
    Run the consuming SQLite contracts with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py
"""

from __future__ import annotations

import sys
import types


def _install_legacy_module_aliases() -> None:
    """
    Install a historical LiuXin module and attempt to attach LiuXin_alpha.preferences under its old import path.

    Return immediately if LiuXin is already present. Suppress ordinary preference-import
    errors, leaving the top-level alias in place. No restoration is scheduled.

    Example:
        Run the consuming SQLite contracts with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


    :return: None; may mutate sys.modules and the alias module attributes.
    """

    # Some helpers still import via the historical top-level package name.
    # Example: `from LiuXin.preferences import preferences as prefs`
    if "LiuXin" in sys.modules:
        return

    liuxin = types.ModuleType("LiuXin")
    sys.modules["LiuXin"] = liuxin

    try:
        import importlib

        prefs_mod = importlib.import_module("LiuXin_alpha.preferences")
        sys.modules["LiuXin.preferences"] = prefs_mod
        setattr(liuxin, "preferences", prefs_mod)
    except Exception:
        # If prefs can't import, leave the alias in place and let the
        # underlying failure surface where it matters.
        pass


def _install_clint_stub() -> None:
    """
    Keep usable clint imports or install minimal clint/textui modules after an ordinary import failure.

    Return when both module keys already exist, or importing clint.textui succeeds.
    Otherwise replace both entries with a no-output puts function and identity color
    methods. The substitutions persist in sys.modules.

    Example:
        Run the consuming SQLite contracts with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


    :return: None; may replace imported module entries.
    """

    if "clint" in sys.modules and "clint.textui" in sys.modules:
        return

    try:
        import clint.textui  # noqa: F401

        return
    except Exception:
        pass

    clint = types.ModuleType("clint")
    textui = types.ModuleType("clint.textui")

    def puts(*_args, **_kwargs):  # pragma: no cover
        """
        Discard all terminal-output arguments without writing anything.

        Example:
            Run the consuming SQLite contracts with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


        :param _args: Ignored positional output arguments.
        :param _kwargs: Ignored keyword output options.
        :return: None.
        """
        return None

    class _Colored:  # pragma: no cover
        """
        Supply identity color methods for the fallback terminal-output module.

        Example:
            Run the consuming SQLite contracts with pytest::

                python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py
        """
        def green(self, s):
            """
            Return the supplied value unchanged instead of adding terminal color.

            Example:
                Run the consuming SQLite contracts with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


            :param s: Value to return; no string conversion or validation occurs.
            :return: The same object passed as s.
            """
            return s

        def red(self, s):
            """
            Return the supplied value unchanged instead of adding terminal color.

            Example:
                Run the consuming SQLite contracts with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


            :param s: Value to return; no string conversion or validation occurs.
            :return: The same object passed as s.
            """
            return s

        def yellow(self, s):
            """
            Return the supplied value unchanged instead of adding terminal color.

            Example:
                Run the consuming SQLite contracts with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


            :param s: Value to return; no string conversion or validation occurs.
            :return: The same object passed as s.
            """
            return s

        def blue(self, s):
            """
            Return the supplied value unchanged instead of adding terminal color.

            Example:
                Run the consuming SQLite contracts with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


            :param s: Value to return; no string conversion or validation occurs.
            :return: The same object passed as s.
            """
            return s

        def magenta(self, s):
            """
            Return the supplied value unchanged instead of adding terminal color.

            Example:
                Run the consuming SQLite contracts with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


            :param s: Value to return; no string conversion or validation occurs.
            :return: The same object passed as s.
            """
            return s

        def cyan(self, s):
            """
            Return the supplied value unchanged instead of adding terminal color.

            Example:
                Run the consuming SQLite contracts with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


            :param s: Value to return; no string conversion or validation occurs.
            :return: The same object passed as s.
            """
            return s

        def white(self, s):
            """
            Return the supplied value unchanged instead of adding terminal color.

            Example:
                Run the consuming SQLite contracts with pytest::

                    python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


            :param s: Value to return; no string conversion or validation occurs.
            :return: The same object passed as s.
            """
            return s

    textui.puts = puts  # type: ignore[attr-defined]
    textui.colored = _Colored()  # type: ignore[attr-defined]

    clint.textui = textui  # type: ignore[attr-defined]
    sys.modules["clint"] = clint
    sys.modules["clint.textui"] = textui


def pytest_configure(config) -> None:
    """
    Install the clint fallback and historical module aliases when pytest configures this test subtree.

    Example:
        Run the consuming SQLite contracts with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/SQLite_database_driver/test_sqlite_database_driver_comprehensive.py


    :param config: Unused pytest configuration object required by the hook signature.
    :return: None; modifies process-wide import state when shims are needed.
    """
    _install_clint_stub()
    _install_legacy_module_aliases()
