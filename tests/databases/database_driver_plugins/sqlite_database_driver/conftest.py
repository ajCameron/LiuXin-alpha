"""Local test configuration for the SQLite driver tests.

The project ships two SQLite driver plugins:

* ``SQLite``: pure-python implementation backed by the stdlib ``sqlite3``.
* ``SQLite_apsw``: legacy/optional APSW-backed variant.

These tests are intended to validate the **pure** driver so the project remains
widely deployable even when APSW is unavailable.

The SQLite driver still imports a couple of optional/legacy UI helpers
(notably ``clint``). To keep the test suite runnable in lightweight
environments (CI, contributor machines), we provide a tiny stub for that
dependency when it is missing.

This file is intentionally scoped to the SQLite driver tests only.
"""

from __future__ import annotations

import sys
import types


def _install_legacy_module_aliases() -> None:
    """Provide minimal legacy import paths used by older calibre-derived code."""

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
    """Install a minimal `clint.textui` stub used by terminal helpers."""

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


def pytest_configure(config) -> None:
    _install_clint_stub()
    _install_legacy_module_aliases()
