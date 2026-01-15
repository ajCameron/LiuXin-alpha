"""
Local fixtures for OPF tests.

We provide an *optional* legacy alias shim for `import LiuXin...` to keep
functional tests runnable while the library is being cleaned up.

This fixture is not autouse: the import-smoke tests intentionally run *without* it.
"""

from __future__ import annotations

import sys
import types
import pytest


@pytest.fixture()
def legacy_liuxin_alias(monkeypatch):
    """
    Provide a minimal `LiuXin.file_formats.BeautifulSoup` module that exports
    BeautifulSoup from bs4, to satisfy legacy imports.

    This is intended as a temporary compatibility shim while wiring is fixed.
    """
    # Create a package-ish module hierarchy: LiuXin, LiuXin.file_formats, LiuXin.file_formats.BeautifulSoup
    liuxin = types.ModuleType("LiuXin")
    file_formats = types.ModuleType("LiuXin.file_formats")
    bs_mod = types.ModuleType("LiuXin.file_formats.BeautifulSoup")

    from LiuXin_alpha.utils.libraries.BeautifulSoup import BeautifulSoup

    bs_mod.BeautifulSoup = BeautifulSoup

    monkeypatch.setitem(sys.modules, "LiuXin", liuxin)
    monkeypatch.setitem(sys.modules, "LiuXin.file_formats", file_formats)
    monkeypatch.setitem(sys.modules, "LiuXin.file_formats.BeautifulSoup", bs_mod)

    return True
