"""
Provide conftest utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise conftest through a consuming regression::

        python -m pytest -q tests/file_formats/opf/conftest.py
"""

from __future__ import annotations

import sys
import types
import pytest


@pytest.fixture()
def legacy_liuxin_alias(monkeypatch):
    """
    Provide a minimal `LiuXin.file_formats.BeautifulSoup` module that exports BeautifulSoup from bs4, to satisfy legacy imports.

    Example:
        Exercise legacy liuxin alias through a consuming regression::

            python -m pytest -q tests/file_formats/opf/conftest.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
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
