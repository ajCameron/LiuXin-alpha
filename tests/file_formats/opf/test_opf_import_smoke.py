"""
Provide test opf import smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test opf import smoke through a consuming regression::

        python -m pytest -q tests/file_formats/opf/test_opf_import_smoke.py
"""

from __future__ import annotations

import importlib
import traceback

import pytest


def test_import_opf_package_smoke_no_shims() -> None:
    """
    Smoke test: importing the opf package should not raise.

    Example:
        Exercise test import opf package smoke no shims through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_import_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    try:
        importlib.import_module("LiuXin_alpha.file_formats.opf")
    except Exception as e:
        tb = traceback.format_exc()
        pytest.fail(f"Importing LiuXin_alpha.file_formats.opf raised: {e!r}\n\n{tb}")


def test_import_opf_facade_smoke_with_legacy_alias(legacy_liuxin_alias) -> None:
    """
    Smoke test with legacy alias shim enabled: imports should succeed so functional tests can run.

    Example:
        Exercise test import opf facade smoke with legacy alias through a consuming regression::

            python -m pytest -q tests/file_formats/opf/test_opf_import_smoke.py


    :param legacy_liuxin_alias: Value supplied for legacy liuxin alias under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.opf.opf")
    assert hasattr(mod, "get_metadata")
    assert hasattr(mod, "set_metadata")
