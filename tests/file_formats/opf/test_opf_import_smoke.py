"""
Import smoke tests for LiuXin_alpha.file_formats.opf.

The first test runs *without* any shims — it should pass once wiring is correct.
The second test applies a temporary legacy alias shim to let you iterate on deeper
OPF functionality even if the legacy import path is still present.
"""

from __future__ import annotations

import importlib
import traceback

import pytest


def test_import_opf_package_smoke_no_shims() -> None:
    """
    Smoke test: importing the opf package should not raise.

    If this fails, it usually indicates a hard import-time dependency / legacy path
    that should be moved behind a conditional import.
    """
    try:
        importlib.import_module("LiuXin_alpha.file_formats.opf")
    except Exception as e:
        tb = traceback.format_exc()
        pytest.fail(f"Importing LiuXin_alpha.file_formats.opf raised: {e!r}\n\n{tb}")


def test_import_opf_facade_smoke_with_legacy_alias(legacy_liuxin_alias) -> None:
    """
    Smoke test with legacy alias shim enabled: imports should succeed so functional tests can run.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.opf.opf")
    assert hasattr(mod, "get_metadata")
    assert hasattr(mod, "set_metadata")
