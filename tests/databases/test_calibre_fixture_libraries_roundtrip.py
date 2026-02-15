from __future__ import annotations

from typing import List

import pytest

from .calibre_fixture_libraries import (
    CalibreFixtureSpec,
    discover_calibre_fixtures,
    extract_library_zip,
    find_data_repo_root,
    load_expected_snapshot,
    normalize_snapshot,
    snapshot_calibre_library,
)


_DATA_ROOT = find_data_repo_root()
_SPECS: List[CalibreFixtureSpec] = []
if _DATA_ROOT is not None:
    _SPECS = discover_calibre_fixtures(_DATA_ROOT)

pytestmark = pytest.mark.skipif(
    not _SPECS,
    reason=(
        "No Calibre fixture libraries found. "
        "Check out LiuXin_alpha_data or set LIUXIN_ALPHA_DATA_ROOT."
    ),
)


@pytest.mark.parametrize("spec", _SPECS, ids=lambda s: s.id())
def test_fixture_library_snapshot_roundtrip(spec: CalibreFixtureSpec, tmp_path):
    expected = load_expected_snapshot(spec)
    lib_root = extract_library_zip(spec, tmp_path / spec.id().replace("/", "_"))
    actual = snapshot_calibre_library(lib_root)

    assert normalize_snapshot(actual) == normalize_snapshot(expected)
