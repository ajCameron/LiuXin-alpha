"""
Compare extracted Calibre fixture libraries with their complete normalized saved snapshots.

Discover fixture specifications at import and retain the skip when no data fixtures
are available.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_fixture_libraries_roundtrip.py
"""
from __future__ import annotations

from typing import List

import pytest

from tests.databases.calibre_fixture_libraries import (
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


@pytest.mark.parametrize("spec", _SPECS, ids=[s.id() for s in _SPECS])
def test_fixture_library_snapshot_roundtrip(spec: CalibreFixtureSpec, tmp_path):
    """
    Extract a fixture into temporary storage, read its live snapshot, and compare the whole normalized mapping with the saved expectation.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_fixture_libraries_roundtrip.py::test_fixture_library_snapshot_roundtrip


    :param spec: Discovered fixture descriptor with archive and expected-snapshot paths.
    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """
    expected = load_expected_snapshot(spec)
    lib_root = extract_library_zip(spec, tmp_path / spec.id().replace("/", "_"))
    actual = snapshot_calibre_library(lib_root)

    assert normalize_snapshot(actual) == normalize_snapshot(expected)
