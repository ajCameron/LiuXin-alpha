"""
Verify the Markdown fixture corpus remains complete and internally consistent.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test md fixtures integrity through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.md_test_fixture_hashes import EXPECTED_MD_TEST_FILE_HASHES, legacy_sha512_size_hash


def test_md_fixture_corpus_has_expected_file_set(md_test_files_dir: Path) -> None:
    """
    Verify md fixture corpus has expected file set.

    Example:
        Exercise test md fixture corpus has expected file set through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    expected = set(EXPECTED_MD_TEST_FILE_HASHES.keys())
    actual = {p.name for p in md_test_files_dir.iterdir() if p.is_file() and not p.name.startswith(".")}

    missing = sorted(expected - actual)
    extra = sorted(actual - expected)

    assert not missing and not extra, (
        "Metadata fixture corpus differs from expected baseline.\n"
        f"Directory: {md_test_files_dir}\n"
        f"Missing files ({len(missing)}): {missing}\n"
        f"Unexpected files ({len(extra)}): {extra}"
    )


@pytest.mark.parametrize("filename,expected_hash", sorted(EXPECTED_MD_TEST_FILE_HASHES.items()))
def test_md_fixture_file_hashes_match_legacy_baseline(
    md_test_files_dir: Path,
    filename: str,
    expected_hash: str,
) -> None:
    """
    Verify md fixture file hashes match legacy baseline.

    Example:
        Exercise test md fixture file hashes match legacy baseline through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir in the focused test
        operation.
    :param filename: Value supplied for filename in the focused test operation.
    :param expected_hash: Value supplied for expected hash in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    target = md_test_files_dir / filename
    assert target.is_file(), f"Expected metadata fixture is missing: {target}"

    actual_hash = legacy_sha512_size_hash(target)
    assert actual_hash == expected_hash, (
        f"Fixture hash mismatch for {filename}\n"
        f"Expected: {expected_hash}\n"
        f"Actual:   {actual_hash}"
    )
