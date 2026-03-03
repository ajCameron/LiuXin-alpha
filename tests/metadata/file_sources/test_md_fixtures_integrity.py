from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.md_test_fixture_hashes import EXPECTED_MD_TEST_FILE_HASHES, legacy_sha512_size_hash


def test_md_fixture_corpus_has_expected_file_set(md_test_files_dir: Path) -> None:
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
    target = md_test_files_dir / filename
    assert target.is_file(), f"Expected metadata fixture is missing: {target}"

    actual_hash = legacy_sha512_size_hash(target)
    assert actual_hash == expected_hash, (
        f"Fixture hash mismatch for {filename}\n"
        f"Expected: {expected_hash}\n"
        f"Actual:   {actual_hash}"
    )
