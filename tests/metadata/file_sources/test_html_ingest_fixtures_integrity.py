from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.html_ingest_fixture_hashes import (
    EXPECTED_HTML_INGEST_FIXTURE_HASHES,
    legacy_sha512_size_hash,
)


def test_html_ingest_fixture_corpus_has_expected_file_set(html_ingest_fixtures_dir: Path) -> None:
    expected = set(EXPECTED_HTML_INGEST_FIXTURE_HASHES.keys())
    actual = {p.name for p in html_ingest_fixtures_dir.iterdir() if p.is_file() and not p.name.startswith(".")}

    missing = sorted(expected - actual)
    extra = sorted(actual - expected)

    assert not missing and not extra, (
        "HTML ingest fixture corpus differs from expected baseline.\n"
        f"Directory: {html_ingest_fixtures_dir}\n"
        f"Missing files ({len(missing)}): {missing}\n"
        f"Unexpected files ({len(extra)}): {extra}"
    )


@pytest.mark.parametrize("filename,expected_hash", sorted(EXPECTED_HTML_INGEST_FIXTURE_HASHES.items()))
def test_html_ingest_fixture_hashes_match_baseline(
    html_ingest_fixtures_dir: Path,
    filename: str,
    expected_hash: str,
) -> None:
    target = html_ingest_fixtures_dir / filename
    assert target.is_file(), f"Expected HTML ingest fixture is missing: {target}"

    actual_hash = legacy_sha512_size_hash(target)
    assert actual_hash == expected_hash, (
        f"Fixture hash mismatch for {filename}\n"
        f"Expected: {expected_hash}\n"
        f"Actual:   {actual_hash}"
    )
