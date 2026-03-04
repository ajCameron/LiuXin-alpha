from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.html_ingest_fixture_access import get_verified_html_ingest_fixture_path


def test_html_ingest_fixture_access_returns_hash_verified_path(html_ingest_fixtures_dir: Path) -> None:
    path = get_verified_html_ingest_fixture_path(
        html_ingest_fixtures_dir,
        filename="html_ingest_case_001_comment_overrides_meta.html",
        verify_hash=True,
    )
    assert path.is_file()


def test_html_ingest_fixture_access_detects_hash_drift(tmp_path: Path, html_ingest_fixtures_dir: Path) -> None:
    source = html_ingest_fixtures_dir / "html_ingest_case_001_comment_overrides_meta.html"
    local_dir = tmp_path / "html_ingest"
    local_dir.mkdir()
    local_copy = local_dir / source.name
    local_copy.write_bytes(source.read_bytes() + b"\n")

    with pytest.raises(AssertionError):
        get_verified_html_ingest_fixture_path(
            local_dir,
            filename=source.name,
            verify_hash=True,
        )
