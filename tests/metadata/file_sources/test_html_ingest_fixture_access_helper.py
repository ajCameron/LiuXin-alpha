"""
Verify safe access to HTML ingest fixtures.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test html ingest fixture access helper through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.support.html_ingest_fixture_access import get_verified_html_ingest_fixture_path


def test_html_ingest_fixture_access_returns_hash_verified_path(html_ingest_fixtures_dir: Path) -> None:
    """
    Verify html ingest fixture access returns hash verified path.

    Example:
        Exercise test html ingest fixture access returns hash verified path through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param html_ingest_fixtures_dir: Value supplied for html ingest fixtures dir in the
        focused test operation.
    :return: None; the function records state or raises through its assertions.
    """
    path = get_verified_html_ingest_fixture_path(
        html_ingest_fixtures_dir,
        filename="html_ingest_case_001_comment_overrides_meta.html",
        verify_hash=True,
    )
    assert path.is_file()


def test_html_ingest_fixture_access_detects_hash_drift(tmp_path: Path, html_ingest_fixtures_dir: Path) -> None:
    """
    Verify html ingest fixture access detects hash drift.

    Example:
        Exercise test html ingest fixture access detects hash drift through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param html_ingest_fixtures_dir: Value supplied for html ingest fixtures dir in the
        focused test operation.
    :return: None; the function records state or raises through its assertions.
    """
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
