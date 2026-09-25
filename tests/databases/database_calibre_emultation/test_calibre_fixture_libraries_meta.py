"""
Check fixture discovery exclusions, extraction errors, and portable normalization of warning/context paths using temporary fixtures.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_fixture_libraries_meta.py
"""
from __future__ import annotations

import json
import zipfile
from pathlib import Path

import pytest

from tests.databases.calibre_fixture_libraries import (
    CalibreFixtureSpec,
    discover_calibre_fixtures,
    extract_library_zip,
    normalize_snapshot,
)


def test_discover_calibre_fixtures_ignores_incomplete_and_hidden_dirs(tmp_path: Path) -> None:
    """
    Create complete, incomplete, and underscore-prefixed fixture directories and require only the complete visible fixture to be discovered.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_fixture_libraries_meta.py::test_discover_calibre_fixtures_ignores_incomplete_and_hidden_dirs


    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """
    data_root = tmp_path / "LiuXin_alpha_data"
    root = data_root / "calibre_libraries"
    root.mkdir(parents=True)

    schema = root / "uv999_demo"
    schema.mkdir()

    good = schema / "01_good"
    good.mkdir()
    (good / "library.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)  # empty zip
    (good / "expected.json").write_text("{}", encoding="utf-8")

    bad_missing = schema / "02_missing_expected"
    bad_missing.mkdir()
    (bad_missing / "library.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)

    hidden = schema / "_scratch"
    hidden.mkdir()
    (hidden / "library.zip").write_bytes(b"PK\x05\x06" + b"\x00" * 18)
    (hidden / "expected.json").write_text("{}", encoding="utf-8")

    specs = discover_calibre_fixtures(data_root)
    assert [s.name for s in specs] == ["01_good"]


def test_extract_library_zip_raises_when_metadata_db_is_missing(tmp_path: Path) -> None:
    # Make a zip with no metadata.db
    """
    Create a fixture ZIP without metadata.db and check extraction raises RuntimeError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_fixture_libraries_meta.py::test_extract_library_zip_raises_when_metadata_db_is_missing


    :param tmp_path: Pytest-provided temporary directory for isolated database and
        fixture files.
    :return: None; failed expectations raise AssertionError.
    """
    z = tmp_path / "library.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("calibre_library/README.txt", "no db here")

    spec = CalibreFixtureSpec(
        schema_key="uv0_test",
        name="no_db",
        fixture_dir=tmp_path,
        library_zip=z,
        expected_json=tmp_path / "expected.json",
    )
    spec.expected_json.write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError):
        extract_library_zip(spec, tmp_path / "out")


def test_normalize_snapshot_strips_temp_prefixes_from_warnings_and_contexts() -> None:
    """
    Normalize POSIX and Windows-style temporary paths and check schema, warning, and drift prefixes begin at calibre_library.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_fixture_libraries_meta.py::test_normalize_snapshot_strips_temp_prefixes_from_warnings_and_contexts


    :return: None; failed expectations raise AssertionError.
    """
    snap = {
        "schema": {"issues": [{"context": {"path": "/tmp/foo/calibre_library/A/B"}}]},
        "books": [
            {
                "warnings": ["missing_format_file:/tmp/foo/calibre_library/A/B/book.epub"],
                "drift": [{"context": {"expected": "C:\\tmp\\foo\\calibre_library\\A\\B"}}],
            }
        ],
    }

    out = normalize_snapshot(snap)
    assert out["schema"]["issues"][0]["context"]["path"].startswith("calibre_library/")
    assert out["books"][0]["warnings"][0].startswith("missing_format_file:calibre_library/")
    assert out["books"][0]["drift"][0]["context"]["expected"].startswith("calibre_library/")
