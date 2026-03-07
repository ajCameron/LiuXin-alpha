from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from tests.support.md_test_fixture_access import (
    build_md_fixture_filename,
    get_verified_md_fixture_path,
    parse_md_fixture_filename,
)


def test_md_fixture_access_build_and_parse_roundtrip() -> None:
    filename = build_md_fixture_filename("pdb", 4)
    assert filename == "pdb_md_test_file_4.pdb"
    ext, num = parse_md_fixture_filename(filename)
    assert (ext, num) == ("pdb", 4)


def test_md_fixture_access_returns_hash_verified_path(md_test_files_dir: Path) -> None:
    path = get_verified_md_fixture_path(md_test_files_dir, file_ext="pdb", file_num=1, verify_hash=True)
    assert path.name == "pdb_md_test_file_1.pdb"
    assert path.is_file()


def test_md_fixture_access_detects_hash_drift(tmp_path: Path, md_test_files_dir: Path) -> None:
    # Copy a known fixture to a temp folder and mutate bytes while keeping filename.
    source = md_test_files_dir / "pdb_md_test_file_1.pdb"
    local_dir = tmp_path / "md_test_books"
    local_dir.mkdir()
    local_copy = local_dir / source.name
    shutil.copy2(source, local_copy)
    local_copy.write_bytes(local_copy.read_bytes() + b"\ncorruption\n")

    with pytest.raises(AssertionError, match="Fixture hash mismatch"):
        get_verified_md_fixture_path(local_dir, filename=source.name, verify_hash=True)
