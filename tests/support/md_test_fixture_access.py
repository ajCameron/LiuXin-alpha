from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

from tests.support.md_test_fixture_hashes import EXPECTED_MD_TEST_FILE_HASHES, legacy_sha512_size_hash

_NAME_RE = re.compile(r"^(?P<ext>[a-z0-9]+)_md_test_file_(?P<num>[0-9]+)\.(?P<suffix>[a-z0-9]+)$")


def build_md_fixture_filename(file_ext: str, file_num: int) -> str:
    ext = str(file_ext).lower().lstrip(".")
    return f"{ext}_md_test_file_{int(file_num)}.{ext}"


def parse_md_fixture_filename(filename: str) -> tuple[str, int]:
    match = _NAME_RE.match(filename)
    if match is None:
        raise ValueError(f"Invalid md fixture filename format: {filename!r}")
    ext = match.group("ext")
    suffix = match.group("suffix")
    if ext != suffix:
        raise ValueError(f"Fixture filename ext/suffix mismatch: {filename!r}")
    return ext, int(match.group("num"))


def expected_md_fixture_hash(filename: str) -> str:
    try:
        return EXPECTED_MD_TEST_FILE_HASHES[filename]
    except KeyError as exc:
        raise KeyError(f"No expected hash registered for fixture: {filename}") from exc


def resolve_md_fixture_path(
    md_test_files_dir: Path,
    *,
    filename: str | None = None,
    file_ext: str | None = None,
    file_num: int | None = None,
) -> Path:
    if filename is None:
        if file_ext is None or file_num is None:
            raise ValueError("Provide either filename or (file_ext and file_num).")
        filename = build_md_fixture_filename(file_ext=file_ext, file_num=file_num)

    path = md_test_files_dir / filename
    if not path.is_file():
        raise FileNotFoundError(f"Metadata fixture not found: {path}")
    return path


def verify_md_fixture_hash(path: Path) -> str:
    filename = path.name
    expected = expected_md_fixture_hash(filename)
    actual = legacy_sha512_size_hash(path)
    if actual != expected:
        raise AssertionError(
            "Fixture hash mismatch for %s\nexpected=%s\nactual=%s" % (filename, expected, actual)
        )
    return actual


def get_verified_md_fixture_path(
    md_test_files_dir: Path,
    *,
    filename: str | None = None,
    file_ext: str | None = None,
    file_num: int | None = None,
    verify_hash: bool = True,
) -> Path:
    path = resolve_md_fixture_path(
        md_test_files_dir=md_test_files_dir,
        filename=filename,
        file_ext=file_ext,
        file_num=file_num,
    )
    if verify_hash:
        verify_md_fixture_hash(path)
    return path


def iter_verified_md_fixtures(
    md_test_files_dir: Path,
    *,
    file_ext: str | None = None,
    verify_hash: bool = True,
) -> Iterable[Path]:
    for filename in sorted(EXPECTED_MD_TEST_FILE_HASHES):
        ext, _ = parse_md_fixture_filename(filename)
        if file_ext is not None and ext != file_ext.lower().lstrip("."):
            continue
        yield get_verified_md_fixture_path(
            md_test_files_dir,
            filename=filename,
            verify_hash=verify_hash,
        )
