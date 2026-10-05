"""
Resolve and validate versioned metadata test fixtures.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise md test fixture access through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable

from tests.support.md_test_fixture_hashes import EXPECTED_MD_TEST_FILE_HASHES, legacy_sha512_size_hash

_NAME_RE = re.compile(r"^(?P<ext>[a-z0-9]+)_md_test_file_(?P<num>[0-9]+)\.(?P<suffix>[a-z0-9]+)$")


def build_md_fixture_filename(file_ext: str, file_num: int) -> str:
    """
    Build md fixture filename for deterministic fixture consumers.

    Example:
        Exercise build md fixture filename through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py


    :param file_ext: Value supplied for file ext under the deterministic fixture
        contract.
    :param file_num: Value supplied for file num under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    ext = str(file_ext).lower().lstrip(".")
    return f"{ext}_md_test_file_{int(file_num)}.{ext}"


def parse_md_fixture_filename(filename: str) -> tuple[str, int]:
    """
    Parse md fixture filename under the fixture contract.

    Example:
        Exercise parse md fixture filename through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py


    :param filename: Archive member or fixture filename.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    match = _NAME_RE.match(filename)
    if match is None:
        raise ValueError(f"Invalid md fixture filename format: {filename!r}")
    ext = match.group("ext")
    suffix = match.group("suffix")
    if ext != suffix:
        raise ValueError(f"Fixture filename ext/suffix mismatch: {filename!r}")
    return ext, int(match.group("num"))


def expected_md_fixture_hash(filename: str) -> str:
    """
    Perform the expected md fixture hash step with deterministic fixture inputs.

    Example:
        Exercise expected md fixture hash through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py


    :param filename: Archive member or fixture filename.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Resolve md fixture path under the fixture contract.

    Example:
        Exercise resolve md fixture path through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :param filename: Archive member or fixture filename.
    :param file_ext: Value supplied for file ext under the deterministic fixture
        contract.
    :param file_num: Value supplied for file num under the deterministic fixture
        contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    if filename is None:
        if file_ext is None or file_num is None:
            raise ValueError("Provide either filename or (file_ext and file_num).")
        filename = build_md_fixture_filename(file_ext=file_ext, file_num=file_num)

    path = md_test_files_dir / filename
    if not path.is_file():
        raise FileNotFoundError(f"Metadata fixture not found: {path}")
    return path


def verify_md_fixture_hash(path: Path) -> str:
    """
    Perform the verify md fixture hash step with deterministic fixture inputs.

    Example:
        Exercise verify md fixture hash through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Return verified md fixture path under the fixture contract.

    Example:
        Exercise get verified md fixture path through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :param filename: Archive member or fixture filename.
    :param file_ext: Value supplied for file ext under the deterministic fixture
        contract.
    :param file_num: Value supplied for file num under the deterministic fixture
        contract.
    :param verify_hash: Whether to validate fixture content against its recorded digest.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
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
    """
    Iterate verified md fixtures under the fixture contract.

    Example:
        Exercise iter verified md fixtures through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixture_access_helper.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :param file_ext: Value supplied for file ext under the deterministic fixture
        contract.
    :param verify_hash: Whether to validate fixture content against its recorded digest.
    :return: An iterator yielding the deterministic fixture values described above.
    """
    for filename in sorted(EXPECTED_MD_TEST_FILE_HASHES):
        ext, _ = parse_md_fixture_filename(filename)
        if file_ext is not None and ext != file_ext.lower().lstrip("."):
            continue
        yield get_verified_md_fixture_path(
            md_test_files_dir,
            filename=filename,
            verify_hash=verify_hash,
        )
