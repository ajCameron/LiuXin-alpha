"""
Resolve and validate the checked-in HTML ingest fixture corpus.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise html ingest fixture access through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from tests.support.html_ingest_fixture_hashes import (
    EXPECTED_HTML_INGEST_FIXTURE_HASHES,
    legacy_sha512_size_hash,
)


def resolve_html_ingest_fixture_dir(project_root: Path) -> Path:
    """
    Resolve html ingest fixture dir under the fixture contract.

    Example:
        Exercise resolve html ingest fixture dir through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param project_root: Repository root used to resolve checked-in fixture data.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    fixture_dir = project_root / "tests" / "fixtures" / "html_ingest"
    if not fixture_dir.is_dir():
        raise FileNotFoundError(f"HTML ingest fixture directory not found: {fixture_dir}")
    return fixture_dir


def expected_html_ingest_fixture_hash(filename: str) -> str:
    """
    Perform the expected html ingest fixture hash step with deterministic fixture inputs.

    Example:
        Exercise expected html ingest fixture hash through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param filename: Archive member or fixture filename.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    try:
        return EXPECTED_HTML_INGEST_FIXTURE_HASHES[filename]
    except KeyError as exc:
        raise KeyError(f"No expected hash registered for HTML ingest fixture: {filename}") from exc


def verify_html_ingest_fixture_hash(path: Path) -> str:
    """
    Perform the verify html ingest fixture hash step with deterministic fixture inputs.

    Example:
        Exercise verify html ingest fixture hash through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param path: Filesystem path read, written or validated by the fixture operation.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    filename = path.name
    expected = expected_html_ingest_fixture_hash(filename)
    actual = legacy_sha512_size_hash(path)
    if actual != expected:
        raise AssertionError(
            "HTML ingest fixture hash mismatch for %s\nexpected=%s\nactual=%s" % (filename, expected, actual)
        )
    return actual


def get_verified_html_ingest_fixture_path(
    fixture_dir: Path,
    *,
    filename: str,
    verify_hash: bool = True,
) -> Path:
    """
    Return verified html ingest fixture path under the fixture contract.

    Example:
        Exercise get verified html ingest fixture path through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param fixture_dir: Value supplied for fixture dir under the deterministic fixture
        contract.
    :param filename: Archive member or fixture filename.
    :param verify_hash: Whether to validate fixture content against its recorded digest.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    path = fixture_dir / filename
    if not path.is_file():
        raise FileNotFoundError(f"HTML ingest fixture not found: {path}")
    if verify_hash:
        verify_html_ingest_fixture_hash(path)
    return path


def iter_verified_html_ingest_fixtures(
    fixture_dir: Path,
    *,
    verify_hash: bool = True,
) -> Iterable[Path]:
    """
    Iterate verified html ingest fixtures under the fixture contract.

    Example:
        Exercise iter verified html ingest fixtures through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_html_ingest_fixture_access_helper.py


    :param fixture_dir: Value supplied for fixture dir under the deterministic fixture
        contract.
    :param verify_hash: Whether to validate fixture content against its recorded digest.
    :return: An iterator yielding the deterministic fixture values described above.
    """
    for filename in sorted(EXPECTED_HTML_INGEST_FIXTURE_HASHES):
        yield get_verified_html_ingest_fixture_path(
            fixture_dir=fixture_dir,
            filename=filename,
            verify_hash=verify_hash,
        )
