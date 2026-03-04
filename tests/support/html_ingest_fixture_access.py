from __future__ import annotations

from pathlib import Path
from typing import Iterable

from tests.support.html_ingest_fixture_hashes import (
    EXPECTED_HTML_INGEST_FIXTURE_HASHES,
    legacy_sha512_size_hash,
)


def resolve_html_ingest_fixture_dir(project_root: Path) -> Path:
    fixture_dir = project_root / "tests" / "fixtures" / "html_ingest"
    if not fixture_dir.is_dir():
        raise FileNotFoundError(f"HTML ingest fixture directory not found: {fixture_dir}")
    return fixture_dir


def expected_html_ingest_fixture_hash(filename: str) -> str:
    try:
        return EXPECTED_HTML_INGEST_FIXTURE_HASHES[filename]
    except KeyError as exc:
        raise KeyError(f"No expected hash registered for HTML ingest fixture: {filename}") from exc


def verify_html_ingest_fixture_hash(path: Path) -> str:
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
    for filename in sorted(EXPECTED_HTML_INGEST_FIXTURE_HASHES):
        yield get_verified_html_ingest_fixture_path(
            fixture_dir=fixture_dir,
            filename=filename,
            verify_hash=verify_hash,
        )
