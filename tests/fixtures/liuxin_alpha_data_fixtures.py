"""Fixtures for the optional external data repo: ``LiuXin_alpha_data``.

Context
-------

The main LiuXin-alpha repository *may* have a sibling folder at its top-level:

    ./LiuXin_alpha_data/

This is (typically) a separate Git repository containing larger / evolving fixture
corpora. The test-suite should still be runnable when the external repo is absent,
so these fixtures will skip cleanly if they cannot locate it.

Locating the data repo
----------------------

Environment variable:

  - ``LIUXIN_ALPHA_DATA_DIR``: absolute/relative path to the data repo.

Fallback locations (in order):

  1) ``$LIUXIN_ALPHA_DATA_DIR``
  2) ``<project_root>/LiuXin_alpha_data``
  3) ``<project_root>/../LiuXin_alpha_data``

Metadata-test corpus directory
------------------------------

Historically this folder has been called ``md_test_files``. In the current data repo
snapshot, it appears as ``md_test_books`` (a set of many ebook/document formats used
for metadata parsing).

The fixtures below treat *either* name as valid, preferring ``md_test_files`` when
present and falling back to ``md_test_books``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pytest


def _find_project_root(start: Path) -> Path:
    """Best-effort locate the main project root from an arbitrary start path."""

    start = start.resolve()
    candidates = [start] + list(start.parents)
    for p in candidates:
        # Heuristic: both folders exist in this repo.
        if (p / "src" / "LiuXin_alpha").is_dir() and (p / "tests").is_dir():
            return p
    # Fallback: tests are *in* the repo, so going up a couple levels is usually enough.
    return start.parents[2] if len(start.parents) >= 3 else start


def _resolve_data_repo_root(project_root: Path) -> Path | None:
    """Return data repo root if present, else None."""

    # 1) Explicit env var wins.
    env = os.environ.get("LIUXIN_ALPHA_DATA_DIR")
    if env:
        p = Path(env).expanduser()
        if not p.is_absolute():
            p = (project_root / p).resolve()
        if p.is_dir():
            return p

    # 2) Conventional checkout at repo top-level.
    p = project_root / "LiuXin_alpha_data"
    if p.is_dir():
        return p

    # 3) Sometimes checked out adjacent to the main repo.
    p2 = project_root.parent / "LiuXin_alpha_data"
    if p2.is_dir():
        return p2

    return None


def _resolve_md_corpus_dir(data_repo_root: Path) -> Tuple[Path | None, str | None]:
    """Return (dir_path, kind_name) for the metadata-test corpus, else (None, None)."""

    for name in ("md_test_files", "md_test_books"):
        p = data_repo_root / name
        if p.is_dir():
            return p, name
    return None, None


def _discover_all_files(root: Path) -> List[Path]:
    """Return all files under root (recursively), stable-sorted."""

    out: List[Path] = []
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        if p.name.startswith("."):
            continue
        out.append(p)

    out.sort(key=lambda x: str(x.relative_to(root)).casefold())
    return out


def _group_by_suffix(files: Iterable[Path]) -> Dict[str, List[Path]]:
    by: Dict[str, List[Path]] = {}
    for p in files:
        ext = p.suffix.lower().lstrip(".")
        by.setdefault(ext, []).append(p)
    for k in by:
        by[k].sort(key=lambda x: x.name.casefold())
    return dict(sorted(by.items(), key=lambda kv: kv[0]))


def _guarded_resolve(base: Path, relpath: str) -> Path:
    p = (base / relpath).resolve()
    # Guard against path traversal when relpath is user-controlled.
    if base not in p.parents and p != base:
        raise ValueError(f"Refusing to access outside corpus dir: {relpath}")
    return p


@pytest.fixture(scope="session")
def project_root() -> Path:
    """Main LiuXin-alpha repository root (best-effort)."""

    here = Path(__file__).resolve()
    return _find_project_root(here)


@pytest.fixture(scope="session")
def liuxin_alpha_data_root(project_root: Path) -> Path:
    """Path to the optional external data repo root.

    If the external repo isn't available, tests depending on it are skipped.
    """

    root = _resolve_data_repo_root(project_root)
    if root is None:
        pytest.skip(
            "Optional external fixture repo not found. "
            "Expected ./LiuXin_alpha_data (or set LIUXIN_ALPHA_DATA_DIR)."
        )
    return root


@pytest.fixture(scope="session")
def md_test_files_dir(liuxin_alpha_data_root: Path) -> Path:
    """Directory containing the metadata-test corpus (md_test_files/md_test_books)."""

    d, kind = _resolve_md_corpus_dir(liuxin_alpha_data_root)
    if d is None:
        pytest.skip(
            "External repo present but no metadata-test corpus directory found. "
            "Expected one of: md_test_files/, md_test_books/."
        )
    return d


@pytest.fixture(scope="session")
def md_test_files_kind(liuxin_alpha_data_root: Path) -> str:
    """Which corpus directory name was detected (md_test_files or md_test_books)."""

    d, kind = _resolve_md_corpus_dir(liuxin_alpha_data_root)
    if d is None or kind is None:
        pytest.skip(
            "External repo present but no metadata-test corpus directory found. "
            "Expected one of: md_test_files/, md_test_books/."
        )
    return kind


@pytest.fixture(scope="session")
def md_test_files(md_test_files_dir: Path) -> List[Path]:
    """All files in the metadata-test corpus (recursive, stable-sorted)."""

    return _discover_all_files(md_test_files_dir)


@pytest.fixture(scope="session")
def md_test_files_relpaths(md_test_files_dir: Path, md_test_files: List[Path]) -> List[str]:
    """Relative paths (POSIX) for corpus files (useful for parametrization)."""

    return [p.relative_to(md_test_files_dir).as_posix() for p in md_test_files]


@pytest.fixture(scope="session")
def md_test_files_by_ext(md_test_files: List[Path]) -> Dict[str, List[Path]]:
    """Corpus files grouped by extension (key is lowercased suffix without dot)."""

    return _group_by_suffix(md_test_files)


@pytest.fixture
def md_test_file_path(md_test_files_dir: Path):
    """Factory fixture: get a corpus file path by relative path."""

    def _get(relpath: str) -> Path:
        return _guarded_resolve(md_test_files_dir, relpath)

    return _get


@pytest.fixture
def md_test_file_bytes(md_test_files_dir: Path):
    """Factory fixture: read a corpus file as bytes by relative path."""

    def _read(relpath: str) -> bytes:
        p = _guarded_resolve(md_test_files_dir, relpath)
        return p.read_bytes()

    return _read


@pytest.fixture
def md_test_file_text(md_test_files_dir: Path):
    """Factory fixture: read a *textual* corpus file by relative path.

    This is intended for things like .txt/.html/.htm/.fb2/.pml/.rtf etc.
    For binary formats (epub/mobi/pdf/...) use ``md_test_file_bytes`` or
    ``md_test_file_path``.
    """

    allowed = {
        ".txt",
        ".html",
        ".htm",
        ".fb2",
        ".pml",
        ".rtf",
        ".md",
        ".xml",
    }

    def _read(relpath: str, *, encoding: str = "utf-8") -> str:
        p = _guarded_resolve(md_test_files_dir, relpath)
        if p.suffix.lower() not in allowed:
            raise TypeError(
                f"md_test_file_text is for textual fixtures only (got {p.suffix}). "
                "Use md_test_file_bytes or md_test_file_path instead."
            )
        return p.read_text(encoding=encoding, errors="replace")

    return _read


@dataclass(frozen=True)
class MDTestFile:
    """A single metadata-test corpus entry."""

    path: Path
    relpath: str
    suffix: str
    size_bytes: int


@pytest.fixture
def load_md_test_file(md_test_files_dir: Path):
    """Factory fixture: load a corpus entry with lightweight metadata.

    If you want the bytes, call ``md_test_file_bytes(relpath)``.
    """

    def _load(relpath: str) -> MDTestFile:
        p = _guarded_resolve(md_test_files_dir, relpath)
        return MDTestFile(
            path=p,
            relpath=relpath,
            suffix=p.suffix.lower(),
            size_bytes=p.stat().st_size,
        )

    return _load


# Convenience aliases (some folks mentally map "md_test_books" to these names).


@pytest.fixture(scope="session")
def md_test_books_dir(md_test_files_dir: Path) -> Path:
    return md_test_files_dir


@pytest.fixture(scope="session")
def md_test_books(md_test_files: List[Path]) -> List[Path]:
    return md_test_files


@pytest.fixture
def md_test_fixture(md_test_files_dir: Path):
    """
    Factory fixture: return one md fixture path with optional hash verification.

    Usage:
      - md_test_fixture(file_ext="pdb", file_num=1)
      - md_test_fixture(filename="pdb_md_test_file_1.pdb")
    """
    from tests.support.md_test_fixture_access import get_verified_md_fixture_path

    def _get(
        *,
        filename: str | None = None,
        file_ext: str | None = None,
        file_num: int | None = None,
        verify_hash: bool = True,
    ) -> Path:
        return get_verified_md_fixture_path(
            md_test_files_dir,
            filename=filename,
            file_ext=file_ext,
            file_num=file_num,
            verify_hash=verify_hash,
        )

    return _get


@pytest.fixture
def md_test_fixtures_for_ext(md_test_files_dir: Path):
    """
    Factory fixture: return all md fixtures for an extension, verified by hash.
    """
    from tests.support.md_test_fixture_access import iter_verified_md_fixtures

    def _get(*, file_ext: str, verify_hash: bool = True) -> List[Path]:
        return list(iter_verified_md_fixtures(md_test_files_dir, file_ext=file_ext, verify_hash=verify_hash))

    return _get
