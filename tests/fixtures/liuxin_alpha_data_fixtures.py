"""
Resolve and parametrize versioned fixture data from the LiuXin data checkout.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise liuxin alpha data fixtures through a consuming regression::

        python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import pytest


def _find_project_root(start: Path) -> Path:
    """
    Best-effort locate the main project root from an arbitrary start path.

    Example:
        Exercise  find project root through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param start: Value supplied for start under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    start = start.resolve()
    candidates = [start] + list(start.parents)
    for p in candidates:
        # Heuristic: both folders exist in this repo.
        if (p / "src" / "LiuXin_alpha").is_dir() and (p / "tests").is_dir():
            return p
    # Fallback: tests are *in* the repo, so going up a couple levels is usually enough.
    return start.parents[2] if len(start.parents) >= 3 else start


def _resolve_data_repo_root(project_root: Path) -> Path | None:
    """
    Return data repo root if present, else None.

    Example:
        Exercise  resolve data repo root through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param project_root: Repository root used to resolve checked-in fixture data.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

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
    """
    Return (dir_path, kind_name) for the metadata-test corpus, else (None, None).

    Example:
        Exercise  resolve md corpus dir through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param data_repo_root: Value supplied for data repo root under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    for name in ("md_test_files", "md_test_books"):
        p = data_repo_root / name
        if p.is_dir():
            return p, name
    return None, None


def _discover_all_files(root: Path) -> List[Path]:
    """
    Return all files under root (recursively), stable-sorted.

    Example:
        Exercise  discover all files through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param root: Root directory containing the fixture corpus or generated tree.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

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
    """
    Perform the group by suffix step with deterministic fixture inputs.

    Example:
        Exercise  group by suffix through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param files: Files included in the generated fixture or assertion.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    by: Dict[str, List[Path]] = {}
    for p in files:
        ext = p.suffix.lower().lstrip(".")
        by.setdefault(ext, []).append(p)
    for k in by:
        by[k].sort(key=lambda x: x.name.casefold())
    return dict(sorted(by.items(), key=lambda kv: kv[0]))


def _guarded_resolve(base: Path, relpath: str) -> Path:
    """
    Perform the guarded resolve step with deterministic fixture inputs.

    Example:
        Exercise  guarded resolve through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param base: Value supplied for base under the deterministic fixture contract.
    :param relpath: Value supplied for relpath under the deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    p = (base / relpath).resolve()
    # Guard against path traversal when relpath is user-controlled.
    if base not in p.parents and p != base:
        raise ValueError(f"Refusing to access outside corpus dir: {relpath}")
    return p


@pytest.fixture(scope="session")
def project_root() -> Path:
    """
    Main LiuXin-alpha repository root (best-effort).

    Example:
        Exercise project root through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    here = Path(__file__).resolve()
    return _find_project_root(here)


@pytest.fixture(scope="session")
def liuxin_alpha_data_root(project_root: Path) -> Path:
    """
    Path to the optional external data repo root.

    Example:
        Exercise liuxin alpha data root through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param project_root: Repository root used to resolve checked-in fixture data.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
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
    """
    Directory containing the metadata-test corpus (md_test_files/md_test_books).

    Example:
        Exercise md test files dir through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param liuxin_alpha_data_root: Value supplied for liuxin alpha data root under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    d, kind = _resolve_md_corpus_dir(liuxin_alpha_data_root)
    if d is None:
        pytest.skip(
            "External repo present but no metadata-test corpus directory found. "
            "Expected one of: md_test_files/, md_test_books/."
        )
    return d


@pytest.fixture(scope="session")
def md_test_files_kind(liuxin_alpha_data_root: Path) -> str:
    """
    Which corpus directory name was detected (md_test_files or md_test_books).

    Example:
        Exercise md test files kind through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param liuxin_alpha_data_root: Value supplied for liuxin alpha data root under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    d, kind = _resolve_md_corpus_dir(liuxin_alpha_data_root)
    if d is None or kind is None:
        pytest.skip(
            "External repo present but no metadata-test corpus directory found. "
            "Expected one of: md_test_files/, md_test_books/."
        )
    return kind


@pytest.fixture(scope="session")
def md_test_files(md_test_files_dir: Path) -> List[Path]:
    """
    All files in the metadata-test corpus (recursive, stable-sorted).

    Example:
        Exercise md test files through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    return _discover_all_files(md_test_files_dir)


@pytest.fixture(scope="session")
def md_test_files_relpaths(md_test_files_dir: Path, md_test_files: List[Path]) -> List[str]:
    """
    Relative paths (POSIX) for corpus files (useful for parametrization).

    Example:
        Exercise md test files relpaths through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :param md_test_files: Value supplied for md test files under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    return [p.relative_to(md_test_files_dir).as_posix() for p in md_test_files]


@pytest.fixture(scope="session")
def md_test_files_by_ext(md_test_files: List[Path]) -> Dict[str, List[Path]]:
    """
    Corpus files grouped by extension (key is lowercased suffix without dot).

    Example:
        Exercise md test files by ext through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files: Value supplied for md test files under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    return _group_by_suffix(md_test_files)


@pytest.fixture
def md_test_file_path(md_test_files_dir: Path):
    """
    Factory fixture: get a corpus file path by relative path.

    Example:
        Exercise md test file path through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    def _get(relpath: str) -> Path:
        """
        Perform the get step with deterministic fixture inputs.

        Example:
            Exercise md test file path. get through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


        :param relpath: Value supplied for relpath under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return _guarded_resolve(md_test_files_dir, relpath)

    return _get


@pytest.fixture
def md_test_file_bytes(md_test_files_dir: Path):
    """
    Factory fixture: read a corpus file as bytes by relative path.

    Example:
        Exercise md test file bytes through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    def _read(relpath: str) -> bytes:
        """
        Perform the read step with deterministic fixture inputs.

        Example:
            Exercise md test file bytes. read through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


        :param relpath: Value supplied for relpath under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        p = _guarded_resolve(md_test_files_dir, relpath)
        return p.read_bytes()

    return _read


@pytest.fixture
def md_test_file_text(md_test_files_dir: Path):
    """
    Factory fixture: read a *textual* corpus file by relative path.

    Example:
        Exercise md test file text through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
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
        """
        Perform the read step with deterministic fixture inputs.

        Example:
            Exercise md test file text. read through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


        :param relpath: Value supplied for relpath under the deterministic fixture contract.
        :param encoding: Character encoding used for deterministic fixture bytes.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
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
    """
    A single metadata-test corpus entry.

    Example:
        Exercise MDTestFile through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py
    """

    path: Path
    relpath: str
    suffix: str
    size_bytes: int


@pytest.fixture
def load_md_test_file(md_test_files_dir: Path):
    """
    Factory fixture: load a corpus entry with lightweight metadata.

    Example:
        Exercise load md test file through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """

    def _load(relpath: str) -> MDTestFile:
        """
        Perform the load step with deterministic fixture inputs.

        Example:
            Exercise load md test file. load through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


        :param relpath: Value supplied for relpath under the deterministic fixture contract.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
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
    """
    Perform the md test books dir step with deterministic fixture inputs.

    Example:
        Exercise md test books dir through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return md_test_files_dir


@pytest.fixture(scope="session")
def md_test_books(md_test_files: List[Path]) -> List[Path]:
    """
    Perform the md test books step with deterministic fixture inputs.

    Example:
        Exercise md test books through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files: Value supplied for md test files under the deterministic
        fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    return md_test_files


@pytest.fixture
def md_test_fixture(md_test_files_dir: Path):
    """
    Factory fixture: return one md fixture path with optional hash verification.

    Example:
        Exercise md test fixture through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    from tests.support.md_test_fixture_access import get_verified_md_fixture_path

    def _get(
        *,
        filename: str | None = None,
        file_ext: str | None = None,
        file_num: int | None = None,
        verify_hash: bool = True,
    ) -> Path:
        """
        Perform the get step with deterministic fixture inputs.

        Example:
            Exercise md test fixture. get through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


        :param filename: Archive member or fixture filename.
        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param file_num: Value supplied for file num under the deterministic fixture
            contract.
        :param verify_hash: Whether to validate fixture content against its recorded digest.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
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

    Example:
        Exercise md test fixtures for ext through a consuming regression::

            python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


    :param md_test_files_dir: Value supplied for md test files dir under the
        deterministic fixture contract.
    :return: The deterministic fixture value, path, bytes, record or collection
        described above.
    """
    from tests.support.md_test_fixture_access import iter_verified_md_fixtures

    def _get(*, file_ext: str, verify_hash: bool = True) -> List[Path]:
        """
        Perform the get step with deterministic fixture inputs.

        Example:
            Exercise md test fixtures for ext. get through a consuming regression::

                python -m pytest -q tests/metadata/file_sources/test_md_fixtures_integrity.py


        :param file_ext: Value supplied for file ext under the deterministic fixture
            contract.
        :param verify_hash: Whether to validate fixture content against its recorded digest.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return list(iter_verified_md_fixtures(md_test_files_dir, file_ext=file_ext, verify_hash=verify_hash))

    return _get
