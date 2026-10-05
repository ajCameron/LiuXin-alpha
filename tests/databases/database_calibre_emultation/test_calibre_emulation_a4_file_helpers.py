"""
Check format-file reads and strict versus non-strict handling of escaping book paths in generated libraries.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_a4_file_helpers.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreReader, CalibreUnsafePathError
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def test_open_format_reads_exact_bytes(provision_calibre_library) -> None:
    """
    Create one EPUB and check open_format returns its exact bytes through a managed file handle.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_a4_file_helpers.py::test_open_format_reads_exact_bytes


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_a4_open")
    b = CalibreLibraryBuilder(lib.root)

    payload_bytes = b"hello-format-bytes"
    b.add_book(
        title="File Helper Test",
        authors=["A. Author"],
        formats={"EPUB": payload_bytes},
    )

    r = CalibreReader.from_root(lib.root)
    p = next(iter(r.iter_book_payloads(batch_size=10)))
    assert len(p.formats) == 1

    with r.open_format(p.formats[0]) as fh:
        assert fh.read() == payload_bytes


def test_iter_book_payloads_strict_paths_raises_on_escape_attempt(provision_calibre_library) -> None:
    """
    Set books.path to a parent-directory escape and check strict payload iteration raises CalibreUnsafePathError.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_a4_file_helpers.py::test_iter_book_payloads_strict_paths_raises_on_escape_attempt


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_a4_strict")
    b = CalibreLibraryBuilder(lib.root)

    b.add_book(
        title="Escape Attempt",
        authors=["A. Author"],
        formats={"EPUB": b"x"},
    )

    # Simulate a malicious/broken DB where books.path tries to escape the library root.
    conn = b.connect()
    try:
        conn.execute("UPDATE books SET path='../escape' WHERE id=1")
        conn.commit()
    finally:
        conn.close()

    r = CalibreReader.from_root(lib.root)
    with pytest.raises(CalibreUnsafePathError):
        _ = next(iter(r.iter_book_payloads(batch_size=10, strict_paths=True)))


def test_iter_book_payloads_non_strict_paths_warns_and_continues(provision_calibre_library) -> None:
    """
    Set books.path to a parent-directory escape and check non-strict iteration yields a payload with an unsafe-path warning.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_a4_file_helpers.py::test_iter_book_payloads_non_strict_paths_warns_and_continues


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks the required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_a4_non_strict")
    b = CalibreLibraryBuilder(lib.root)

    b.add_book(
        title="Escape Attempt",
        authors=["A. Author"],
        formats={"EPUB": b"x"},
    )

    conn = b.connect()
    try:
        conn.execute("UPDATE books SET path='../escape' WHERE id=1")
        conn.commit()
    finally:
        conn.close()

    r = CalibreReader.from_root(lib.root)
    p = next(iter(r.iter_book_payloads(batch_size=10, strict_paths=False)))
    assert any("unsafe_book_path" in w for w in p.warnings)
