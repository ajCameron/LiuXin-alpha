from __future__ import annotations

import sqlite3

import pytest

from LiuXin_alpha.databases.calibre_emulation import CalibreReader, CalibreUnsafePathError
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def test_open_format_reads_exact_bytes(provision_calibre_library) -> None:
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
