from __future__ import annotations

from pathlib import Path
import os
import sqlite3

from LiuXin_alpha.databases.calibre_emulation import CalibreReader


def _payload_by_id(reader: CalibreReader, book_id: int, **kwargs):
    for p in reader.iter_book_payloads(**kwargs):
        if p.calibre_book_id == book_id:
            return p
    raise AssertionError(f"book_id {book_id} not found")


def test_reconcile_salvages_formats_when_data_rows_missing(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_lib_c_salvage")

    added = builder.add_book(
        title="DB Missing Formats",
        authors=["Casefold Cat"],
        formats={"EPUB": b"epub-bytes"},
        cover_bytes=b"\xff\xd8\xff\xd9",
    )

    # Delete the DB format rows, leaving the format file on disk.
    conn = sqlite3.connect(str(Path(lib.root) / "metadata.db"))
    try:
        conn.execute("DELETE FROM data WHERE book=?", (added.book_id,))
        conn.commit()
    finally:
        conn.close()

    reader = CalibreReader.from_root(lib.root)
    payload = _payload_by_id(reader, added.book_id)

    assert any(f.fmt == "EPUB" for f in payload.formats)
    assert any(d.code == "db_missing_format_entries" for d in payload.drift_events)


def test_reconcile_recovers_missing_format_file_by_extension_scan(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_lib_c_recover")

    added = builder.add_book(
        title="Missing Format File",
        authors=["Casefold Cat"],
        formats={"EPUB": b"epub-bytes"},
    )

    # Remove the DB-expected file, but leave a replacement with the same extension.
    expected = added.formats["EPUB"].file_path
    expected.unlink()
    replacement = expected.parent / "Recovered.epub"
    replacement.write_bytes(b"epub-replacement")

    reader = CalibreReader.from_root(lib.root)
    payload = _payload_by_id(reader, added.book_id)

    assert any(f.file_path.name == "Recovered.epub" for f in payload.formats)
    assert any(d.code == "format_recovered_by_scan" for d in payload.drift_events)


def test_reconcile_flags_orphan_files_and_can_include_them(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_lib_c_orphans")

    added = builder.add_book(
        title="Orphan Files",
        authors=["Casefold Cat"],
        formats={"EPUB": b"epub-bytes"},
    )

    # Create an orphan format-like file not referenced by `data`.
    book_dir = added.folder_path
    orphan = book_dir / "extra.pdf"
    orphan.write_bytes(b"%PDF-1.4\n")

    reader = CalibreReader.from_root(lib.root)

    payload = _payload_by_id(reader, added.book_id, include_orphan_formats=False)
    assert any(d.code == "orphan_file" for d in payload.drift_events)
    assert not any(f.fmt == "PDF" for f in payload.formats)

    payload2 = _payload_by_id(reader, added.book_id, include_orphan_formats=True)
    assert any(f.fmt == "PDF" for f in payload2.formats)


def test_reconcile_detects_duplicate_format_files(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_lib_c_dupes")

    added = builder.add_book(
        title="Duplicate Files",
        authors=["Casefold Cat"],
        formats={"EPUB": b"epub-bytes"},
    )

    # Add a second file with the same extension.
    book_dir = added.folder_path
    dup = book_dir / "Another.epub"
    dup.write_bytes(b"epub-2")

    # Ensure mtime differs so newest is deterministic (pick_newest uses mtime_ns).
    try:
        os.utime(dup, None)
    except Exception:
        pass

    reader = CalibreReader.from_root(lib.root)
    payload = _payload_by_id(reader, added.book_id)

    assert any(d.code == "duplicate_format_files" for d in payload.drift_events)
