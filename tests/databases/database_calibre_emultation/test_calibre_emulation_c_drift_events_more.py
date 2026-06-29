from __future__ import annotations

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreReader
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def _payload_by_id(reader: CalibreReader, book_id: int, **kwargs):
    for p in reader.iter_book_payloads(batch_size=50, **kwargs):
        if p.calibre_book_id == book_id:
            return p
    raise AssertionError(f"book_id {book_id} not found")


def test_c_orphan_files_are_reported_and_optionally_exposed_as_formats(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_c_orphans")
    b = CalibreLibraryBuilder(lib.root)

    added = b.add_book(
        title="Orphan Test",
        authors=["O. Author"],
        formats={"EPUB": b"epub"},
        cover_bytes=b"\xff\xd8\xff\xd9",
    )

    # Create an extra file that the DB doesn't know about.
    orphan = added.folder_path / "mystery.mobi"
    orphan.write_bytes(b"mobi-bytes")

    r = CalibreReader.from_root(lib.root)

    p = _payload_by_id(r, added.book_id, include_orphan_formats=False)
    assert any(d.code == "orphan_file" for d in p.drift_events)
    assert not any(f.fmt == "MOBI" for f in p.formats)

    p2 = _payload_by_id(r, added.book_id, include_orphan_formats=True)
    assert any(f.fmt == "MOBI" for f in p2.formats)


def test_c_missing_cover_file_is_reported(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_c_missing_cover")
    b = CalibreLibraryBuilder(lib.root)

    added = b.add_book(
        title="Cover Missing",
        authors=["C. Author"],
        formats={"EPUB": b"epub"},
        cover_bytes=b"\xff\xd8\xff\xd9",
    )

    cover = added.folder_path / "cover.jpg"
    assert cover.exists()
    cover.unlink()

    r = CalibreReader.from_root(lib.root)
    p = _payload_by_id(r, added.book_id)

    assert any(d.code == "missing_cover_file" for d in p.drift_events)


def test_c_duplicate_format_files_are_reported(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_c_dupe")
    b = CalibreLibraryBuilder(lib.root)

    added = b.add_book(
        title="Duplicate Formats",
        authors=["D. Author"],
        formats={"EPUB": b"epub"},
    )

    # Leave the DB-expected file in place, but add a second file with the same extension.
    expected = added.formats["EPUB"].file_path
    dupe = expected.parent / "SecondCopy.EPUB"
    dupe.write_bytes(b"epub-2")

    r = CalibreReader.from_root(lib.root)
    p = _payload_by_id(r, added.book_id)

    assert any(d.code == "duplicate_format_files" for d in p.drift_events)


def test_c_non_strict_unsafe_books_path_yields_drift_event_instead_of_raising(provision_calibre_library) -> None:
    lib = provision_calibre_library(name="lib_c_unsafe_non_strict")
    b = CalibreLibraryBuilder(lib.root)

    added = b.add_book(
        title="Unsafe Path",
        authors=["X. Author"],
        formats={"EPUB": b"x"},
    )

    # Force an escape attempt
    conn = b.connect()
    try:
        conn.execute("UPDATE books SET path='../escape' WHERE id=?", (added.book_id,))
        conn.commit()
    finally:
        conn.close()

    r = CalibreReader.from_root(lib.root)
    p = next(iter(r.iter_book_payloads(strict_paths=False)))

    assert p.title == "Unsafe Path"
    assert any(d.code == "unsafe_book_path" for d in p.drift_events)
