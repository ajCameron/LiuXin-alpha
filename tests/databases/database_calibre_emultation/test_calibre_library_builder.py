"""
Check generated book folders, format/cover presence, stored title/path, and insertions that exercise Calibre trigger functions.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_builder.py
"""
from __future__ import annotations

from pathlib import Path

def test_builder_can_add_book_and_files(provision_populated_calibre_library):
    """
    Add a book and check a positive ID, nested relative path, folder/file presence, format size, and stored title/path.

    The selected has_cover value and exact file contents are not asserted.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_builder.py::test_builder_can_add_book_and_files


    :param provision_populated_calibre_library: Fixture factory returning an isolated
        blank library and a builder for adding books; requires FTS5.
    :return: None; failed expectations raise AssertionError.
    """
    lib, builder = provision_populated_calibre_library(name="calibre_lib_builder")

    # Minimal valid JPEG (SOI + EOI) is enough for file presence testing.
    cover_bytes = b"\xff\xd8\xff\xd9"

    added = builder.add_book(
        title="The Nine Mirrors of Footfall",
        authors=["Septimus Vell"],
        tags=["grimdark", "warp"],
        languages=["eng"],
        identifiers={"isbn": "9780000000002"},
        comments_html="<p>Test import payload.</p>",
        formats={"EPUB": b"dummy-epub-bytes"},
        cover_bytes=cover_bytes,
    )

    # DB row was created and a sane relative path assigned.
    assert added.book_id > 0
    assert "/" in added.relative_path
    assert added.folder_path.is_dir()

    # Format file exists and matches schema expectations.
    assert "EPUB" in added.formats
    fmt = added.formats["EPUB"]
    assert fmt.file_path.exists()
    assert fmt.size == fmt.file_path.stat().st_size

    # cover.jpg is the canonical Calibre cover filename.
    assert (added.folder_path / "cover.jpg").exists()

    # Spot-check DB state.
    import sqlite3

    conn = sqlite3.connect(str(Path(lib.root) / "metadata.db"))
    try:
        row = conn.execute("SELECT title, path, has_cover FROM books WHERE id=?", (added.book_id,)).fetchone()
    finally:
        conn.close()

    assert row is not None
    assert row[0] == "The Nine Mirrors of Footfall"
    assert row[1] == added.relative_path


def test_builder_inserts_dont_trip_calibre_triggers(provision_populated_calibre_library):
    """
    Insert a PDF-bearing book through the builder and require a positive ID, exercising trigger-referenced functions without an exception.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_library_builder.py::test_builder_inserts_dont_trip_calibre_triggers


    :param provision_populated_calibre_library: Fixture factory returning an isolated
        blank library and a builder for adding books; requires FTS5.
    :return: None; failed expectations raise AssertionError.
    """
    _lib, builder = provision_populated_calibre_library(name="calibre_lib_triggers")
    added = builder.add_book(title="A Trigger Test", authors=["A. UDF"], formats={"PDF": b"%PDF-1.4\n"})
    assert added.book_id > 0
