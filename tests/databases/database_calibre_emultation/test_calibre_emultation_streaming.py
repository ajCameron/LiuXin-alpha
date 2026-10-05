"""
Check streamed Calibre book metadata, file paths, batching, and warnings for a deleted format file.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emultation_streaming.py
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import CalibreReader
from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator import CalibreLibraryBuilder


def _fetch_one_book_paths(metadata_db: Path) -> tuple[Path, str, str]:
    """
    Query the lowest-ID book and its first format by format order, then close the SQLite connection in finally.

    Require both rows to exist. Combine the stored book path with the database parent
    without independently checking containment.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emultation_streaming.py


    :param metadata_db: Database file to open with sqlite3; missing book or format rows
        raise AssertionError.
    :return: Book-directory Path, data-name string, and format string tuple.
    """
    conn = sqlite3.connect(str(metadata_db))
    try:
        row = conn.execute("SELECT id, path FROM books ORDER BY id LIMIT 1").fetchone()
        assert row is not None
        book_id, rel_path = int(row[0]), str(row[1])

        d = conn.execute(
            "SELECT name, format FROM data WHERE book=? ORDER BY format LIMIT 1",
            (book_id,),
        ).fetchone()
        assert d is not None
        data_name, fmt = str(d[0]), str(d[1])
        return (metadata_db.parent / rel_path, data_name, fmt)
    finally:
        conn.close()


def test_iter_book_payloads_basic_roundtrip(provision_calibre_library) -> None:
    """
    Check a generated book’s metadata, series, custom values, and existing EPUB/cover paths through one streamed payload.

    Tags are compared as a set and comments only need to contain Hello.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emultation_streaming.py::test_iter_book_payloads_basic_roundtrip


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_stream_basic")
    b = CalibreLibraryBuilder(lib.root)

    # Custom columns
    b.create_custom_column(label="cc_text", name="Note", datatype="text")
    b.create_custom_column(label="cc_series", name="AltSeries", datatype="series")

    b.add_book(
        title="The Nine Mirrors of Footfall",
        authors=["Septimus Vell"],
        tags=["grimdark", "warp"],
        languages=["eng"],
        identifiers={"isbn": "9780000000002"},
        comments_html="<p>Hello</p>",
        formats={"EPUB": b"dummy-epub"},
        cover_bytes=b"\xff\xd8\xff\xd9",
        custom_values={
            "cc_text": "import me",
            "cc_series": ("AltSaga", 2),
        },
        series=("MainSaga", 1),
    )

    r = CalibreReader.from_root(lib.root)
    payloads = list(r.iter_book_payloads(batch_size=10))

    assert len(payloads) == 1
    p = payloads[0]
    assert p.title == "The Nine Mirrors of Footfall"
    assert p.authors == ("Septimus Vell",)
    assert set(p.tags) == {"grimdark", "warp"}
    assert p.languages == ("eng",)
    assert p.identifiers.get("isbn") == "9780000000002"
    assert p.comments_html and "Hello" in p.comments_html

    assert p.series is not None
    assert p.series.name == "MainSaga"
    assert p.series.index == 1.0

    # Formats should be resolved to disk paths
    assert len(p.formats) == 1
    assert p.formats[0].fmt.upper() == "EPUB"
    assert p.formats[0].file_path.exists()

    # Cover path
    assert p.cover_path is not None
    assert p.cover_path.exists()

    # Custom values JSON-friendly
    assert p.custom_values["cc_text"] == "import me"
    assert p.custom_values["cc_series"]["name"] == "AltSaga"
    assert p.custom_values["cc_series"]["index"] == 2.0


def test_iter_book_payloads_batches(provision_calibre_library) -> None:
    """
    Generate twenty-five books, read with batch_size=7, and check total count and that the first ID is below the last.

    Intermediate ordering and ID uniqueness are not asserted.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emultation_streaming.py::test_iter_book_payloads_batches


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_stream_batches")
    b = CalibreLibraryBuilder(lib.root)

    for i in range(25):
        b.add_book(
            title=f"Book {i}",
            authors=[f"Author {i%3}"],
            tags=[f"tag{i%5}"],
            languages=["eng"],
            formats={"EPUB": b"epub"},
        )

    r = CalibreReader.from_root(lib.root)
    got = list(r.iter_book_payloads(batch_size=7))
    assert len(got) == 25
    assert got[0].calibre_book_id < got[-1].calibre_book_id


def test_iter_book_payloads_warns_on_missing_format_file(provision_calibre_library) -> None:
    """
    Delete the generated format file, trying lowercase then uppercase extension, and require a missing_format_file warning.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emultation_streaming.py::test_iter_book_payloads_warns_on_missing_format_file


    :param provision_calibre_library: Fixture factory creating an isolated blank
        library; skips when SQLite lacks required FTS5 support.
    :return: None; failed expectations raise AssertionError.
    """
    lib = provision_calibre_library(name="lib_stream_drift")
    b = CalibreLibraryBuilder(lib.root)

    b.add_book(
        title="Drift Test",
        authors=["A. Author"],
        formats={"EPUB": b"epub"},
    )

    # Delete the first format file on disk to simulate drift.
    book_dir, data_name, fmt = _fetch_one_book_paths(lib.metadata_db)

    # Expected file name is data_name + '.' + lower(fmt); fallback logic should still point there.
    drift_path = book_dir / f"{data_name}.{fmt.lower()}"
    if not drift_path.exists():
        # If builder used a different casing, try uppercase extension.
        drift_path = book_dir / f"{data_name}.{fmt.upper()}"
    assert drift_path.exists()
    drift_path.unlink()

    r = CalibreReader.from_root(lib.root)
    p = next(iter(r.iter_book_payloads(batch_size=10)))
    assert any("missing_format_file" in w for w in p.warnings)
