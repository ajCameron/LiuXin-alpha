from __future__ import annotations

import shutil


def test_iter_import_jobs_full_for_clean_books(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_jobs_clean")

    builder.add_book(
        title="Clean Book",
        authors=["A. Author"],
        tags=["tag"],
        languages=["eng"],
        identifiers={"isbn": "9780000000000"},
        formats={"EPUB": b"dummy-epub"},
        cover_bytes=b"\xff\xd8\xff\xd9",
    )

    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import iter_import_jobs

    it = iter_import_jobs(lib.root)
    job = next(it)

    assert job.action == "full"
    assert job.payload is not None
    assert job.payload.title == "Clean Book"
    assert len(job.payload.formats) == 1
    assert job.payload.formats[0].fmt == "EPUB"


def test_iter_import_jobs_metadata_only_when_book_folder_missing(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_jobs_missing_folder")

    added = builder.add_book(
        title="Folder Missing",
        authors=["B. Author"],
        formats={"PDF": b"%PDF-1.4\n"},
        cover_bytes=b"\xff\xd8\xff\xd9",
    )

    # Simulate a common real-world drift case: DB exists but folders were not copied.
    shutil.rmtree(added.folder_path)

    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import iter_import_jobs

    job = next(iter_import_jobs(lib.root))

    assert job.action == "metadata_only"
    assert job.payload is not None
    assert job.payload.title == "Folder Missing"
    # In metadata-only mode we strip file references by default.
    assert job.payload.formats == ()
    assert job.payload.cover_path is None
    assert any(r.startswith("drift:error:missing_book_folder") for r in (job.reasons or ()))


def test_iter_import_jobs_full_when_one_of_multiple_formats_missing(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_jobs_missing_one_format")

    added = builder.add_book(
        title="Two Formats",
        authors=["C. Author"],
        formats={"EPUB": b"dummy-epub", "PDF": b"%PDF-1.4\n"},
    )

    # Delete one of the formats to provoke a warning drift event.
    pdf_path = added.formats["PDF"].file_path
    pdf_path.unlink()

    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import iter_import_jobs

    job = next(iter_import_jobs(lib.root))
    assert job.action == "full"
    assert job.payload is not None
    fmts = {f.fmt for f in job.payload.formats}
    assert "EPUB" in fmts
    # The missing PDF is dropped from resolved formats when reconcile is enabled.
    assert "PDF" not in fmts
    assert any(r.startswith("drift:warning:missing_format_file") for r in (job.reasons or ()))


def test_iter_import_jobs_metadata_only_on_unsafe_book_path(provision_populated_calibre_library):
    lib, builder = provision_populated_calibre_library(name="calibre_jobs_unsafe_path")

    added = builder.add_book(
        title="Unsafe Path",
        authors=["D. Author"],
        formats={"EPUB": b"dummy-epub"},
    )

    # Force a malicious-ish relative path into books.path.
    conn = builder.connect()
    try:
        conn.execute("UPDATE books SET path=? WHERE id=?", ("../evil_escape", added.book_id))
        conn.commit()
    finally:
        conn.close()

    from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import iter_import_jobs

    job = next(iter_import_jobs(lib.root, strict_paths=False))
    assert job.action == "metadata_only"
    assert job.payload is not None
    assert any(r.startswith("drift:error:unsafe_book_path") for r in (job.reasons or ()))
