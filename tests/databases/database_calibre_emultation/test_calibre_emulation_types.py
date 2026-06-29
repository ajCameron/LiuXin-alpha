"""Stage A1 tests for Calibre emulation reader types."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from LiuXin_alpha.utils.calibre_compat.calibre_database_emulation import (
    CalibreLibraryPaths,
    CalibreSchemaInfo,
    CalibreCustomColumnDef,
    CalibreSeriesRef,
    CalibreFormatRef,
    CalibreBookRow,
    CalibreBookNormalized,
    CalibreDriftEvent,
)


def test_calibre_library_paths_from_root(tmp_path: Path) -> None:
    root = tmp_path / "My Calibre Library"
    paths = CalibreLibraryPaths.from_root(root)

    assert paths.library_root == root
    assert paths.metadata_db_path == root / "metadata.db"
    assert paths.notes_db_path == root / ".calnotes" / "notes.db"
    assert paths.fts_db_path == root / "full-text-search.db"

    # JSON serialisable
    json.dumps(paths.to_dict())


def test_schema_info_to_dict_is_json_serialisable() -> None:
    cc = CalibreCustomColumnDef(
        num=7,
        label="my_col",
        name="My Column",
        datatype="series",
        is_multiple=False,
        display={"heading": "🚀"},
    )
    info = CalibreSchemaInfo(
        application_id=0x1234,
        user_version=27,
        tables=("books", "authors"),
        triggers=("books_delete_trg",),
        has_fts=True,
        has_notes=False,
        custom_columns=(cc,),
    )

    as_dict = info.to_dict()
    assert as_dict["user_version"] == 27
    assert as_dict["custom_columns"][0]["datatype"] == "series"
    json.dumps(as_dict)


def test_book_row_and_normalized_to_dict_are_json_serialisable(tmp_path: Path) -> None:
    fmt = CalibreFormatRef(fmt="EPUB", file_path=tmp_path / "book.epub", size_bytes=12)
    series = CalibreSeriesRef(name="The Saga", index=2.0)

    raw = CalibreBookRow(
        book_id=1,
        book_row={"id": 1, "title": "Hello", "path": "Hello (1)"},
        authors=({"id": 5, "name": "Alice"},),
        tags=("x", "y"),
        languages=("eng",),
        identifiers={"isbn": "9780000000002"},
        series=series,
        formats=(fmt,),
        comments_html="<p>Hi</p>",
        cover_path=tmp_path / "cover.jpg",
        custom_values={"#my_series": ("The Saga", 2.0)},
    )

    drift = CalibreDriftEvent(severity="warning", code="missing_format_file", message="missing", context={"fmt": "PDF"})

    norm = CalibreBookNormalized(
        calibre_book_id=1,
        title="Hello",
        authors=("Alice",),
        tags=("x", "y"),
        languages=("eng",),
        identifiers={"isbn": "9780000000000000"},
        series=series,
        formats=(fmt,),
        comments_html="<p>Hi</p>",
        cover_path=tmp_path / "cover.jpg",
        custom_values={"#my_series": {"name": "The Saga", "index": 2.0}},
        drift_events=(drift,),
        warnings=("missing_format:PDF",),
    )

    json.dumps(raw.to_dict())
    json.dumps(norm.to_dict())


def test_types_are_frozen() -> None:
    paths = CalibreLibraryPaths.from_root(Path("/tmp/x"))
    with pytest.raises(Exception):
        # frozen dataclass should not allow setting
        paths.library_root = Path("/tmp/y")
