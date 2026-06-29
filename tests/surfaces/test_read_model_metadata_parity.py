from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata import metadata_from_database
from LiuXin_alpha.metadata.containers import WorkMetadata

from tests.surfaces.test_read_model_api import (
    _build_backend,
    _insert_agent_row,
    _insert_expression_row,
    _insert_file_row_for_item,
    _insert_item_row,
    _insert_label_row,
    _insert_manifestation_row,
    _insert_series_row,
    _insert_store_row,
    _insert_tag_row,
    _insert_work_row,
)


def _fixture_row_value(row: Any, column: str) -> Any:
    if isinstance(row, Row):
        return row[column]
    if isinstance(row, Mapping):
        return row.get(column)
    try:
        return row[column]
    except Exception:
        return getattr(row, column, None)


def _first_text(target: Any, *columns: str) -> str:
    for column in columns:
        value = _fixture_row_value(target, column)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _relation_texts(metadata: WorkMetadata, relation: str, *columns: str) -> list[str]:
    return [
        text
        for text in (
            _first_text(link.target, *columns)
            for link in metadata.get_relation_links(relation)
        )
        if text
    ]


def _wemi_relation_texts(metadata: Any, level: str, relation: str, *columns: str) -> list[str]:
    return [
        text
        for text in (
            _first_text(link.target, *columns)
            for link in metadata.get_wemi_relation_links(level, relation)
        )
        if text
    ]


def _build_metadata_fixture(
    db: Database,
    tmp_path: Path,
    *,
    include_real_tag: bool = True,
    include_legacy_label: bool = True,
) -> dict[str, int]:
    book_path = tmp_path / "alpha-book.epub"
    book_path.write_bytes(b"epub payload")

    work_id = _insert_work_row(db, title="Alpha Book")
    store_id = _insert_store_row(db, name="Shelf", root_uri=str(tmp_path))
    agent_id = _insert_agent_row(db, name="Alice Author")
    series_id = _insert_series_row(db, name="Library Shelf")
    expression_id = _insert_expression_row(db, title_override="Alpha Book")
    manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
    item_id = _insert_item_row(
        db,
        manifestation_id=manifestation_id,
        source_path=str(book_path),
        source_name=book_path.name,
    )
    file_id = _insert_file_row_for_item(
        db,
        store_id=store_id,
        item_id=item_id,
        file_path=book_path,
    )

    work_row = db.get_row_from_id("works", work_id)
    db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id))
    db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("series", series_id))

    label_id = None
    if include_legacy_label:
        label_id = _insert_label_row(db, text="Adventure")
        db.interlink_rows(
            primary_row=work_row,
            secondary_row=db.get_row_from_id("labels", label_id),
        )

    tag_id = None
    if include_real_tag:
        tag_id = _insert_tag_row(db, text="Canonical Tag")
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("tags", tag_id))

    expression_row = db.get_row_from_id("expressions", expression_id)
    manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
    db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
    db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

    ids = {
        "work_id": work_id,
        "store_id": store_id,
        "agent_id": agent_id,
        "series_id": series_id,
        "expression_id": expression_id,
        "manifestation_id": manifestation_id,
        "item_id": item_id,
        "file_id": file_id,
    }
    if label_id is not None:
        ids["label_id"] = int(label_id)
    if tag_id is not None:
        ids["tag_id"] = int(tag_id)
    return ids


def test_read_model_payload_matches_work_metadata_relations(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "read_model_work_metadata_parity.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        ids = _build_metadata_fixture(db, tmp_path)
        work_row = db.get_row_from_id("works", ids["work_id"])
        _app, backend = _build_backend(db)

        payload = backend.work_metadata_payload(work_row)
        metadata = WorkMetadata.from_database(db, work_id=ids["work_id"])

        assert metadata.work is not None
        assert payload["id"] == metadata.work.work_id
        assert payload["title"] == metadata.work.work_title
        assert payload["authors"] == _relation_texts(
            metadata,
            "agents",
            "agent_canonical_name",
            "agent_sort_name",
        )
        assert payload["tags"] == _relation_texts(metadata, "tags", "tag", "tag_name")
        assert payload["series"] == _relation_texts(metadata, "series", "series", "series_name")[0]
        assert payload["formats"] == ["EPUB"]
        assert payload["format_metadata"]["EPUB"]["name"] == _relation_texts(
            metadata,
            "files",
            "file_name",
            "file_original_name",
        )[0]


def test_read_model_payload_matches_item_wemi_metadata_projection(
    driver_spec,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "read_model_wemi_metadata_parity.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        ids = _build_metadata_fixture(db, tmp_path)
        work_row = db.get_row_from_id("works", ids["work_id"])
        _app, backend = _build_backend(db)

        payload = backend.work_metadata_payload(work_row)
        metadata = metadata_from_database(db, item_id=ids["item_id"])

        assert payload["id"] == metadata.database_ids["work_id"]
        assert payload["title"] == metadata.title
        assert payload["tags"] == list(metadata.tags.keys())
        assert payload["series"] == next(iter(metadata.series.keys()))
        assert payload["formats"] == ["EPUB"]
        assert payload["format_metadata"]["EPUB"]["name"] == _wemi_relation_texts(
            metadata,
            "item",
            "files",
            "file_name",
            "file_original_name",
        )[0]


def test_read_model_label_fallback_matches_wemi_labels_when_real_tags_are_empty(
    driver_spec,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "read_model_wemi_label_fallback_parity.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        ids = _build_metadata_fixture(db, tmp_path, include_real_tag=False)
        work_row = db.get_row_from_id("works", ids["work_id"])
        _app, backend = _build_backend(db)

        payload = backend.work_metadata_payload(work_row)
        metadata = metadata_from_database(db, item_id=ids["item_id"])

        assert backend.tag_category_table() == "labels"
        assert list(metadata.tags.keys()) == []
        assert payload["tags"] == list(metadata.labels.keys())
