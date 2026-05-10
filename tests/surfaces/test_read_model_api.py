from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.caches import create_storage_cache
from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.read_sources import CacheMetadataReadSource
from LiuXin_alpha.surfaces.read_model import ReadModelBackend
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication, ReadOnlyWebConfig
from LiuXin_alpha.metadata.standardization import make_tag_search_term
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _build_backend(
    db: Database,
    *,
    read_source=None,
) -> tuple[ReadOnlyWebApplication, ReadModelBackend]:
    app = ReadOnlyWebApplication(
        db,
        config=ReadOnlyWebConfig(title="Read Model Test"),
        read_source=read_source,
    )
    return app, app.read_model


def _insert_work_row(db: Database, *, title: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "work_title": title,
            "work_canonical_title": title,
            "work_sort_title": title,
        },
        table="works",
    )
    return int(row["work_id"])


def _insert_store_row(db: Database, *, name: str, root_uri: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": "filesystem",
            "store_access_protocol": "file",
            "store_root_uri": root_uri,
        },
        table="stores",
    )
    return int(row["store_id"])


def _insert_agent_row(db: Database, *, name: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "agent_type": "person",
            "agent_canonical_name": name,
            "agent_sort_name": name,
        },
        table="agents",
    )
    return int(row["agent_id"])


def _insert_label_row(db: Database, *, text: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "label_text": text,
            "label_text_norm": make_tag_search_term(text),
        },
        table="labels",
    )
    return int(row["label_id"])


def _insert_tag_row(db: Database, *, text: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "tag": text,
            "tag_phash": make_tag_search_term(text),
        },
        table="tags",
    )
    return int(row["tag_id"])


def _insert_series_row(db: Database, *, name: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "series": name,
            "series_sort": name,
            "series_name_norm": make_tag_search_term(name),
        },
        table="series",
    )
    return int(row["series_id"])


def _insert_expression_row(db: Database, *, title_override: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={"expression_title_override": title_override},
        table="expressions",
    )
    return int(row["expression_id"])


def _insert_manifestation_row(db: Database, *, format_detail: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "manifestation_format_detail": format_detail,
            "manifestation_carrier_type": "ebook",
        },
        table="manifestations",
    )
    return int(row["manifestation_id"])


def _insert_item_row(db: Database, *, manifestation_id: int, source_path: str, source_name: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "item_manifestation_id": int(manifestation_id),
            "item_type": "ebook",
            "item_source": "fixture",
            "item_source_path": source_path,
            "item_source_name": source_name,
        },
        table="items",
    )
    return int(row["item_id"])


def _insert_file_row_for_item(db: Database, *, store_id: int, item_id: int, file_path: Path) -> int:
    ensure_surface_asset_tables(db)
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "file_store_id": int(store_id),
            "file_item_id": int(item_id),
            "file_storage_key": str(file_path.name),
            "file_name": str(file_path.name),
            "file_base_name": str(file_path.stem),
            "file_extension": str(file_path.suffix.lower().lstrip(".")),
            "file_original_path": str(file_path),
            "file_original_name": str(file_path.name),
            "file_role": "primary",
            "file_media_category": "ebook",
            "file_size_bytes": int(file_path.stat().st_size),
            "file_source": "fixture",
        },
        table="files",
    )
    return int(row["file_id"])


def _insert_image_row_for_item(db: Database, *, store_id: int, item_id: int, file_path: Path) -> int:
    ensure_surface_asset_tables(db, include_images=True)
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "image_item_id": int(item_id),
            "image_store_id": int(store_id),
            "image_storage_key": str(file_path.name),
            "image_name": str(file_path.name),
            "image_base_name": str(file_path.stem),
            "image_extension": str(file_path.suffix.lower().lstrip(".")),
            "image_original_path": str(file_path),
            "image_original_name": str(file_path.name),
            "image_mime_type": "image/png",
            "image_role": "cover",
            "image_media_category": "cover",
            "image_size_bytes": int(file_path.stat().st_size),
            "image_source": "fixture",
        },
        table="images",
    )
    return int(row["image_id"])


def test_read_model_category_rows_and_counts(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "read_model_categories.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Alpha Book")
        _insert_work_row(db, title="Beta Book")
        agent_id = _insert_agent_row(db, name="Alice Author")
        label_id = _insert_label_row(db, text="Adventure")
        series_id = _insert_series_row(db, name="Library Shelf")

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("series", series_id))

        _app, backend = _build_backend(db)

        author_rows = backend.category_rows("authors")
        tag_rows = backend.category_rows("tags")
        series_rows = backend.category_rows("series")
        summary = backend.category_summary_payload()
        author_collection = backend.category_items_payload("authors", num=10, offset=0, sort="name", sort_order="asc")
        assert backend.browse_count("titles") == 2
        assert backend.browse_count("authors") == len(author_rows)
        assert backend.browse_count("tags") == len(tag_rows)
        assert backend.browse_count("series") == len(series_rows)
        assert [entry["category"] for entry in summary] == ["allbooks", "newest", "authors", "tags", "series"]
        assert summary[0]["count"] == 2
        assert author_collection["category"] == "authors"
        assert author_collection["total_num"] == len(author_rows)
        assert author_collection["items"][0]["label"] == "Alice Author"
        assert any(row["label"] == "Alice Author" and row["count"] == 1 for row in author_rows)
        assert any(row["label"] == "Adventure" and row["count"] == 1 for row in tag_rows)
        assert any(row["label"] == "Library Shelf" and row["count"] == 1 for row in series_rows)


def test_read_model_prefers_real_tags_over_legacy_labels(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "read_model_real_tags.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Tagged Book")
        label_id = _insert_label_row(db, text="Legacy Label")
        tag_id = _insert_tag_row(db, text="Canonical Tag")

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("tags", tag_id))

        _app, backend = _build_backend(db)

        tag_rows = backend.category_rows("tags")
        metadata = backend.work_metadata_payload(work_row)

        assert backend.tag_category_table() == "tags"
        assert [row["table"] for row in tag_rows] == ["tags"]
        assert [row["label"] for row in tag_rows] == ["Canonical Tag"]
        assert metadata["tags"] == ["Canonical Tag"]


def test_read_model_work_and_file_payloads(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "read_model_work.sqlite"
    book_path = tmp_path / "alpha-book.epub"
    book_path.write_bytes(b"epub payload")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Alpha Book")
        store_id = _insert_store_row(db, name="Shelf", root_uri=str(tmp_path))
        agent_id = _insert_agent_row(db, name="Alice Author")
        label_id = _insert_label_row(db, text="Adventure")
        series_id = _insert_series_row(db, name="Library Shelf")
        expression_id = _insert_expression_row(db, title_override="Alpha Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(db, manifestation_id=manifestation_id, source_path=str(book_path), source_name=book_path.name)
        file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=book_path)

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("series", series_id))
        expression_row = db.get_row_from_id("expressions", expression_id)
        manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
        db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
        db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

        _app, backend = _build_backend(db)

        metadata = backend.work_metadata_payload(work_row)
        detail = backend.work_detail_payload(work_row)
        work_list = backend.work_list_payload(
            list(db.get_all_rows("works", iterator_return=False)),
            num=1,
            offset=0,
            sort="title",
            sort_order="asc",
        )
        books_metadata = backend.books_metadata_payload(list(db.get_all_rows("works", iterator_return=False)))
        file_payload = backend.file_detail_payload(db.get_row_from_id("files", file_id))

        assert metadata["title"] == "Alpha Book"
        assert metadata["authors"] == ["Alice Author"]
        assert metadata["tags"] == ["Adventure"]
        assert metadata["series"] == "Library Shelf"
        assert metadata["formats"] == ["EPUB"]
        assert detail["credits"][0]["entity"]["primary"] == "Alice Author"
        assert detail["files"][0]["id"] == file_id
        assert detail["related"]["labels"][0]["primary"] == "Adventure"
        assert work_list["total_num"] == 1
        assert work_list["num"] == 1
        assert work_list["book_ids"] == [work_id]
        assert str(work_id) in books_metadata
        assert books_metadata[str(work_id)]["title"] == "Alpha Book"
        assert file_payload["file"]["store_id"] == store_id
        assert file_payload["file"]["item_id"] == item_id
        assert file_payload["download_url"].endswith("/download")


def test_read_model_can_use_cache_read_source_without_database_fallback(
    driver_spec,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "read_model_cache_source.sqlite"
    book_path = tmp_path / "cached-book.epub"
    book_path.write_bytes(b"epub payload")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Cached Book")
        store_id = _insert_store_row(db, name="Shelf", root_uri=str(tmp_path))
        agent_id = _insert_agent_row(db, name="Cache Author")
        tag_id = _insert_tag_row(db, text="Cached Tag")
        series_id = _insert_series_row(db, name="Cached Series")
        expression_id = _insert_expression_row(db, title_override="Cached Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(
            db,
            manifestation_id=manifestation_id,
            source_path=str(book_path),
            source_name=book_path.name,
        )
        _insert_file_row_for_item(
            db,
            store_id=store_id,
            item_id=item_id,
            file_path=book_path,
        )

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(
            primary_row=work_row,
            secondary_row=db.get_row_from_id("agents", agent_id),
        )
        db.interlink_rows(
            primary_row=work_row,
            secondary_row=db.get_row_from_id("tags", tag_id),
        )
        db.interlink_rows(
            primary_row=work_row,
            secondary_row=db.get_row_from_id("series", series_id),
        )
        expression_row = db.get_row_from_id("expressions", expression_id)
        manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
        db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
        db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

        cache = create_storage_cache(db, "schema_backed")
        cache.read()
        read_source = CacheMetadataReadSource(
            cache,
            database=db,
            allow_database_fallback=False,
        )

        _insert_work_row(db, title="Uncached Book")
        _app, backend = _build_backend(db, read_source=read_source)

        work_rows = backend.work_rows(sorted_by="title")
        payload = backend.work_metadata_payload(work_rows[0])

        assert [row["work_title"] for row in work_rows] == ["Cached Book"]
        assert backend.browse_count("titles") == 1
        assert payload["authors"] == ["Cache Author"]
        assert payload["tags"] == ["Cached Tag"]
        assert payload["series"] == "Cached Series"
        assert payload["formats"] == ["EPUB"]
        assert payload["format_metadata"]["EPUB"]["name"] == "cached-book.epub"


def test_read_model_discovers_images_and_resolves_targets(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "read_model_assets.sqlite"
    book_path = tmp_path / "asset-book.epub"
    image_path = tmp_path / "cover.png"
    book_path.write_bytes(b"epub payload")
    image_path.write_bytes(b"\x89PNG\r\n\x1a\n")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Asset Book")
        store_id = _insert_store_row(db, name="Shelf", root_uri=str(tmp_path))
        expression_id = _insert_expression_row(db, title_override="Asset Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(db, manifestation_id=manifestation_id, source_path=str(book_path), source_name=book_path.name)
        _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=book_path)
        image_id = _insert_image_row_for_item(db, store_id=store_id, item_id=item_id, file_path=image_path)

        work_row = db.get_row_from_id("works", work_id)
        expression_row = db.get_row_from_id("expressions", expression_id)
        manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
        db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
        db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

        _app, backend = _build_backend(db)

        related_rows_by_table = _app._related_rows_by_table(work_row)
        image_rows = backend.work_image_rows(related_rows_by_table)
        image_row = backend.work_image_row(work_row)
        resolved = backend.resolve_image_target(image_row)
        placeholder = backend.placeholder_cover_svg(work_row, width=120, height=180)

        assert len(image_rows) == 1
        assert int(image_rows[0]["image_id"]) == image_id
        assert resolved is not None
        assert resolved.mode == "local"
        assert resolved.location.endswith("cover.png")
        assert backend.image_content_type(image_row) == "image/png"
        assert b"Asset Book" in placeholder
