"""
Exercise shared catalogue browsing, metadata, cache-only reads, and image delivery against real temporary databases.

Fixture helpers persist WEMI entities and linkable metadata directly rather than
running ingestion. Ebook bytes and the PNG signature are transfer fixtures, not
validated publications or renderable images. Database-driver selection is provided
by driver_spec; cache isolation is verified with a deliberately uncached work.
"""

from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.caches import Cache, create_storage_cache
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
    """
    Build a read-only application around a borrowed database and expose its shared read-model instance.

    Example:
        >>> app, backend = _build_backend(database)  # doctest: +SKIP


    :param db: Open fixture catalogue borrowed by the application's Core session.
    :param read_source: Optional metadata provider forwarded unchanged for cache-backed composition.
    :return: Application configured as Read Model Test and its own read_model object.
    """
    app = ReadOnlyWebApplication(
        db,
        config=ReadOnlyWebConfig(title="Read Model Test"),
        read_source=read_source,
    )
    return app, app.read_model


def _insert_work_row(db: Database, *, title: str) -> int:
    """
    Persist a work whose display, canonical, and sort titles all use the supplied text.

    Example:
        >>> work_id = _insert_work_row(database, title="Alpha Book")  # doctest: +SKIP


    :param db: Open database receiving the work row.
    :param title: Text stored unchanged in all three title columns.
    :return: Integer primary key of the new work.
    """
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
    """
    Declare a local filesystem store using the file protocol without creating its directory.

    Example:
        >>> store_id = _insert_store_row(database, name="Shelf", root_uri=str(tmp_path))  # doctest: +SKIP


    :param db: Open database receiving the store declaration.
    :param name: Display name of the fixture store.
    :param root_uri: Existing temporary directory recorded as the store root.
    :return: Integer primary key of the new store.
    """
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
    """
    Persist a person agent with matching canonical and sort names, without linking it to a work.

    Example:
        >>> agent_id = _insert_agent_row(database, name="Alice Author")  # doctest: +SKIP


    :param db: Open database receiving the agent row.
    :param name: Display/sort name stored unchanged.
    :return: Integer agent identifier for later credit linking.
    """
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
    """
    Persist a legacy label and its standardized search text for tag-fallback tests.

    Example:
        >>> label_id = _insert_label_row(database, text="Adventure")  # doctest: +SKIP


    :param db: Open database receiving the label row.
    :param text: Visible label text also passed through make_tag_search_term for normalization.
    :return: Integer identifier of the inserted label.
    """
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
    """
    Persist a canonical tag with its standardized matching value in tag_phash.

    Example:
        >>> tag_id = _insert_tag_row(database, text="Canonical Tag")  # doctest: +SKIP


    :param db: Open database receiving the tag row.
    :param text: Visible tag text retained in tag and normalized through make_tag_search_term.
    :return: Integer identifier for later work/tag linking.
    """
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
    """
    Persist a series with matching display/sort names and standardized matching text.

    Example:
        >>> series_id = _insert_series_row(database, name="Library Shelf")  # doctest: +SKIP


    :param db: Open database receiving the series row.
    :param name: Series display/sort text also used to derive series_name_norm.
    :return: Integer identifier of the inserted series.
    """
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
    """
    Persist an expression title override without establishing its work relationship.

    Example:
        >>> expression_id = _insert_expression_row(database, title_override="Alpha Book")  # doctest: +SKIP


    :param db: Open database receiving the expression row.
    :param title_override: Expression-specific title stored unchanged.
    :return: Integer identifier used by later graph construction.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={"expression_title_override": title_override},
        table="expressions",
    )
    return int(row["expression_id"])


def _insert_manifestation_row(db: Database, *, format_detail: str) -> int:
    """
    Persist an ebook manifestation with a caller-selected format description.

    Example:
        >>> manifestation_id = _insert_manifestation_row(database, format_detail="EPUB")  # doctest: +SKIP


    :param db: Open database receiving the manifestation row.
    :param format_detail: Format label recorded without inspecting any physical payload.
    :return: Integer identifier of the still-unlinked manifestation.
    """
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
    """
    Persist an ebook item under a manifestation with fixture source-file provenance.

    The helper records metadata only and neither opens nor ingests the source.

    Example:
        >>> item_id = _insert_item_row(database, manifestation_id=manifestation_id, source_path=str(book_path), source_name=book_path.name)  # doctest: +SKIP


    :param db: Open database receiving the item row.
    :param manifestation_id: Parent identifier converted with int for the foreign-key column.
    :param source_path: Source location stored in item provenance.
    :param source_name: Source filename stored separately from its full path.
    :return: Integer identifier of the new item.
    """
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
    """
    Ensure file support tables and register an existing fixture payload as an item's primary ebook.

    The basename becomes the storage key, the suffix supplies a lowercase
    extension, and stat supplies its size. No format parser or ingestion runs.

    Example:
        >>> file_id = _insert_file_row_for_item(database, store_id=store_id, item_id=item_id, file_path=book_path)  # doctest: +SKIP


    :param db: Open database receiving any required support tables and the file row.
    :param store_id: Store identifier converted to int for the file reference.
    :param item_id: Owning item identifier converted to int for the file reference.
    :param file_path: Existing payload whose path/name/suffix/size become file metadata.
    :return: Integer primary key of the inserted file.
    :raises OSError: If obtaining the fixture file's stat fails.
    """
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
    """
    Ensure image support tables and register an existing fixture payload as an item cover with fixed PNG MIME metadata.

    Name, basename, lowercase extension, and size come from file_path; neither
    bytes nor suffix are checked against the declared image/png MIME type.

    Example:
        >>> image_id = _insert_image_row_for_item(database, store_id=store_id, item_id=item_id, file_path=image_path)  # doctest: +SKIP


    :param db: Open database receiving any required support tables and the cover row.
    :param store_id: Local store identifier converted to int for the image reference.
    :param item_id: Owning item identifier converted to int for the image reference.
    :param file_path: Existing image fixture supplying storage/path/name/size metadata.
    :return: Integer primary key of the inserted image.
    :raises OSError: If reading the fixture file's stat fails.
    """
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
    """
    Build linked work/agent/label/series fixtures and verify browse counts, navigation order, and author paging.

    Example:
        >>> test_read_model_category_rows_and_counts(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parametrized database backend selected by the test configuration.
    :param tmp_path: Isolated directory containing the temporary catalogue.
    :return: None after category labels, linked-work counts, fixed summary order, and page assertions.
    """
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
    """
    Prefer a populated canonical tag table over simultaneously linked legacy labels in browsing and metadata.

    Example:
        >>> test_read_model_prefers_real_tags_over_legacy_labels(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parametrized backend used for the fixture database.
    :param tmp_path: Temporary catalogue directory.
    :return: None after selected table, browse label, and work-tag metadata assertions.
    """
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
    """
    Traverse a real work/expression/manifestation/item graph into work, credit, file, paging, and related-entity payloads.

    Example:
        >>> test_read_model_work_and_file_payloads(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parametrized backend for the temporary catalogue.
    :param tmp_path: Isolated root for database and ebook-transfer bytes.
    :return: None after metadata facets/formats, detail projections, visible IDs, and file-reference/route checks.
    """
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
    """
    Keep reads on a loaded schema-backed cache after inserting an additional uncached database work.

    The selected CacheMetadataReadSource explicitly disables database fallback.
    Original cached WEMI relationships and file metadata remain available while
    the newly inserted work must not leak into browse counts or work listings.

    Example:
        >>> test_read_model_can_use_cache_read_source_without_database_fallback(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parametrized database backend underlying the loaded cache.
    :param tmp_path: Temporary database and cached-book fixture directory.
    :return: None after cache-only visibility and related author/tag/series/format assertions.
    """
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
        cache = Cache.from_storage(cache)
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
    """
    Discover an item cover through a work graph and delegate local image bytes, MIME, and SVG fallback through the read model.

    Example:
        >>> test_read_model_discovers_images_and_resolves_targets(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parametrized backend used to store the temporary WEMI/image graph.
    :param tmp_path: Isolated root for the database, ebook bytes, and PNG-signature fixture.
    :return: None after image identity, no-local-redirect, stored-byte equality, MIME, and placeholder-title assertions.
    """
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
        assert resolved is None
        stored = backend.resolve_storage_image(image_row)
        assert stored is not None
        assert stored.read_bytes() == image_path.read_bytes()
        assert backend.image_content_type(image_row) == "image/png"
        assert b"Asset Book" in placeholder
