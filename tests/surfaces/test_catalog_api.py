"""
Exercise the shared Calibre catalogue projection against a temporary database.

Fixtures build explicit Work/Expression/Manifestation/Item links and legacy
asset rows. Ebook and image payloads are opaque test bytes, not valid media to
parse. These tests cover backend queries and Core-mediated image reads without
starting an HTTP server; helpers mutate the supplied database in place.
"""

from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.catalog.api import CalibreCatalogBackend
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication, ReadOnlyWebConfig
from LiuXin_alpha.metadata.standardization import make_tag_search_term
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _build_backend(db: Database) -> tuple[ReadOnlyWebApplication, CalibreCatalogBackend]:
    """
    Compose a read-only host and a separate catalogue backend over its Core model.

    The supplied database remains owned by the caller. Construction may initialize
    the host's database-compatibility Core session; no socket is opened.

    Example:
        >>> app, backend = _build_backend(db)  # doctest: +SKIP


    :param db: Open fixture database containing the rows to expose.
    :return: Host titled Catalog Test and a backend borrowing its read/image owners.
    """
    app = ReadOnlyWebApplication(db, config=ReadOnlyWebConfig(title="Catalog Test"))
    return app, CalibreCatalogBackend(app)


def _insert_work_row(db: Database, *, title: str) -> int:
    """
    Insert a work with identical display, canonical, and sort titles.

    Example:
        >>> work_id = _insert_work_row(db, title="Alpha Book")  # doctest: +SKIP


    :param db: Open fixture database receiving the new works row.
    :param title: Text stored unchanged in all three title fields.
    :return: Integer ID assigned to the inserted work; no relationships are added.
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
    Register fixture metadata for a filesystem store using the file protocol.

    No directory is created or probed by this helper.

    Example:
        >>> store_id = _insert_store_row(db, name="Shelf", root_uri=str(tmp_path))  # doctest: +SKIP


    :param db: Open fixture database receiving the stores row.
    :param name: Display name for the store.
    :param root_uri: Filesystem root recorded verbatim for later asset resolution.
    :return: Integer ID of the inserted store.
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
    Insert a person agent with matching canonical and sort names.

    Example:
        >>> agent_id = _insert_agent_row(db, name="Alice Author")  # doctest: +SKIP


    :param db: Open fixture database receiving the agents row.
    :param name: Name stored without additional normalization.
    :return: Integer agent ID; linking the person to a work is the caller's job.
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
    Insert a legacy label and its standardized tag-search spelling.

    Example:
        >>> label_id = _insert_label_row(db, text="Adventure")  # doctest: +SKIP


    :param db: Open fixture database receiving the labels row.
    :param text: Display label, also passed through make_tag_search_term for lookup.
    :return: Integer label ID, without a work link or duplicate lookup.
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


def _insert_series_row(db: Database, *, name: str) -> int:
    """
    Insert a series with its display/sort name and standardized lookup text.

    Example:
        >>> series_id = _insert_series_row(db, name="Library Shelf")  # doctest: +SKIP


    :param db: Open fixture database receiving the series row.
    :param name: Unchanged display/sort text and input to make_tag_search_term.
    :return: Integer series ID; no work membership is created.
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
    Insert an unlinked expression carrying a title override.

    Example:
        >>> expression_id = _insert_expression_row(db, title_override="Alpha")  # doctest: +SKIP


    :param db: Open fixture database receiving the expressions row.
    :param title_override: Expression-specific title text stored verbatim.
    :return: Integer expression ID, ready for explicit fixture relationships.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={"expression_title_override": title_override},
        table="expressions",
    )
    return int(row["expression_id"])


def _insert_manifestation_row(db: Database, *, format_detail: str) -> int:
    """
    Insert an ebook manifestation with the requested format declaration.

    Example:
        >>> manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")  # doctest: +SKIP


    :param db: Open fixture database receiving the manifestations row.
    :param format_detail: Format label; this helper does not inspect ebook bytes.
    :return: Integer manifestation ID, without expression or item links.
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
    Insert a fixture ebook item referencing an existing manifestation.

    Source fields are recorded metadata rather than a request to open the file.

    Example:
        >>> item_id = _insert_item_row(db, manifestation_id=1, source_path="book.epub", source_name="book.epub")  # doctest: +SKIP


    :param db: Open fixture database receiving the items row.
    :param manifestation_id: Manifestation foreign key, converted to int.
    :param source_path: Source pathname recorded unchanged.
    :param source_name: Source filename recorded unchanged.
    :return: Integer item ID; a files row still needs to be added separately.
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
    Record a primary ebook asset using an existing file's names and byte size.

    Ensure the fixture asset schema first, then stat the path. The storage key is
    the basename, so the caller must align the chosen store root with that file.
    Content is neither copied nor parsed; schema/stat/insert failures propagate.

    Example:
        >>> file_id = _insert_file_row_for_item(db, store_id=1, item_id=2, file_path=book_path)  # doctest: +SKIP


    :param db: Open database whose SQLite-compatible asset tables may be added.
    :param store_id: Store foreign key converted to int.
    :param item_id: Item foreign key converted to int.
    :param file_path: Existing file supplying original path, basename, suffix, and size.
    :return: Integer ID of the inserted legacy files row.
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
    Record a fixture cover asset declared as PNG without validating its content.

    Ensure both file/image fixture tables and stat the path before insertion.
    The lowercase suffix supplies the extension even if it disagrees with PNG.

    Example:
        >>> image_id = _insert_image_row_for_item(db, store_id=1, item_id=2, file_path=image_path)  # doctest: +SKIP


    :param db: Open database whose SQLite-compatible asset tables may be added.
    :param store_id: Store foreign key converted to int; its root must match the basename key.
    :param item_id: Item foreign key converted to int.
    :param file_path: Existing file supplying path, filename, suffix, and size metadata.
    :return: Integer ID of the inserted images row; bytes remain at their original path.
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


def test_catalog_backend_category_summary_and_tag_browser(driver_spec, tmp_path: Path) -> None:
    """
    Verify linked category counts, work selection, and the three tag-browser roots.

    Two works and one linked person/legacy label/series exercise both all-books
    ordering and singleton facet membership through the shared backend.

    Example:
        >>> test_catalog_backend_category_summary_and_tag_browser(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest database-driver specification used to create the catalogue.
    :param tmp_path: Isolated pytest directory for the new catalogue database.
    :return: None after all category and navigation assertions pass.
    """
    db_path = tmp_path / "catalog_summary.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Alpha Book")
        beta_id = _insert_work_row(db, title="Beta Book")
        agent_id = _insert_agent_row(db, name="Alice Author")
        label_id = _insert_label_row(db, text="Adventure")
        series_id = _insert_series_row(db, name="Library Shelf")

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("series", series_id))

        _app, backend = _build_backend(db)

        summary = backend.category_summary_payload()
        author_rows = backend.category_rows("authors")
        tag_rows = backend.category_rows("tags")
        series_rows = backend.category_rows("series")
        assert [entry["category"] for entry in summary] == ["allbooks", "newest", "authors", "tags", "series"]
        assert summary[0]["count"] == 2
        assert summary[2]["count"] == len(author_rows)
        assert summary[3]["count"] == len(tag_rows)
        assert summary[4]["count"] == len(series_rows)

        assert any(row["label"] == "Alice Author" and row["count"] == 1 for row in author_rows)
        assert any(row["label"] == "Adventure" and row["count"] == 1 for row in tag_rows)
        assert any(row["label"] == "Library Shelf" and row["count"] == 1 for row in series_rows)
        assert [int(row["work_id"]) for row in backend.work_rows_for_category_item("authors", agent_id)] == [work_id]
        assert [int(row["work_id"]) for row in backend.work_rows_for_category_item("tags", label_id)] == [work_id]
        assert [int(row["work_id"]) for row in backend.work_rows_for_category_item("series", series_id)] == [work_id]
        assert [int(row["work_id"]) for row in backend.work_rows_for_category_item("allbooks", 0)] == [work_id, beta_id]

        tag_browser = backend.tag_browser_payload()
        assert len(tag_browser["root"]["children"]) == 3
        names = {payload["name"] for payload in tag_browser["item_map"].values()}
        assert "Authors" in names
        assert "Tags" in names
        assert "Series" in names
        assert "Adventure" in names


def test_catalog_backend_work_metadata_and_search_payload(driver_spec, tmp_path: Path) -> None:
    """
    Verify compatibility metadata, tokens, search receipts, and linked EPUB discovery.

    Build one complete WEMI path plus author/label/series links. The file contains
    opaque bytes; assertions cover metadata and URLs, not EPUB parsing or HTTP.

    Example:
        >>> test_catalog_backend_work_metadata_and_search_payload(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest driver specification for the fresh fixture catalogue.
    :param tmp_path: Isolated directory receiving the database and fake ebook.
    :return: None after the augmented metadata and search projections match the fixture.
    """
    db_path = tmp_path / "catalog_metadata.sqlite"
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
        _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=book_path)

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
        assert metadata["title"] == "Alpha Book"
        assert metadata["authors"] == ["Alice Author"]
        assert metadata["tags"] == ["Adventure"]
        assert metadata["series"] == "Library Shelf"
        assert metadata["formats"] == ["EPUB"]
        assert metadata["formats_detail"][0]["download_url"].endswith("/download")
        assert metadata["category_urls"]["authors"]
        assert metadata["thumbnail"].startswith("/get/thumb/")
        token_row = backend.work_row_from_token("{}_main".format(work_id))
        assert token_row is not None
        assert int(token_row["work_id"]) == work_id
        assert backend.work_row_from_token("not-a-book") is None
        assert [int(row["work_id"]) for row in backend.work_rows_for_ids("{},9999".format(work_id))] == [work_id]
        assert [int(row["work_id"]) for row in backend.search_work_rows("Alpha")] == [work_id]
        assert [int(row["work_id"]) for row in backend.work_rows_for_query_or_recent("Alpha")] == [work_id]

        payload = backend.search_result_payload(
            query_text="Alpha",
            rows=backend.work_rows(sorted_by="title"),
            num=10,
            offset=0,
            sort="title",
            sort_order="asc",
            base_url="/ajax/search/main",
        )
        assert payload["book_ids"] == [work_id]
        assert payload["num_books_without_search"] == 1
        assert payload["query"] == "Alpha"
        assert [int(row["work_id"]) for row in backend.metadata_rows_for_search_result([work_row], payload)] == [work_id]

        books_metadata = backend.books_metadata_payload([work_row])
        assert str(work_id) in books_metadata
        assert books_metadata[str(work_id)]["title"] == "Alpha Book"
        assert books_metadata[str(work_id)]["category_urls"]["authors"]


def test_catalog_backend_discovers_files_and_images_and_resolves_targets(driver_spec, tmp_path: Path) -> None:
    """
    Verify WEMI asset discovery, Core image bytes, metadata aliases, and fallback SVG.

    The image fixture is only a short PNG-like prefix. A local image yields no
    redirect target but does yield a Core byte reader; no raster decoder is used.

    Example:
        >>> test_catalog_backend_discovers_files_and_images_and_resolves_targets(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest database-driver specification for the asset catalogue.
    :param tmp_path: Isolated directory for the database, ebook bytes, and cover bytes.
    :return: None after discovery, byte equality, alias, and title-bearing SVG checks pass.
    """
    db_path = tmp_path / "catalog_assets.sqlite"
    book_path = tmp_path / "asset-book.epub"
    image_path = tmp_path / "cover.png"
    book_path.write_bytes(b"book bytes")
    image_path.write_bytes(b"\x89PNG\r\n")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Asset Book")
        store_id = _insert_store_row(db, name="Assets", root_uri=str(tmp_path))
        expression_id = _insert_expression_row(db, title_override="Asset Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(db, manifestation_id=manifestation_id, source_path=str(book_path), source_name=book_path.name)
        file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=book_path)
        image_id = _insert_image_row_for_item(db, store_id=store_id, item_id=item_id, file_path=image_path)

        work_row = db.get_row_from_id("works", work_id)
        expression_row = db.get_row_from_id("expressions", expression_id)
        manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
        db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
        db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

        app, backend = _build_backend(db)
        related = app._related_rows_by_table(work_row)

        file_rows = backend.work_file_rows(related)
        image_rows = backend.work_image_rows(related)
        assert [int(row["file_id"]) for row in file_rows] == [file_id]
        assert [int(row["image_id"]) for row in image_rows] == [image_id]

        image_row = backend.work_image_row(work_row)
        assert image_row is not None
        target = backend.resolve_image_target(image_row)
        assert target is None
        stored = backend.resolve_storage_image(image_row)
        assert stored is not None
        assert stored.read_bytes() == image_path.read_bytes()

        metadata = backend.image_storage_lookup_metadata(image_row)
        assert metadata["file_store_id"] == store_id
        assert metadata["file_storage_key"] == "cover.png"

        svg = backend.placeholder_cover_svg(work_row, width=80, height=120)
        assert b"<svg" in svg
        assert b"Asset Book" in svg
