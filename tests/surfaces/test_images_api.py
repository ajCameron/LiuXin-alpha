"""
Exercise shared image discovery and local payload resolution against real catalogue databases.

Fixtures build a work/expression/manifestation/item graph and attach a cover to
the item. The PNG signature is only a byte-transfer fixture: these tests do not
decode raster data or prove that the payload is a complete, renderable image.
Database-driver parametrization comes from the project's driver_spec fixture.
"""

from __future__ import annotations

from pathlib import Path

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.catalog.api import CalibreCatalogBackend
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication, ReadOnlyWebConfig
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _build_app(db: Database) -> ReadOnlyWebApplication:
    """
    Wrap the supplied catalogue in a read-only application with shared surface backends.

    Example:
        >>> app = _build_app(database)  # doctest: +SKIP


    :param db: Open fixture database borrowed by the application session.
    :return: Application configured with the title ``Images Test``.
    """
    return ReadOnlyWebApplication(db, config=ReadOnlyWebConfig(title="Images Test"))


def _insert_work_row(db: Database, *, title: str) -> int:
    """
    Persist a work using the same title for display, canonical, and sorting fields.

    Example:
        >>> work_id = _insert_work_row(database, title="Image Book")  # doctest: +SKIP


    :param db: Open fixture database receiving the work row.
    :param title: Text assigned unchanged to all three work-title fields.
    :return: Integer identifier of the inserted work.
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
    Declare a filesystem store using the file access protocol without creating its directory.

    Example:
        >>> store_id = _insert_store_row(database, name="Shelf", root_uri=str(tmp_path))  # doctest: +SKIP


    :param db: Open fixture database receiving the store declaration.
    :param name: Display name recorded for the local store.
    :param root_uri: Existing fixture directory recorded as the store root.
    :return: Integer identifier of the inserted store.
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


def _insert_expression_row(db: Database, *, title_override: str) -> int:
    """
    Persist an expression title override without linking the expression to a work.

    Example:
        >>> expression_id = _insert_expression_row(database, title_override="Image Book")  # doctest: +SKIP


    :param db: Open fixture database receiving the expression row.
    :param title_override: Expression-specific title stored unchanged.
    :return: Integer identifier for subsequent graph linking.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={"expression_title_override": title_override},
        table="expressions",
    )
    return int(row["expression_id"])


def _insert_manifestation_row(db: Database, *, format_detail: str) -> int:
    """
    Persist an ebook manifestation with a caller-selected format label.

    Example:
        >>> manifestation_id = _insert_manifestation_row(database, format_detail="EPUB")  # doctest: +SKIP


    :param db: Open fixture database receiving the manifestation row.
    :param format_detail: Format text recorded without checking a physical ebook.
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
    Attach an ebook item to a manifestation and record fixture source-location metadata.

    This helper inserts metadata only; it does not open or ingest the source file.

    Example:
        >>> item_id = _insert_item_row(database, manifestation_id=manifestation_id, source_path=str(book_path), source_name=book_path.name)  # doctest: +SKIP


    :param db: Open fixture database receiving the item row.
    :param manifestation_id: Parent identifier converted to int for the foreign-key field.
    :param source_path: Source-file path recorded as item provenance.
    :param source_name: Source basename recorded separately from its path.
    :return: Integer identifier of the inserted item.
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


def _insert_image_row_for_item(db: Database, *, store_id: int, item_id: int, file_path: Path) -> int:
    """
    Ensure image support tables and register an existing fixture file as an item cover.

    The basename is the storage key, the file's stat supplies its size, and the
    suffix supplies its lowercase extension. MIME type remains ``image/png``
    regardless of the suffix or bytes; no image decoding takes place.

    Example:
        >>> image_id = _insert_image_row_for_item(database, store_id=store_id, item_id=item_id, file_path=image_path)  # doctest: +SKIP


    :param db: Open fixture database receiving any needed tables and the image row.
    :param store_id: Local store identifier converted to int for the image reference.
    :param item_id: Owning item identifier converted to int for the image reference.
    :param file_path: Existing cover fixture whose name, path, extension, and size are recorded.
    :return: Integer identifier of the inserted cover-image row.
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


def test_image_backend_discovers_and_resolves_cover_images(driver_spec, tmp_path: Path) -> None:
    """
    Discover an item cover through the full work graph and read its bytes through Core-backed storage.

    Also verify that a local file has no redirect target, its declared MIME type
    survives projection, and the generated placeholder includes the work title.
    All files and database state live under the temporary fixture directory.

    Example:
        >>> test_image_backend_discovers_and_resolves_cover_images(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parametrized database backend selected by the test configuration.
    :param tmp_path: Isolated directory for the catalogue, ebook bytes, and PNG-signature fixture.
    :return: None after graph discovery, storage-byte, MIME, and placeholder assertions.
    """
    db_path = tmp_path / "images_backend.sqlite"
    book_path = tmp_path / "image-book.epub"
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
        work_id = _insert_work_row(db, title="Image Book")
        store_id = _insert_store_row(db, name="Shelf", root_uri=str(tmp_path))
        expression_id = _insert_expression_row(db, title_override="Image Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(db, manifestation_id=manifestation_id, source_path=str(book_path), source_name=book_path.name)
        image_id = _insert_image_row_for_item(db, store_id=store_id, item_id=item_id, file_path=image_path)

        work_row = db.get_row_from_id("works", work_id)
        expression_row = db.get_row_from_id("expressions", expression_id)
        manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
        db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
        db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

        app = _build_app(db)
        backend = app.images
        related_rows_by_table = app._related_rows_by_table(work_row)

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
        assert b"Image Book" in placeholder


def test_readonly_app_shares_image_backend_across_layers(driver_spec, tmp_path: Path) -> None:
    """
    Require the read model and catalogue adapter to reuse the application's image backend by identity.

    Example:
        >>> test_readonly_app_shares_image_backend_across_layers(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Parametrized database backend used for the temporary catalogue.
    :param tmp_path: Isolated directory containing the database created for application wiring.
    :return: None after both consumer layers are shown to share the same backend object.
    """
    db_path = tmp_path / "images_shared.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        app = _build_app(db)
        catalog = CalibreCatalogBackend(app, read_model=app.read_model, images=app.images)

        assert app.read_model.images is app.images
        assert catalog.images is app.images
