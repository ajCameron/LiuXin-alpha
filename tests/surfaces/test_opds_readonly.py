"""
Verify standalone OPDS composition, cached reads, routing, and local acquisition.

Requests call the WSGI application in process and close its response iterable;
no HTTP listener is started. Database fixtures construct an explicit WEMI path
to opaque ebook bytes, testing delivery rather than ebook format validity.
"""

from __future__ import annotations

from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.opds_readonly import (
    OpdsReadOnlyApplication,
    OpdsReadOnlyConfig,
    build_arg_parser,
)
from LiuXin_alpha.surfaces.opds.api import opds_nav_token
from LiuXin_alpha.surfaces.web_calibre_readonly import CalibreReadOnlyWebApplication
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _call_app(app, path: str, *, method: str = "GET"):
    """
    Execute one in-process WSGI request and collect the complete byte response.

    Split the path at its first question mark and preserve the raw path/query
    spelling. Repeated response headers collapse to the last value. Close a
    closeable response even if joining its chunks fails; application errors
    themselves propagate rather than becoming synthetic HTTP responses.

    Example:
        >>> status, headers, body = _call_app(app, "/opds")  # doctest: +SKIP


    :param app: Callable WSGI application accepting environ and start_response.
    :param path: Request path with an optional raw query string.
    :param method: HTTP method passed unchanged in REQUEST_METHOD.
    :return: Status text, a header dict, and all response bytes.
    """
    environ = {}
    setup_testing_defaults(environ)
    if "?" in path:
        raw_path, query_string = path.split("?", 1)
    else:
        raw_path, query_string = path, ""
    environ["REQUEST_METHOD"] = method
    environ["PATH_INFO"] = raw_path
    environ["QUERY_STRING"] = query_string
    captured: dict[str, object] = {}

    def start_response(status, headers, exc_info=None):
        """
        Capture the latest WSGI status and headers without implementing write().

        Example:
            >>> start_response("200 OK", [("Content-Type", "text/plain")])  # doctest: +SKIP


        :param status: WSGI status text retained in the enclosing capture dict.
        :param headers: Header pairs converted to a dict, collapsing duplicate names.
        :param exc_info: Ignored error context; this test callback does not re-raise it.
        :return: None; applications using WSGI's returned write callable are unsupported.
        """
        del exc_info
        captured["status"] = status
        captured["headers"] = dict(headers)

    result = app(environ, start_response)
    try:
        body = b"".join(result)
    finally:
        close = getattr(result, "close", None)
        if callable(close):
            close()
    return str(captured["status"]), dict(captured["headers"]), body


def _insert_work_row(db: Database, *, title: str) -> int:
    """
    Insert a work with the same display, canonical, and sort title.

    Example:
        >>> work_id = _insert_work_row(db, title="OPDS Book")  # doctest: +SKIP


    :param db: Open fixture database receiving the works row.
    :param title: Text copied unchanged into all three title fields.
    :return: Integer ID of the newly inserted, unlinked work.
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
    Insert file-protocol filesystem-store metadata without creating a directory.

    Example:
        >>> store_id = _insert_store_row(db, name="Downloads", root_uri=str(tmp_path))  # doctest: +SKIP


    :param db: Open fixture database receiving the stores row.
    :param name: Human-readable store name.
    :param root_uri: Root path stored unchanged for subsequent file resolution.
    :return: Integer ID of the inserted store, without a filesystem availability check.
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
    Insert an expression with a title override and no explicit work link.

    Example:
        >>> expression_id = _insert_expression_row(db, title_override="OPDS Book")  # doctest: +SKIP


    :param db: Open fixture database receiving the expressions row.
    :param title_override: Text assigned to expression_title_override unchanged.
    :return: Integer expression ID for later fixture linking.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={"expression_title_override": title_override},
        table="expressions",
    )
    return int(row["expression_id"])


def _insert_manifestation_row(db: Database, *, format_detail: str) -> int:
    """
    Insert an ebook manifestation bearing the supplied format label.

    Example:
        >>> manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")  # doctest: +SKIP


    :param db: Open fixture database receiving the manifestations row.
    :param format_detail: Declared format text, not a content-validation request.
    :return: Integer ID of the unlinked manifestation.
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
    Insert a fixture ebook item whose parent manifestation is already known.

    Example:
        >>> item_id = _insert_item_row(db, manifestation_id=1, source_path="book.epub", source_name="book.epub")  # doctest: +SKIP


    :param db: Open fixture database receiving the items row.
    :param manifestation_id: Parent manifestation ID, converted to int.
    :param source_path: Path metadata recorded without opening the source.
    :param source_name: Source filename recorded unchanged.
    :return: Integer item ID; no file asset is inserted by this helper.
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


def _insert_file_row_for_item(db: Database, *, store_id: int, item_id: int | None, file_path: Path) -> int:
    """
    Record an existing file as a primary ebook, optionally linked to an item.

    Ensure the SQLite-compatible asset table before statting the file. The
    basename becomes its storage key; bytes are not copied or parsed. A None
    item ID omits file_item_id altogether rather than storing an explicit null.

    Example:
        >>> file_id = _insert_file_row_for_item(db, store_id=1, item_id=None, file_path=book_path)  # doctest: +SKIP


    :param db: Open fixture database whose asset schema may be extended.
    :param store_id: Store foreign key converted to int.
    :param item_id: Item foreign key converted to int, or None for an unlinked asset.
    :param file_path: Existing file supplying original path, names, suffix, and byte size.
    :return: Integer ID of the inserted legacy files row.
    """
    ensure_surface_asset_tables(db)
    row_dict = {
        "file_store_id": int(store_id),
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
    }
    if item_id is not None:
        row_dict["file_item_id"] = int(item_id)
    row = Row.from_idless_row_dict(db, row_dict=row_dict, table="files")
    return int(row["file_id"])


def test_opds_readonly_is_not_a_calibre_ui_subclass() -> None:
    """
    Keep standalone OPDS on the shared web base, independent of the Calibre UI.

    Example:
        >>> test_opds_readonly_is_not_a_calibre_ui_subclass()


    :return: None after both positive and negative inheritance assertions pass.
    """
    assert issubclass(OpdsReadOnlyApplication, ReadOnlyWebApplication)
    assert not issubclass(OpdsReadOnlyApplication, CalibreReadOnlyWebApplication)


def test_opds_readonly_parser_accepts_cache_read_source_options(tmp_path: Path) -> None:
    """
    Verify cache-source CLI options project into the frozen OPDS configuration.

    The database pathname is only parsed; neither a database nor a server opens.

    Example:
        >>> test_opds_readonly_parser_accepts_cache_read_source_options(Path("/unused"))


    :param tmp_path: Pytest path used to construct an otherwise unused database argument.
    :return: None after cache mode, cache type, and disabled-fallback assertions pass.
    """
    db_path = tmp_path / "opds_cli.sqlite"
    args = build_arg_parser().parse_args(
        [
            "--database",
            str(db_path),
            "--metadata-read-source",
            "cache",
            "--cache-type",
            "schema_backed",
            "--no-cache-db-fallback",
        ]
    )

    assert args.metadata_read_source == "cache"
    assert args.cache_type == "schema_backed"
    assert args.no_cache_db_fallback is True
    config = OpdsReadOnlyConfig(
        metadata_read_source=str(args.metadata_read_source),
        metadata_cache_type=str(args.cache_type),
        metadata_cache_allow_database_fallback=not bool(args.no_cache_db_fallback),
    )
    assert config.metadata_read_source == "cache"
    assert config.metadata_cache_type == "schema_backed"
    assert config.metadata_cache_allow_database_fallback is False


def test_opds_readonly_cache_read_source_route_serves_snapshot(driver_spec, tmp_path: Path) -> None:
    """
    Verify an OPDS title feed reads its initial cache snapshot without DB fallback.

    A second work inserted after application construction must remain absent
    from the feed; this is not a test of refreshing or invalidating that snapshot.

    Example:
        >>> test_opds_readonly_cache_read_source_route_serves_snapshot(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest database-driver specification for the fixture catalogue.
    :param tmp_path: Isolated directory receiving the cache-source database.
    :return: None after the feed contains only the pre-construction title.
    """
    db_path = tmp_path / "opds_cache_source.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_work_row(db, title="Cached OPDS Route Title")
        app = OpdsReadOnlyApplication(
            db,
            config=OpdsReadOnlyConfig(
                default_page_size=10,
                max_page_size=25,
                metadata_read_source="cache",
                metadata_cache_type="schema_backed",
                metadata_cache_allow_database_fallback=False,
            ),
        )
        _insert_work_row(db, title="Uncached OPDS Route Title")

        status, headers, body = _call_app(app, "/opds/navcatalog/{}".format(opds_nav_token("titles")))

        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/atom+xml")
        text = body.decode("utf-8")
        assert "Cached OPDS Route Title" in text
        assert "Uncached OPDS Route Title" not in text


def test_opds_readonly_root_and_feed_routes(driver_spec, tmp_path: Path) -> None:
    """
    Verify root redirection, Atom navigation, and rejection of HTML browse routes.

    Example:
        >>> test_opds_readonly_root_and_feed_routes(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest driver specification used to create one stored work.
    :param tmp_path: Isolated directory receiving the routing-test database.
    :return: None after status, location, media-type, and body assertions pass.
    """
    db_path = tmp_path / "opds_readonly_routes.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_work_row(db, title="Standalone OPDS Book")
        app = OpdsReadOnlyApplication(db, config=OpdsReadOnlyConfig(default_page_size=10, max_page_size=25))

        status, headers, body = _call_app(app, "/")
        assert status == "302 Found"
        assert headers["Location"] == "/opds"
        assert body == b""

        status, headers, body = _call_app(app, "/opds")
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/atom+xml")
        text = body.decode("utf-8")
        assert "<title>LiuXin OPDS Read-Only</title>" in text
        assert "/opds/navcatalog/" in text

        status, headers, body = _call_app(app, "/browse/titles")
        assert status == "404 Not Found"
        assert headers["Content-Type"].startswith("text/plain")
        assert "Unknown OPDS route" in body.decode("utf-8")


def test_opds_readonly_get_routes_serve_format_downloads(driver_spec, tmp_path: Path) -> None:
    """
    Verify current and legacy acquisition URLs deliver the same linked file bytes.

    A real local file and WEMI/store rows exercise Core-backed delivery in process.
    The payload is not a valid EPUB and no ebook decoder or network listener runs.

    Example:
        >>> test_opds_readonly_get_routes_serve_format_downloads(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest driver specification for the fresh download catalogue.
    :param tmp_path: Isolated directory containing the database and opaque ebook payload.
    :return: None after both routes return the expected filename and exact bytes.
    """
    db_path = tmp_path / "opds_readonly_get.sqlite"
    payload = b"opds epub payload"
    file_path = tmp_path / "opds-book.epub"
    file_path.write_bytes(payload)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="OPDS Download Book")
        store_id = _insert_store_row(db, name="Downloads", root_uri=str(tmp_path))
        expression_id = _insert_expression_row(db, title_override="OPDS Download Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(
            db,
            manifestation_id=manifestation_id,
            source_path=str(file_path),
            source_name=file_path.name,
        )
        _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=file_path)

        db.interlink_rows(primary_row=db.get_row_from_id("works", work_id), secondary_row=db.get_row_from_id("expressions", expression_id))
        db.interlink_rows(primary_row=db.get_row_from_id("expressions", expression_id), secondary_row=db.get_row_from_id("manifestations", manifestation_id))

        app = OpdsReadOnlyApplication(db)

        status, headers, body = _call_app(app, "/get/epub/{}/main".format(work_id))
        assert status == "200 OK"
        assert headers["Content-Disposition"].startswith('attachment; filename="opds-book.epub"')
        assert body == payload

        status, headers, body = _call_app(app, "/legacy/get/epub/{}/main/opds-book.epub".format(work_id))
        assert status == "200 OK"
        assert headers["Content-Disposition"].startswith('attachment; filename="opds-book.epub"')
        assert body == payload
