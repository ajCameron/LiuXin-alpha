"""
Verify JSON catalogue projections and inherited acquisition with fixture databases.

An in-process WSGI harness checks serialized metadata, links, status, headers,
and downloaded bytes without a live listener. Fixtures create explicit work,
expression, manifestation, item, file, and category relationships. The tests
cover cache parser configuration, snapshot exclusion, index/work projections,
category navigation, search, and file metadata/downloads. They do not establish
every malformed-route contract, media validity, browser rendering, or network
transport behavior. Driver coverage depends on the selected driver_spec fixture.
"""

from __future__ import annotations

import json

from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.api_readonly import (
    ApiReadOnlyApplication,
    ApiReadOnlyConfig,
    build_arg_parser,
)
from LiuXin_alpha.metadata.standardization import make_tag_search_term
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _call_app(app, path: str, *, method: str = "GET"):
    """
    Execute one WSGI request and collect bytes while closing its response iterable.

    The first question mark separates path from query. Headers collapse into a
    dictionary, exc_info is ignored, and the test callback supplies no write
    callable. A callable iterable closer runs even if byte joining raises.

    Example:
        >>> status, headers, body = _call_app(app, "/api/works?limit=1")  # doctest: +SKIP


    :param app: WSGI application accepting environ and start_response.
    :param path: Request path optionally followed by an encoded query string.
    :param method: HTTP method copied into the testing environment unchanged.
    :return: String status, collapsed header mapping, and concatenated body bytes.
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
        Capture WSGI response metadata in the enclosing request state.

        Example:
            >>> start_response("200 OK", [("Content-Type", "application/json")])  # doctest: +SKIP


        :param status: HTTP status line retained for the harness result.
        :param headers: Header pairs converted to a dict, losing duplicate entries.
        :param exc_info: Optional exception context deliberately ignored by this harness.
        :return: None; the optional WSGI write API is not implemented.
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


def _json(app, path: str) -> tuple[str, dict[str, str], dict[str, object]]:
    """
    Make an in-process GET and decode its UTF-8 JSON body without checking status.

    The annotation reflects expected object responses, but json.loads is not
    constrained to mappings at runtime. Decoding/parsing failures propagate.

    Example:
        >>> status, headers, payload = _json(app, "/api/works")  # doctest: +SKIP


    :param app: WSGI application exercised by the shared request harness.
    :param path: API path and optional encoded query string.
    :return: Captured status, collapsed headers, and decoded JSON value.
    """
    status, headers, body = _call_app(app, path)
    return status, headers, json.loads(body.decode("utf-8"))


def _insert_work_row(db: Database, *, title: str) -> int:
    """
    Insert a work with identical display, canonical, and sort titles.

    Example:
        >>> work_id = _insert_work_row(db, title="API fixture")  # doctest: +SKIP


    :param db: Writable fixture database receiving the work metadata.
    :param title: Text stored in all three conventional title fields.
    :return: Assigned integer work ID without creating linked records.
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
    Insert filesystem-store metadata without creating or validating its root.

    Example:
        >>> store_id = _insert_store_row(db, name="API shelf", root_uri=str(tmp_path))  # doctest: +SKIP


    :param db: Writable fixture database receiving the store row.
    :param name: Public display name for the fixture store.
    :param root_uri: Filesystem root hint stored with file protocol and filesystem kind.
    :return: Assigned integer store ID; no bytes are written by this helper.
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
    Insert a person contributor with matching canonical and sort names.

    Example:
        >>> agent_id = _insert_agent_row(db, name="API Author")  # doctest: +SKIP


    :param db: Writable fixture database receiving contributor metadata.
    :param name: Text used for both conventional name fields.
    :return: Assigned integer agent ID without linking it to a work.
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
    Insert a label with display text and its project-normalized search form.

    Example:
        >>> label_id = _insert_label_row(db, text="API Tag")  # doctest: +SKIP


    :param db: Writable fixture database receiving the label row.
    :param text: Display text also passed to make_tag_search_term.
    :return: Assigned integer label ID; work links are created separately.
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
    Insert series metadata with matching display/sort names and a normalized token.

    Example:
        >>> series_id = _insert_series_row(db, name="API Series")  # doctest: +SKIP


    :param db: Writable fixture database receiving series metadata.
    :param name: Series and sort text also normalized for series_name_norm.
    :return: Assigned integer series ID without creating membership relationships.
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
    Insert an expression carrying the supplied title override.

    Example:
        >>> expression_id = _insert_expression_row(db, title_override="API fixture")  # doctest: +SKIP


    :param db: Writable fixture database receiving the expression row.
    :param title_override: Text stored as expression_title_override.
    :return: Assigned integer expression ID, not yet linked to a work or manifestation.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={"expression_title_override": title_override},
        table="expressions",
    )
    return int(row["expression_id"])


def _insert_manifestation_row(db: Database, *, format_detail: str) -> int:
    """
    Insert an ebook manifestation with a format-description hint.

    The format string is metadata only and does not validate fixture bytes.

    Example:
        >>> manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")  # doctest: +SKIP


    :param db: Writable fixture database receiving manifestation metadata.
    :param format_detail: Text stored alongside the fixed ebook carrier type.
    :return: Assigned integer manifestation ID without expression linking.
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
    Insert an ebook item referencing a manifestation and fixture source metadata.

    Source path/name are stored as supplied; this helper neither opens the path
    nor creates a legacy file row or work/expression interlinks.

    Example:
        >>> item_id = _insert_item_row(db, manifestation_id=manifestation_id, source_path=str(asset_path), source_name=asset_path.name)  # doctest: +SKIP


    :param db: Writable fixture database receiving the item row.
    :param manifestation_id: Parent manifestation identity converted to int.
    :param source_path: Source-path metadata retained without existence checks.
    :param source_name: Source display name paired with the fixed fixture marker.
    :return: Assigned integer item ID.
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
    Ensure compatibility asset tables and insert a statted local file for an item.

    Path components supply storage key, filename, stem, extension, and original
    path/name. The file must already exist for size lookup; its bytes are not
    copied or parsed. Role/category/source use fixed primary/ebook/fixture values.

    Example:
        >>> file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=asset_path)  # doctest: +SKIP


    :param db: Writable fixture database whose compatibility tables may be created.
    :param store_id: Store identity converted to int in file metadata.
    :param item_id: Item identity converted to int to connect the legacy file.
    :param file_path: Existing local asset path supplying naming and byte-size metadata.
    :return: Assigned integer file ID after metadata insertion.
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


def test_api_readonly_parser_accepts_cache_read_source_options(tmp_path: Path) -> None:
    """
    Verify cache CLI flags translate to the expected API configuration fields.

    Only parsing and value construction occur: no database is created and no
    cache, Core session, or web listener is started.

    Example:
        >>> test_api_readonly_parser_accepts_cache_read_source_options(tmp_path)  # doctest: +SKIP


    :param tmp_path: Fixture directory used to form a parser-only database path.
    :return: None; assertions check selected source/type and inverted fallback policy.
    """
    db_path = tmp_path / "api_cli.sqlite"
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
    config = ApiReadOnlyConfig(
        metadata_read_source=str(args.metadata_read_source),
        metadata_cache_type=str(args.cache_type),
        metadata_cache_allow_database_fallback=not bool(args.no_cache_db_fallback),
    )
    assert config.metadata_read_source == "cache"
    assert config.metadata_cache_type == "schema_backed"
    assert config.metadata_cache_allow_database_fallback is False


def test_api_readonly_cache_read_source_route_serves_snapshot(driver_spec, tmp_path: Path) -> None:
    """
    Verify cached collection/detail routes exclude a row inserted after cache creation.

    With fallback disabled, the work list contains only the initial row and the
    later ID returns JSON missing_work with 404 rather than a live database read.

    Example:
        >>> test_api_readonly_cache_read_source_route_serves_snapshot(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver selected for the real snapshot fixture.
    :param tmp_path: Directory holding the temporary catalogue database.
    :return: None; assertions establish collection count/title and late-row absence.
    """
    db_path = tmp_path / "api_cache_source.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_work_row(db, title="Cached API Route Title")
        app = ApiReadOnlyApplication(
            db,
            config=ApiReadOnlyConfig(
                default_page_size=10,
                max_page_size=25,
                metadata_read_source="cache",
                metadata_cache_type="schema_backed",
                metadata_cache_allow_database_fallback=False,
            ),
        )
        uncached_id = _insert_work_row(db, title="Uncached API Route Title")

        status, _headers, payload = _json(app, "/api/works?sort=title&limit=10")

        assert status == "200 OK"
        assert payload["pagination"]["total"] == 1
        titles = [str(item["title"]) for item in payload["items"]]
        assert titles == ["Cached API Route Title"]

        status, _headers, payload = _json(app, "/api/works/{}".format(uncached_id))
        assert status == "404 Not Found"
        assert payload["error"] == "missing_work"


def test_api_readonly_index_and_work_routes(driver_spec, tmp_path: Path) -> None:
    """
    Verify index, work-list, and work-detail projections across an explicitly linked fixture graph.

    Author, tag, series, expression, manifestation, item, and file metadata feed
    credits, formats, and related summaries. Arbitrary EPUB-named fixture bytes
    are not parsed as a valid publication, and this test does not download them.

    Example:
        >>> test_api_readonly_index_and_work_routes(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Selected database driver for metadata and relationship operations.
    :param tmp_path: Temporary directory containing the catalogue and statted asset.
    :return: None; assertions check JSON media type, service identity, counts, and projections.
    """
    db_path = tmp_path / "api_readonly_works.sqlite"
    file_path = tmp_path / "api-book.epub"
    file_path.write_bytes(b"api epub payload")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="API Alpha Book")
        store_id = _insert_store_row(db, name="API Shelf", root_uri=str(tmp_path))
        agent_id = _insert_agent_row(db, name="API Author")
        label_id = _insert_label_row(db, text="API Tag")
        series_id = _insert_series_row(db, name="API Series")
        expression_id = _insert_expression_row(db, title_override="API Alpha Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(db, manifestation_id=manifestation_id, source_path=str(file_path), source_name=file_path.name)
        file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=file_path)

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("series", series_id))
        expression_row = db.get_row_from_id("expressions", expression_id)
        manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
        db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
        db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

        app = ApiReadOnlyApplication(db, config=ApiReadOnlyConfig(default_page_size=10, max_page_size=25))

        status, headers, payload = _json(app, "/api")
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/json")
        assert payload["service"] == "api_readonly"
        assert payload["endpoints"]["works"] == "/api/works"
        assert payload["counts"]["works"] == 1

        status, _headers, payload = _json(app, "/api/works?sort=title&limit=10")
        assert status == "200 OK"
        assert payload["kind"] == "works"
        assert payload["pagination"]["total"] == 1
        assert payload["items"][0]["title"] == "API Alpha Book"
        assert payload["items"][0]["authors"] == ["API Author"]

        status, _headers, payload = _json(app, f"/api/works/{work_id}")
        assert status == "200 OK"
        assert payload["work"]["title"] == "API Alpha Book"
        assert payload["work"]["formats"] == ["EPUB"]
        assert payload["credits"][0]["entity"]["primary"] == "API Author"
        assert payload["files"][0]["id"] == file_id
        assert payload["related"]["labels"][0]["primary"] == "API Tag"
        assert payload["related"]["series"][0]["primary"] == "API Series"


def test_api_readonly_categories_search_and_file_metadata(driver_spec, tmp_path: Path) -> None:
    """
    Verify category navigation, ranked search, file capability metadata, and downloaded bytes.

    A text-file fixture is linked through item/manifestation/expression to one
    work with author/tag/series relationships. The test follows author detail
    and linked-work URLs, checks preview hints, and exercises download delivery;
    it does not request the preview URL or verify every category-detail route.

    Example:
        >>> test_api_readonly_categories_search_and_file_metadata(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver used for the catalogue and linked fixture graph.
    :param tmp_path: Directory holding the database and UTF-8 text asset.
    :return: None; assertions establish projected navigation, search, and exact byte delivery.
    """
    db_path = tmp_path / "api_readonly_categories.sqlite"
    file_path = tmp_path / "api-search-book.txt"
    file_path.write_text("api text payload", encoding="utf-8")

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="API Search Book")
        store_id = _insert_store_row(db, name="API Search Shelf", root_uri=str(tmp_path))
        agent_id = _insert_agent_row(db, name="API Search Author")
        label_id = _insert_label_row(db, text="API Search Tag")
        series_id = _insert_series_row(db, name="API Search Series")
        expression_id = _insert_expression_row(db, title_override="API Search Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="TXT")
        item_id = _insert_item_row(db, manifestation_id=manifestation_id, source_path=str(file_path), source_name=file_path.name)
        file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=file_path)

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("series", series_id))
        expression_row = db.get_row_from_id("expressions", expression_id)
        manifestation_row = db.get_row_from_id("manifestations", manifestation_id)
        db.interlink_rows(primary_row=work_row, secondary_row=expression_row)
        db.interlink_rows(primary_row=expression_row, secondary_row=manifestation_row)

        app = ApiReadOnlyApplication(db, config=ApiReadOnlyConfig(default_page_size=10, max_page_size=25))

        status, _headers, payload = _json(app, "/api/categories")
        assert status == "200 OK"
        assert [entry["category"] for entry in payload["items"]] == ["allbooks", "newest", "authors", "tags", "series"]
        assert payload["items"][0]["api_url"] == "/api/works?sort=title"
        assert payload["items"][2]["api_url"] == "/api/authors"

        status, _headers, payload = _json(app, "/api/authors?limit=20")
        assert status == "200 OK"
        author_item = next(item for item in payload["items"] if item["name"] == "API Search Author")
        assert author_item["works_url"].endswith("/works")

        status, _headers, author_payload = _json(app, author_item["api_url"])
        assert status == "200 OK"
        assert author_payload["entity"]["primary"] == "API Search Author"
        assert author_payload["works_count"] == 1

        status, _headers, works_payload = _json(app, author_item["works_url"])
        assert status == "200 OK"
        assert works_payload["items"][0]["title"] == "API Search Book"

        status, _headers, payload = _json(app, "/api/search?q=API%20Search")
        assert status == "200 OK"
        assert payload["query"] == "API Search"
        assert any(item["table"] == "works" and item["primary"] == "API Search Book" for item in payload["results"])
        assert payload["group_counts"]["works"] >= 1

        status, _headers, payload = _json(app, f"/api/files/{file_id}")
        assert status == "200 OK"
        assert payload["id"] == file_id
        assert payload["downloadable"] is True
        assert payload["preview_kind"] == "text"
        assert payload["download_url"].endswith("/download")
        assert payload["preview_url"].endswith("/preview")

        status, headers, body = _call_app(app, f"/files/{file_id}/download")
        assert status == "200 OK"
        assert headers["Content-Disposition"].startswith('attachment; filename="api-search-book.txt"')
        assert body == b"api text payload"
