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
    status, headers, body = _call_app(app, path)
    return status, headers, json.loads(body.decode("utf-8"))


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


def test_api_readonly_parser_accepts_cache_read_source_options(tmp_path: Path) -> None:
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
        _insert_work_row(db, title="Uncached API Route Title")

        status, _headers, payload = _json(app, "/api/works?sort=title&limit=10")

        assert getattr(app.read_model.read_source, "allow_database_fallback") is False
        assert status == "200 OK"
        assert payload["pagination"]["total"] == 1
        titles = [str(item["title"]) for item in payload["items"]]
        assert titles == ["Cached API Route Title"]


def test_api_readonly_index_and_work_routes(driver_spec, tmp_path: Path) -> None:
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
