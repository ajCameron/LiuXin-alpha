from __future__ import annotations

from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.opds_readonly import OpdsReadOnlyApplication, OpdsReadOnlyConfig
from LiuXin_alpha.surfaces.web_calibre_readonly import CalibreReadOnlyWebApplication
from LiuXin_alpha.surfaces.web_readonly.app import ReadOnlyWebApplication
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


def _insert_file_row_for_item(db: Database, *, store_id: int, item_id: int | None, file_path: Path) -> int:
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
    assert issubclass(OpdsReadOnlyApplication, ReadOnlyWebApplication)
    assert not issubclass(OpdsReadOnlyApplication, CalibreReadOnlyWebApplication)


def test_opds_readonly_root_and_feed_routes(driver_spec, tmp_path: Path) -> None:
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
