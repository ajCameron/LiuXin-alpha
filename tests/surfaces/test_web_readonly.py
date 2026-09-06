from __future__ import annotations

from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.web_readonly import (
    ReadOnlyWebApplication,
    ReadOnlyWebConfig,
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


def _insert_store_row(db: Database, *, name: str, root_uri: str, credentials: str = "") -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": "filesystem",
            "store_access_protocol": "file",
            "store_root_uri": root_uri,
            "store_credentials": credentials,
            "store_policy_json": '{"secret":"hidden"}',
        },
        table="stores",
    )
    return int(row["store_id"])


def _insert_file_row(
    db: Database,
    *,
    store_id: int,
    file_path: Path | None = None,
    file_storage_key: str | None = None,
    file_name: str | None = None,
    file_source: str = "local-test",
) -> int:
    ensure_surface_asset_tables(db)
    row_dict = {
        "file_store_id": int(store_id),
        "file_storage_key": str(file_storage_key or (file_path.name if file_path is not None else "")),
        "file_name": str(file_name or (file_path.name if file_path is not None else "download.bin")),
        "file_base_name": str((file_path.stem if file_path is not None else Path(file_name or "download.bin").stem)),
        "file_extension": str((file_path.suffix.lower().lstrip(".") if file_path is not None else Path(file_name or "download.bin").suffix.lower().lstrip("."))),
        "file_role": "primary",
        "file_media_category": "ebook",
        "file_source": file_source,
    }
    if file_path is not None:
        row_dict["file_size_bytes"] = int(file_path.stat().st_size)
        row_dict["file_original_name"] = str(file_path.name)
        row_dict["file_original_path"] = str(file_path)
    else:
        row_dict["file_original_name"] = str(file_name or "download.bin")
    row = Row.from_idless_row_dict(
        db,
        row_dict=row_dict,
        table="files",
    )
    return int(row["file_id"])


class _UnsupportedStorageManager:
    def read_bytes(self, location) -> bytes:
        del location
        raise NotImplementedError("no byte access")


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


def _insert_note_row(db: Database, *, note: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={"note": note},
        table="notes",
    )
    return int(row["note_id"])


def _insert_agent_row(db: Database, *, name: str, agent_type: str = "person") -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "agent_type": agent_type,
            "agent_canonical_name": name,
            "agent_sort_name": name,
        },
        table="agents",
    )
    return int(row["agent_id"])


def test_web_readonly_home_table_row_and_search(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="The Public Domain Web Test")
        app = ReadOnlyWebApplication(db, config=ReadOnlyWebConfig(title="Test Web"))

        status, _headers, body = _call_app(app, "/")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Main tables" in text
        assert "Helper tables" in text
        assert "Interlink tables" in text
        assert "Intralink tables" in text
        assert "/tables/works" in text
        assert "/tables/database_version" in text
        assert "/tables/digital_asset_derivations" in text
        assert "/tables/work_work_intralinks" in text

        status, _headers, body = _call_app(app, "/tables/works")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "The Public Domain Web Test" in text
        assert "/tables/works/{}".format(work_id) in text
        assert "class='table-wrap'" in text
        assert "overflow-x: auto" in text

        status, _headers, body = _call_app(app, "/tables/works/{}".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Work record" in text
        assert "Titles" in text
        assert "work_title" in text
        assert "The Public Domain Web Test" in text
        assert "work-hero" in text
        assert "detail-grid" in text

        status, _headers, body = _call_app(
            app,
            "/search?table=works&column=work_title&q=The%20Public%20Domain%20Web%20Test",
        )
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Search results" in text
        assert "matches=1" in text
        assert "The Public Domain Web Test" in text
        assert "class='table-wrap'" in text


def test_web_readonly_cache_read_source_cli_options_serve_snapshot(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_cache_source.sqlite"
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

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_work_row(db, title="Cached Web Title")
        config = ReadOnlyWebConfig(
            metadata_read_source=str(args.metadata_read_source),
            metadata_cache_type=str(args.cache_type),
            metadata_cache_allow_database_fallback=not bool(args.no_cache_db_fallback),
        )
        app = ReadOnlyWebApplication(db, config=config)
        uncached_id = _insert_work_row(db, title="Uncached Web Title")

        status, _headers, body = _call_app(app, "/search?global_q=Title&search_table=works")

        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Cached Web" in text
        assert "Uncached Web" not in text

        status, _headers, body = _call_app(app, "/tables/works/{}".format(uncached_id))
        assert status == "200 OK"
        assert "Row not found" in body.decode("utf-8")

        status, _headers, body = _call_app(app, "/")
        assert status == "200 OK"
        assert "Main tables" in body.decode("utf-8")
        assert "count unavailable" in body.decode("utf-8")


def test_web_readonly_table_classifier_splits_main_helper_interlink_and_intralink() -> None:
    assert ReadOnlyWebApplication._table_category("works") == "main"
    assert ReadOnlyWebApplication._table_category("stores") == "main"
    assert ReadOnlyWebApplication._table_category("database_version") == "helper"
    assert ReadOnlyWebApplication._table_category("works_plugin_data") == "helper"
    assert ReadOnlyWebApplication._table_category("transform_runs") == "helper"
    assert ReadOnlyWebApplication._table_category("digital_asset_derivations") == "interlink"
    assert ReadOnlyWebApplication._table_category("entity_identifiers") == "interlink"
    assert ReadOnlyWebApplication._table_category("agent_work_links") == "interlink"
    assert ReadOnlyWebApplication._table_category("work_work_intralinks") == "intralink"


def test_web_readonly_file_row_exposes_download_and_serves_bytes(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_files.sqlite"
    payload = b"ebook payload"
    file_path = tmp_path / "sample.epub"
    file_path.write_bytes(payload)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(db, name="Downloads", root_uri=str(tmp_path))
        file_id = _insert_file_row(db, store_id=store_id, file_path=file_path)
        app = ReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/files/{}".format(file_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "File record" in text
        assert "file-hero" in text
        assert "Identity" in text
        assert "Location and access" in text
        assert "/files/{}/download".format(file_id) in text
        assert "sample.epub" in text

        status, headers, body = _call_app(app, "/files/{}/download".format(file_id))
        assert status == "200 OK"
        assert headers["Content-Disposition"].startswith('attachment; filename="sample.epub"')
        assert body == payload


def test_web_readonly_file_download_uses_store_manager_for_blob_store(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_blob.sqlite"
    blob_store_path = tmp_path / "blob_store.sqlite"

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "store_name": "blob_store",
                "store_kind": "single_file_sqlite",
                "store_access_protocol": "sqlite",
                "store_root_uri": str(blob_store_path),
                "store_is_read_only": 0,
            },
            table="stores",
        )
        store_id = int(store_row["store_id"])
        db.bootstrap_storage_manager(startup_on_add=False, clear_existing=True)
        store = next(
            store
            for store in db.storage.iter_stores()
            if store.configuration.store_name == "blob_store"
        )
        stored = db.storage.store_bytes(
            b"blob-store-payload",
            store=store,
        )
        location = db.storage.locate_digital_asset(
            stored.digital_asset_id,
            preferred_store_ref=store.store_ref,
        )
        file_id = _insert_file_row(
            db,
            store_id=store_id,
            file_storage_key=location.key,
            file_name="blob-book.epub",
            file_source="",
        )
        app = ReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/files/{}".format(file_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "/files/{}/download".format(file_id) in text

        status, headers, body = _call_app(app, "/files/{}/download".format(file_id))
        assert status == "200 OK"
        assert headers["Content-Disposition"].startswith('attachment; filename="blob-book.epub"')
        assert body == b"blob-store-payload"


def test_web_readonly_unsupported_store_download_returns_501(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_unsupported.sqlite"

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(db, name="Unavailable backend", root_uri=str(tmp_path / "missing_store"))
        file_id = _insert_file_row(
            db,
            store_id=store_id,
            file_storage_key="remote-book.epub",
            file_name="remote-book.epub",
            file_source="",
        )
        db.storage = _UnsupportedStorageManager()
        app = ReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/files/{}".format(file_id))
        assert status == "200 OK"
        assert "/files/{}/download".format(file_id) in body.decode("utf-8")

        status, _headers, body = _call_app(app, "/files/{}/download".format(file_id))
        assert status == "501 Not Implemented"
        assert "does not support direct downloads" in body.decode("utf-8")


def test_web_readonly_hides_sensitive_store_columns(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_hidden.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(db, name="Public Store", root_uri=str(tmp_path), credentials="top-secret-token")
        app = ReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/stores/{}".format(store_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Store record" in text
        assert "store-hero" in text
        assert "Identity" in text
        assert "Access" in text
        assert "Public Store" in text
        assert "store_credentials" not in text
        assert "top-secret-token" not in text
        assert "store_policy_json" not in text


def test_web_readonly_row_page_renders_specialized_linked_entities(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_linked.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="A Wizard of Related Rows")
        label_id = _insert_label_row(db, text="Space Opera")
        note_id = _insert_note_row(db, note="Public note excerpt for related-row rendering.")
        agent_id = _insert_agent_row(db, name="Ursula K. Le Guin")

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("notes", note_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id))

        app = ReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/works/{}".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Work record" in text
        assert "work-hero" in text
        assert "Linked entities" in text
        assert "Labels" in text
        assert "Space Opera" in text
        assert "/tables/labels/{}".format(label_id) in text
        assert "class='pill related-pill'" in text
        assert "Notes" in text
        assert "Public note excerpt for related-row rendering." in text
        assert "/tables/notes/{}".format(note_id) in text
        assert "Agents" in text
        assert "Ursula K. Le Guin" in text
        assert "person" in text
        assert "/tables/agents/{}".format(agent_id) in text
