from __future__ import annotations

import re

from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.interfaces.web_readonly import ReadOnlyWebApplication, ReadOnlyWebConfig
from LiuXin_alpha.metadata.standardization import make_tag_search_term


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


def _strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", "", str(text or ""))


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
    row_dict = {
        "file_store_id": int(store_id),
        "file_storage_key": str(file_storage_key or (file_path.name if file_path is not None else "")),
        "file_name": str(file_name or (file_path.name if file_path is not None else "download.bin")),
        "file_role": "primary",
        "file_media_category": "ebook",
        "file_source": file_source,
    }
    if file_path is not None:
        row_dict["file_original_path"] = str(file_path)
    row = Row.from_idless_row_dict(
        db,
        row_dict=row_dict,
        table="files",
    )
    return int(row["file_id"])


class _UnsupportedStoredFile:
    def as_bytes(self) -> bytes:
        raise NotImplementedError("no byte access")


class _UnsupportedStorageManager:
    def retrieve_file(self, *, metadata=None, preferred_store=None, file_url=None):
        del metadata, preferred_store, file_url
        return _UnsupportedStoredFile()


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
        _insert_agent_row(db, name="Public Domain Editor")
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
        assert "/tables/file_derivations" in text
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

        status, _headers, body = _call_app(app, "/search?global_q=Public%20Domain")
        assert status == "200 OK"
        text = body.decode("utf-8")
        plain = _strip_tags(text)
        assert "Library results" in text
        assert "The Public Domain Web Test" in plain
        assert "Public Domain Editor" in plain
        assert "Works" in text
        assert "Agents" in text
        assert "name='global_limit'" in text
        assert "name='exact_limit'" in text


def test_web_readonly_global_search_is_paginated(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_global_pager.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_work_row(db, title="Paged Public Work One")
        _insert_work_row(db, title="Paged Public Work Two")
        _insert_work_row(db, title="Paged Public Work Three")
        app = ReadOnlyWebApplication(db, config=ReadOnlyWebConfig(title="Pager Test", default_page_size=2, max_page_size=5))

        status, _headers, body = _call_app(app, "/search?global_q=Paged%20Public&global_limit=2")
        assert status == "200 OK"
        text = body.decode("utf-8")
        plain = _strip_tags(text)
        assert "Library results" in text
        assert "Showing 1-2 of 3 results. Page 1 of 2." in text
        assert "Paged Public Work One" in plain
        assert "Paged Public Work Two" in plain
        assert "Paged Public Work Three" not in plain
        assert "global_offset=2" in text

        status, _headers, body = _call_app(app, "/search?global_q=Paged%20Public&global_limit=2&global_offset=2")
        assert status == "200 OK"
        text = body.decode("utf-8")
        plain = _strip_tags(text)
        assert "Showing 3-3 of 3 results. Page 2 of 2." in text
        assert "Paged Public Work One" not in plain
        assert "Paged Public Work Two" not in plain
        assert "Paged Public Work Three" in plain


def test_web_readonly_table_pager_uses_neutral_navigation_labels(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_table_pager.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_work_row(db, title="Pager Work One")
        _insert_work_row(db, title="Pager Work Two")
        _insert_work_row(db, title="Pager Work Three")
        app = ReadOnlyWebApplication(db, config=ReadOnlyWebConfig(title="Pager Test", default_page_size=2, max_page_size=5))

        status, _headers, body = _call_app(app, "/tables/works?limit=2")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert ">Forward<" in text
        assert ">Older<" not in text

        status, _headers, body = _call_app(app, "/tables/works?limit=2&offset=2")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert ">Back<" in text
        assert ">Newer<" not in text


def test_web_readonly_global_search_ranks_and_highlights_results(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_global_rank.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        _insert_work_row(db, title="Needle")
        _insert_work_row(db, title="Needle in a Haystack")
        _insert_agent_row(db, name="Needle Agent")
        app = ReadOnlyWebApplication(db, config=ReadOnlyWebConfig(title="Rank Test", default_page_size=10, max_page_size=10))

        status, _headers, body = _call_app(app, "/search?global_q=Needle")
        assert status == "200 OK"
        text = body.decode("utf-8")
        plain = _strip_tags(text)
        exact_index = plain.index("Needle")
        loose_index = plain.index("Needle in a Haystack")
        assert exact_index < loose_index
        assert "<mark>Needle</mark>" in text
        assert "matched in work_title" in text or "matched in agent_canonical_name" in text


def test_web_readonly_table_classifier_splits_main_helper_interlink_and_intralink() -> None:
    assert ReadOnlyWebApplication._table_category("works") == "main"
    assert ReadOnlyWebApplication._table_category("stores") == "main"
    assert ReadOnlyWebApplication._table_category("database_version") == "helper"
    assert ReadOnlyWebApplication._table_category("works_plugin_data") == "helper"
    assert ReadOnlyWebApplication._table_category("transform_runs") == "helper"
    assert ReadOnlyWebApplication._table_category("file_derivations") == "interlink"
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


def test_web_readonly_file_row_exposes_preview_for_safe_types(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_preview.sqlite"
    payload = b"plain text preview"
    file_path = tmp_path / "sample.txt"
    file_path.write_bytes(payload)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(db, name="Preview store", root_uri=str(tmp_path))
        file_id = _insert_file_row(db, store_id=store_id, file_path=file_path)
        app = ReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/files/{}".format(file_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "/files/{}/preview".format(file_id) in text
        assert "preview: text" in text
        assert "downloadable" in text

        status, headers, body = _call_app(app, "/files/{}/preview".format(file_id))
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("text/plain")
        assert headers["Content-Disposition"].startswith('inline; filename="sample.txt"')
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
        stored = db.storage.add_file(b"blob-store-payload", preferred_store="blob_store")
        file_hash = str(stored.file_url).rstrip("/").split("/")[-1]
        file_id = _insert_file_row(
            db,
            store_id=store_id,
            file_storage_key=file_hash,
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


def test_web_readonly_detail_pages_format_machine_values(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_machine_values.sqlite"
    payload = b"machine value payload"
    file_path = tmp_path / "deep" / "sample.epub"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(payload)

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
                "store_name": "Machine Store",
                "store_kind": "filesystem",
                "store_access_protocol": "file",
                "store_root_uri": "file:///srv/liuxin/library",
                "store_policy_json": '{"mode":"strict","retry":2}',
                "store_last_seen_online_timestamp_ep_k": 1742387640000,
            },
            table="stores",
        )
        store_id = int(store_row["store_id"])
        file_row = Row.from_idless_row_dict(
            db,
            row_dict={
                "file_store_id": store_id,
                "file_storage_key": "books/sample.epub",
                "file_name": "sample.epub",
                "file_original_path": str(file_path),
                "file_last_seen_timestamp_ep_k": 1742387640000,
                "file_source": "local-test",
            },
            table="files",
        )
        file_id = int(file_row["file_id"])
        app = ReadOnlyWebApplication(
            db,
            config=ReadOnlyWebConfig(
                title="Machine Values",
                hidden_column_tokens=(),
            ),
        )

        status, _headers, body = _call_app(app, "/tables/stores/{}".format(store_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "2025-03-19 12:34 UTC" in text
        assert "<code>1742387640000</code>" in text
        assert "<code>file:///srv/liuxin/library</code>" in text
        assert "store_policy_json" in text
        assert "<pre class='field-value field-value-block'><code>{" in text
        assert "&quot;mode&quot;: &quot;strict&quot;" in text
        assert "&quot;retry&quot;: 2" in text

        status, _headers, body = _call_app(app, "/tables/files/{}".format(file_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "2025-03-19 12:34 UTC" in text
        assert "<code>books/sample.epub</code>" in text
        assert "<code>{}</code>".format(str(file_path)) in text


def test_web_readonly_browse_cells_format_machine_values(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readonly_browse_machine_values.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        Row.from_idless_row_dict(
            db,
            row_dict={
                "work_title": "Browse Machine Work",
                "work_canonical_title": "Browse Machine Work",
                "work_sort_title": "Browse Machine Work",
                "work_source_created_datestamp_ep_k": 1742387640000,
            },
            table="works",
        )
        app = ReadOnlyWebApplication(db, config=ReadOnlyWebConfig(title="Browse Values", hidden_column_tokens=()))

        assert "<code>file:///srv/liuxin/library</code>" == app._render_browse_value_html(column="store_root_uri", value="file:///srv/liuxin/library")
        assert "&quot;mode&quot;: &quot;strict&quot;" in app._render_browse_value_html(column="store_policy_json", value='{"mode":"strict","retry":2}')
        assert "<code>/srv/liuxin/books/sample.epub</code>" == app._render_browse_value_html(column="file_original_path", value="/srv/liuxin/books/sample.epub")

        status, _headers, body = _call_app(app, "/tables/works")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "2025-03-19 12:34 UTC" in text
        assert "<code>1742387640000</code>" in text

        status, _headers, body = _call_app(app, "/search?table=works&column=work_title&q=Browse%20Machine%20Work")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "2025-03-19 12:34 UTC" in text
        assert "<code>1742387640000</code>" in text


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
        translator_id = _insert_agent_row(db, name="Jane Translator")

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("notes", note_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id), priority=10)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", translator_id), priority=5)

        app = ReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/works/{}".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Work record" in text
        assert "work-hero" in text
        assert "Credits" in text
        assert "Contributors" in text
        assert "Jane Translator" in text
        assert "Linked entities" in text
        assert "Labels" in text
        assert "Space Opera" in text
        assert "/tables/labels/{}".format(label_id) in text
        assert "class='pill related-pill'" in text
        assert "Notes" in text
        assert "Public note excerpt for related-row rendering." in text
        assert "/tables/notes/{}".format(note_id) in text
        assert "Ursula K. Le Guin" in text
        assert "person" in text
        assert "/tables/agents/{}".format(agent_id) in text
