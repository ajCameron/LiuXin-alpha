from __future__ import annotations

from datetime import datetime
from io import BytesIO
from pathlib import Path
from urllib.parse import urlencode
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.web_readwrite import ReadWriteWebApplication, ReadWriteWebConfig
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _call_app(app, path: str, *, method: str = "GET", form: dict[str, object] | None = None):
    environ = {}
    setup_testing_defaults(environ)
    if "?" in path:
        raw_path, query_string = path.split("?", 1)
    else:
        raw_path, query_string = path, ""
    body = b""
    if form is not None:
        body = urlencode({str(key): "" if value is None else str(value) for key, value in form.items()}).encode("utf-8")
    environ["REQUEST_METHOD"] = method
    environ["PATH_INFO"] = raw_path
    environ["QUERY_STRING"] = query_string
    environ["CONTENT_TYPE"] = "application/x-www-form-urlencoded; charset=utf-8"
    environ["CONTENT_LENGTH"] = str(len(body))
    environ["wsgi.input"] = BytesIO(body)
    captured: dict[str, object] = {}

    def start_response(status, headers, exc_info=None):
        del exc_info
        captured["status"] = status
        captured["headers"] = dict(headers)

    result = app(environ, start_response)
    try:
        response_body = b"".join(result)
    finally:
        close = getattr(result, "close", None)
        if callable(close):
            close()
    return str(captured["status"]), dict(captured["headers"]), response_body


def _call_app_multipart(
    app,
    path: str,
    *,
    method: str = "POST",
    fields: dict[str, object] | None = None,
    files: dict[str, tuple[str, str, bytes]] | None = None,
):
    environ = {}
    setup_testing_defaults(environ)
    if "?" in path:
        raw_path, query_string = path.split("?", 1)
    else:
        raw_path, query_string = path, ""

    boundary = "----LiuXinMultipartBoundary"
    body_parts: list[bytes] = []
    for key, value in (fields or {}).items():
        body_parts.extend(
            [
                "--{}\r\n".format(boundary).encode("utf-8"),
                'Content-Disposition: form-data; name="{}"\r\n\r\n'.format(str(key)).encode("utf-8"),
                str(value).encode("utf-8"),
                b"\r\n",
            ]
        )
    for key, (filename, content_type, payload) in (files or {}).items():
        body_parts.extend(
            [
                "--{}\r\n".format(boundary).encode("utf-8"),
                'Content-Disposition: form-data; name="{}"; filename="{}"\r\n'.format(str(key), str(filename)).encode("utf-8"),
                "Content-Type: {}\r\n\r\n".format(str(content_type)).encode("utf-8"),
                bytes(payload),
                b"\r\n",
            ]
        )
    body_parts.append("--{}--\r\n".format(boundary).encode("utf-8"))
    body = b"".join(body_parts)

    environ["REQUEST_METHOD"] = method
    environ["PATH_INFO"] = raw_path
    environ["QUERY_STRING"] = query_string
    environ["CONTENT_TYPE"] = "multipart/form-data; boundary={}".format(boundary)
    environ["CONTENT_LENGTH"] = str(len(body))
    environ["wsgi.input"] = BytesIO(body)
    captured: dict[str, object] = {}

    def start_response(status, headers, exc_info=None):
        del exc_info
        captured["status"] = status
        captured["headers"] = dict(headers)

    result = app(environ, start_response)
    try:
        response_body = b"".join(result)
    finally:
        close = getattr(result, "close", None)
        if callable(close):
            close()
    return str(captured["status"]), dict(captured["headers"]), response_body


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


def _insert_store_row(db: Database, *, name: str) -> int:
    ensure_surface_asset_tables(db)
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": "single_file_sqlite",
            "store_root_uri": "sqlite:///tmp/{}.sqlite".format(name.lower().replace(" ", "_")),
        },
        table="stores",
    )
    return int(row["store_id"])


def _insert_manifestation_row(db: Database, *, format_detail: str) -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "manifestation_format_detail": format_detail,
        },
        table="manifestations",
    )
    return int(row["manifestation_id"])


def _insert_item_row(db: Database, *, manifestation_id: int, source: str = "fixture") -> int:
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "item_manifestation_id": int(manifestation_id),
            "item_source": source,
        },
        table="items",
    )
    return int(row["item_id"])


def _insert_managed_store_row(db: Database, *, name: str, root_uri: str) -> int:
    ensure_surface_asset_tables(db)
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "store_name": name,
            "store_kind": "on_disk_existing_managed_drive",
            "store_access_protocol": "file",
            "store_root_uri": root_uri,
            "store_is_read_only": 0,
            "store_online_status": "online",
        },
        table="stores",
    )
    return int(row["store_id"])


def test_web_readwrite_row_and_table_pages_expose_write_actions(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_actions.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Writable Work")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/works")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "/tables/works/new" in text
        assert "Create row" in text

        status, _headers, body = _call_app(app, "/tables/works/{}".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "/tables/works/{}/edit".format(work_id) in text
        assert "/tables/works/{}/delete".format(work_id) in text
        assert "Write Interface" in text


def test_web_readwrite_can_create_edit_and_delete_work_rows(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_crud.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Original Work")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/works/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Create row" in text
        assert "work_title" in text

        status, headers, _body = _call_app(
            app,
            "/tables/works/new",
            method="POST",
            form={
                "work_title": "Created Work",
                "work_canonical_title": "Created Work",
                "work_sort_title": "Created Work",
            },
        )
        assert status == "302 Found"
        created_location = str(headers["Location"])
        assert created_location.startswith("/tables/works/")
        assert "notice_kind=success" in created_location
        assert "notice_title=Row+created" in created_location

        created_row_id = int(created_location.split("?", 1)[0].rstrip("/").split("/")[-1])
        created_row = db.get_row_from_id("works", created_row_id)
        assert created_row is not None
        assert created_row["work_title"] == "Created Work"
        status, _headers, body = _call_app(app, created_location)
        assert status == "200 OK"
        assert "Row created" in body.decode("utf-8")

        status, _headers, _body = _call_app(
            app,
            "/tables/works/{}/edit".format(work_id),
            method="POST",
            form={
                "work_title": "Edited Work",
                "work_canonical_title": "Edited Work",
                "work_sort_title": "Edited Work",
            },
        )
        assert status == "302 Found"
        edit_location = str(_headers["Location"])
        assert "notice_title=Row+updated" in edit_location
        edited_row = db.get_row_from_id("works", work_id)
        assert edited_row is not None
        assert edited_row["work_title"] == "Edited Work"
        status, _headers, body = _call_app(app, edit_location)
        assert status == "200 OK"
        assert "Row updated" in body.decode("utf-8")

        status, _headers, body = _call_app(app, "/tables/works/{}/delete".format(created_row_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Delete <code>works:{}".format(created_row_id) in text
        assert "Delete row" in text

        status, headers, _body = _call_app(app, "/tables/works/{}/delete".format(created_row_id), method="POST", form={})
        assert status == "302 Found"
        assert headers["Location"].startswith("/tables/works?")
        assert "notice_title=Row+deleted" in str(headers["Location"])
        assert db.get_row_from_id("works", created_row_id) is None


def test_web_readwrite_rejects_generic_create_for_view_tables(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_view_guard.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        app = ReadWriteWebApplication(db)

        status, _headers, body = _call_app(app, "/tables/titles/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Read-only table" in text
        assert "cannot be created" in text


def test_web_readwrite_row_pages_can_add_edit_and_remove_interlinks(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_interlinks.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Interlinked Work")
        agent_id = _insert_agent_row(db, name="Writer Person")
        link_type_rows = db.driver_wrapper.get_all_rows("agent_work_links__types")
        link_type = str(link_type_rows[0]["type"])
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/works/{}".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Manage linked entities" in text
        assert "/tables/works/{}/links/agents/new".format(work_id) in text
        assert "/tables/works/{}/links/agents/create".format(work_id) in text
        assert "secondary_row_id" in text
        assert "<datalist" in text
        assert "agent_work_link_type" in text
        assert "Manage credits" in text
        assert "Manage tags" in text
        assert "Manage series" in text
        assert "Manage languages" in text
        assert "Contributor row id" in text
        assert "Role" in text
        assert "Suggestions from <code>agents</code>." in text
        assert "value='{}'".format(link_type) in text
        assert "Create contributor and link" in text
        assert "Create tag and link" in text
        assert "Create series and link" in text
        assert "Languages are reference data and cannot be created from this page." in text

        status, headers, _body = _call_app(
            app,
            "/tables/works/{}/links/agents/new".format(work_id),
            method="POST",
            form={
                "secondary_row_id": agent_id,
                "agent_work_link_type": link_type,
                "agent_work_link_priority": 9,
            },
        )
        assert status == "302 Found"
        add_location = str(headers["Location"])
        assert add_location.endswith("#links-agents")
        assert "notice_title=Link+added" in add_location

        work_row = db.get_row_from_id("works", work_id)
        assert work_row is not None
        linked_agents = db.get_interlinked_rows(target_row=work_row, secondary_table="agents")
        assert [int(row["agent_id"]) for row in linked_agents] == [agent_id]

        link_rows = db.get_interlink_rows(primary_row=work_row, secondary_table="agents")
        assert len(link_rows) == 1
        link_row_id = int(link_rows[0]["agent_work_link_id"])
        assert int(link_rows[0]["agent_work_link_priority"]) == 9
        assert str(link_rows[0]["agent_work_link_type"]) == link_type
        status, _headers, body = _call_app(app, add_location)
        assert status == "200 OK"
        add_text = body.decode("utf-8")
        assert "Link added" in add_text
        assert "Added credit." in add_text

        status, headers, _body = _call_app(
            app,
            "/tables/works/{}/links/agents/{}/edit".format(work_id, link_row_id),
            method="POST",
            form={
                "agent_work_link_priority": 4,
                "agent_work_link_type": link_type,
            },
        )
        assert status == "302 Found"
        edit_location = str(headers["Location"])
        assert edit_location.endswith("#links-agents")
        assert "notice_title=Link+updated" in edit_location
        link_row = db.get_row_from_id("agent_work_links", link_row_id)
        assert link_row is not None
        assert int(link_row["agent_work_link_priority"]) == 4
        status, _headers, body = _call_app(app, edit_location)
        assert status == "200 OK"
        assert "Link updated" in body.decode("utf-8")

        status, headers, _body = _call_app(
            app,
            "/tables/works/{}/links/agents/{}/delete".format(work_id, link_row_id),
            method="POST",
            form={},
        )
        assert status == "302 Found"
        delete_location = str(headers["Location"])
        assert delete_location.endswith("#links-agents")
        assert "notice_title=Link+removed" in delete_location
        assert db.get_row_from_id("agent_work_links", link_row_id) is None
        status, _headers, body = _call_app(app, delete_location)
        assert status == "200 OK"
        assert "Link removed" in body.decode("utf-8")


def test_web_readwrite_work_pages_can_create_and_link_new_targets(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_create_link_target.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Create Link Target Work")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, headers, _body = _call_app(
            app,
            "/tables/works/{}/links/agents/create".format(work_id),
            method="POST",
            form={
                "create__agent_canonical_name": "New Contributor",
                "create__agent_type": "person",
                "agent_work_link_priority": 7,
            },
        )
        assert status == "302 Found"
        location = str(headers["Location"])
        assert location.endswith("#links-agents")
        assert "notice_title=Linked+row+created" in location

        work_row = db.get_row_from_id("works", work_id)
        assert work_row is not None
        linked_agents = db.get_interlinked_rows(target_row=work_row, secondary_table="agents")
        assert len(linked_agents) == 1
        assert str(linked_agents[0]["agent_canonical_name"]) == "New Contributor"
        link_rows = db.get_interlink_rows(primary_row=work_row, secondary_table="agents")
        assert len(link_rows) == 1
        assert int(link_rows[0]["agent_work_link_priority"]) == 7

        status, _headers, body = _call_app(app, location)
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Linked row created" in text
        assert "Created and linked credit." in text


def test_web_readwrite_uses_specialized_grouped_forms_for_core_tables(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_special_forms.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Grouped Form Work")
        store_id = _insert_store_row(db, name="Grouped Store")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/works/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Create work" in text
        assert "Identity" in text
        assert "Classification" in text
        assert "Origin" in text
        assert "Notes" in text
        assert "Original language row id" in text

        status, _headers, body = _call_app(app, "/tables/files/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Create file" in text
        assert "Storage" in text
        assert "Naming" in text
        assert "Integrity" in text
        assert "file_store_id" in text

        status, _headers, body = _call_app(app, "/tables/stores/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Create store" in text
        assert "Identity" in text
        assert "Capabilities" in text
        assert "Consistency" in text
        assert "single_file_sqlite" in text
        assert "on_disk_existing_managed_drive" in text
        assert "Allowed values:" in text

        status, _headers, body = _call_app(app, "/tables/works/{}/edit".format(work_id))
        assert status == "200 OK"
        assert "Edit work" in body.decode("utf-8")

        status, _headers, body = _call_app(app, "/tables/stores/{}/edit".format(store_id))
        assert status == "200 OK"
        assert "Edit store" in body.decode("utf-8")


def test_web_readwrite_agent_forms_show_allowed_agent_types(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_agent_type_choices.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Agent Type Choices Work")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/agents/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "<select" in text
        assert "name='agent_type'" in text
        assert "Allowed values:" in text
        assert "value='person'" in text
        assert "value='organisation'" in text
        assert "value='group'" in text
        assert "value='pseudonym'" in text

        status, _headers, body = _call_app(app, "/tables/works/{}".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Create contributor and link" in text
        assert "name='create__agent_type'" in text
        assert "value='person'" in text
        assert "value='organisation'" in text
        assert "value='group'" in text
        assert "value='pseudonym'" in text


def test_web_readwrite_uses_date_datetime_json_and_path_widgets(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_widget_types.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_store_row(db, name="Widget Store")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/works/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "name='work_original_date' type='date'" in text
        assert "Use YYYY-MM-DD." in text

        status, _headers, body = _call_app(app, "/tables/stores/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "name='store_last_seen_online_timestamp_ep_k' type='datetime-local'" in text
        assert "Use local date/time or epoch milliseconds; stored as epoch ms." in text
        assert "name='store_root_uri' type='text'" in text
        assert "spellcheck='false'" in text
        assert "Enter an absolute URI or URL when possible." in text
        assert "<textarea id='store_policy_json' name='store_policy_json' spellcheck='false'" in text
        assert "Expected valid JSON text. Invalid JSON will be rejected." in text

        status, _headers, _body = _call_app(
            app,
            "/tables/stores/{}/edit".format(store_id),
            method="POST",
            form={
                "store_policy_json": '{"mode":"strict","retry":2}',
                "store_last_seen_online_timestamp_ep_k": "2025-03-19T12:34",
                "store_root_uri": "file:///tmp/widget-store",
            },
        )
        assert status == "302 Found"
        store_row = db.get_row_from_id("stores", store_id)
        assert store_row is not None
        assert str(store_row["store_policy_json"]) == '{"mode":"strict","retry":2}'
        assert str(store_row["store_root_uri"]) == "file:///tmp/widget-store"
        assert int(store_row["store_last_seen_online_timestamp_ep_k"]) == int(datetime.strptime("2025-03-19T12:34", "%Y-%m-%dT%H:%M").timestamp() * 1000)

        status, _headers, body = _call_app(
            app,
            "/tables/stores/{}/edit".format(store_id),
            method="POST",
            form={
                "store_policy_json": "{bad json",
            },
        )
        assert status == "400 Bad Request"
        assert "Invalid JSON for store_policy_json" in body.decode("utf-8")


def test_web_readwrite_table_and_search_pages_inherit_machine_value_formatting(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_machine_values.sqlite"
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
                "work_title": "Write Browse Machine Work",
                "work_canonical_title": "Write Browse Machine Work",
                "work_sort_title": "Write Browse Machine Work",
                "work_source_created_datestamp_ep_k": 1742387640000,
            },
            table="works",
        )
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/works")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "2025-03-19 12:34 UTC" in text
        assert "<code>1742387640000</code>" in text
        assert "Create row" in text

        status, _headers, body = _call_app(app, "/search?table=works&column=work_title&q=Write%20Browse%20Machine%20Work")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "2025-03-19 12:34 UTC" in text
        assert "<code>1742387640000</code>" in text


def test_web_readwrite_respects_trigger_locked_reference_tables(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_readonly_reference.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/languages/new")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Read-only table" in text
        assert "managed reference data" in text


def test_web_readwrite_can_upload_file_into_store(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_upload.sqlite"
    managed_root = tmp_path / "managed_store"
    managed_root.mkdir(parents=True, exist_ok=True)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_managed_store_row(db, name="managed_uploads", root_uri=str(managed_root))
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/files/upload")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Upload file" in text
        assert "managed_uploads" in text

        status, headers, _body = _call_app_multipart(
            app,
            "/files/upload",
            fields={
                "store_id": store_id,
                "file_name": "uploaded.epub",
                "file_source": "web_upload_test",
                "file_role": "primary",
            },
            files={
                "upload_file": ("original-name.epub", "application/epub+zip", b"EPUB-UPLOAD-BYTES"),
            },
        )
        assert status == "302 Found"
        location = str(headers["Location"])
        assert location.startswith("/tables/files/")
        assert "notice_title=File+uploaded" in location

        file_row_id = int(location.split("?", 1)[0].rstrip("/").split("/")[-1])
        file_row = db.get_row_from_id("files", file_row_id)
        assert file_row is not None
        assert int(file_row["file_store_id"]) == store_id
        assert str(file_row["file_name"]) == "uploaded.epub"
        assert str(file_row["file_original_name"]) == "original-name.epub"
        assert str(file_row["file_source"]) == "web_upload_test"
        assert int(file_row["file_size_bytes"]) == len(b"EPUB-UPLOAD-BYTES")

        stored_path = managed_root / str(file_row["file_storage_key"])
        assert stored_path.is_file() is True
        assert stored_path.read_bytes() == b"EPUB-UPLOAD-BYTES"

        status, _headers, body = _call_app(app, "/files/{}/download".format(file_row_id))
        assert status == "200 OK"
        assert body == b"EPUB-UPLOAD-BYTES"


def test_web_readwrite_can_attach_uploaded_file_to_existing_item(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_item_upload.sqlite"
    managed_root = tmp_path / "managed_item_store"
    managed_root.mkdir(parents=True, exist_ok=True)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_managed_store_row(db, name="item_uploads", root_uri=str(managed_root))
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(db, manifestation_id=manifestation_id, source="existing_item")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/items/{}/upload".format(item_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Attachment target" in text
        assert "/tables/items/{}/upload".format(item_id) in text
        assert "/tables/items/{}".format(item_id) in text

        status, headers, _body = _call_app_multipart(
            app,
            "/tables/items/{}/upload".format(item_id),
            fields={
                "store_id": store_id,
                "file_name": "attached.epub",
                "file_source": "item_upload_test",
                "file_role": "primary",
                "item_source_name": "Existing Item Upload",
            },
            files={
                "upload_file": ("attached-original.epub", "application/epub+zip", b"ITEM-UPLOAD-BYTES"),
            },
        )
        assert status == "302 Found"
        location = str(headers["Location"])
        assert location.startswith("/tables/items/{}".format(item_id))
        assert "notice_title=File+uploaded" in location

        file_rows = [row for row in db.get_all_rows("files") if int(row["file_item_id"] or 0) == item_id]
        assert len(file_rows) == 1
        file_row = file_rows[0]
        assert int(file_row["file_store_id"]) == store_id
        assert str(file_row["file_name"]) == "attached.epub"
        assert str(file_row["file_source"]) == "item_upload_test"
        assert str(file_row["file_original_name"]) == "attached-original.epub"

        stored_path = managed_root / str(file_row["file_storage_key"])
        assert stored_path.is_file() is True
        assert stored_path.read_bytes() == b"ITEM-UPLOAD-BYTES"

        status, _headers, body = _call_app(app, "/files/{}/download".format(int(file_row["file_id"])))
        assert status == "200 OK"
        assert body == b"ITEM-UPLOAD-BYTES"


def test_web_readwrite_can_upload_file_from_work_page_and_create_wemi_chain(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_readwrite_work_upload.sqlite"
    managed_root = tmp_path / "managed_work_store"
    managed_root.mkdir(parents=True, exist_ok=True)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        store_id = _insert_managed_store_row(db, name="work_uploads", root_uri=str(managed_root))
        work_id = _insert_work_row(db, title="Upload Target Work")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, _headers, body = _call_app(app, "/tables/works/{}".format(work_id))
        assert status == "200 OK"
        assert "/tables/works/{}/upload".format(work_id) in body.decode("utf-8")

        status, _headers, body = _call_app(app, "/tables/works/{}/upload".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Generated chain" in text
        assert "/tables/works/{}/upload".format(work_id) in text
        assert "/tables/works/{}".format(work_id) in text

        status, headers, _body = _call_app_multipart(
            app,
            "/tables/works/{}/upload".format(work_id),
            fields={
                "store_id": store_id,
                "file_name": "work-upload.epub",
                "file_source": "work_upload_test",
                "expression_label": "Web Upload Expression",
                "manifestation_format_detail": "EPUB",
                "manifestation_carrier_type": "ebook",
                "item_type": "digital",
                "item_source": "web_upload",
                "item_source_name": "Work Upload Item",
                "item_location": "shelf://uploads/work",
            },
            files={
                "upload_file": ("work-original.epub", "application/epub+zip", b"WORK-UPLOAD-BYTES"),
            },
        )
        assert status == "302 Found"
        location = str(headers["Location"])
        assert location.startswith("/tables/works/{}".format(work_id))
        assert "notice_title=File+uploaded" in location

        work_row = db.get_row_from_id("works", work_id)
        assert work_row is not None
        expression_rows = db.get_interlinked_rows(target_row=work_row, secondary_table="expressions")
        assert len(expression_rows) == 1
        expression_row = expression_rows[0]
        assert str(expression_row["expression_label"]) == "Web Upload Expression"

        manifestation_rows = db.get_interlinked_rows(target_row=expression_row, secondary_table="manifestations")
        assert len(manifestation_rows) == 1
        manifestation_row = manifestation_rows[0]
        manifestation_id = int(manifestation_row["manifestation_id"])
        assert str(manifestation_row["manifestation_format_detail"]) == "EPUB"
        assert str(manifestation_row["manifestation_carrier_type"]) == "ebook"

        item_rows = [row for row in db.get_all_rows("items") if int(row["item_manifestation_id"] or 0) == manifestation_id]
        assert len(item_rows) == 1
        item_row = item_rows[0]
        item_id = int(item_row["item_id"])
        assert str(item_row["item_type"]) == "digital"
        assert str(item_row["item_source_name"]) == "Work Upload Item"

        file_rows = [row for row in db.get_all_rows("files") if int(row["file_item_id"] or 0) == item_id]
        assert len(file_rows) == 1
        file_row = file_rows[0]
        assert str(file_row["file_name"]) == "work-upload.epub"
        assert str(file_row["file_source"]) == "work_upload_test"

        stored_path = managed_root / str(file_row["file_storage_key"])
        assert stored_path.is_file() is True
        assert stored_path.read_bytes() == b"WORK-UPLOAD-BYTES"

        status, _headers, body = _call_app(app, "/files/{}/download".format(int(file_row["file_id"])))
        assert status == "200 OK"
        assert body == b"WORK-UPLOAD-BYTES"
