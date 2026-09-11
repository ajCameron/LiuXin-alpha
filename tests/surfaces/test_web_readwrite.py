"""
Exercise writable HTML forms, relationship edits, and managed-store uploads.

The in-process WSGI harnesses use real temporary catalogues and the configured
database driver, without opening an HTTP listener. Upload cases write and read
real local files through Core-backed storage; EPUB names and media types label
arbitrary fixture bytes, not validated publications. Form, notice, and cache
assertions describe the current interface, not authentication or transaction
atomicity guarantees.
"""

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
    """
    Collect one WSGI response to an optionally URL-encoded form request.

    Split the first question mark into path and query. Form keys and values are
    stringified, with None becoming an empty value, independently of the method.
    Always supply form content headers and an in-memory input stream. Duplicate
    response headers collapse to their last value; close the result iterable
    even if joining its byte chunks fails. Application errors propagate.

    Example:
        >>> status, headers, body = _call_app(app, '/tables/works')  # doctest: +SKIP


    :param app: WSGI application accepting an environment and response callback.
    :param path: Request path, optionally followed by a query string.
    :param method: Request method copied unchanged into the environment.
    :param form: Scalar form values to encode, or None for an empty body.
    :return: Status string, collapsed header dictionary, and joined response bytes.
    """
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
        """
        Capture response metadata without implementing WSGI error replacement.

        Ignore exc_info and overwrite previous metadata on repeated calls. No
        legacy write callable is provided.

        Example:
            >>> start_response('200 OK', [('Content-Type', 'text/html')])  # doctest: +SKIP


        :param status: Application-provided HTTP status string.
        :param headers: Header pairs collapsed into a dictionary.
        :param exc_info: Optional exception context, deliberately ignored.
        :return: None; update the enclosing captured-response mapping.
        """
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
    """
    Submit a buffered multipart fixture and collect its WSGI response.

    Use a fixed boundary and CRLF framing, placing text fields before files.
    Field values use str directly, so None becomes the literal ``None``. Names,
    filenames, and media types are interpolated without header escaping; this
    is a trusted-fixture builder, not a general upload encoder. Split path and
    query, collapse duplicate response headers, and close the result iterable
    even when response joining fails. No network request is made.

    Example:
        >>> response = _call_app_multipart(  # doctest: +SKIP
        ...     app, '/files/upload', fields={'store_id': 1},
        ...     files={'upload_file': ('sample.txt', 'text/plain', b'hello')})


    :param app: WSGI application under test.
    :param path: Request path with an optional query string.
    :param method: Request method copied unchanged; defaults to POST.
    :param fields: Scalar text parts, in mapping iteration order, or None.
    :param files: Part names mapped to filename, media type, and byte payload triples.
    :return: Status string, collapsed header dictionary, and joined response bytes.
    """
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
        """
        Record multipart-response metadata while ignoring exception context.

        Repeated calls replace captured values. Duplicate header names collapse
        and no WSGI write callable is returned.

        Example:
            >>> start_response('302 Found', [('Location', '/tables/files/1')])  # doctest: +SKIP


        :param status: Application-provided HTTP status string.
        :param headers: Response header pairs to store as a dictionary.
        :param exc_info: Optional WSGI exception context, deliberately ignored.
        :return: None; mutate the enclosing captured-response mapping.
        """
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
    """
    Insert a work whose display, canonical, and sort titles are identical.

    Example:
        >>> work_id = _insert_work_row(db, title='Upload target')  # doctest: +SKIP


    :param db: Open temporary catalogue receiving the row.
    :param title: Text assigned to all three title columns without normalization.
    :return: Integer identity assigned to the inserted work.
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


def _insert_agent_row(db: Database, *, name: str) -> int:
    """
    Insert a person with matching canonical and sort names for credit tests.

    Example:
        >>> agent_id = _insert_agent_row(db, name='Fixture Author')  # doctest: +SKIP


    :param db: Open temporary catalogue receiving the agent.
    :param name: Unmodified canonical and sort name.
    :return: Integer identity assigned to the person row.
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


def _insert_tag_row(db: Database, *, text: str) -> int:
    """
    Insert a tag with a whitespace-free lowercase search value when supported.

    Populate tag_phash only if that column exists; no Unicode normalization or
    existing-tag lookup is performed.

    Example:
        >>> tag_id = _insert_tag_row(db, text='Surface Tag')  # doctest: +SKIP


    :param db: Open catalogue providing the tag schema and insertion operation.
    :param text: Raw display text stored in the tag column.
    :return: Integer identity of the newly inserted tag.
    """
    payload: dict[str, object] = {"tag": text}
    if "tag_phash" in set(db.get_column_headings("tags")):
        payload["tag_phash"] = "".join(str(text or "").split()).lower()
    row = Row.from_idless_row_dict(db, row_dict=payload, table="tags")
    return int(row["tag_id"])


def _insert_store_row(db: Database, *, name: str) -> int:
    """
    Register metadata for a SQLite-file store without creating its payload file.

    Ensure the surface asset tables first. Derive the root URI below /tmp from
    the lowercased name with spaces replaced by underscores; this is fixture
    naming, not arbitrary-path sanitization or backend initialization.

    Example:
        >>> store_id = _insert_store_row(db, name='Widget Store')  # doctest: +SKIP


    :param db: Open catalogue whose asset tables may be added by the fixture helper.
    :param name: Store display name and source of its synthetic SQLite URI.
    :return: Integer identity assigned to the store metadata row.
    """
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
    """
    Insert a manifestation with only its format-detail field explicitly set.

    No expression link or carrier-type metadata is created here.

    Example:
        >>> manifestation_id = _insert_manifestation_row(db, format_detail='EPUB')  # doctest: +SKIP


    :param db: Open temporary catalogue receiving the manifestation.
    :param format_detail: Unmodified format label for the fixture.
    :return: Integer identity assigned to the manifestation row.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={
            "manifestation_format_detail": format_detail,
        },
        table="manifestations",
    )
    return int(row["manifestation_id"])


def _insert_item_row(db: Database, *, manifestation_id: int, source: str = "fixture") -> int:
    """
    Insert an item referencing an existing manifestation by integer identity.

    Example:
        >>> item_id = _insert_item_row(db, manifestation_id=manifestation_id)  # doctest: +SKIP


    :param db: Open temporary catalogue receiving the item.
    :param manifestation_id: Referenced manifestation identity, coerced to int.
    :param source: Item provenance label; defaults to fixture.
    :return: Integer identity assigned to the item row.
    """
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
    """
    Register an online, writable managed-directory store for upload fixtures.

    Ensure asset tables and insert metadata with the file protocol. The caller
    creates the directory; this helper does not bootstrap or probe the backend.

    Example:
        >>> store_id = _insert_managed_store_row(  # doctest: +SKIP
        ...     db, name='uploads', root_uri=str(managed_root))


    :param db: Open catalogue whose asset tables may be added by the fixture helper.
    :param name: Store display name.
    :param root_uri: Existing directory path or URI passed unchanged into metadata.
    :return: Integer identity assigned to the managed-store row.
    """
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
    """
    Check that work browsing exposes create, edit, delete, and write-banner UI.

    Example:
        >>> test_web_readwrite_row_and_table_pages_expose_write_actions(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Pytest-selected database driver for the real catalogue.
    :param tmp_path: Isolated directory receiving the test catalogue.
    :return: None; assert successful pages and their write-action links.
    """
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
    """
    Exercise work creation, editing, delete preview, and deletion through forms.

    Check persisted titles, redirect notices, and removal of the created row.
    Follow create/edit redirects to verify the rendered success messages.

    Example:
        >>> test_web_readwrite_can_create_edit_and_delete_work_rows(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver selected by pytest for this integration case.
    :param tmp_path: Isolated directory for the mutable catalogue.
    :return: None; assert response, notice, and database-state transitions.
    """
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
    """
    Check that the titles-view create page explains its read-only restriction.

    This GET-only case expects HTTP 200 with an explanatory page; it does not
    submit a create request or assert a mutation-rejection status.

    Example:
        >>> test_web_readwrite_rejects_generic_create_for_view_tables(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver used to create the catalogue and its views.
    :param tmp_path: Isolated directory for the catalogue fixture.
    :return: None; assert the read-only explanation on the titles create page.
    """
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
    """
    Exercise contributor-link forms, metadata updates, and link-row deletion.

    Inspect work-specific relation controls and allowed roles, then add a
    contributor, change its priority, and remove the link. Check stored metadata,
    anchored redirects, and rendered notices at each mutation step.

    Example:
        >>> test_web_readwrite_row_pages_can_add_edit_and_remove_interlinks(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver supplying the real relation schema.
    :param tmp_path: Isolated directory for the catalogue and link fixtures.
    :return: None; assert relation UI, persisted link changes, and success notices.
    """
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


def test_web_readwrite_work_tag_links_use_core_relation_receipts(
    driver_spec,
    tmp_path: Path,
) -> None:
    """
    Check the persisted tag-link metadata and notices from a Core-backed write.

    The evidence is the linked tag, its priority/source, and the resulting HTML
    notice; this case does not inspect the raw Core receipt object.

    Example:
        >>> test_web_readwrite_work_tag_links_use_core_relation_receipts(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver for the work, tag, and relation fixtures.
    :param tmp_path: Isolated directory receiving the catalogue.
    :return: None; assert stored relation values and the added-tag notice.
    """
    db_path = tmp_path / "web_readwrite_metadata_tag_link.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Metadata Link Work")
        tag_id = _insert_tag_row(db, text="Surface Metadata Tag")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, headers, _body = _call_app(
            app,
            "/tables/works/{}/links/tags/new".format(work_id),
            method="POST",
            form={
                "secondary_row_id": tag_id,
                "tag_work_link_priority": 5,
                "tag_work_link_source": "web-test",
            },
        )
        assert status == "302 Found"
        location = str(headers["Location"])
        assert location.endswith("#links-tags")
        assert "notice_title=Link+added" in location

        work_row = db.get_row_from_id("works", work_id)
        assert work_row is not None
        linked_tags = db.get_interlinked_rows(target_row=work_row, secondary_table="tags")
        assert [int(row["tag_id"]) for row in linked_tags] == [tag_id]

        link_rows = db.get_interlink_rows(primary_row=work_row, secondary_table="tags")
        assert len(link_rows) == 1
        assert int(link_rows[0]["tag_work_link_priority"]) == 5
        assert str(link_rows[0]["tag_work_link_source"]) == "web-test"

        status, _headers, body = _call_app(app, location)
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Link added" in text
        assert "Added tag." in text


def test_web_readwrite_core_read_model_observes_metadata_write(
    driver_spec,
    tmp_path: Path,
) -> None:
    """
    Verify that the shared read model sees a tag added through writable web.

    Configure the schema-backed cache without database fallback, assert the
    Core model is the read source, and compare linked tags before and after the
    POST. The redirected page must show the tag and a cache-refresh notice.

    Example:
        >>> test_web_readwrite_core_read_model_observes_metadata_write(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver used for the cache-backed integration case.
    :param tmp_path: Isolated directory for the catalogue.
    :return: None; assert read-source identity and post-write visibility.
    """
    db_path = tmp_path / "web_readwrite_metadata_cache_refresh.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Cache Refresh Work")
        tag_id = _insert_tag_row(db, text="Fresh Cache Tag")
        app = ReadWriteWebApplication(
            db,
            config=ReadWriteWebConfig(
                title="Write Test",
                metadata_read_source="cache",
                metadata_cache_type="schema_backed",
                metadata_cache_allow_database_fallback=False,
            ),
        )

        assert app.read_model.read_source is app.model

        work_row = db.get_row_from_id("works", work_id)
        assert work_row is not None
        assert app.read_model.interlinked_rows(work_row, "tags") == []

        status, headers, response_body = _call_app(
            app,
            "/tables/works/{}/links/tags/new".format(work_id),
            method="POST",
            form={"secondary_row_id": tag_id},
        )
        assert status == "302 Found", response_body.decode("utf-8", "replace")

        linked_tags = app.read_model.interlinked_rows(work_row, "tags")
        assert [int(row["tag_id"]) for row in linked_tags] == [tag_id]

        status, _headers, body = _call_app(app, str(headers["Location"]))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Fresh Cache Tag" in text
        assert "Read cache refreshed." in text


def test_web_readwrite_work_pages_can_create_and_link_new_targets(driver_spec, tmp_path: Path) -> None:
    """
    Create a contributor from a work form and verify its new credit relation.

    Submit a prefixed person-name/type payload and link priority, then check
    the linked person, priority, anchored redirect, and created-credit notice.

    Example:
        >>> test_web_readwrite_work_pages_can_create_and_link_new_targets(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver providing work and contributor relations.
    :param tmp_path: Isolated directory for the catalogue fixture.
    :return: None; assert contributor creation and linking through the form.
    """
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


def test_web_readwrite_work_tag_create_uses_core_relation_receipts(
    driver_spec,
    tmp_path: Path,
) -> None:
    """
    Create and link a tag, checking durable rows and the resulting success UI.

    Search for the created tag and verify its work relation and source metadata.
    Receipt behavior is observed through persisted results and the redirect
    notice, not by inspecting the raw Core return value.

    Example:
        >>> test_web_readwrite_work_tag_create_uses_core_relation_receipts(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver for the work, tag, and link tables.
    :param tmp_path: Isolated directory receiving the test catalogue.
    :return: None; assert the created tag, relation source, and success message.
    """
    db_path = tmp_path / "web_readwrite_metadata_tag_create.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Metadata Create Link Work")
        app = ReadWriteWebApplication(db, config=ReadWriteWebConfig(title="Write Test"))

        status, headers, _body = _call_app(
            app,
            "/tables/works/{}/links/tags/create".format(work_id),
            method="POST",
            form={
                "create__tag": "Created Surface Tag",
                "tag_work_link_source": "web-create-test",
            },
        )
        assert status == "302 Found"
        location = str(headers["Location"])
        assert location.endswith("#links-tags")
        assert "notice_title=Linked+row+created" in location

        tag_rows = list(db.search("tags", "tag", "Created Surface Tag"))
        assert len(tag_rows) == 1
        assert str(tag_rows[0]["tag"]) == "Created Surface Tag"

        work_row = db.get_row_from_id("works", work_id)
        assert work_row is not None
        linked_tags = db.get_interlinked_rows(target_row=work_row, secondary_table="tags")
        assert [int(row["tag_id"]) for row in linked_tags] == [int(tag_rows[0]["tag_id"])]

        link_rows = db.get_interlink_rows(primary_row=work_row, secondary_table="tags")
        assert len(link_rows) == 1
        assert str(link_rows[0]["tag_work_link_source"]) == "web-create-test"

        status, _headers, body = _call_app(app, location)
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Linked row created" in text
        assert "Created and linked tag." in text


def test_web_readwrite_uses_specialized_grouped_forms_for_core_tables(driver_spec, tmp_path: Path) -> None:
    """
    Check specialized create/edit labels and field groups for core table forms.

    Inspect work, file, and store create pages, including store backend choices,
    and work/store edit pages. The SQLite-store fixture registers metadata only;
    this rendering case does not initialize a storage backend or submit forms.

    Example:
        >>> test_web_readwrite_uses_specialized_grouped_forms_for_core_tables(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver supplying table and editable-field metadata.
    :param tmp_path: Isolated directory for the catalogue and fixture rows.
    :return: None; assert grouped headings, key controls, and available store kinds.
    """
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
    """
    Check the four agent-type choices in standalone and inline creation forms.

    Inspect person, organisation, group, and pseudonym options without posting
    an agent or testing enforcement of those choices during mutation.

    Example:
        >>> test_web_readwrite_agent_forms_show_allowed_agent_types(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver for agent/work form metadata.
    :param tmp_path: Isolated directory receiving the catalogue.
    :return: None; assert choice widgets and their prefixed inline field name.
    """
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
    """
    Verify typed form controls, local timestamp conversion, and JSON rejection.

    Render date, datetime-local, URI, and JSON controls. Submit valid store JSON,
    a local minute-resolution timestamp, and a URI, then inspect stored values.
    The expected epoch uses this process's local timezone. A subsequent malformed
    JSON submission must return 400 with its field-specific error.

    Example:
        >>> test_web_readwrite_uses_date_datetime_json_and_path_widgets(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver providing work/store schema and persistence.
    :param tmp_path: Isolated directory for the catalogue fixture.
    :return: None; assert controls, stored coercions, and invalid-JSON feedback.
    """
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
    """
    Check inherited UTC and raw-epoch rendering on work table and search pages.

    The table page must also retain its create action. This case tests display
    of an existing machine timestamp, not local-time form-input conversion.

    Example:
        >>> test_web_readwrite_table_and_search_pages_inherit_machine_value_formatting(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver storing and searching the timestamped work.
    :param tmp_path: Isolated directory for the catalogue.
    :return: None; assert both human-readable UTC and original numeric output.
    """
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
    """
    Check the managed-reference explanation on the language create page.

    Only GET rendering is exercised: the page responds with HTTP 200 and a
    read-only notice. No insert is attempted and no trigger failure is induced.

    Example:
        >>> test_web_readwrite_respects_trigger_locked_reference_tables(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver supplying the managed language reference data.
    :param tmp_path: Isolated directory for the catalogue fixture.
    :return: None; assert the managed-reference read-only explanation.
    """
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
    """
    Upload fixture bytes into a real managed directory and download them again.

    Verify upload-page store selection, redirect notice, file/store identities,
    names, provenance, byte count, physical storage, and download contents. The
    EPUB-named payload is arbitrary bytes, not a valid EPUB archive.

    Example:
        >>> test_web_readwrite_can_upload_file_into_store(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver for the catalogue and managed-store metadata.
    :param tmp_path: Isolated directory receiving the catalogue and payload store.
    :return: None; assert persisted metadata and byte-for-byte storage/download parity.
    """
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
    """
    Attach uploaded bytes to an existing item and verify its file association.

    Check attachment-page links, item-directed redirect, file metadata, physical
    storage, and download bytes. Although item_source_name is submitted, this
    case does not assert an update to the pre-existing item's metadata.

    Example:
        >>> test_web_readwrite_can_attach_uploaded_file_to_existing_item(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver for the manifestation, item, and file fixtures.
    :param tmp_path: Isolated directory for the catalogue and managed file storage.
    :return: None; assert the linked file, upload notice, and preserved payload bytes.
    """
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
    """
    Upload from a work page and inspect the generated expression-to-file chain.

    Starting from one work, create an expression, manifestation, item, and file
    through the upload form. Check graph cardinality and selected submitted
    metadata, then compare physical and downloaded bytes. No EPUB validation,
    partial-failure rollback, or transaction atomicity is asserted.

    Example:
        >>> test_web_readwrite_can_upload_file_from_work_page_and_create_wemi_chain(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database driver supporting the real WEMI and storage schema.
    :param tmp_path: Isolated directory receiving the catalogue and managed store.
    :return: None; assert the generated chain, redirect, metadata, and payload bytes.
    """
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
        assert status == "302 Found", _body.decode("utf-8", errors="replace")
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
