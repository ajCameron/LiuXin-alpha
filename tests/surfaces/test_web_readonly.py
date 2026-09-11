"""
Exercise generic web browsing and legacy-file delivery with real temporary databases.

The in-process WSGI harness checks status, headers, and bytes rather than browser
rendering or a network listener. Fixtures cover schema grouping, cache snapshots,
specialized work/file/store details, linked entities, sensitive-column hiding,
filesystem and SQLite-blob acquisitions, and unsupported byte access. Hiding
assertions cover the supplied fields, not a complete privacy or content-safety
audit. Driver coverage is determined by the selected driver_spec parametrization.
"""

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
    """
    Invoke WSGI in process and collect the response while closing its iterable.

    Split at the first question mark and populate stdlib testing defaults.
    Duplicate headers collapse into a dict, exc_info is ignored, and no WSGI
    write callable is supplied. Cleanup runs even when body joining fails.

    Example:
        >>> status, headers, body = _call_app(app, "/tables/works?limit=1")  # doctest: +SKIP


    :param app: WSGI callable accepting an environment and start_response callback.
    :param path: Root-relative path optionally followed by an encoded query string.
    :param method: Request method forwarded unchanged into the environment.
    :return: String status, collapsed header mapping, and concatenated response bytes.
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
        Capture response metadata in the enclosing request harness.

        Example:
            >>> start_response("200 OK", [("Content-Type", "text/plain")])  # doctest: +SKIP


        :param status: HTTP status line retained for the harness result.
        :param headers: Header pairs collapsed into a dictionary, losing duplicates.
        :param exc_info: Optional WSGI exception context deliberately ignored here.
        :return: None; this test callback does not implement the optional write API.
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
    Insert a work whose display, canonical, and sort titles share one value.

    Example:
        >>> work_id = _insert_work_row(db, title="A test work")  # doctest: +SKIP


    :param db: Open writable fixture database receiving the new row.
    :param title: Text stored identically in the three conventional title fields.
    :return: Assigned work ID converted to int; no relationships are created.
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


def _insert_store_row(db: Database, *, name: str, root_uri: str, credentials: str = "") -> int:
    """
    Insert a filesystem-store row with explicit sensitive fields for hiding tests.

    The policy JSON contains a fixed secret marker. Inserting metadata does not
    create the storage root or prove that its files can be read.

    Example:
        >>> store_id = _insert_store_row(db, name="Fixtures", root_uri=str(tmp_path))  # doctest: +SKIP


    :param db: Open writable fixture database receiving the store metadata.
    :param name: Store display name.
    :param root_uri: Filesystem root hint stored without existence validation here.
    :param credentials: Credential text expected to remain absent from rendered detail.
    :return: Newly assigned integer store ID.
    """
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
    """
    Ensure asset tables and insert file metadata for a local or store-key payload.

    Truthy explicit key/name values override path-derived defaults. A supplied
    path is statted for size and original-path fields; without one, no file is
    opened and size/original-path fields are omitted. This helper does not store
    bytes or create work relationships.

    Example:
        >>> file_id = _insert_file_row(db, store_id=store_id, file_path=asset_path)  # doctest: +SKIP


    :param db: Open writable database whose compatibility asset tables may be created.
    :param store_id: Store identity converted to int in the inserted row.
    :param file_path: Optional existing path providing size and original naming fields.
    :param file_storage_key: Truthy explicit key, otherwise path basename or empty text.
    :param file_name: Truthy display name, otherwise path basename or download.bin.
    :param file_source: Source marker retained verbatim, including an empty marker.
    :return: Integer ID of the inserted primary ebook-file metadata row.
    """
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
    """
    Simulate a storage manager whose direct byte access is explicitly unsupported.

    Only the read_bytes shape needed by the acquisition path is provided; this
    is not a complete storage-manager implementation.

    Example:
        >>> manager = _UnsupportedStorageManager()
        >>> manager.read_bytes(None)
        Traceback (most recent call last):
        ...
        NotImplementedError: no byte access
    """
    def read_bytes(self, location) -> bytes:
        """
        Reject direct byte access regardless of the supplied location.

        Example:
            >>> _UnsupportedStorageManager().read_bytes("unused")
            Traceback (most recent call last):
            ...
            NotImplementedError: no byte access


        :param location: Acquisition location deliberately ignored by this test double.
        :return: Never returns a payload.
        :raises NotImplementedError: Always, to exercise unsupported-download translation.
        """
        del location
        raise NotImplementedError("no byte access")


def _insert_label_row(db: Database, *, text: str) -> int:
    """
    Insert a label with a normalized search token for linked-entity rendering.

    Example:
        >>> label_id = _insert_label_row(db, text="Space Opera")  # doctest: +SKIP


    :param db: Writable fixture database receiving a label, not its relationships.
    :param text: Display text also passed to the project's tag-search normalizer.
    :return: Assigned label ID converted to int.
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


def _insert_note_row(db: Database, *, note: str) -> int:
    """
    Insert plain note text for later explicit work linking.

    Example:
        >>> note_id = _insert_note_row(db, note="A public fixture note")  # doctest: +SKIP


    :param db: Writable fixture database receiving the new note.
    :param note: Text stored verbatim in the conventional note field.
    :return: Assigned integer note ID without creating relationships.
    """
    row = Row.from_idless_row_dict(
        db,
        row_dict={"note": note},
        table="notes",
    )
    return int(row["note_id"])


def _insert_agent_row(db: Database, *, name: str, agent_type: str = "person") -> int:
    """
    Insert a contributor with matching canonical and sort names.

    Example:
        >>> agent_id = _insert_agent_row(db, name="Example Author")  # doctest: +SKIP


    :param db: Writable fixture database receiving contributor metadata.
    :param name: Text stored in both conventional name fields.
    :param agent_type: Type hint used by linked-contributor rendering.
    :return: Assigned integer agent ID; no work-credit relationship is added.
    """
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
    """
    Verify table-group navigation, work browsing/detail, and exact-title search HTML.

    Assertions cover response text and layout hooks, not browser rendering or
    interactive behavior. One stored work supplies the exact-search match.

    Example:
        >>> test_web_readonly_home_table_row_and_search(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Selected database-driver fixture used to create the catalogue.
    :param tmp_path: Isolated directory for the temporary database file.
    :return: None; assertions establish in-process route and presentation contracts.
    """
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
    """
    Verify cache-selected browsing excludes rows inserted after snapshot creation.

    Parsed flags disable database fallback. The late row is absent from search
    and produces a 200 Row not found page; home exposes unavailable counts.
    This uses direct application composition, not a spawned CLI server.

    Example:
        >>> test_web_readonly_cache_read_source_cli_options_serve_snapshot(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Database-driver fixture for the real cached catalogue.
    :param tmp_path: Directory containing the temporary catalogue database.
    :return: None; assertions compare snapshot-visible and later database records.
    """
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
    """
    Verify representative explicit table categories and relationship-name suffixes.

    Example:
        >>> test_web_readonly_table_classifier_splits_main_helper_interlink_and_intralink()


    :return: None; assertions cover the selected names without requiring a database.
    """
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
    """
    Verify a filesystem asset gets specialized detail markup and byte-identical download.

    The EPUB suffix is a naming fixture, not a claim that the arbitrary payload
    is a valid ebook. Attachment disposition and filename are checked explicitly.

    Example:
        >>> test_web_readonly_file_row_exposes_download_and_serves_bytes(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Selected driver for metadata-store and file-row fixtures.
    :param tmp_path: Temporary directory holding the database and physical asset bytes.
    :return: None; assertions verify detail links, disposition, and complete payload.
    """
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
    """
    Verify store-key acquisition serves bytes persisted through a SQLite blob store.

    The fixture bootstraps storage, stores and locates a digital asset, then
    inserts legacy-file metadata pointing to that key without a local file path.

    Example:
        >>> test_web_readonly_file_download_uses_store_manager_for_blob_store(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Driver selected for the metadata catalogue, separate from blob storage.
    :param tmp_path: Directory holding both metadata and single-file blob databases.
    :return: None; assertions verify a download link, filename, and original blob bytes.
    """
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
    """
    Verify advertised byte capability can still fail as an explicit unsupported download.

    A minimal replacement storage manager raises NotImplementedError. The file
    detail remains 200 with a link, while actual acquisition returns 501 and an
    explanatory message rather than a successful empty payload.

    Example:
        >>> test_web_readonly_unsupported_store_download_returns_501(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Driver fixture used for the legacy-file and store metadata.
    :param tmp_path: Directory containing the catalogue and an intentionally absent store root.
    :return: None; assertions distinguish advertised availability from delivery outcome.
    """
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
    """
    Verify store-detail markup includes public fields but omits fixture secrets.

    Coverage is limited to credentials, the supplied credential value, and policy
    JSON column naming; it is not proof of whole-schema privacy or authorization.

    Example:
        >>> test_web_readonly_hides_sensitive_store_columns(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Selected database driver for the store-row fixture.
    :param tmp_path: Directory used for the metadata database and displayed store root.
    :return: None; assertions check specialized store sections and sensitive omissions.
    """
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
    """
    Verify explicitly linked labels, notes, and contributors receive specialized markup.

    The fixture creates the three relationships after inserting their rows.
    Assertions check labels, note excerpts, contributor type, and detail URLs
    within the work page, without claiming browser interaction coverage.

    Example:
        >>> test_web_readonly_row_page_renders_specialized_linked_entities(driver_spec, tmp_path)  # doctest: +SKIP


    :param driver_spec: Selected driver for the catalogue and interlink operations.
    :param tmp_path: Isolated directory for the relationship-test database.
    :return: None; assertions establish text, link, and section-class contracts.
    """
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
