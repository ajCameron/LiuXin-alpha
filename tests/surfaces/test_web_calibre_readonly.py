from __future__ import annotations

import json
from pathlib import Path
from wsgiref.util import setup_testing_defaults

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.surfaces.web_calibre_readonly import (
    CalibreReadOnlyWebApplication,
    CalibreReadOnlyWebConfig,
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


def _enc(text: str) -> str:
    return text.encode("utf-8").hex()


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


def _insert_file_row(db: Database, *, store_id: int, file_path: Path) -> int:
    return _insert_file_row_for_item(db, store_id=store_id, item_id=None, file_path=file_path)


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
    row = Row.from_idless_row_dict(
        db,
        row_dict=row_dict,
        table="files",
    )
    return int(row["file_id"])


def _insert_image_row(db: Database, *, store_id: int, file_path: Path) -> int:
    ensure_surface_asset_tables(db, include_images=True)
    row = Row.from_idless_row_dict(
        db,
        row_dict={
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
        row_dict={
            "expression_title_override": title_override,
        },
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


def test_web_calibre_readonly_parser_accepts_cache_read_source_options(tmp_path: Path) -> None:
    db_path = tmp_path / "calibre_cli.sqlite"
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
    config = CalibreReadOnlyWebConfig(
        metadata_read_source=str(args.metadata_read_source),
        metadata_cache_type=str(args.cache_type),
        metadata_cache_allow_database_fallback=not bool(args.no_cache_db_fallback),
    )
    assert config.metadata_read_source == "cache"
    assert config.metadata_cache_type == "schema_backed"
    assert config.metadata_cache_allow_database_fallback is False


def test_web_calibre_readonly_cache_read_source_detail_routes_serve_snapshot(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_cache_source.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        cached_id = _insert_work_row(db, title="Cached Calibre Route Title")
        app = CalibreReadOnlyWebApplication(
            db,
            config=CalibreReadOnlyWebConfig(
                default_page_size=10,
                max_page_size=25,
                metadata_read_source="cache",
                metadata_cache_type="schema_backed",
                metadata_cache_allow_database_fallback=False,
            ),
        )
        uncached_id = _insert_work_row(db, title="Uncached Calibre Route Title")

        status, _headers, body = _call_app(
            app,
            "/ajax/books?ids={},{}".format(cached_id, uncached_id),
        )

        assert getattr(app.read_model.read_source, "allow_database_fallback") is False
        assert status == "200 OK"
        payload = json.loads(body.decode("utf-8"))
        assert sorted(payload) == [str(cached_id)]
        assert payload[str(cached_id)]["title"] == "Cached Calibre Route Title"

        status, _headers, body = _call_app(app, "/ajax/book/{}/main".format(uncached_id))
        assert status == "404 Not Found"
        assert "Book row not found" in body.decode("utf-8")

        status, _headers, body = _call_app(app, "/interface-data/book-metadata/{}".format(uncached_id))
        assert status == "404 Not Found"
        assert "Book row not found" in body.decode("utf-8")


def test_web_calibre_readonly_home_and_browse_pages(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_home.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="A Calibre-Like Title")
        _insert_agent_row(db, name="Calibre Author")
        _insert_label_row(db, text="Adventure")
        _insert_series_row(db, name="Library Shelf")
        app = CalibreReadOnlyWebApplication(db, config=CalibreReadOnlyWebConfig(title="Calibre Mirror"))

        status, _headers, body = _call_app(app, "/")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "/browse/titles" in text
        assert "/browse/authors" in text
        assert "/browse/tags" in text
        assert "/browse/series" in text
        assert "/browse/recent" in text
        assert "id='listing'" in text
        assert "Calibre-shaped public browse surface" in text

        status, _headers, body = _call_app(app, "/browse/titles?limit=10")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Titles" in text
        assert "/book/{}".format(work_id) in text
        assert "A Calibre-Like Title" in text
        assert "id='listing'" in text


def test_web_calibre_readonly_cover_and_thumb_routes_use_linked_images(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_cover.sqlite"
    image_payload = (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\x9cc`\x00\x01"
        b"\x00\x00\x05\x00\x01\r\n-\xb4\x00\x00\x00\x00IEND\xaeB`\x82"
    )
    image_path = tmp_path / "cover.png"
    image_path.write_bytes(image_payload)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Cover Book")
        store_id = _insert_store_row(db, name="Images", root_uri=str(tmp_path))
        image_id = _insert_image_row(db, store_id=store_id, file_path=image_path)
        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("images", image_id))

        app = CalibreReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/browse/titles?limit=10")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "/get/thumb/{}/main?sz=60x80".format(work_id) in text

        status, headers, body = _call_app(app, "/get/thumb/{}/main?sz=60x80".format(work_id))
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("image/png")
        assert body == image_payload

        status, headers, body = _call_app(app, "/get/cover/{}/main".format(work_id))
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("image/png")
        assert body == image_payload


def test_web_calibre_readonly_book_page_exposes_authors_tags_series_and_formats(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_book.sqlite"
    payload = b"book payload"
    file_path = tmp_path / "calibre-book.epub"
    file_path.write_bytes(payload)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Calibre Book Page")
        store_id = _insert_store_row(db, name="Downloads", root_uri=str(tmp_path))
        agent_id = _insert_agent_row(db, name="Ursula Author")
        label_id = _insert_label_row(db, text="Science Fiction")
        series_id = _insert_series_row(db, name="Hainish Cycle")
        expression_id = _insert_expression_row(db, title_override="Calibre Book Page")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(
            db,
            manifestation_id=manifestation_id,
            source_path=str(file_path),
            source_name=file_path.name,
        )
        file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=file_path)

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id), priority=10)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("series", series_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("expressions", expression_id))
        db.interlink_rows(
            primary_row=db.get_row_from_id("expressions", expression_id),
            secondary_row=db.get_row_from_id("manifestations", manifestation_id),
        )

        app = CalibreReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/book/{}".format(work_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Calibre Book Page" in text
        assert "Available formats" in text
        assert "/files/{}/download".format(file_id) in text
        assert "EPUB" in text
        assert "/author/agents/{}".format(agent_id) in text
        assert "/tag/{}".format(label_id) in text
        assert "/series/{}".format(series_id) in text
        assert "Credits" in text


def test_web_calibre_readonly_author_and_tag_pages_list_linked_books(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_linked.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="The Linked Book")
        agent_id = _insert_agent_row(db, name="Linked Author")
        label_id = _insert_label_row(db, text="Linked Tag")

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id), priority=10)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))

        app = CalibreReadOnlyWebApplication(db)

        status, _headers, body = _call_app(app, "/author/agents/{}".format(agent_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Author: Linked Author" in text
        assert "/book/{}".format(work_id) in text
        assert "The Linked Book" in text

        status, _headers, body = _call_app(app, "/tag/{}".format(label_id))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Tag: Linked Tag" in text
        assert "/book/{}".format(work_id) in text


def test_web_calibre_readonly_search_results_use_calibre_routes(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_search.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Needle Title")
        agent_id = _insert_agent_row(db, name="Needle Author")
        app = CalibreReadOnlyWebApplication(db, config=CalibreReadOnlyWebConfig(default_page_size=10, max_page_size=10))

        status, _headers, body = _call_app(app, "/search?global_q=Needle")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "/book/{}".format(work_id) in text
        assert "/author/agents/{}".format(agent_id) in text


def test_web_calibre_readonly_mobile_and_legacy_routes(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_legacy.sqlite"
    payload = b"legacy epub payload"
    file_path = tmp_path / "legacy-book.epub"
    file_path.write_bytes(payload)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Legacy Mobile Book")
        store_id = _insert_store_row(db, name="Downloads", root_uri=str(tmp_path))
        expression_id = _insert_expression_row(db, title_override="Legacy Mobile Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(
            db,
            manifestation_id=manifestation_id,
            source_path=str(file_path),
            source_name=file_path.name,
        )
        file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=file_path)

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("expressions", expression_id))
        db.interlink_rows(
            primary_row=db.get_row_from_id("expressions", expression_id),
            secondary_row=db.get_row_from_id("manifestations", manifestation_id),
        )

        app = CalibreReadOnlyWebApplication(db, config=CalibreReadOnlyWebConfig(default_page_size=10, max_page_size=25))

        status, _headers, body = _call_app(app, "/mobile?search=Legacy&num=10&sort=title&order=ascending")
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "Books 1 to 1 of 1" in text
        assert "Legacy Mobile Book" in text

        status, headers, body = _call_app(app, "/browse/book/{}".format(work_id))
        assert status == "302 Found"
        assert headers["Location"] == "/book/{}".format(work_id)
        assert body == b""

        status, headers, body = _call_app(app, "/legacy/get/epub/{}/main/legacy-book.epub".format(work_id))
        assert status == "200 OK"
        assert headers["Content-Disposition"].startswith('attachment; filename="legacy-book.epub"')
        assert body == payload


def test_web_calibre_readonly_static_icon_and_opds_routes(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_static.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="OPDS Book")
        app = CalibreReadOnlyWebApplication(db)

        status, headers, body = _call_app(app, "/robots.txt")
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("text/plain")
        assert "Allow: /" in body.decode("utf-8")

        status, headers, body = _call_app(app, "/static/mobile.css")
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("text/css")
        assert b"--paper" in body

        status, headers, body = _call_app(app, "/icon/lt.png?sz=48")
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("image/png")
        assert body.startswith(b"\x89PNG")

        status, headers, body = _call_app(app, "/opds")
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/atom+xml")
        text = body.decode("utf-8")
        assert "<title>LiuXin Calibre-Style Read-Only Web</title>" in text
        assert "/opds/navcatalog/{}".format(_enc("Nauthors")) in text
        assert "<icon>/favicon.png</icon>" in text
        assert "http://opds-spec.org/2010/catalog" in text
        assert "<link rel='self' href='/opds'/>" in text

        status, headers, body = _call_app(app, "/opds/navcatalog/{}".format(_enc("Otitle")))
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/atom+xml")
        assert "OPDS Book" in body.decode("utf-8")

        status, headers, body = _call_app(app, "/stanza")
        assert status == "302 Found"
        assert headers["Location"] == "/opds"
        assert body == b""

        status, headers, body = _call_app(app, "/browse/book/{}".format(work_id))
        assert status == "302 Found"
        assert headers["Location"] == "/book/{}".format(work_id)


def test_web_calibre_readonly_opds_paging_links(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_opds_paging.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        agent_id = _insert_agent_row(db, name="Paged Author")
        for index in range(12):
            work_id = _insert_work_row(db, title="Paged Book {:02d}".format(index))
            work_row = db.get_row_from_id("works", work_id)
            db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id), priority=10)

        app = CalibreReadOnlyWebApplication(db, config=CalibreReadOnlyWebConfig(default_page_size=5, max_page_size=10))

        titles_token = _enc("Otitle")
        authors_token = _enc("Nauthors")
        author_item_token = _enc("I{}:authors".format(agent_id))

        status, headers, body = _call_app(app, "/opds/navcatalog/{}".format(titles_token))
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/atom+xml")
        text = body.decode("utf-8")
        assert "<link rel='self' href='/opds/navcatalog/{}'/>".format(titles_token) in text
        assert "<link rel='up' href='/opds'/>" in text
        assert "<link rel='first' href='/opds/navcatalog/{}'/>".format(titles_token) in text
        assert "<link rel='last' href='/opds/navcatalog/{}?offset=10'/>".format(titles_token) in text
        assert "<link rel='next' href='/opds/navcatalog/{}?offset=5'/>".format(titles_token) in text
        assert "rel='previous'" not in text

        status, headers, body = _call_app(app, "/opds/navcatalog/{}?offset=5".format(titles_token))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "<link rel='self' href='/opds/navcatalog/{}?offset=5'/>".format(titles_token) in text
        assert "<link rel='previous' href='/opds/navcatalog/{}'/>".format(titles_token) in text
        assert "<link rel='next' href='/opds/navcatalog/{}?offset=10'/>".format(titles_token) in text
        assert "<link rel='last' href='/opds/navcatalog/{}?offset=10'/>".format(titles_token) in text

        status, headers, body = _call_app(app, "/opds/category/{}/{}".format(authors_token, author_item_token))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "<link rel='up' href='/opds/navcatalog/{}'/>".format(authors_token) in text
        assert "<link rel='next' href='/opds/category/{}/{}?offset=5'/>".format(authors_token, author_item_token) in text

        status, headers, body = _call_app(app, "/opds/category/{}/{}?offset=5".format(authors_token, author_item_token))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "<link rel='self' href='/opds/category/{}/{}?offset=5'/>".format(authors_token, author_item_token) in text
        assert "<link rel='previous' href='/opds/category/{}/{}'/>".format(authors_token, author_item_token) in text
        assert "<link rel='next' href='/opds/category/{}/{}?offset=10'/>".format(authors_token, author_item_token) in text


def test_web_calibre_readonly_opds_categorygroup_routes(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_opds_categorygroup.sqlite"
    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        for index in range(7):
            agent_id = _insert_agent_row(db, name="Alpha {:02d}".format(index))
            work_id = _insert_work_row(db, title="Alpha Book {:02d}".format(index))
            db.interlink_rows(primary_row=db.get_row_from_id("works", work_id), secondary_row=db.get_row_from_id("agents", agent_id), priority=10)
        for index in range(2):
            agent_id = _insert_agent_row(db, name="Beta {:02d}".format(index))
            work_id = _insert_work_row(db, title="Beta Book {:02d}".format(index))
            db.interlink_rows(primary_row=db.get_row_from_id("works", work_id), secondary_row=db.get_row_from_id("agents", agent_id), priority=10)

        app = CalibreReadOnlyWebApplication(
            db,
            config=CalibreReadOnlyWebConfig(default_page_size=5, max_page_size=10, opds_max_ungrouped_items=4),
        )

        authors_token = _enc("Nauthors")
        grouped_category_token = _enc("authors")
        alpha_group_token = _enc("A")

        status, headers, body = _call_app(app, "/opds/navcatalog/{}".format(authors_token))
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/atom+xml")
        text = body.decode("utf-8")
        assert "/opds/categorygroup/{}/{}".format(grouped_category_token, alpha_group_token) in text
        assert "/opds/categorygroup/{}/{}".format(grouped_category_token, _enc("B")) in text
        assert "Alpha 00" not in text

        status, headers, body = _call_app(app, "/opds/categorygroup/{}/{}".format(grouped_category_token, alpha_group_token))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "<link rel='up' href='/opds/navcatalog/{}'/>".format(authors_token) in text
        assert "<link rel='next' href='/opds/categorygroup/{}/{}?offset=5'/>".format(grouped_category_token, alpha_group_token) in text
        assert "Alpha 00" in text
        assert "/opds/category/{}/".format(authors_token) in text

        status, headers, body = _call_app(app, "/opds/categorygroup/{}/{}?offset=5".format(grouped_category_token, alpha_group_token))
        assert status == "200 OK"
        text = body.decode("utf-8")
        assert "<link rel='self' href='/opds/categorygroup/{}/{}?offset=5'/>".format(grouped_category_token, alpha_group_token) in text
        assert "<link rel='previous' href='/opds/categorygroup/{}/{}'/>".format(grouped_category_token, alpha_group_token) in text
        assert "Alpha 06" in text


def test_web_calibre_readonly_ajax_and_interface_data_routes(driver_spec, tmp_path: Path) -> None:
    db_path = tmp_path / "web_calibre_ajax.sqlite"
    payload = b"ajax epub payload"
    file_path = tmp_path / "ajax-book.epub"
    file_path.write_bytes(payload)

    with Database(
        metadata={"database_path": str(db_path)},
        db_type=driver_spec.db_type,
        create=True,
        backup=False,
        storage_startup_on_add=False,
    ) as db:
        work_id = _insert_work_row(db, title="Ajax Book")
        store_id = _insert_store_row(db, name="Downloads", root_uri=str(tmp_path))
        agent_id = _insert_agent_row(db, name="Ajax Author")
        label_id = _insert_label_row(db, text="Ajax Tag")
        expression_id = _insert_expression_row(db, title_override="Ajax Book")
        manifestation_id = _insert_manifestation_row(db, format_detail="EPUB")
        item_id = _insert_item_row(
            db,
            manifestation_id=manifestation_id,
            source_path=str(file_path),
            source_name=file_path.name,
        )
        file_id = _insert_file_row_for_item(db, store_id=store_id, item_id=item_id, file_path=file_path)

        work_row = db.get_row_from_id("works", work_id)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("agents", agent_id), priority=10)
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("labels", label_id))
        db.interlink_rows(primary_row=work_row, secondary_row=db.get_row_from_id("expressions", expression_id))
        db.interlink_rows(
            primary_row=db.get_row_from_id("expressions", expression_id),
            secondary_row=db.get_row_from_id("manifestations", manifestation_id),
        )

        app = CalibreReadOnlyWebApplication(db)

        status, headers, body = _call_app(app, "/ajax-setup")
        assert status == "200 OK"
        assert headers["Content-Type"].startswith("application/json")
        payload_json = json.loads(body.decode("utf-8"))
        assert payload_json["library_id"] == "main"
        assert payload_json["opds_url"] == "/opds"

        status, headers, body = _call_app(app, "/ajax/categories/main")
        assert status == "200 OK"
        categories_json = json.loads(body.decode("utf-8"))
        assert isinstance(categories_json, list)
        assert any(one["name"] == "Authors" for one in categories_json)
        assert any(one["is_category"] is False for one in categories_json)
        authors_entry = next(one for one in categories_json if one["name"] == "Authors")
        assert authors_entry["icon"] == "/icon/user_profile.png"
        assert authors_entry["encoded_name"] == _enc("authors")
        assert authors_entry["url"] == "/ajax/category/{}/main".format(_enc("authors"))

        status, headers, body = _call_app(app, "/ajax/category/{}/main".format(_enc("authors")))
        assert status == "200 OK"
        category_json = json.loads(body.decode("utf-8"))
        assert category_json["category_name"] == "Authors"
        assert category_json["base_url"] == "/ajax/category/{}/main".format(_enc("authors"))
        assert category_json["sort"] == "name"
        assert category_json["sort_order"] == "asc"
        assert any(one["name"] == "Ajax Author" for one in category_json["items"])
        assert category_json["icon"] == "/icon/user_profile.png"
        assert category_json["items"][0]["url"] == "/ajax/books_in/{}/{}/main".format(_enc("authors"), _enc(str(agent_id)))

        status, headers, body = _call_app(app, "/ajax/books_in/{}/{}/main".format(_enc("authors"), _enc(str(agent_id))))
        assert status == "200 OK"
        books_in_json = json.loads(body.decode("utf-8"))
        assert books_in_json["base_url"] == "/ajax/books_in/{}/{}/main".format(_enc("authors"), _enc(str(agent_id)))
        assert books_in_json["sort"] == "title"
        assert books_in_json["book_ids"] == [work_id]

        status, headers, body = _call_app(app, "/ajax/category/{}/main".format(_enc("allbooks")))
        assert status == "200 OK"
        allbooks_json = json.loads(body.decode("utf-8"))
        assert allbooks_json["base_url"] == "/ajax/books_in/{}/{}/main".format(_enc("allbooks"), _enc("0"))
        assert allbooks_json["book_ids"] == [work_id]

        status, headers, body = _call_app(app, "/ajax/book/{}/main".format(work_id))
        assert status == "200 OK"
        book_json = json.loads(body.decode("utf-8"))
        assert book_json["title"] == "Ajax Book"
        assert book_json["authors"] == ["Ajax Author"]
        assert book_json["formats"] == ["EPUB"]
        assert book_json["format_metadata"]["EPUB"]["path"] == "/files/{}/download".format(file_id)
        assert book_json["uuid"] == "work-{}".format(work_id)
        assert book_json["category_urls"]["authors"] == ["/ajax/books_in/{}/{}/main".format(_enc("authors"), _enc(str(agent_id)))]
        assert book_json["category_urls"]["tags"] == ["/ajax/books_in/{}/{}/main".format(_enc("tags"), _enc(str(label_id)))]

        status, headers, body = _call_app(app, "/ajax/search/main?query=Ajax")
        assert status == "200 OK"
        search_json = json.loads(body.decode("utf-8"))
        assert search_json["base_url"] == "/ajax/search/main"
        assert search_json["query"] == "Ajax"
        assert search_json["book_ids"] == [work_id]

        status, headers, body = _call_app(app, "/interface-data/init")
        assert status == "200 OK"
        init_json = json.loads(body.decode("utf-8"))
        assert init_json["default_library_id"] == "main"
        assert init_json["search_result"]["book_ids"] == [work_id]
        assert init_json["metadata"][str(work_id)]["title"] == "Ajax Book"
        assert init_json["library_id"] == "main"

        status, headers, body = _call_app(app, "/interface-data/book-metadata/{}".format(work_id))
        assert status == "200 OK"
        metadata_json = json.loads(body.decode("utf-8"))
        assert metadata_json["title"] == "Ajax Book"

        status, headers, body = _call_app(app, "/interface-data/tag-browser")
        assert status == "200 OK"
        tag_browser_json = json.loads(body.decode("utf-8"))
        assert tag_browser_json["root"]["id"] is None
        assert len(tag_browser_json["root"]["children"]) >= 3
        item_map = tag_browser_json["item_map"]
        top_level_names = {item_map[node["id"]]["name"] for node in tag_browser_json["root"]["children"]}
        assert {"Authors", "Tags", "Series"}.issubset(top_level_names)
        author_root = next(node for node in tag_browser_json["root"]["children"] if item_map[node["id"]]["category"] == "authors")
        assert item_map[author_root["id"]]["icon"] == "/icon/user_profile.png"
        assert len(author_root["children"]) >= 1
        author_item = next(item_map[node["id"]] for node in author_root["children"] if item_map[node["id"]]["name"] == "Ajax Author")
        assert author_item["name"] == "Ajax Author"
        assert author_item["url"] == "/ajax/books_in/{}/{}/main".format(_enc("authors"), _enc(str(agent_id)))
        assert author_item["item_url"] == "/author/agents/{}".format(agent_id)

        status, headers, body = _call_app(app, "/interface-data/get-books?ids={}".format(work_id))
        assert status == "200 OK"
        books_json = json.loads(body.decode("utf-8"))
        assert books_json["search_result"]["book_ids"] == [work_id]
        assert books_json["metadata"][str(work_id)]["title"] == "Ajax Book"
