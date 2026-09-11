"""
Check native discovery registration against real temporary catalogue rows.

Page content is injected and no ebook bytes are downloaded. The separate robots
lookup is not patched here and retains the crawler's ordinary failure policy;
passing these tests does not establish successful live crawling or robots access.
"""

from __future__ import annotations

import json

import pytest

from LiuXin_alpha.ingest import (
    register_native_html_readonly_store_files,
    register_native_html_readonly_with_database_path,
)
from LiuXin_alpha.ingest.remote_html import ensure_native_html_readonly_store
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    native_html_storage_backend as backend_module,
)
from tests.support._surface_storage_tables import ensure_surface_asset_tables


def _html_result(url: str, body: str) -> object:
    """
    Build an untruncated successful UTF-8 HTML fetch record without performing HTTP.

    Example:
        >>> _html_result('https://example.test/', '<p>page</p>').status
        200


    :param url: Address used as both requested and final URL.
    :param body: HTML text encoded as UTF-8 bytes.
    :return: Injected fetch record with status 200 and an HTML content type.
    """
    return backend_module._FetchResult(
        requested_url=url,
        final_url=url,
        status=200,
        content_type="text/html; charset=utf-8",
        body=body.encode("utf-8"),
        charset="utf-8",
    )


def test_register_native_html_store_files_inserts_rows_and_tracks_policy(db, monkeypatch) -> None:
    """
    Register a three-page injected HTML graph and inspect file keys, counters, and persisted crawler
    policy.

    HTML is included in the default ebook extension set. Page fetching is mocked; the separate
    robots lookup retains its normal failure policy.

    Example:
        >>> test_register_native_html_store_files_inserts_rows_and_tracks_policy(db, monkeypatch)  # doctest: +SKIP


    :param db: Provisioned test catalogue receiving real Store/file/link writes.
    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            """
            <html><body>
              <a href=\"files/one.epub\">One</a>
              <a href=\"catalog/next\">Next</a>
              <a href=\"guide.html\">Guide</a>
            </body></html>
            """,
        ),
        "https://example.com/library/catalog/next": _html_result(
            "https://example.com/library/catalog/next",
            "<html><body><a href=\"../files/two.mobi\">Two</a></body></html>",
        ),
        "https://example.com/library/guide.html": _html_result(
            "https://example.com/library/guide.html",
            "<html><body></body></html>",
        ),
    }

    def _fake_fetch(self, url: str):
        """
        Return the response assigned to this exact URL in the enclosing fixture mapping.

        Example:
            >>> fetched = _fake_fetch(store, store.url)  # doctest: +SKIP


        :param self: Injected backend instance, unused by the mapping lookup.
        :param url: Exact response-map key; absent entries raise KeyError.
        :return: Existing fetch record retained in the enclosing response mapping.
        """
        return responses[url]

    monkeypatch.setattr(backend_module.NativeHtmlReadOnlyStorageBackend, "_fetch_url", _fake_fetch)

    report = register_native_html_readonly_store_files(
        db,
        remote_url="https://example.com/library/",
        store_name="native_web_mirror",
        max_http_requests_per_hour=60.0,
        max_pages=7,
        max_observed_urls=13,
        refresh_storage_manager=False,
    )

    assert report.errors == []
    assert report.scanned_files == 3
    assert report.ebook_candidates == 3
    assert report.inserted_files == 3
    assert report.crawler_urls_observed == 4
    assert report.crawler_html_seen == 1
    assert report.crawler_book_like_found == 3

    file_rows = db.search("files", "file_store_id", report.store_row_id)
    assert len(file_rows) == 3
    keys = {str(row["file_storage_key"]) for row in file_rows}
    assert "files/one.epub" in keys
    assert "files/two.mobi" in keys
    assert "guide.html" in keys

    store_row = db.get_row_from_id("stores", report.store_row_id)
    assert store_row is not None
    policy_raw = store_row["store_policy_json"]
    assert policy_raw
    policy = json.loads(str(policy_raw))
    assert policy["backend"] == "native_html_readonly"
    assert policy["native_html"]["max_http_requests_per_hour"] == 60.0
    assert policy["native_html"]["max_pages"] == 7
    assert policy["native_html"]["max_observed_urls"] == 13
    assert int(store_row["store_supports_checksums"] or 0) == 0


def test_register_native_html_with_database_path_helper(provision_test_database, driver_spec, monkeypatch) -> None:
    """
    Seed a temporary catalogue, reopen it through the path helper, and assert one injected URL is
    registered.

    This exercises real database context ownership with mocked page content; robots lookup is
    separate.

    Example:
        >>> test_register_native_html_with_database_path_helper(provision_test_database, driver_spec, monkeypatch)  # doctest: +SKIP


    :param provision_test_database: Fixture callable cloning the named test catalogue into temporary storage.
    :param driver_spec: Fixture selecting the database adapter for the provisioned catalogue.
    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    from LiuXin_alpha.databases.database import Database

    provisioned = provision_test_database("test_db_13")
    with Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    ) as seeded:
        ensure_surface_asset_tables(seeded)
    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            "<html><body><a href=\"files/one.epub\">One</a></body></html>",
        ),
    }

    def _fake_fetch(self, url: str):
        """
        Return the response assigned to this exact URL in the enclosing fixture mapping.

        Example:
            >>> fetched = _fake_fetch(store, store.url)  # doctest: +SKIP


        :param self: Injected backend instance, unused by the mapping lookup.
        :param url: Exact response-map key; absent entries raise KeyError.
        :return: Existing fetch record retained in the enclosing response mapping.
        """
        return responses[url]

    monkeypatch.setattr(backend_module.NativeHtmlReadOnlyStorageBackend, "_fetch_url", _fake_fetch)

    report = register_native_html_readonly_with_database_path(
        database_path=provisioned.db_path,
        remote_url="https://example.com/library/",
        db_type=driver_spec.db_type,
        refresh_storage_manager=False,
    )
    assert report.inserted_files == 1
    assert report.errors == []


@pytest.mark.parametrize(
    "invalid_root",
    [
        "https://example.test/library/bad-%GG/",
        "https://example.test/library/bad%00path/",
        "https://example.test/library/\ud800/",
        "https://user:secret@example.test/library/",
    ],
)
def test_native_html_invalid_roots_create_no_database_rows(
    db,
    invalid_root: str,
) -> None:
    """
    Check rejected root inputs leave the total Store-row count unchanged.

    The assertion counts Store rows, not every possible database or filesystem effect.

    Example:
        >>> test_native_html_invalid_roots_create_no_database_rows(db, invalid_root)  # doctest: +SKIP


    :param db: Provisioned test catalogue receiving real Store/file/link writes.
    :param invalid_root: Parametrized malformed, control-bearing, surrogate, or credential-bearing root.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    before = len(db.get_all_rows("stores", iterator_return=False) or ())

    with pytest.raises(ValueError, match="valid safe HTTP"):
        ensure_native_html_readonly_store(db, invalid_root)

    after = len(db.get_all_rows("stores", iterator_return=False) or ())
    assert after == before


def test_native_html_db_ingest_canonicalizes_unicode_and_drops_bad_link_bytes(
    db,
    monkeypatch,
) -> None:
    """
    Persist the normalized IDNA root and one valid Unicode key while dropping malformed injected
    links.

    HTML fetches are injected; this is catalogue registration without downloading ebook bytes.

    Example:
        >>> test_native_html_db_ingest_canonicalizes_unicode_and_drops_bad_link_bytes(db, monkeypatch)  # doctest: +SKIP


    :param db: Provisioned test catalogue receiving real Store/file/link writes.
    :param monkeypatch: Pytest fixture restoring injected fetch/process/preference seams after the test.
    :return: None after the stated regression assertions pass.
    """
    ensure_surface_asset_tables(db)
    normalized_root = (
        "https://xn--bcher-kva.example/%E6%96%87%E5%BA%93/"
    )
    body = (
        b'<a href="valid-\xe4\xb9\xa6.epub">valid</a>'
        b'<a href="bad-\xff.epub">bad</a>'
        b'<a href="bad-%GG.epub">bad percent</a>'
    )

    monkeypatch.setattr(
        backend_module.NativeHtmlReadOnlyStorageBackend,
        "_fetch_url",
        lambda self, url: backend_module._FetchResult(
            requested_url=url,
            final_url=url,
            status=200,
            content_type="text/html; charset=utf-8",
            body=body,
            charset="utf-8",
        ),
    )

    report = register_native_html_readonly_store_files(
        db,
        remote_url="HTTPS://Bücher.example/文库/",
        refresh_storage_manager=False,
    )

    assert report.errors == []
    assert report.inserted_files == 1
    assert report.store_root_uri == normalized_root
    [row] = db.search("files", "file_store_id", report.store_row_id)
    assert row["file_storage_key"] == "valid-%E4%B9%A6.epub"
    store_row = db.get_row_from_id("stores", report.store_row_id)
    assert store_row is not None
    assert store_row["store_root_uri"] == normalized_root
