from __future__ import annotations

import json

from LiuXin_alpha.ingest import (
    register_native_html_readonly_store_files,
    register_native_html_readonly_with_database_path,
)
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    native_html_storage_backend as backend_module,
)


def _html_result(url: str, body: str) -> object:
    return backend_module._FetchResult(
        requested_url=url,
        final_url=url,
        status=200,
        content_type="text/html; charset=utf-8",
        body=body.encode("utf-8"),
        charset="utf-8",
    )


def test_register_native_html_store_files_inserts_rows_and_tracks_policy(db, monkeypatch) -> None:
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
        return responses[url]

    monkeypatch.setattr(backend_module.NativeHtmlReadOnlyStorageBackend, "_fetch_url", _fake_fetch)

    report = register_native_html_readonly_store_files(
        db,
        remote_url="https://example.com/library/",
        store_name="native_web_mirror",
        max_http_requests_per_hour=60.0,
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
    assert int(store_row["store_supports_checksums"] or 0) == 0


def test_register_native_html_with_database_path_helper(provision_test_database, driver_spec, monkeypatch) -> None:
    provisioned = provision_test_database("test_db_13")
    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            "<html><body><a href=\"files/one.epub\">One</a></body></html>",
        ),
    }

    def _fake_fetch(self, url: str):
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
