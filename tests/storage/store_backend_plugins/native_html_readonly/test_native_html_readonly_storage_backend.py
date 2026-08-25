from __future__ import annotations

import io

import pytest

from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage.api import EnumerationCompleteness, StoreReadOnly
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    NativeHtmlBackendOptions,
    NativeHtmlReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    native_html_storage_backend as backend_module,
)
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_FILENAME,
    UNICODE_PAYLOAD,
    UNICODE_URL_KEY,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def _html_result(url: str, body: str) -> object:
    return backend_module._FetchResult(
        requested_url=url,
        final_url=url,
        status=200,
        content_type="text/html; charset=utf-8",
        body=body.encode("utf-8"),
        charset="utf-8",
    )


def test_native_backend_preserves_unicode_url_names_and_bytes(
    monkeypatch,
    tmp_path,
) -> None:
    root = "https://example.com/library/"
    object_url = root + UNICODE_URL_KEY

    monkeypatch.setattr(
        backend_module.NativeHtmlReadOnlyStorageBackend,
        "_fetch_url",
        lambda self, url: _html_result(
            url,
            f'<html><body><a href="{UNICODE_URL_KEY}">book</a></body></html>',
        ),
    )

    class _Response(io.BytesIO):
        status = 200

        def __init__(self, url: str, payload: bytes) -> None:
            super().__init__(payload)
            self.headers = {
                "Content-Length": str(len(UNICODE_PAYLOAD)),
                "Content-Type": "application/epub+zip",
                "ETag": '"unicode-v1"',
            }
            self._url = url

        def geturl(self) -> str:
            return self._url

    def _open_http(request, timeout_s):
        del timeout_s
        return _Response(
            request.full_url,
            b"" if request.method == "HEAD" else UNICODE_PAYLOAD,
        )

    monkeypatch.setattr(
        backend_module.NativeHtmlReadOnlyStorageBackend,
        "_open_http_request",
        staticmethod(_open_http),
    )
    store = NativeHtmlReadOnlyStorageBackend(
        root,
        options=NativeHtmlBackendOptions(
            max_http_requests_per_hour=0,
            respect_robots=False,
        ),
    )

    [location] = list(store.iter_locations())
    info = store.stat_file(location)

    assert location.key == UNICODE_URL_KEY
    assert store.location_uri(location) == object_url
    assert info.hints.suggested_filename == UNICODE_FILENAME
    assert info.hints.media_type == "application/epub+zip"
    assert store.read_file(info) == UNICODE_PAYLOAD

    destination = FilesystemStore(tmp_path / "native-html-ingest-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )
    report = ingest_store(manager, store)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert item.source_info.location.key == UNICODE_URL_KEY
    assert item.result.asset_record.metadata.original_name == UNICODE_FILENAME
    assert manager.read_file(item.result.asset_record) == UNICODE_PAYLOAD


def test_native_backend_applies_generic_unicode_torture_contract(monkeypatch) -> None:
    root = "https://example.com/library/"
    payloads = {
        root + case.url_key: case.payload
        for case in TORTURED_UNICODE_PATH_CASES
    }
    links = "".join(
        f'<a href="{case.url_key}">{case.case_id}</a>'
        for case in TORTURED_UNICODE_PATH_CASES
    )
    monkeypatch.setattr(
        backend_module.NativeHtmlReadOnlyStorageBackend,
        "_fetch_url",
        lambda self, url: _html_result(url, f"<html><body>{links}</body></html>"),
    )

    class _Response(io.BytesIO):
        def __init__(self, url: str, payload: bytes, *, status: int, total: int) -> None:
            super().__init__(payload)
            self.status = status
            self.headers = {
                "Content-Length": str(len(payload) if status == 206 else total),
                "Content-Type": "application/epub+zip",
                "ETag": '"unicode-v1"',
            }
            if status == 206:
                start = int(self._range_start)
                self.headers["Content-Range"] = (
                    f"bytes {start}-{start + len(payload) - 1}/{total}"
                )
            self._url = url

        _range_start = 0

        def geturl(self) -> str:
            return self._url

    def _open_http(request, timeout_s):
        del timeout_s
        complete = payloads[request.full_url]
        if request.method == "HEAD":
            return _Response(request.full_url, b"", status=200, total=len(complete))
        byte_range = request.get_header("Range")
        if byte_range:
            interval = byte_range.removeprefix("bytes=")
            start_text, end_text = interval.split("-", 1)
            start = int(start_text)
            end = len(complete) - 1 if not end_text else int(end_text)
            _Response._range_start = start
            return _Response(
                request.full_url,
                complete[start : end + 1],
                status=206,
                total=len(complete),
            )
        return _Response(request.full_url, complete, status=200, total=len(complete))

    monkeypatch.setattr(
        backend_module.NativeHtmlReadOnlyStorageBackend,
        "_open_http_request",
        staticmethod(_open_http),
    )
    store = NativeHtmlReadOnlyStorageBackend(
        root,
        options=NativeHtmlBackendOptions(
            max_http_requests_per_hour=0,
            respect_robots=False,
        ),
    )

    exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        key_for_case=lambda case: case.url_key,
        check_uri_round_trip=True,
    )


def test_native_backend_crawl_descends_through_non_file_like_pages(monkeypatch) -> None:
    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            """
            <html><body>
              <a href=\"files/one.epub\">One</a>
              <a href=\"catalog/next\">Next</a>
              <a href=\"guide.html\">Guide</a>
              <a href=\"https://other.example.com/out.epub\">Other</a>
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

    store = NativeHtmlReadOnlyStorageBackend(
        url="https://example.com/library/",
        options=NativeHtmlBackendOptions(max_http_requests_per_hour=None, respect_robots=False),
    )
    urls = store.crawl_urls(force=True)

    assert urls == [
        "https://example.com/library/files/one.epub",
        "https://example.com/library/guide.html",
        "https://example.com/library/files/two.mobi",
    ]


def test_native_backend_reports_observed_url_decisions(monkeypatch) -> None:
    observed: list[dict[str, object]] = []
    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            """
            <html><body>
              <a href=\"catalog/next\">Next</a>
              <a href=\"guide.html\">Guide</a>
              <a href=\"https://other.example.com/out.epub\">Other</a>
            </body></html>
            """,
        ),
        "https://example.com/library/catalog/next": _html_result(
            "https://example.com/library/catalog/next",
            "<html><body></body></html>",
        ),
        "https://example.com/library/guide.html": _html_result(
            "https://example.com/library/guide.html",
            "<html><body></body></html>",
        ),
    }

    def _fake_fetch(self, url: str):
        return responses[url]

    monkeypatch.setattr(backend_module.NativeHtmlReadOnlyStorageBackend, "_fetch_url", _fake_fetch)

    store = NativeHtmlReadOnlyStorageBackend(
        url="https://example.com/library/",
        options=NativeHtmlBackendOptions(max_http_requests_per_hour=None, respect_robots=False),
    )
    urls = store.crawl_urls(force=True, observed_url_callback=observed.append)

    assert urls == ["https://example.com/library/guide.html"]
    assert [str(item.get("reason")) for item in observed] == [
        "not_file_like",
        "accepted",
        "out_of_scope",
    ]



def test_native_backend_iter_locations_and_stat_follow_new_plugin_api(monkeypatch) -> None:
    responses = {
        "https://example.com/library/": _html_result(
            "https://example.com/library/",
            """
            <html><body>
              <a href="files/one.epub">One</a>
              <a href="files/two.mobi">Two</a>
            </body></html>
            """,
        ),
    }

    def _fake_fetch(self, url: str):
        return responses[url]

    monkeypatch.setattr(backend_module.NativeHtmlReadOnlyStorageBackend, "_fetch_url", _fake_fetch)
    payloads = {
        "https://example.com/library/files/one.epub": b"one-book",
        "https://example.com/library/files/two.mobi": b"two-book",
    }

    class _Response(io.BytesIO):
        def __init__(self, url: str, payload: bytes, *, status: int) -> None:
            super().__init__(payload)
            self.status = status
            self.headers = {
                "Content-Length": str(
                    len(payload) if status == 206 else len(payloads[url])
                ),
                "Content-Type": "application/octet-stream",
                "ETag": '"version-1"',
            }
            if status == 206:
                self.headers["Content-Range"] = (
                    f"bytes 1-3/{len(payloads[url])}"
                )
            self._url = url

        def geturl(self) -> str:
            return self._url

    def _open_http(request, timeout_s):
        del timeout_s
        payload = payloads[request.full_url]
        byte_range = request.get_header("Range")
        if request.method == "HEAD":
            return _Response(request.full_url, b"", status=200)
        if byte_range == "bytes=1-3":
            return _Response(request.full_url, payload[1:4], status=206)
        return _Response(request.full_url, payload, status=200)

    monkeypatch.setattr(
        backend_module.NativeHtmlReadOnlyStorageBackend,
        "_open_http_request",
        staticmethod(_open_http),
    )

    store = NativeHtmlReadOnlyStorageBackend(
        url="https://example.com/library/",
        options=NativeHtmlBackendOptions(max_http_requests_per_hour=None, respect_robots=False),
    )
    store.crawl_urls(force=True)

    locations = list(store.iter_locations())

    assert [loc.key for loc in locations] == [
        "files/one.epub",
        "files/two.mobi",
    ]
    assert all(loc.store_ref == store.store_ref for loc in locations)
    assert store.capabilities.enumeration is EnumerationCompleteness.PARTIAL
    assert store.file_exists(locations[0]) is True
    assert store.stat_file(locations[0]).size == len(payloads[
        "https://example.com/library/files/one.epub"
    ])
    assert store.read_file(locations[0]) == b"one-book"
    assert store.read_file(locations[0], offset=1, length=3) == b"ne-"
    with pytest.raises(StoreReadOnly):
        store.store_bytes(b"replacement", location=locations[0])
