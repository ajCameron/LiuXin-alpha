from __future__ import annotations

import io

import pytest

from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage.api import EnumerationCompleteness, StoreReadOnly
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    WgetBackendOptions,
    WgetHtmlReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    wget_html_storage_backend as backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly.wget_utils import WgetResult
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_FILENAME,
    UNICODE_PAYLOAD,
    UNICODE_URL_KEY,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def _ok_wget_result(*, args: list[str], stdout: str = "", stderr: str = "") -> WgetResult:
    return WgetResult(args=list(args), returncode=0, stdout=stdout, stderr=stderr)


def test_wget_backend_preserves_unicode_url_names_and_bytes(
    monkeypatch,
    tmp_path,
) -> None:
    root = "https://example.com/books/"
    object_url = root + UNICODE_URL_KEY

    monkeypatch.setattr(
        backend_module,
        "run_wget",
        lambda args, **kwargs: _ok_wget_result(
            args=list(args),
            stdout=object_url,
        ),
    )

    class _Response(io.BytesIO):
        status = 200

        def __init__(self, url: str, payload: bytes) -> None:
            super().__init__(payload)
            self.headers = {
                "Content-Length": str(len(UNICODE_PAYLOAD)),
                "Content-Type": "application/epub+zip",
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
        backend_module.WgetHtmlReadOnlyStorageBackend,
        "_open_http_request",
        staticmethod(_open_http),
    )
    store = WgetHtmlReadOnlyStorageBackend(
        root,
        options=WgetBackendOptions(max_http_requests_per_hour=0),
    )

    [location] = list(store.iter_locations())
    info = store.stat_file(location)

    assert location.key == UNICODE_URL_KEY
    assert store.location_uri(location) == object_url
    assert info.hints.suggested_filename == UNICODE_FILENAME
    assert info.hints.media_type == "application/epub+zip"
    assert store.read_file(info) == UNICODE_PAYLOAD

    destination = FilesystemStore(tmp_path / "wget-html-ingest-destination")
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


def test_wget_backend_applies_generic_unicode_torture_contract(monkeypatch) -> None:
    root = "https://example.com/books/"
    payloads = {
        root + case.url_key: case.payload
        for case in TORTURED_UNICODE_PATH_CASES
    }
    monkeypatch.setattr(
        backend_module,
        "run_wget",
        lambda args, **kwargs: _ok_wget_result(
            args=list(args),
            stdout="\n".join(payloads),
        ),
    )

    class _Response(io.BytesIO):
        def __init__(
            self,
            url: str,
            payload: bytes,
            *,
            status: int,
            total: int,
            range_start: int = 0,
        ) -> None:
            super().__init__(payload)
            self.status = status
            self.headers = {
                "Content-Length": str(len(payload) if status == 206 else total),
                "Content-Type": "application/epub+zip",
            }
            if status == 206:
                self.headers["Content-Range"] = (
                    f"bytes {range_start}-{range_start + len(payload) - 1}/{total}"
                )
            self._url = url

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
            return _Response(
                request.full_url,
                complete[start : end + 1],
                status=206,
                total=len(complete),
                range_start=start,
            )
        return _Response(request.full_url, complete, status=200, total=len(complete))

    monkeypatch.setattr(
        backend_module.WgetHtmlReadOnlyStorageBackend,
        "_open_http_request",
        staticmethod(_open_http),
    )
    store = WgetHtmlReadOnlyStorageBackend(
        root,
        options=WgetBackendOptions(max_http_requests_per_hour=0),
    )

    exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        key_for_case=lambda case: case.url_key,
        check_uri_round_trip=True,
    )


def test_wget_backend_default_rate_limit_is_20_per_minute(monkeypatch) -> None:
    captured_args: list[list[str]] = []

    def _fake_run_wget(args, **kwargs):
        captured_args.append(list(args))
        return _ok_wget_result(args=list(args), stdout="https://example.com/books/one.epub")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    store = WgetHtmlReadOnlyStorageBackend(url="https://example.com/")
    urls = store.crawl_urls(force=True)

    assert urls == ["https://example.com/books/one.epub"]
    assert captured_args
    assert "--wait=3.000" in captured_args[0]


def test_wget_backend_default_rate_limit_reads_preferences(monkeypatch) -> None:
    captured_args: list[list[str]] = []

    def _fake_run_wget(args, **kwargs):
        captured_args.append(list(args))
        return _ok_wget_result(args=list(args), stdout="https://example.com/books/one.epub")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    monkeypatch.setattr(backend_module, "get_default_crawler_http_requests_per_hour", lambda: 300.0)

    store = WgetHtmlReadOnlyStorageBackend(url="https://example.com/")
    store.crawl_urls(force=True)

    assert captured_args
    assert "--wait=12.000" in captured_args[0]


def test_get_default_wget_http_requests_per_hour_falls_back_on_invalid_crawler_value(monkeypatch) -> None:
    import LiuXin_alpha.preferences as preferences_module

    original_get = preferences_module.preferences.get

    def _fake_get(option: str, default=None):
        if option == backend_module.WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY:
            return "not-a-number"
        return original_get(option, default)

    monkeypatch.setattr(preferences_module.preferences, "get", _fake_get)
    value = backend_module.get_default_wget_http_requests_per_hour()
    assert value == backend_module.WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT


def test_get_default_wget_http_requests_per_hour_falls_back_to_legacy_wget_key(monkeypatch) -> None:
    import LiuXin_alpha.preferences as preferences_module

    original_get = preferences_module.preferences.get

    def _fake_get(option: str, default=None):
        if option == backend_module.WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY:
            return default
        if option == "wget_http_max_requests_per_hour_default":
            return "300"
        return original_get(option, default)

    monkeypatch.setattr(preferences_module.preferences, "get", _fake_get)
    value = backend_module.get_default_wget_http_requests_per_hour()
    assert value == 300.0


def test_wget_backend_can_disable_rate_limit_wait(monkeypatch) -> None:
    captured_args: list[list[str]] = []

    def _fake_run_wget(args, **kwargs):
        captured_args.append(list(args))
        return _ok_wget_result(args=list(args), stdout="https://example.com/books/one.epub")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    store = WgetHtmlReadOnlyStorageBackend(
        url="https://example.com/",
        options=WgetBackendOptions(max_http_requests_per_hour=0.0),
    )
    store.crawl_urls(force=True)

    assert captured_args
    assert all(not arg.startswith("--wait=") for arg in captured_args[0])


def test_wget_backend_crawl_filters_scope_and_non_file_urls(monkeypatch) -> None:
    discovered = "\n".join(
        [
            "https://example.com/books/",
            "https://example.com/books/one.epub",
            "https://example.com/books/two.mobi",
            "https://example.com/books/index",
            "https://example.com/books/cover.jpg",
            "https://other.example.com/books/three.epub",
        ]
    )

    def _fake_run_wget(args, **kwargs):
        return _ok_wget_result(args=list(args), stdout=discovered)

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    store = WgetHtmlReadOnlyStorageBackend(
        url="https://example.com/books/",
        options=WgetBackendOptions(max_http_requests_per_hour=None),
    )

    urls = store.crawl_urls(force=True)
    assert urls == [
        "https://example.com/books/one.epub",
        "https://example.com/books/two.mobi",
        "https://example.com/books/cover.jpg",
    ]


def test_wget_backend_crawl_reports_observed_url_decisions(monkeypatch) -> None:
    observed: list[dict[str, object]] = []
    discovered = "\n".join(
        [
            "https://example.com/books/index",
            "https://example.com/books/one.epub",
            "https://example.com/books/guide.html",
            "https://other.example.com/books/author.html",
            "https://example.com/books/one.epub",
        ]
    )

    def _fake_run_wget(args, **kwargs):
        return _ok_wget_result(args=list(args), stdout=discovered)

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    store = WgetHtmlReadOnlyStorageBackend(
        url="https://example.com/books/",
        options=WgetBackendOptions(max_http_requests_per_hour=None),
    )

    urls = store.crawl_urls(force=True, observed_url_callback=observed.append)
    assert urls == [
        "https://example.com/books/one.epub",
        "https://example.com/books/guide.html",
    ]
    assert [str(item.get("url")) for item in observed] == [
        "https://example.com/books/index",
        "https://example.com/books/one.epub",
        "https://example.com/books/guide.html",
        "https://other.example.com/books/author.html",
    ]
    assert [str(item.get("reason")) for item in observed] == [
        "not_file_like",
        "accepted",
        "accepted",
        "out_of_scope",
    ]
    assert [bool(item.get("accepted")) for item in observed] == [
        False,
        True,
        True,
        False,
    ]


def test_wget_backend_startup_checks_binary(monkeypatch) -> None:
    captured_args: list[list[str]] = []

    def _fake_run_wget(args, **kwargs):
        captured_args.append(list(args))
        return _ok_wget_result(args=list(args), stdout="GNU Wget 1.21")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    store = WgetHtmlReadOnlyStorageBackend(url="https://example.com/")
    store.startup()

    assert captured_args
    assert captured_args[0] == ["--version"]


def test_wget_backend_crawl_forwards_log_lines(monkeypatch) -> None:
    seen_lines: list[str] = []

    def _fake_run_wget(args, **kwargs):
        callback = kwargs.get("line_callback")
        if callable(callback):
            callback("spider: queued https://example.com/books/one.epub")
        return _ok_wget_result(args=list(args), stdout="https://example.com/books/one.epub")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    store = WgetHtmlReadOnlyStorageBackend(url="https://example.com/")
    urls = store.crawl_urls(force=True, log_line_callback=seen_lines.append)

    assert urls == ["https://example.com/books/one.epub"]
    assert seen_lines == ["spider: queued https://example.com/books/one.epub"]


def test_wget_backend_crawl_forwards_discovered_urls_incrementally(monkeypatch) -> None:
    seen_urls: list[str] = []

    def _fake_run_wget(args, **kwargs):
        callback = kwargs.get("line_callback")
        if callable(callback):
            callback("queued https://example.com/books/one.epub")
            callback("queued https://example.com/books/two.mobi")
            callback("queued https://example.com/books/two.mobi")
        return _ok_wget_result(args=list(args), stdout="")

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)
    store = WgetHtmlReadOnlyStorageBackend(url="https://example.com/")
    urls = store.crawl_urls(force=True, discovered_url_callback=seen_urls.append)

    assert urls == [
        "https://example.com/books/one.epub",
        "https://example.com/books/two.mobi",
    ]
    assert seen_urls == [
        "https://example.com/books/one.epub",
        "https://example.com/books/two.mobi",
    ]



def test_wget_backend_iter_locations_and_stat_follow_new_plugin_api(monkeypatch) -> None:
    discovered = "\n".join(
        [
            "https://example.com/books/one.epub",
            "https://example.com/books/two.mobi",
        ]
    )

    def _fake_run_wget(args, **kwargs):
        return _ok_wget_result(args=list(args), stdout=discovered)

    monkeypatch.setattr(backend_module, "run_wget", _fake_run_wget)

    payloads = {
        "https://example.com/books/one.epub": b"epub-one",
        "https://example.com/books/two.mobi": b"mobi-two",
    }

    class _Response(io.BytesIO):
        def __init__(self, url: str, payload: bytes, *, status: int) -> None:
            super().__init__(payload)
            self.status = status
            self.headers = {
                "Content-Length": str(
                    len(payload) if status == 206 else len(payloads[url])
                )
            }
            if status == 206:
                self.headers["Content-Range"] = (
                    f"bytes 2-5/{len(payloads[url])}"
                )
            self._url = url

        def geturl(self) -> str:
            return self._url

    def _open_http(request, timeout_s):
        del timeout_s
        payload = payloads[request.full_url]
        if request.method == "HEAD":
            return _Response(request.full_url, b"", status=200)
        if request.get_header("Range") == "bytes=2-5":
            return _Response(request.full_url, payload[2:6], status=206)
        return _Response(request.full_url, payload, status=200)

    monkeypatch.setattr(
        backend_module.WgetHtmlReadOnlyStorageBackend,
        "_open_http_request",
        staticmethod(_open_http),
    )
    store = WgetHtmlReadOnlyStorageBackend(
        url="https://example.com/books/",
        options=WgetBackendOptions(max_http_requests_per_hour=None),
    )

    locations = list(store.iter_locations())

    assert [loc.key for loc in locations] == ["one.epub", "two.mobi"]
    assert all(loc.store_ref == store.store_ref for loc in locations)
    assert store.capabilities.enumeration is EnumerationCompleteness.PARTIAL
    assert store.stat_file(locations[0]).size == 8
    assert store.read_file(locations[0]) == b"epub-one"
    assert store.read_file(locations[0], offset=2, length=4) == b"ub-o"
    with pytest.raises(StoreReadOnly):
        store.delete_file(locations[0])
