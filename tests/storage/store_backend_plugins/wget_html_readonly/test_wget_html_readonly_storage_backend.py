from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    WgetBackendOptions,
    WgetHtmlReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
    wget_html_storage_backend as backend_module,
)
from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly.wget_utils import WgetResult


def _ok_wget_result(*, args: list[str], stdout: str = "", stderr: str = "") -> WgetResult:
    return WgetResult(args=list(args), returncode=0, stdout=stdout, stderr=stderr)


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
    monkeypatch.setattr(backend_module, "get_default_wget_http_requests_per_hour", lambda: 300.0)

    store = WgetHtmlReadOnlyStorageBackend(url="https://example.com/")
    store.crawl_urls(force=True)

    assert captured_args
    assert "--wait=12.000" in captured_args[0]


def test_get_default_wget_http_requests_per_hour_falls_back_on_invalid_value(monkeypatch) -> None:
    import LiuXin_alpha.preferences as preferences_module

    original_get = preferences_module.preferences.get

    def _fake_get(option: str, default=None):
        if option == backend_module.WGET_HTTP_MAX_REQUESTS_PER_HOUR_PREF_KEY:
            return "not-a-number"
        return original_get(option, default)

    monkeypatch.setattr(preferences_module.preferences, "get", _fake_get)
    value = backend_module.get_default_wget_http_requests_per_hour()
    assert value == backend_module.WGET_HTTP_MAX_REQUESTS_PER_HOUR_DEFAULT


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

    assert captured_args == [["--version"]]


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
