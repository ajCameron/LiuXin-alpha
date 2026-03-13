from __future__ import annotations

from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
    NativeHtmlBackendOptions,
    NativeHtmlReadOnlyStorageBackend,
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
