from __future__ import annotations

import queue
from threading import Event

from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy


class _Log:
    def __init__(self) -> None:
        self.events = []

    def __call__(self, *parts):
        self.events.append(("call", parts))

    def info(self, *parts):
        self.events.append(("info", parts))

    def warning(self, *parts):
        self.events.append(("warning", parts))

    def error(self, *parts):
        self.events.append(("error", parts))

    def exception(self, *parts):
        self.events.append(("exception", parts))


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def read(self) -> bytes:
        return self.payload


def test_web_sources_big_book_search_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.big_book_search as bbs

    assert bbs is not None


def test_big_book_search_helper_edges() -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import (
        _as_text,
        _build_query,
        _normalize_image_url,
        _search_urls_for_query,
        parse_image_urls,
    )

    class BadString:
        def __str__(self):
            raise RuntimeError("broken")

    assert _as_text(b"caf\xc3\xa9") == "café"
    assert _as_text(BadString()) == ""
    assert _normalize_image_url("", "https://bigbooksearch.com/books/query") is None
    assert _normalize_image_url(BadString(), "https://bigbooksearch.com/books/query") is None
    assert _normalize_image_url("//img.example/a.jpg", "https://bigbooksearch.com/books/query") == (
        "https://img.example/a.jpg"
    )
    assert _normalize_image_url("/covers/a.jpg", "https://bigbooksearch.com/books/query") == (
        "https://bigbooksearch.com/covers/a.jpg"
    )
    assert _normalize_image_url("https://img.example/a.jpg", "https://bigbooksearch.com/books/query") == (
        "https://img.example/a.jpg"
    )
    assert _normalize_image_url("relative/a.jpg", "https://bigbooksearch.com/books/query") is None
    assert parse_image_urls("", base_url="https://bigbooksearch.com/books/query") == []
    assert parse_image_urls(b'<img src="relative/a.jpg"><img src="">', "https://bigbooksearch.com/books/query") == []
    assert _build_query([" The Hobbit ", "", BadString()]) == "+The+Hobbit+"
    search_urls = _search_urls_for_query("The+Hobbit")
    assert search_urls[0].endswith("/books/The+Hobbit")
    assert search_urls[1] == "https://www.bigbooksearch.com/books/The+Hobbit"
    assert search_urls[3] == "https://bigbooksearch.com/books/The+Hobbit"


def test_parse_image_urls_extracts_and_normalizes() -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import parse_image_urls

    html = """
    <img src="https://img.example/a.jpg" />
    <img src="//img.example/b.jpg" />
    <img data-src="/covers/c.jpg" />
    <img src="https://img.example/a.jpg" />
    """
    urls = parse_image_urls(html, base_url="https://bigbooksearch.com/books/test")
    assert urls == [
        "https://img.example/a.jpg",
        "https://img.example/b.jpg",
        "https://bigbooksearch.com/covers/c.jpg",
    ]


def test_get_urls_falls_back_between_search_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import get_urls

    class _Transient(Exception):
        @staticmethod
        def getcode():
            return 503

    class _Resp:
        @staticmethod
        def read():
            return b'<img src="https://img.example/cover.jpg" />'

    calls = []

    class _Browser:
        def open_novisit(self, url, timeout=30):
            del timeout
            calls.append(url)
            if "please-dont-scrape" in url:
                raise _Transient("busy")
            return _Resp()

    log = _Log()
    urls = get_urls(
        br=_Browser(),
        tokens=["The", "Hobbit"],
        log=log,
        abort=Event(),
        timeout=15,
        retry_policy=RetryPolicy(attempts=1, base_delay=0.01, max_delay=0.02),
    )
    assert urls == ["https://img.example/cover.jpg"]
    assert len(calls) >= 2
    assert "please-dont-scrape" in calls[0]
    assert calls[1].startswith("https://www.bigbooksearch.com/books/")


def test_get_urls_non_retryable_errors_return_empty() -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import get_urls

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
            del url, timeout
            raise ValueError("boom")

    urls = get_urls(
        br=_Browser(),
        tokens=["query"],
        log=_Log(),
        abort=Event(),
        timeout=10,
        retry_policy=RetryPolicy(attempts=1, base_delay=0.01, max_delay=0.02),
    )
    assert urls == []


def test_get_urls_empty_query_empty_payload_and_no_images() -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import get_urls

    assert get_urls(br=object(), tokens=["", "   "], log=_Log(), abort=Event()) == []

    calls = []
    payloads = iter([b"", b"<html><body>No covers</body></html>"])

    class _Browser:
        def open_novisit(self, url, timeout=30):
            calls.append((url, timeout))
            return _Response(next(payloads))

    urls = get_urls(
        br=_Browser(),
        tokens=["Query"],
        log=_Log(),
        abort=Event(),
        timeout=9,
        retry_policy=RetryPolicy(attempts=1, base_delay=0.01, max_delay=0.02),
    )
    assert urls == []
    assert len(calls) == 4
    assert calls[0][1] == 9
    assert "please-dont-scrape" in calls[0][0]
    assert calls[1][0].startswith("https://www.bigbooksearch.com/books/")
    assert calls[3][0].startswith("https://bigbooksearch.com/books/")


def test_big_book_search_retry_helpers_and_get_image_urls(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch

    plugin = BigBookSearch()
    policy = plugin._retry_policy()
    assert policy.attempts == plugin.HTTP_RETRY_ATTEMPTS
    assert plugin._retry_backoff(1) == plugin.HTTP_RETRY_BASE_SECONDS
    assert plugin._retry_backoff(10) == plugin.HTTP_RETRY_MAX_SECONDS
    assert plugin._wait_for_backoff(Event(), 0) is False
    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0) is True

    calls = []

    class _Browser:
        def open_novisit(self, url, timeout=30):
            calls.append((url, timeout))
            return _Response(b'<img src="https://img.example/cover.jpg" />')

    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    log = _Log()
    urls = plugin.get_image_urls("The Hobbit", ["J. R. R. Tolkien"], log, Event(), timeout=11)

    assert urls == ["https://img.example/cover.jpg"]
    assert calls and calls[0][1] == 11
    assert any(level == "info" and "Big Book Search query tokens" in str(parts) for level, parts in log.events)
    assert plugin.get_image_urls(None, None, log, Event(), timeout=11) == []


def test_big_book_search_download_cover_wires_to_multiple_cover_download(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch

    plugin = BigBookSearch()
    monkeypatch.setattr(plugin, "get_image_urls", lambda *args, **kwargs: ["https://img.example/cover.jpg"])
    called = {}

    def _download_multiple_covers(title, authors, urls, get_best_cover, timeout, result_queue, abort, log):
        called["title"] = title
        called["authors"] = authors
        called["urls"] = urls
        called["get_best_cover"] = get_best_cover
        called["timeout"] = timeout
        called["result_queue"] = result_queue
        called["abort"] = abort
        called["log"] = log

    monkeypatch.setattr(plugin, "download_multiple_covers", _download_multiple_covers)

    out = queue.Queue()
    abort = Event()
    logger = _Log()
    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=abort,
        title="A Title",
        authors=["An Author"],
        timeout=22,
        get_best_cover=True,
    )
    assert called["title"] == "A Title"
    assert called["authors"] == ["An Author"]
    assert called["urls"] == ["https://img.example/cover.jpg"]
    assert called["get_best_cover"] is True
    assert called["timeout"] == 22
    assert called["result_queue"] is out
    assert called["abort"] is abort
    assert called["log"] is logger


def test_big_book_search_download_cover_without_title_is_noop(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch

    plugin = BigBookSearch()
    called = {"get_image_urls": False, "download_multiple_covers": False}

    def _get_image_urls(*args, **kwargs):
        called["get_image_urls"] = True
        return []

    def _download_multiple_covers(*args, **kwargs):
        called["download_multiple_covers"] = True

    monkeypatch.setattr(plugin, "get_image_urls", _get_image_urls)
    monkeypatch.setattr(plugin, "download_multiple_covers", _download_multiple_covers)

    plugin.download_cover(log=_Log(), result_queue=queue.Queue(), abort=Event(), title=None, authors=["Author"])

    assert called == {"get_image_urls": False, "download_multiple_covers": False}


def test_big_book_search_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("big_book_search")
    assert hasattr(mod, "BigBookSearch")
