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


def test_web_sources_big_book_search_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.big_book_search as bbs

    assert bbs is not None


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
    assert calls[1].startswith("https://bigbooksearch.com/books/")


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


def test_big_book_search_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("big_book_search")
    assert hasattr(mod, "BigBookSearch")
