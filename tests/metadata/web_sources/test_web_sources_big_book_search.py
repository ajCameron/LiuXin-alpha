"""
Verify Big Book Search parsing, retry and cover selection.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources big book search through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
"""
from __future__ import annotations

import queue
from threading import Event

from LiuXin_alpha.metadata.web_sources.http_client import RetryPolicy


class _Log:
    """
    Provide the Log test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Log through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


class _Response:
    """
    Provide the Response test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Response through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
    """
    def __init__(self, payload: bytes) -> None:
        """
        Initialize the Response test double.

        Example:
            Exercise Response.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.payload = payload

    def read(self) -> bytes:
        """
        Perform the read test-helper operation with deterministic inputs.

        Example:
            Exercise Response.read through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.payload


def test_web_sources_big_book_search_import_smoke() -> None:
    """
    Verify web sources big book search import smoke.

    Example:
        Exercise test web sources big book search import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.big_book_search as bbs

    assert bbs is not None


def test_big_book_search_helper_edges() -> None:
    """
    Verify big book search helper edges.

    Example:
        Exercise test big book search helper edges through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.big_book_search import (
        _as_text,
        _build_query,
        _normalize_image_url,
        _search_urls_for_query,
        parse_image_urls,
    )

    class BadString:
        """
        Provide the BadString test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test big book search helper edges.BadString through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
        """
        def __str__(self):
            """
            Perform the str test-helper operation with deterministic inputs.

            Example:
                Exercise test big book search helper edges.BadString.str through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify parse image urls extracts and normalizes.

    Example:
        Exercise test parse image urls extracts and normalizes through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify get urls falls back between search paths.

    Example:
        Exercise test get urls falls back between search paths through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.big_book_search import get_urls

    class _Transient(Exception):
        """
        Provide the Transient test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test get urls falls back between search paths.Transient through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test get urls falls back between search paths.Transient.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 503

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test get urls falls back between search paths.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test get urls falls back between search paths.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b'<img src="https://img.example/cover.jpg" />'

    calls = []

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test get urls falls back between search paths.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
        """
        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test get urls falls back between search paths.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify get urls non retryable errors return empty.

    Example:
        Exercise test get urls non retryable errors return empty through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.big_book_search import get_urls

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test get urls non retryable errors return empty.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test get urls non retryable errors return empty.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify get urls empty query empty payload and no images.

    Example:
        Exercise test get urls empty query empty payload and no images through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.big_book_search import get_urls

    assert get_urls(br=object(), tokens=["", "   "], log=_Log(), abort=Event()) == []

    calls = []
    payloads = iter([b"", b"<html><body>No covers</body></html>"])

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test get urls empty query empty payload and no images.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
        """
        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test get urls empty query empty payload and no images.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify big book search retry helpers and get image urls.

    Example:
        Exercise test big book search retry helpers and get image urls through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test big book search retry helpers and get image urls.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py
        """
        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test big book search retry helpers and get image urls.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify big book search download cover wires to multiple cover download.

    Example:
        Exercise test big book search download cover wires to multiple cover download through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch

    plugin = BigBookSearch()
    monkeypatch.setattr(plugin, "get_image_urls", lambda *args, **kwargs: ["https://img.example/cover.jpg"])
    called = {}

    def _download_multiple_covers(title, authors, urls, get_best_cover, timeout, result_queue, abort, log):
        """
        Perform the download multiple covers test-helper operation with deterministic inputs.

        Example:
            Exercise test big book search download cover wires to multiple cover download.download multiple covers through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param urls: Value supplied for urls in the focused test operation.
        :param get_best_cover: Value supplied for get best cover in the focused test
            operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param result_queue: Value supplied for result queue in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param log: Value supplied for log in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify big book search download cover without title remains noop.

    Example:
        Exercise test big book search download cover without title is noop through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.big_book_search import BigBookSearch

    plugin = BigBookSearch()
    called = {"get_image_urls": False, "download_multiple_covers": False}

    def _get_image_urls(*args, **kwargs):
        """
        Return image urls from deterministic test state.

        Example:
            Exercise test big book search download cover without title is noop.get image urls through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param args: Positional values forwarded by the test double.
        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        called["get_image_urls"] = True
        return []

    def _download_multiple_covers(*args, **kwargs):
        """
        Perform the download multiple covers test-helper operation with deterministic inputs.

        Example:
            Exercise test big book search download cover without title is noop.download multiple covers through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


        :param args: Positional values forwarded by the test double.
        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        called["download_multiple_covers"] = True

    monkeypatch.setattr(plugin, "get_image_urls", _get_image_urls)
    monkeypatch.setattr(plugin, "download_multiple_covers", _download_multiple_covers)

    plugin.download_cover(log=_Log(), result_queue=queue.Queue(), abort=Event(), title=None, authors=["Author"])

    assert called == {"get_image_urls": False, "download_multiple_covers": False}


def test_big_book_search_import_web_source_module() -> None:
    """
    Verify big book search import web source module.

    Example:
        Exercise test big book search import web source module through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_big_book_search.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("big_book_search")
    assert hasattr(mod, "BigBookSearch")
