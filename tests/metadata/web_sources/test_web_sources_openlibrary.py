"""
Verify Open Library ISBN validation, cover variants, retries and cancellation.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources openlibrary through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
"""
from __future__ import annotations

import queue
from threading import Event
from urllib.error import URLError


def test_web_sources_openlibrary_import_smoke() -> None:
    """
    Verify web sources openlibrary import smoke.

    Example:
        Exercise test web sources openlibrary import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.openlibrary as openlibrary

    assert openlibrary is not None


def test_openlibrary_helper_normalization_edges() -> None:
    """
    Verify openlibrary helper normalization edges.

    Example:
        Exercise test openlibrary helper normalization edges through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import (
        _coerce_text,
        _first_value,
        _isbn_from_identifiers,
        _normalize_isbn,
    )

    class BadString:
        """
        Provide the BadString test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary helper normalization edges.BadString through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        def __str__(self):
            """
            Perform the str test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary helper normalization edges.BadString.str through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("broken")

    assert _first_value(None) is None
    assert _first_value("isbn") == "isbn"
    assert _first_value({"first": "ignored"}) == "first"
    assert _first_value({}) is None
    assert _first_value(item for item in ["one"]) == "one"
    assert _first_value(7) == 7

    assert _coerce_text(None) is None
    assert _coerce_text(b"9780306406157") == "9780306406157"
    assert _coerce_text("9780306406157") == "9780306406157"
    assert _coerce_text(BadString()) is None

    assert _normalize_isbn(None) is None
    assert _normalize_isbn("   ") is None
    assert _normalize_isbn("---") is None
    assert _normalize_isbn(" 978-0-306-40615-7 ") == "9780306406157"
    assert _normalize_isbn("0-306-40615-X") == "030640615X"

    assert _isbn_from_identifiers([]) is None
    assert _isbn_from_identifiers({"isbn": "---", "isbn13": "9780306406157"}) == "9780306406157"
    assert _isbn_from_identifiers({"isbn": None, "isbn10": {"030640615X"}}) == "030640615X"
    assert _isbn_from_identifiers({"asin": "B000000"}) is None


def test_openlibrary_get_book_url_and_cached_cover_url() -> None:
    """
    Verify openlibrary get book url and cached cover url.

    Example:
        Exercise test openlibrary get book url and cached cover url through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    plugin = OpenLibrary()
    assert plugin.get_book_url({}) is None
    assert plugin.get_cached_cover_url({}) is None
    assert plugin.get_book_url({"isbn": "9780306406157"}) == (
        "isbn",
        "9780306406157",
        "https://openlibrary.org/isbn/9780306406157",
    )
    assert (
        plugin.get_cached_cover_url({"isbn": "9780306406157"})
        == "https://covers.openlibrary.org/b/isbn/9780306406157-L.jpg?default=false"
    )


def test_openlibrary_download_cover_happy_path(monkeypatch) -> None:
    """
    Verify openlibrary download cover happy path.

    Example:
        Exercise test openlibrary download cover happy path through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover happy path.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover happy path.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"jpeg-bytes"

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover happy path.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover happy path.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            assert "covers.openlibrary.org/b/isbn/9780306406157-L.jpg" in url
            assert timeout == 17
            return _Resp()

    plugin = OpenLibrary()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    out = queue.Queue()
    log = []

    plugin.download_cover(
        log=type(
            "L",
            (),
            {
                "error": staticmethod(lambda *a: log.append(("error", a))),
                "exception": staticmethod(lambda *a: log.append(("exception", a))),
            },
        )(),
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
        timeout=17,
    )

    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"jpeg-bytes"
    assert log == []


def test_openlibrary_download_cover_retries_transient_timeout(monkeypatch) -> None:
    """
    Verify openlibrary download cover retries transient timeout.

    Example:
        Exercise test openlibrary download cover retries transient timeout through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover retries transient timeout.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover retries transient timeout.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"jpeg-bytes"

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover retries transient timeout.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        def __init__(self):
            """
            Initialize the Browser test double.

            Example:
                Exercise test openlibrary download cover retries transient timeout.Browser.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: None; the function records state or raises through its assertions.
            """
            self.calls = 0

        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover retries transient timeout.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del url, timeout
            self.calls += 1
            if self.calls == 1:
                raise URLError(TimeoutError("handshake operation timed out"))
            return _Resp()

    browser = _Browser()
    plugin = OpenLibrary()
    monkeypatch.setattr(plugin, "browser", lambda: browser)
    monkeypatch.setattr(plugin, "_wait_for_backoff", lambda abort, delay: False)
    out = queue.Queue()
    events = []
    logger = type(
        "L",
        (),
        {
            "warning": staticmethod(lambda *a: events.append(("warning", a))),
            "error": staticmethod(lambda *a: events.append(("error", a))),
            "exception": staticmethod(lambda *a: events.append(("exception", a))),
        },
    )()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
        timeout=17,
    )

    assert out.get_nowait() == (plugin, b"jpeg-bytes")
    assert browser.calls == 2
    assert any("retrying with backoff" in " ".join(map(str, parts)) for _level, parts in events)


def test_openlibrary_download_cover_404_logs_error(monkeypatch) -> None:
    """
    Verify openlibrary download cover 404 logs error.

    Example:
        Exercise test openlibrary download cover 404 logs error through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _E(Exception):
        """
        Provide the E test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover 404 logs error.E through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover 404 logs error.E.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 404

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover 404 logs error.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover 404 logs error.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            raise _E("missing")

    plugin = OpenLibrary()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    out = queue.Queue()
    events = []
    logger = type(
        "L",
        (),
        {
            "error": staticmethod(lambda *a: events.append(("error", a))),
            "exception": staticmethod(lambda *a: events.append(("exception", a))),
        },
    )()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )

    assert out.empty()
    assert events and events[0][0] == "error"


def test_openlibrary_download_cover_tries_next_size_after_404(monkeypatch) -> None:
    """
    Verify openlibrary download cover tries next size after 404.

    Example:
        Exercise test openlibrary download cover tries next size after 404 through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _E(Exception):
        """
        Provide the E test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover tries next size after 404.E through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover tries next size after 404.E.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 404

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover tries next size after 404.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover tries next size after 404.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"medium-cover"

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover tries next size after 404.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        def __init__(self):
            """
            Initialize the Browser test double.

            Example:
                Exercise test openlibrary download cover tries next size after 404.Browser.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: None; the function records state or raises through its assertions.
            """
            self.urls = []

        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover tries next size after 404.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del timeout
            self.urls.append(url)
            if len(self.urls) == 1:
                raise _E("large missing")
            return _Resp()

    plugin = OpenLibrary()
    browser = _Browser()
    monkeypatch.setattr(plugin, "browser", lambda: browser)
    out = queue.Queue()
    events = []
    logger = type(
        "L",
        (),
        {
            "error": staticmethod(lambda *a: events.append(("error", a))),
            "warning": staticmethod(lambda *a: events.append(("warning", a))),
            "exception": staticmethod(lambda *a: events.append(("exception", a))),
        },
    )()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )

    assert out.get_nowait() == (plugin, b"medium-cover")
    assert browser.urls[0].endswith("-L.jpg?default=false")
    assert browser.urls[1].endswith("-M.jpg?default=false")
    assert events and events[0][0] == "error"


def test_openlibrary_download_cover_without_isbn_is_noop(monkeypatch) -> None:
    """
    Verify openlibrary download cover without isbn remains noop.

    Example:
        Exercise test openlibrary download cover without isbn is noop through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    plugin = OpenLibrary()
    called = {"browser": False}

    def _browser():
        """
        Perform the browser test-helper operation with deterministic inputs.

        Example:
            Exercise test openlibrary download cover without isbn is noop.browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :return: The deterministic value, row, identity or collection described above.
        """
        called["browser"] = True
        raise AssertionError("browser should not be called without ISBN")

    monkeypatch.setattr(plugin, "browser", _browser)
    out = queue.Queue()
    logger = type("L", (), {"error": staticmethod(lambda *a: None), "exception": staticmethod(lambda *a: None)})()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={},
    )

    assert out.empty()
    assert called["browser"] is False


def test_openlibrary_download_cover_supports_set_identifier(monkeypatch) -> None:
    """
    Verify openlibrary download cover supports set identifier.

    Example:
        Exercise test openlibrary download cover supports set identifier through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover supports set identifier.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover supports set identifier.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"x"

    seen = {}

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover supports set identifier.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover supports set identifier.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            seen["url"] = url
            return _Resp()

    plugin = OpenLibrary()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    out = queue.Queue()
    logger = type("L", (), {"error": staticmethod(lambda *a: None), "exception": staticmethod(lambda *a: None)})()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": {"9780306406157"}},
    )

    assert "9780306406157-L.jpg" in seen["url"]


def test_openlibrary_sanitizes_mangled_isbn_in_url(monkeypatch) -> None:
    """
    Verify openlibrary sanitizes mangled isbn in url.

    Example:
        Exercise test openlibrary sanitizes mangled isbn in url through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary sanitizes mangled isbn in url.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary sanitizes mangled isbn in url.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"x"

    seen = {}

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary sanitizes mangled isbn in url.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary sanitizes mangled isbn in url.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            seen["url"] = url
            return _Resp()

    plugin = OpenLibrary()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    out = queue.Queue()
    logger = type("L", (), {"error": staticmethod(lambda *a: None), "exception": staticmethod(lambda *a: None)})()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": " 978-0-306-40615-7 \n?? "},
    )

    assert "9780306406157-L.jpg" in seen["url"]


def test_openlibrary_get_urls_accept_bytes_isbn() -> None:
    """
    Verify openlibrary get urls accept bytes isbn.

    Example:
        Exercise test openlibrary get urls accept bytes isbn through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    plugin = OpenLibrary()
    assert plugin.get_book_url({"isbn": b"9780306406157"}) == (
        "isbn",
        "9780306406157",
        "https://openlibrary.org/isbn/9780306406157",
    )
    assert (
        plugin.get_cached_cover_url({"isbn": b"9780306406157"})
        == "https://covers.openlibrary.org/b/isbn/9780306406157-L.jpg?default=false"
    )


def test_openlibrary_download_cover_non_404_logs_exception(monkeypatch) -> None:
    """
    Verify openlibrary download cover non 404 logs exception.

    Example:
        Exercise test openlibrary download cover non 404 logs exception through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover non 404 logs exception.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover non 404 logs exception.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("boom")

    plugin = OpenLibrary()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    out = queue.Queue()
    events = []
    logger = type(
        "L",
        (),
        {
            "error": staticmethod(lambda *a: events.append(("error", a))),
            "exception": staticmethod(lambda *a: events.append(("exception", a))),
        },
    )()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )

    assert out.empty()
    assert events and events[0][0] == "exception"
    meta = events[0][1][-1]
    assert meta["url"] == "https://covers.openlibrary.org/b/isbn/9780306406157-L.jpg?default=false"
    assert meta["error_type"] == "RuntimeError"
    assert meta["error"] == "boom"


def test_openlibrary_download_cover_empty_payload_not_queued(monkeypatch) -> None:
    """
    Verify openlibrary download cover empty payload not queued.

    Example:
        Exercise test openlibrary download cover empty payload not queued through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover empty payload not queued.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover empty payload not queued.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b""

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test openlibrary download cover empty payload not queued.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test openlibrary download cover empty payload not queued.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            return _Resp()

    plugin = OpenLibrary()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    out = queue.Queue()
    logger = type("L", (), {"error": staticmethod(lambda *a: None), "exception": staticmethod(lambda *a: None)})()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )

    assert out.empty()


def test_openlibrary_abort_prevents_request(monkeypatch) -> None:
    """
    Verify openlibrary abort prevents request.

    Example:
        Exercise test openlibrary abort prevents request through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    plugin = OpenLibrary()
    called = {"browser": False}

    def _browser():
        """
        Perform the browser test-helper operation with deterministic inputs.

        Example:
            Exercise test openlibrary abort prevents request.browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_openlibrary.py


        :return: The deterministic value, row, identity or collection described above.
        """
        called["browser"] = True
        raise AssertionError("browser should not be called if abort is already set")

    monkeypatch.setattr(plugin, "browser", _browser)
    out = queue.Queue()
    logger = type("L", (), {"error": staticmethod(lambda *a: None), "exception": staticmethod(lambda *a: None)})()
    abort = Event()
    abort.set()

    plugin.download_cover(
        log=logger,
        result_queue=out,
        abort=abort,
        identifiers={"isbn": "9780306406157"},
    )

    assert out.empty()
    assert called["browser"] is False
