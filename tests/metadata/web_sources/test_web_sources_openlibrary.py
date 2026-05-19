from __future__ import annotations

import queue
from threading import Event
from urllib.error import URLError


def test_web_sources_openlibrary_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.openlibrary as openlibrary

    assert openlibrary is not None


def test_openlibrary_helper_normalization_edges() -> None:
    from LiuXin_alpha.metadata.web_sources.openlibrary import (
        _coerce_text,
        _first_value,
        _isbn_from_identifiers,
        _normalize_isbn,
    )

    class BadString:
        def __str__(self):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        @staticmethod
        def read():
            return b"jpeg-bytes"

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        @staticmethod
        def read():
            return b"jpeg-bytes"

    class _Browser:
        def __init__(self):
            self.calls = 0

        def open_novisit(self, url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _E(Exception):
        @staticmethod
        def getcode():
            return 404

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _E(Exception):
        @staticmethod
        def getcode():
            return 404

    class _Resp:
        @staticmethod
        def read():
            return b"medium-cover"

    class _Browser:
        def __init__(self):
            self.urls = []

        def open_novisit(self, url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    plugin = OpenLibrary()
    called = {"browser": False}

    def _browser():
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        @staticmethod
        def read():
            return b"x"

    seen = {}

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        @staticmethod
        def read():
            return b"x"

    seen = {}

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    class _Resp:
        @staticmethod
        def read():
            return b""

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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
    from LiuXin_alpha.metadata.web_sources.openlibrary import OpenLibrary

    plugin = OpenLibrary()
    called = {"browser": False}

    def _browser():
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
