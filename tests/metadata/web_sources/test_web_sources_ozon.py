from __future__ import annotations

import queue
from threading import Event

import pytest


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


def _sample_search_html() -> str:
    return """
    <html>
      <body>
        <a href="/context/detail/id/1009493080/">one</a>
        <a href="/product/na-vse-chetyre-storony-1000000001/">two</a>
      </body>
    </html>
    """


def _sample_detail_html() -> str:
    return """
    <html>
      <head>
        <meta property="og:title" content="На все четыре стороны" />
        <meta property="og:image" content="https://cdn1.ozone.ru/s3/multimedia-1-c200/1005748980.jpg" />
        <script type="application/ld+json">
        {
          "@context": "https://schema.org",
          "@type": "Book",
          "name": "На все четыре стороны",
          "author": [{"@type": "Person", "name": "А. А. Гилл"}],
          "publisher": {"@type": "Organization", "name": "АСТ"},
          "description": "Книга о путешествиях.",
          "inLanguage": "Русский",
          "datePublished": "2009",
          "isbn": ["9785916572629"],
          "isPartOf": {"name": "Путешествия (2)"},
          "aggregateRating": {"ratingValue": "4.5"},
          "keywords": ["Путешествия", "Эссе"],
          "image": "https://cdn1.ozone.ru/s3/multimedia-1-c200/1005748980.jpg"
        }
        </script>
      </head>
      <body></body>
    </html>
    """


def test_web_sources_ozon_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.ozon as ozon

    assert ozon is not None


def test_ozon_get_book_url_and_id_from_url() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    assert plugin.get_book_url({"ozon": {"1009493080"}}) == (
        "ozon",
        "1009493080",
        "https://www.ozon.ru/context/detail/id/1009493080/",
    )
    assert plugin.id_from_url("https://www.ozon.ru/context/detail/id/1009493080/") == ("ozon", "1009493080")


def test_ozon_create_query_prefers_id_then_isbn_then_title_author() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    q = plugin.create_query(identifiers={"ozon": "1009493080"})
    assert q[0] == "detail"
    assert q[1].endswith("/context/detail/id/1009493080/")

    q = plugin.create_query(identifiers={"isbn": "9785916572629"})
    assert q[0] == "search"
    assert "9785916572629" in q[1]

    q = plugin.create_query(title="На все четыре стороны", authors=["Гилл"], identifiers={})
    assert q[0] == "search"
    assert "search/?text=" in q[1]


def test_ozon_extract_ids_from_search_html() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    assert plugin._extract_ozon_ids_from_search_html(_sample_search_html()) == ["1009493080", "1000000001"]


def test_ozon_metadata_from_detail_html_parses_fields_and_caches() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    mi = plugin._metadata_from_detail_html(_sample_detail_html(), ozon_id="1009493080", relevance=1)

    assert mi.title == "На все четыре стороны"
    assert mi.authors == ["А. А. Гилл"]
    assert mi.publisher == "АСТ"
    assert mi.get_identifiers()["ozon"] == "1009493080"
    assert mi.get_identifiers()["isbn"] == "9785916572629"
    assert mi.series == "Путешествия"
    assert mi.series_index == 2
    assert mi.rating == 4.5
    assert mi.language == "ru"
    assert "Путешествия" in (mi.tags or [])
    assert plugin.cached_identifier_to_cover_url("1009493080").endswith("/books_covers/1005748980.jpg")
    assert plugin.cached_isbn_to_identifier("9785916572629") == "1009493080"


def test_ozon_identify_by_id(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    monkeypatch.setattr(plugin, "_open_text_with_backoff", lambda log, abort, url, timeout, context: _sample_detail_html())

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"ozon": "1009493080"},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["ozon"] == "1009493080"


def test_ozon_identify_search_then_details(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()

    def _open(log, abort, url, timeout, context):
        del log, abort, timeout
        if "search" in context.lower():
            return _sample_search_html()
        return _sample_detail_html()

    monkeypatch.setattr(plugin, "_open_text_with_backoff", _open)

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="На все четыре стороны",
        authors=["Гилл"],
        identifiers={},
    )
    first = out.get_nowait()
    second = out.get_nowait()
    assert first.get_identifiers()["ozon"] == "1009493080"
    assert second.get_identifiers()["ozon"] == "1000000001"


def test_ozon_download_cover_uses_cache() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    plugin.cache_identifier_to_cover_url("1009493080", "https://www.ozon.ru/multimedia/books_covers/1005748980.jpg")
    plugin._open_bytes_with_backoff = lambda log, abort, url, timeout, context: b"cover-bytes"

    out = queue.Queue()
    plugin.download_cover(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"ozon": "1009493080"},
    )
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"


def test_ozon_open_bytes_with_backoff_retries_transient(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    class _Transient(Exception):
        @staticmethod
        def getcode():
            return 503

    class _Resp:
        @staticmethod
        def read():
            return b"ok"

    class _Browser:
        def __init__(self):
            self.calls = 0

        def open_novisit(self, url, timeout=30):
            del url, timeout
            self.calls += 1
            if self.calls < 3:
                raise _Transient("busy")
            return _Resp()

    b = _Browser()
    plugin = Ozon()
    monkeypatch.setattr(plugin, "browser", lambda: b)
    delays = []
    monkeypatch.setattr(plugin, "_wait_for_backoff", lambda abort, delay: delays.append(delay) or False)
    log = _Log()

    payload = plugin._open_bytes_with_backoff(
        log=log,
        abort=Event(),
        url="https://www.ozon.ru/context/detail/id/1009493080/",
        timeout=12,
        context="unit-test",
    )
    assert payload == b"ok"
    assert b.calls == 3
    assert len(delays) == 2
    assert any(level == "warning" for level, _parts in log.events)


def test_ozon_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("ozon")
    assert hasattr(mod, "Ozon")
