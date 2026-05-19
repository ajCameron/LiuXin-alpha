from __future__ import annotations

import queue
from datetime import datetime
from threading import Event
from urllib.error import HTTPError

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


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


def test_ozon_identify_search_failure_can_fall_back_to_title_author(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    calls = []

    def _open(log, abort, url, timeout, context):
        del log, abort, url, timeout
        calls.append(context)
        if context == "Ozon search":
            raise RuntimeError("redirect loop")
        if context == "Ozon search fallback":
            return _sample_search_html()
        return _sample_detail_html()

    monkeypatch.setattr(plugin, "_open_text_with_backoff", _open)

    out = queue.Queue()
    log = _Log()
    plugin.identify(
        log=log,
        result_queue=out,
        abort=Event(),
        title="На все четыре стороны",
        authors=["Гилл"],
        identifiers={"isbn": "9785916572629"},
    )

    assert out.get_nowait().get_identifiers()["ozon"] == "1009493080"
    assert "Ozon search fallback" in calls
    assert any(
        level == "warning" and "continuing with available fallback paths" in " ".join(map(str, parts))
        for level, parts in log.events
    )


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


def test_ozon_low_level_helpers_handle_odd_inputs() -> None:
    import LiuXin_alpha.metadata.web_sources.ozon as ozon

    class BadText:
        def __str__(self):
            raise RuntimeError("cannot stringify")

    assert ozon._as_text(b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82") == "Тест"
    assert ozon._as_text(BadText()) == ""
    assert ozon._first({"first": "ignored"}) == "first"
    assert ozon._first(item for item in ["value"]) == "value"
    assert ozon._first_identifier_value([], "ozon") is None
    assert ozon._extract_ozon_id(None) is None
    assert ozon._extract_ozon_id("") is None
    assert ozon._extract_ozon_id("null") is None
    assert ozon._extract_ozon_id("12345") is None
    assert ozon._extract_ozon_id("1009493080") == "1009493080"
    assert ozon._extract_ozon_id("https://www.ozon.ru/product/book-name-1000000001/") == "1000000001"
    assert ozon._extract_ozon_id("https://www.ozon.ru/context/detail/id/1009493080/") == "1009493080"
    assert ozon._safe_isbn({"isbn": ["bad", "9785916572629"]}) is None
    assert ozon._safe_isbn({"isbn13": b"9785916572629"}) == "9785916572629"
    assert ozon._extract_json_ld_objects("<script type='application/ld+json'></script>") == []
    assert ozon._extract_json_ld_objects("<script type='application/ld+json'>{bad}</script>") == []
    assert ozon._extract_json_ld_objects(
        "<script type='application/ld+json'>[{\"name\": \"One\"}, {\"name\": \"Two\"}]</script>"
    ) == [{"name": "One"}, {"name": "Two"}]
    assert ozon._extract_meta_content('<meta name="description" content="Описание" />', "description") == "Описание"
    assert ozon._extract_meta_content("<html></html>", "description") == ""
    assert ozon._parse_pubdate("") is None
    assert ozon._parse_pubdate("2021-04").day == 15
    assert ozon._parse_pubdate("2021").year == 2021
    assert ozon._parse_pubdate("Опубликовано в 1999").year == 1999
    assert ozon._parse_pubdate("not a date") is None
    assert ozon._parse_series_and_index("") == (None, None)
    assert ozon._parse_series_and_index("Series") == ("Series", None)
    assert ozon._parse_series_and_index("Series (2.5)") == ("Series", 2.5)
    assert ozon._parse_series_and_index("Series (volume)") == ("Series", None)
    assert ozon._translate_to_big_cover_url("") == ""
    assert ozon._translate_to_big_cover_url("//cdn.example/path/cover.png") == (
        "https://www.ozon.ru/multimedia/books_covers/cover.jpg"
    )
    assert ozon._translate_to_big_cover_url("https://example.invalid/no-extension") == "https://example.invalid/no-extension"


def test_ozon_url_query_and_search_edge_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    assert plugin.get_book_url({"ozon": "invalid"}) is None
    assert plugin.id_from_url("https://example.invalid/product/book-1000000001/") == ("ozon", "1000000001")
    assert plugin.id_from_url("https://example.invalid/no-id") is None
    assert plugin.create_query(title=None, authors=None, identifiers={}) is None

    html = """
      <a href="/context/detail/id/1009493080/">one</a>
      <a href="/context/detail/id/1009493080/">dup</a>
      <a href="/product/book-name-1000000001/">two</a>
      <a href="/product/book-name-1000000002/">three</a>
    """
    assert plugin._extract_ozon_ids_from_search_html(html, limit=2) == ["1009493080", "1000000001"]


def test_ozon_metadata_parser_uses_fallbacks_and_defaults() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    html = """
    <html>
      <head>
        <meta property="og:title" content="Meta Title" />
        <meta name="description" content="Plain description" />
        <meta property="og:image" content="//cdn.example/path/meta-cover.png" />
        <script type="application/ld+json">
        [
          "ignored",
          {
            "headline": "",
            "publisher": "String Publisher",
            "inLanguage": "Английский",
            "datePublished": "2021/04",
            "image": {"url": "https://cdn.example/path/json-cover.jpg"},
            "keywords": "fiction, adventure, fiction",
            "author": {"name": "Single Author"},
            "series": "Cycle (3.5)",
            "aggregateRating": "4,2",
            "isbn": "9785916572629"
          }
        ]
        </script>
      </head>
      <body></body>
    </html>
    """

    mi = plugin._metadata_from_detail_html(html, ozon_id="1009493080", relevance=2)

    assert mi.title == "Meta Title"
    assert mi.authors == ["Single Author"]
    assert mi.publisher == "String Publisher"
    assert mi.comments == "<p>Plain description</p>"
    assert mi.pubdate.month == 4 and mi.pubdate.day == 15
    assert mi.series == "Cycle"
    assert mi.series_index == 3.5
    assert mi.rating == 4.2
    assert mi.language == "en"
    assert mi.tags == ["fiction", "adventure"]
    assert mi.get_identifiers()["isbn"] == "9785916572629"
    assert plugin.cached_identifier_to_cover_url("1009493080").endswith("/books_covers/json-cover.jpg")

    fallback = plugin._metadata_from_detail_html(
        """
        <html>
          <head>
            <meta property="og:description" content="Meta description" />
            <meta property="og:image" content="https://cdn.example/path/meta.jpg" />
          </head>
          <body>4,7 из 5 ISBN 978-5-91657-262-9</body>
        </html>
        """,
        ozon_id="1000000001",
        relevance=0,
    )

    assert fallback.title == "Unknown"
    assert fallback.authors == ["Unknown"]
    assert fallback.comments == "<p>Meta description</p>"
    assert fallback.rating == 4.7
    assert fallback.get_identifiers()["isbn"] == "9785916572629"
    assert plugin.cached_isbn_to_identifier("9785916572629") == "1000000001"


def test_ozon_metadata_parser_ignores_invalid_optional_fields() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    html = """
    <script type="application/ld+json">
    {
      "name": "Sparse Book",
      "author": ["Text Author", {"name": ""}],
      "description": "<p>Already HTML</p>",
      "publisher": {"name": ""},
      "inLanguage": "Unknown-Language",
      "image": [],
      "keywords": ["tag,with,comma", "", "tag"],
      "aggregateRating": {"ratingValue": "bad"},
      "isbn": ["bad", "9785916572629"]
    }
    </script>
    """

    mi = plugin._metadata_from_detail_html(html, ozon_id="1000000002", relevance=0)

    assert mi.title == "Sparse Book"
    assert mi.authors == ["Text Author"]
    assert mi.comments == "<p>Already HTML</p>"
    assert mi.tags == ["tag;with;comma", "tag"]
    assert "rating" not in mi.all_field_keys() or mi.rating in (None, 0)
    assert mi.get_identifiers()["isbn"] == "9785916572629"


def test_ozon_identify_retry_filter_skip_and_abort_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    log = _Log()
    calls = []

    def fake_open(log, abort, url, timeout, context):
        del log, abort, timeout
        calls.append((context, url))
        if context == "Ozon search":
            return "<html>No matching books</html>"
        if context == "Ozon search fallback":
            return """
              <a href="/context/detail/id/1009493080/">one</a>
              <a href="/context/detail/id/1009493080/">dup</a>
              <a href="/context/detail/id/1000000001/">two</a>
              <a href="/context/detail/id/1000000002/">three</a>
            """
        if url.endswith("/1009493080/"):
            return ""
        if url.endswith("/1000000001/"):
            raise OSError("transient detail failure")
        if url.endswith("/1000000002/"):
            return _sample_detail_html().replace("9785916572629", "9780306406157")
        raise AssertionError(url)

    plugin._open_text_with_backoff = fake_open
    out = queue.Queue()
    plugin.identify(
        log=log,
        result_queue=out,
        abort=Event(),
        title="На все четыре стороны",
        authors=["Гилл"],
        identifiers={"isbn": "9785916572629"},
    )

    assert out.empty()
    assert any("retrying with title/author query" in " ".join(map(str, parts)) for _level, parts in log.events)
    assert any(context == "Ozon search fallback" for context, _url in calls)

    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, identifiers={"ozon": "1009493080"})
    assert out.empty()

    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title=None, authors=None, identifiers={})
    assert out.empty()


def test_ozon_identify_stops_search_variants_after_rr_redirect_loop() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    calls = []

    def _raise_redirect(log, abort, url, timeout, context):
        del log, abort, timeout, context
        calls.append(url)
        raise HTTPError(
            url + "&__rr=9",
            307,
            "Temporary Redirect",
            {"Location": url + "&__rr=1", "Content-Type": "text/html"},
            None,
        )

    plugin._open_text_with_backoff = _raise_redirect

    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), identifiers={"isbn": "9785916572629"})

    assert out.empty()
    assert len(calls) == 1
    assert any(
        level == "warning" and "redirect-loop signature observed" in " ".join(map(str, parts))
        for level, parts in log.events
    )


def test_ozon_download_cover_discovers_from_identify_and_handles_failures() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    log = _Log()
    out = queue.Queue()

    def fake_identify(log, rq, abort, title=None, authors=None, identifiers=None, timeout=30):
        del log, abort, title, authors, identifiers, timeout
        mi = calibreMetaInformation("Cover Book", ["Author"])
        mi.set_identifier("ozon", "1009493080")
        plugin.cache_identifier_to_cover_url("1009493080", "https://cdn.example/cover.jpg")
        rq.put(mi)

    plugin.identify = fake_identify
    plugin._open_bytes_with_backoff = lambda log, abort, url, timeout, context: b"cover-bytes"
    plugin.download_cover(log=log, result_queue=out, abort=Event(), identifiers={})
    assert out.get_nowait() == (plugin, b"cover-bytes")
    assert any("running identify" in " ".join(map(str, parts)) for _level, parts in log.events)

    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=abort, identifiers={})
    assert out.empty()

    plugin.identify = lambda log, rq, abort, **kwargs: None
    out = queue.Queue()
    log = _Log()
    plugin.download_cover(log=log, result_queue=out, abort=Event(), identifiers={})
    assert out.empty()
    assert any("No cover found" in " ".join(map(str, parts)) for _level, parts in log.events)

    plugin.cache_identifier_to_cover_url("1009493080", "https://cdn.example/cover.jpg")
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"ozon": "1009493080"})
    assert out.empty()

    def raise_download(**kwargs):
        raise OSError("download failed")

    plugin._open_bytes_with_backoff = raise_download
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"ozon": "1009493080"})
    assert out.empty()


def test_ozon_open_text_decodes_and_abort_backoff_returns_empty() -> None:
    from LiuXin_alpha.metadata.web_sources.ozon import Ozon

    plugin = Ozon()
    plugin._open_bytes_with_backoff = lambda **kwargs: b"\xd0\xa2\xd0\xb5\xd1\x81\xd1\x82"
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == (
        "Тест"
    )
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == ""

    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0.01) is True
