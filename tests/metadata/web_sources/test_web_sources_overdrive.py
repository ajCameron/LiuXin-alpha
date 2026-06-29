from __future__ import annotations

import queue
from datetime import datetime
from threading import Event

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
        <a href="/media/1234567">one</a>
        <a href="/media/89ABCDEF">two</a>
        <a href="/media/1234567">dup</a>
      </body>
    </html>
    """


def _sample_detail_html(media_id: str = "1234567") -> str:
    return f"""
    <html>
      <head>
        <meta property="og:title" content="The Hobbit" />
        <meta property="og:image" content="https://images.example/ImageType-200/cover.jpg" />
        <script type="application/ld+json">
        {{
          "@context": "https://schema.org",
          "@type": "Book",
          "name": "The Hobbit",
          "author": [{{"@type": "Person", "name": "J. R. R. Tolkien"}}],
          "publisher": {{"@type": "Organization", "name": "Allen & Unwin"}},
          "description": "A hobbit goes on an adventure.",
          "inLanguage": "en",
          "datePublished": "1937-09-21",
          "isbn": ["9780261102217", "0261103342"],
          "isPartOf": {{"name": "Middle-earth Universe (1)"}},
          "keywords": ["Fantasy", "Classics"],
          "image": "https://images.example/ImageType-200/cover.jpg"
        }}
        </script>
      </head>
      <body><h1>The Hobbit</h1></body>
    </html>
    """


def test_web_sources_overdrive_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.overdrive as overdrive

    assert overdrive is not None


def test_overdrive_get_book_url_and_id_from_url() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    assert plugin.get_book_url({"overdrive": {"1234567"}}) == (
        "overdrive",
        "1234567",
        "https://www.overdrive.com/media/1234567",
    )
    assert plugin.id_from_url("https://www.overdrive.com/media/1234567") == ("overdrive", "1234567")


def test_overdrive_extract_ids_from_search_html() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import _extract_overdrive_ids_from_search_html

    assert _extract_overdrive_ids_from_search_html(_sample_search_html()) == ["1234567", "89ABCDEF"]


def test_overdrive_metadata_from_detail_html_parses_fields() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    mi = plugin._metadata_from_detail_html(_sample_detail_html(), media_id="1234567", relevance=3)
    assert mi.title == "The Hobbit"
    assert mi.authors == ["J. R. R. Tolkien"]
    assert mi.publisher == "Allen & Unwin"
    assert mi.get_identifiers()["overdrive"] == "1234567"
    assert mi.get_identifiers()["isbn"] == "9780261102217"
    assert mi.series == "Middle-earth Universe"
    assert mi.series_index == 1
    assert mi.tags == ["Fantasy", "Classics"]
    assert mi.language == "en"
    assert mi.pubdate.year == 1937
    assert plugin.cached_identifier_to_cover_url("1234567").endswith("ImageType-100/cover.jpg")


def test_overdrive_identify_by_id(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    monkeypatch.setattr(plugin, "_open_text_with_backoff", lambda log, abort, url, timeout, context: _sample_detail_html())
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"overdrive": "1234567"},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["overdrive"] == "1234567"
    assert mi.title == "The Hobbit"


def test_overdrive_identify_search_then_details(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()

    def _open(log, abort, url, timeout, context):
        del log, abort, timeout
        if "search" in context.lower():
            return _sample_search_html()
        return _sample_detail_html(media_id=url.rsplit("/", 1)[-1])

    monkeypatch.setattr(plugin, "_open_text_with_backoff", _open)
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="The Hobbit",
        authors=["Tolkien"],
        identifiers={},
    )
    first = out.get_nowait()
    second = out.get_nowait()
    assert first.get_identifiers()["overdrive"] == "1234567"
    assert second.get_identifiers()["overdrive"] == "89ABCDEF"


def test_overdrive_download_cover_uses_cache() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    plugin.cache_identifier_to_cover_url("1234567", "https://images.example/cover.jpg")
    monkeypatch_bytes = lambda log, abort, url, timeout, context: b"cover-bytes"
    plugin._open_bytes_with_backoff = monkeypatch_bytes

    out = queue.Queue()
    plugin.download_cover(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"overdrive": "1234567"},
    )
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"


def test_overdrive_open_bytes_with_backoff_retries_transient(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

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
    plugin = OverDrive()
    monkeypatch.setattr(plugin, "browser", lambda: b)
    delays = []
    monkeypatch.setattr(plugin, "_wait_for_backoff", lambda abort, delay: delays.append(delay) or False)
    log = _Log()

    payload = plugin._open_bytes_with_backoff(
        log=log,
        abort=Event(),
        url="https://www.overdrive.com/media/1234567",
        timeout=12,
        context="unit-test",
    )
    assert payload == b"ok"
    assert b.calls == 3
    assert len(delays) == 2
    assert any(level == "warning" for level, _parts in log.events)


def test_overdrive_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("overdrive")
    assert hasattr(mod, "OverDrive")


def test_overdrive_low_level_helpers_handle_odd_inputs() -> None:
    import LiuXin_alpha.metadata.web_sources.overdrive as overdrive

    class BadText:
        def __str__(self):
            raise RuntimeError("cannot stringify")

    assert overdrive._as_text(b"Caf\xc3\xa9") == "Caf\u00e9"
    assert overdrive._as_text(BadText()) == ""
    assert overdrive._first({"first": "ignored"}) == "first"
    assert overdrive._first(item for item in ["value"]) == "value"
    assert overdrive._first_identifier_value([], "overdrive") is None
    assert overdrive._extract_overdrive_id("") is None
    assert overdrive._extract_overdrive_id("short") is None
    assert overdrive._extract_overdrive_id("1234567") == "1234567"
    assert overdrive._extract_overdrive_id("https://www.overdrive.com/media/89ABCDEF") == "89ABCDEF"
    assert overdrive._strip_tags("<p>One</p>\n<b>Two</b>") == "One Two"
    assert overdrive._safe_isbn({"isbn": ["bad", "9780261102217"]}) is None
    assert overdrive._safe_isbn({"isbn13": b"9780261102217"}) == "9780261102217"
    assert overdrive._parse_pubdate("") is None
    assert overdrive._parse_pubdate("1937-09").day == 15
    assert overdrive._parse_pubdate("1937").year == 1937
    assert overdrive._parse_pubdate("First published in 1937").year == 1937
    assert overdrive._parse_pubdate("not a date") is None
    assert overdrive._extract_json_ld_objects("<script type='application/ld+json'></script>") == []
    assert overdrive._extract_json_ld_objects("<script type='application/ld+json'>{bad}</script>") == []
    assert overdrive._extract_json_ld_objects(
        "<script type='application/ld+json'>[{\"name\": \"One\"}, {\"name\": \"Two\"}]</script>"
    ) == [{"name": "One"}, {"name": "Two"}]
    assert overdrive._extract_meta_content('<meta name="description" content="A book" />', "description") == "A book"
    assert overdrive._extract_meta_content("<html></html>", "description") == ""
    assert overdrive._parse_series_and_index("") == (None, None)
    assert overdrive._parse_series_and_index("Series") == ("Series", None)
    assert overdrive._parse_series_and_index("Series (1.5)") == ("Series", 1.5)
    assert overdrive._parse_series_and_index("Series (volume)") == ("Series", None)
    assert overdrive._safe_cover_url("") == ""
    assert overdrive._safe_cover_url("//images.example/ImageType-200/cover.jpg") == (
        "https://images.example/ImageType-100/cover.jpg"
    )


def test_overdrive_url_query_cache_and_search_edge_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive, _extract_overdrive_ids_from_search_html

    plugin = OverDrive()
    assert plugin.get_book_url({"overdrive": "short"}) is None
    assert plugin.id_from_url("https://example.invalid/no-media") is None
    assert plugin.create_query(title=None, authors=None, identifiers={}) is None
    assert plugin.create_query(identifiers={"isbn": "9780261102217"})[1].endswith("9780261102217")

    plugin.cache_isbn_to_identifier("9780261102217", "1234567")
    plugin.cache_identifier_to_cover_url("1234567", "https://images.example/cover.jpg")
    assert plugin.get_cached_cover_url({"isbn": "9780261102217"}) == "https://images.example/cover.jpg"
    assert plugin.get_cached_cover_url({}) is None

    html = """
      <a href="/media/1234567">one</a>
      <a href="/media/1234567">dup</a>
      <a href="/media/89ABCDEF">two</a>
      <a href="/media/ZZZZ9999">three</a>
    """
    assert _extract_overdrive_ids_from_search_html(html, limit=2) == ["1234567", "89ABCDEF"]


def test_overdrive_metadata_parser_uses_fallbacks_and_defaults() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    html = """
    <html>
      <head>
        <meta property="og:title" content="Meta Title" />
        <meta name="description" content="Plain description" />
        <meta property="og:image" content="//images.example/Image200/cover.jpg" />
        <meta name="author" content="Meta One, Meta Two" />
        <script type="application/ld+json">
        [
          "ignored",
          {
            "headline": "",
            "publisher": "String Publisher",
            "inLanguage": "en-US",
            "datePublished": "2021/04",
            "image": {"url": "https://images.example/ImageType-200/json.jpg"},
            "keywords": "fiction, adventure, fiction",
            "series": {"url": "Cycle (3.5)"},
            "isbn": "9780261102217"
          }
        ]
        </script>
      </head>
      <body></body>
    </html>
    """

    mi = plugin._metadata_from_detail_html(html, media_id="1234567", relevance=2)

    assert mi.title == "Meta Title"
    assert mi.authors == ["Meta One", "Meta Two"]
    assert mi.publisher == "String Publisher"
    assert mi.comments == "<p>Plain description</p>"
    assert mi.pubdate.month == 4 and mi.pubdate.day == 15
    assert mi.series == "Cycle"
    assert mi.series_index == 3.5
    assert mi.language == "en"
    assert mi.tags == ["fiction", "adventure"]
    assert mi.get_identifiers()["overdrive"] == "1234567"
    assert mi.get_identifiers()["isbn"] == "9780261102217"
    assert plugin.cached_identifier_to_cover_url("1234567").endswith("/ImageType-100/json.jpg")
    assert plugin.cached_isbn_to_identifier("9780261102217") == "1234567"

    fallback = plugin._metadata_from_detail_html(
        """
        <html>
          <head>
            <meta property="og:description" content="Meta description" />
            <meta property="og:image" content="https://images.example/Image200/meta.jpg" />
          </head>
        </html>
        """,
        media_id=None,
        relevance=0,
    )
    assert fallback.title == "Unknown"
    assert fallback.authors == ["Unknown"]
    assert fallback.comments == "<p>Meta description</p>"
    assert "overdrive" not in fallback.get_identifiers()


def test_overdrive_metadata_parser_ignores_invalid_optional_fields() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
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
      "isbn": ["bad", "9780261102217"]
    }
    </script>
    """

    mi = plugin._metadata_from_detail_html(html, media_id="89ABCDEF", relevance=0)

    assert mi.title == "Sparse Book"
    assert mi.authors == ["Text Author"]
    assert mi.comments == "<p>Already HTML</p>"
    assert mi.tags == ["tag;with;comma", "tag"]
    assert mi.get_identifiers()["isbn"] == "9780261102217"


def test_overdrive_identify_retry_skip_and_abort_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    log = _Log()
    calls = []

    def fake_open(log, abort, url, timeout, context):
        del log, abort, timeout
        calls.append((context, url))
        if context == "OverDrive search":
            return "<html>No matching books</html>"
        if context == "OverDrive search fallback":
            return """
              <a href="/media/1234567">one</a>
              <a href="/media/1234567">dup</a>
              <a href="/media/EMPTY01">empty</a>
              <a href="/media/RAISE01">raise</a>
              <a href="/media/89ABCDEF">two</a>
            """
        if url.endswith("EMPTY01"):
            return ""
        if url.endswith("RAISE01"):
            raise OSError("detail failed")
        return _sample_detail_html(media_id=url.rsplit("/", 1)[-1])

    plugin._open_text_with_backoff = fake_open
    out = queue.Queue()
    plugin.identify(
        log=log,
        result_queue=out,
        abort=Event(),
        title="The Hobbit",
        authors=["Tolkien"],
        identifiers={"isbn": "9780261102217"},
    )

    assert out.qsize() == 2
    assert any("retrying with title/author query" in " ".join(map(str, parts)) for _level, parts in log.events)
    assert any(context == "OverDrive search fallback" for context, _url in calls)

    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, identifiers={"overdrive": "1234567"})
    assert out.empty()

    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title=None, authors=None, identifiers={})
    assert out.empty()


def test_overdrive_download_cover_discovers_from_identify_and_handles_failures() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    log = _Log()
    out = queue.Queue()

    def fake_identify(log, rq, abort, title=None, authors=None, identifiers=None, timeout=30):
        del log, abort, title, authors, identifiers, timeout
        mi = calibreMetaInformation("Cover Book", ["Author"])
        mi.set_identifier("overdrive", "1234567")
        plugin.cache_identifier_to_cover_url("1234567", "https://images.example/cover.jpg")
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

    plugin.cache_identifier_to_cover_url("1234567", "https://images.example/cover.jpg")
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"overdrive": "1234567"})
    assert out.empty()

    def raise_download(**kwargs):
        raise OSError("download failed")

    plugin._open_bytes_with_backoff = raise_download
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"overdrive": "1234567"})
    assert out.empty()


def test_overdrive_open_text_decodes_and_abort_backoff_returns_empty() -> None:
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    plugin._open_bytes_with_backoff = lambda **kwargs: b"Caf\xc3\xa9"
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == (
        "Caf\u00e9"
    )
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == ""

    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0.01) is True
