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


def _sample_detail_html() -> str:
    return """
    <html>
      <head>
        <title>Example Book: Amazon.com</title>
        <meta property="og:image" content="https://images.example/cover-main.jpg" />
      </head>
      <body>
        <span id="productTitle">Example Book</span>
        <span id="bylineInfo"><a href="/author">Jane Doe</a></span>
        <div id="detailBullets_feature_div">
          <ul>
            <li>Publisher : Example House (January 12, 2021)</li>
            <li>Language : English</li>
            <li>ISBN-13 : 9780306406157</li>
          </ul>
        </div>
        <span id="acrPopover" title="4.5 out of 5 stars"></span>
        <div id="bookDescription_feature_div">
          <p>An example description.</p>
        </div>
      </body>
    </html>
    """


def test_web_sources_amazon_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.amazon as amazon

    assert amazon is not None


def test_amazon_get_domain_and_asin_and_urls() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    assert plugin.get_domain_and_asin({"amazon": {"B00TEST123"}}) == ("com", "B00TEST123")
    assert plugin.get_domain_and_asin({"amazon_uk": ["B00UKTEST1"]}) == ("uk", "B00UKTEST1")
    assert plugin.get_book_url({"amazon_uk": "B00UKTEST1"}) == (
        "amazon_uk",
        "B00UKTEST1",
        "https://www.amazon.co.uk/dp/B00UKTEST1",
    )
    assert plugin.id_from_url("https://www.amazon.co.uk/dp/B00UKTEST1") == ("amazon_uk", "B00UKTEST1")


def test_amazon_create_query_prefers_asin_then_isbn_then_title_author() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    query, domain = plugin.create_query(
        title="Ignored",
        authors=["Ignored"],
        identifiers={"amazon": "B00TEST123"},
    )
    assert domain == "com"
    assert "field-keywords=B00TEST123" in query

    query, domain = plugin.create_query(identifiers={"isbn": {"9780306406157"}})
    assert domain == "com"
    assert "field-isbn=9780306406157" in query

    query, domain = plugin.create_query(title="The Great Gatsby", authors=["F. Scott Fitzgerald"], identifiers={})
    assert domain == "com"
    assert "field-title=Great+Gatsby" in query
    assert "field-author=Scott+Fitzgerald" in query


def test_amazon_parse_results_page_deduplicates_and_limits() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    html = """
    <div data-asin="B000AAAAAA"></div>
    <div data-asin="B000BBBBBB"></div>
    <a href="/dp/B000BBBBBB">dup</a>
    <a href="/gp/product/B000CCCCCC">c</a>
    """
    assert plugin.parse_results_page(html, result_count=2) == ["B000AAAAAA", "B000BBBBBB"]
    assert plugin.parse_results_page(html, result_count=5) == ["B000AAAAAA", "B000BBBBBB", "B000CCCCCC"]


def test_amazon_identify_by_asin_parses_metadata_and_caches() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    monkey_log = _Log()
    plugin._open_text_with_backoff = lambda log, abort, url, timeout, context: _sample_detail_html()

    out = queue.Queue()
    plugin.identify(
        log=monkey_log,
        result_queue=out,
        abort=Event(),
        identifiers={"amazon": "B00TEST123"},
    )

    mi = out.get_nowait()
    assert mi.title == "Example Book"
    assert mi.authors == ["Jane Doe"]
    assert mi.publisher == "Example House"
    assert mi.language == "eng"
    assert mi.get_identifiers()["amazon"] == "B00TEST123"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert mi.rating == 9.0
    assert plugin.cached_isbn_to_identifier("9780306406157") == "B00TEST123"
    assert plugin.cached_identifier_to_cover_url("B00TEST123") == "https://images.example/cover-main.jpg"


def test_amazon_identify_from_search_then_details() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()

    def _fake_open_text(log, abort, url, timeout, context):
        del log, abort, timeout, context
        if "/s/?" in url:
            return '<div data-asin="B000AAAAAA"></div><div data-asin="B000BBBBBB"></div>'
        return _sample_detail_html().replace("Example Book", "Book For " + url.split("/")[-1])

    plugin._open_text_with_backoff = _fake_open_text
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="Some Book",
        authors=["An Author"],
        identifiers={},
    )

    first = out.get_nowait()
    second = out.get_nowait()
    assert first.get_identifiers()["amazon"] == "B000AAAAAA"
    assert second.get_identifiers()["amazon"] == "B000BBBBBB"
    assert first.title.lower().startswith("book for b000aaaaaa")
    assert second.title.lower().startswith("book for b000bbbbbb")


def test_amazon_download_cover_uses_cached_url() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    plugin.cache_identifier_to_cover_url("B00TEST123", "https://images.example/cover-main.jpg")
    out = queue.Queue()
    plugin._open_bytes_with_backoff = lambda log, abort, url, timeout, context: b"cover-bytes"
    plugin.download_cover(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"amazon": "B00TEST123"},
    )
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"


def test_amazon_open_bytes_with_backoff_retries_transient(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

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

    browser = _Browser()
    plugin = Amazon()
    monkeypatch.setattr(plugin, "browser", lambda: browser)
    delays = []
    monkeypatch.setattr(plugin, "_wait_for_backoff", lambda abort, delay: delays.append(delay) or False)
    log = _Log()

    payload = plugin._open_bytes_with_backoff(
        log=log,
        abort=Event(),
        url="https://www.amazon.com/dp/B00TEST123",
        timeout=10,
        context="unit-test",
    )
    assert payload == b"ok"
    assert browser.calls == 3
    assert len(delays) == 2
    assert any(level == "warning" and "retrying with backoff" in " ".join(map(str, parts)) for level, parts in log.events)


def test_amazon_open_bytes_with_backoff_non_retryable_raises(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
            del url, timeout
            raise ValueError("bad response")

    plugin = Amazon()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    with pytest.raises(ValueError):
        plugin._open_bytes_with_backoff(
            log=_Log(),
            abort=Event(),
            url="https://www.amazon.com/dp/B00TEST123",
            timeout=10,
            context="unit-test",
        )


def test_amazon_low_level_helpers_handle_odd_inputs() -> None:
    import LiuXin_alpha.metadata.web_sources.amazon as amazon

    class BadText:
        def __str__(self):
            raise RuntimeError("cannot stringify")

    assert amazon._as_text(b"Caf\xc3\xa9") == "Caf\u00e9"
    assert amazon._as_text(BadText()) == ""
    assert amazon._first({"first": "ignored"}) == "first"
    assert amazon._first(item for item in ["value"]) == "value"
    assert amazon._first_identifier_value([], "isbn") is None
    assert amazon._safe_isbn_from_identifiers({"isbn": ["bad", "9780306406157"]}) is None
    assert amazon._safe_isbn_from_identifiers({"isbn13": b"9780306406157"}) == "9780306406157"
    assert amazon._canonicalize_language("") is None
    assert amazon._canonicalize_language("Unknown-Language") == "unknown"
    assert amazon._parse_pubdate("") is None
    assert amazon._parse_pubdate("January 2nd, 2021") == datetime(2021, 1, 2)
    assert amazon._parse_pubdate("Published (2020)") == datetime(2020, 6, 15)
    assert amazon._parse_pubdate("Published (not a date)") is None


def test_amazon_domain_settings_url_parsing_and_query_edges() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    plugin.prefs["domain"] = "bogus"
    assert plugin._preferred_domain() == "com"
    assert plugin.create_query(title=None, authors=None, identifiers={}) == (None, None)

    plugin.prefs["domain"] = "uk"
    committed = []

    class Widget:
        @staticmethod
        def commit():
            committed.append(True)

    plugin.save_settings(Widget())
    assert committed == [True]
    assert "identifier:amazon_uk" in plugin.touched_fields

    assert plugin.get_domain_and_asin([]) == (None, None)
    assert plugin.get_domain_and_asin({"amazon_fr": ""}) == (None, None)
    assert plugin.get_domain_and_asin({"amazon_mars": "B00UNKNOWN"}) == (None, None)
    assert plugin.get_book_url({}) is None
    assert plugin.get_book_url_name("amazon", "B00TEST123", "https://example.invalid") == "Amazon.com"
    assert plugin.get_book_url_name("amazon_de", "B00TEST123", "https://example.invalid") == "Amazon.de"
    assert plugin.id_from_url(12345) is None
    assert plugin.id_from_url("https://example.invalid/dp/B00TEST123") is None
    assert plugin.id_from_url("https://www.amazon.com/no-asin") is None
    assert plugin.id_from_url("https://www.amazon.co.jp/gp/product/B00JPTEST1") == ("amazon_jp", "B00JPTEST1")
    assert plugin.id_from_url("https://www.amazon.com.br/dp/B00BRTEST1") == ("amazon_br", "B00BRTEST1")
    assert plugin.id_from_url("https://www.amazon.de/dp/B00DETEST1") == ("amazon_de", "B00DETEST1")

    query, domain = plugin.create_query(title="Livro Teste", authors=["Ana Maria"], identifiers={}, domain="br")
    assert domain == "br"
    assert "search-alias=digital-text" in query

    query, domain = plugin.create_query(title="日本語の本", authors=None, identifiers={}, domain="jp")
    assert domain == "jp"
    assert "__mk_ja_JP=" in query

    query, domain = plugin.create_query(title="Fallback", authors=None, identifiers={}, domain="bad")
    assert domain == "com"
    assert "field-title=Fallback" in query


def test_amazon_clean_downloaded_metadata_and_cover_cache_fallbacks() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    mi = calibreMetaInformation("the sample book", ["jane doe"])
    mi.tags = ["science fiction"]
    mi.set_identifier("isbn13", "9780306406157")

    plugin.clean_downloaded_metadata(mi)

    assert mi.title == "The Sample Book"
    assert mi.authors == ["Jane Doe"]
    assert mi.tags == ["Science Fiction"]
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert plugin.get_cached_cover_url({}) is None

    plugin.cache_isbn_to_identifier("9780306406157", "B00TEST123")
    plugin.cache_identifier_to_cover_url("B00TEST123", "https://images.example/from-isbn.jpg")
    assert plugin.get_cached_cover_url({"isbn": "9780306406157"}) == "https://images.example/from-isbn.jpg"
    assert plugin.get_cached_cover_url({"amazon": "B00TEST123"}) == "https://images.example/from-isbn.jpg"


def test_amazon_detail_parser_uses_json_ld_meta_fallbacks_and_cover_variants() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon, CaptchaError

    plugin = Amazon()
    json_html = """
    <html>
      <head>
        <meta property="og:title" content="json title: Amazon.de" />
        <script type="application/ld+json">
          {"author": [{"name": "Anna Autorin"}, "Ignored Text"], "description": "Line <b>one</b>"}
        </script>
      </head>
      <body>
        <span id="landingImage" data-old-hires="https://images.example/hires.jpg"></span>
        <span title="4,2 von 5 Sternen"></span>
        <ul>
          <li>Verlag : Verlagshaus (2020-02-03)</li>
          <li>Sprache : German</li>
          <li>ISBN-10 : 0306406152</li>
        </ul>
      </body>
    </html>
    """
    mi = plugin._parse_metadata_from_details(json_html, domain="de", asin="B00DETEST1", relevance=3)

    assert mi.title == "json title"
    assert mi.authors == ["Anna Autorin", "Ignored Text"]
    assert mi.publisher == "Verlagshaus"
    assert mi.pubdate == datetime(2020, 2, 3)
    assert mi.language == "deu"
    assert mi.rating == 8.4
    assert mi.comments == "<p>Line one</p>"
    assert mi.get_identifiers()["amazon_de"] == "B00DETEST1"
    assert mi.get_identifiers()["isbn"] == "0306406152"
    assert plugin.cached_identifier_to_cover_url("B00DETEST1") == "https://images.example/hires.jpg"

    meta_author_html = """
    <html>
      <head>
        <title>Fallback Title: Amazon.com</title>
        <meta name="author" content="Meta Author" />
      </head>
      <body>
        <img id="landingImage" src="https://images.example/src.jpg" />
        <span>4.8 out of 5 stars</span>
      </body>
    </html>
    """
    mi = plugin._parse_metadata_from_details(meta_author_html, domain="com", asin="B00METAAUT", relevance=0)
    assert mi.title == "Fallback Title"
    assert mi.authors == ["Meta Author"]
    assert mi.rating == 9.6
    assert plugin.cached_identifier_to_cover_url("B00METAAUT") == "https://images.example/src.jpg"

    assert plugin._parse_cover_url('<img id="imgBlkFront" src="https://images.example/front.jpg" />') == (
        "https://images.example/front.jpg"
    )
    assert plugin._parse_rating("4,6 颗星，最多 5") == 9.2
    assert plugin._parse_rating("not a rating") is None

    with pytest.raises(CaptchaError):
        plugin._parse_metadata_from_details("validateCaptcha", domain="com", asin="B00CAPTCHA", relevance=0)


def test_amazon_search_identify_retry_and_error_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    log = _Log()
    calls = []

    def fake_open_text(log, abort, url, timeout, context):
        del log, abort, timeout
        calls.append((context, url))
        if context == "Amazon search":
            if "field-isbn" in url:
                return ""
            return '<a href="/dp/B000AAAAAA">Book</a><a href="/dp/B000BBBBBB">Book</a>'
        if url.endswith("B000AAAAAA"):
            return ""
        if url.endswith("B000BBBBBB"):
            return _sample_detail_html().replace("Example Book", "Retry Book")
        raise AssertionError(url)

    plugin._open_text_with_backoff = fake_open_text
    out = queue.Queue()
    plugin.identify(
        log=log,
        result_queue=out,
        abort=Event(),
        title="Retry Book",
        authors=["Jane Doe"],
        identifiers={"isbn": "9780306406157"},
    )

    assert any("field-isbn" in url for _context, url in calls)
    assert any("field-title=Retry+Book" in url for _context, url in calls)
    assert out.get_nowait().title == "Retry Book"
    assert any("retrying with title/author query" in " ".join(map(str, parts)) for _level, parts in log.events)

    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, identifiers={"amazon": "B00TEST123"})
    assert out.empty()

    plugin._open_text_with_backoff = lambda **kwargs: "validateCaptcha"
    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), identifiers={"amazon": "B00CAPTCHA"})
    assert out.empty()
    assert any(level == "error" for level, _parts in log.events)

    plugin._open_text_with_backoff = lambda **kwargs: "<html><title>Bad</title></html>"
    plugin._parse_metadata_from_details = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("parse failed"))
    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), identifiers={"amazon": "B00BROKEN1"})
    assert out.empty()
    assert any(level == "exception" for level, _parts in log.events)


def test_amazon_download_cover_discovers_from_identify_and_handles_empty_failures() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    log = _Log()
    out = queue.Queue()

    def fake_identify(log, rq, abort, title=None, authors=None, identifiers=None, timeout=30):
        del log, abort, title, authors, identifiers, timeout
        mi = calibreMetaInformation("Cover Book", ["Jane Doe"])
        mi.set_identifier("amazon", "B00COVER1")
        plugin.cache_identifier_to_cover_url("B00COVER1", "https://images.example/discovered.jpg")
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
    assert any("No Amazon cover found" in " ".join(map(str, parts)) for _level, parts in log.events)

    plugin.cache_identifier_to_cover_url("B00EMPTY1", "https://images.example/empty.jpg")
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"amazon": "B00EMPTY1"})
    assert out.empty()

    def raise_download(**kwargs):
        raise OSError("download failed")

    plugin._open_bytes_with_backoff = raise_download
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"amazon": "B00EMPTY1"})
    assert out.empty()


def test_amazon_open_text_decodes_and_abort_backoff_returns_empty() -> None:
    from LiuXin_alpha.metadata.web_sources.amazon import Amazon

    plugin = Amazon()
    plugin._open_bytes_with_backoff = lambda **kwargs: b"Caf\xc3\xa9"
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == (
        "Caf\u00e9"
    )
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == ""

    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0.01) is True
