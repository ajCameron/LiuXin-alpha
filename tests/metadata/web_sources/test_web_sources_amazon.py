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
