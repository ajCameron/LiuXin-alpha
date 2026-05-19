from __future__ import annotations

import queue
from threading import Event


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


class _Browser:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.requests = []

    def open_novisit(self, url, timeout=30):
        self.requests.append((url, timeout))
        return _Response(self.payload)


def _detail_html(sku: str, title: str = "XQuery from the Experts") -> str:
    return f"""
    <html>
      <head>
        <title>{title} | Edelweiss+</title>
      </head>
      <body>
        <div id="title_{sku}">{title}</div>
        <div class="pev_contributor" title="Howard Katz, Don Chamberlin"></div>
        <div class="pev_sku">9780306406157, 0306406152</div>
        <div class="pev_categories">Computers / XML, Databases</div>
        <div class="headerPublisher">Addison-Wesley</div>
        <div class="pev_shipDate">On Sale Date: August 22, 2003</div>
        <div class="bgdColorCommunity" style="width: 36px; max-width: 40px"></div>
        <div id="desc_summary{sku}-content">
          <p>Résumé <a href="https://example.org">link</a><!-- comment --></p>
          <noscript>ignore</noscript>
        </div>
        <img class="title-image" src="https://images.example/jacket_covers/medium/{sku}.jpg" />
      </body>
    </html>
    """


def test_web_sources_edelweiss_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.edelweiss as edelweiss

    assert edelweiss is not None


def test_edelweiss_get_book_url_and_cached_cover_url() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    assert plugin.get_book_url({}) is None
    assert plugin.get_cached_cover_url({}) is None
    assert plugin.get_book_url({"edelweiss": {"0321180607"}}) == (
        "edelweiss",
        "0321180607",
        "https://www.edelweiss.plus/#sku=0321180607&page=1",
    )

    plugin.cache_isbn_to_identifier("9780306406157", "0321180607")
    plugin.cache_identifier_to_cover_url("0321180607", "https://images.example/flyout.jpg")
    assert plugin.get_cached_cover_url({"isbn": "9780306406157"}) == "https://images.example/flyout.jpg"


def test_edelweiss_create_query_prefers_isbn_then_title_and_author(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.edelweiss as edelweiss

    Edelweiss = edelweiss.Edelweiss

    plugin = Edelweiss()
    monkeypatch.setattr(edelweiss.time, "time", lambda: 12345)
    assert plugin.create_query(log=_Log(), identifiers={}) is None

    query = plugin.create_query(log=_Log(), identifiers={"isbn": "9780306406157"})
    assert "q=9780306406157" in query
    assert "_=12345" in query

    query = plugin.create_query(
        log=_Log(),
        title="The Husband's Secret",
        authors=["Liane Moriarty"],
        identifiers={},
    )
    assert "q=Husband%27s+Secret+Liane+Moriarty" in query


def test_edelweiss_helper_normalization_edges() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import (
        _as_text,
        _first,
        _first_identifier_value,
        _identifier_text,
        _normalize_cover_url,
        _sanitize_comments_html,
        _split_csvish,
        _strip_tags,
    )

    class BadString:
        def __str__(self):
            raise RuntimeError("broken")

    assert _as_text(b"caf\xc3\xa9") == "café"
    assert _as_text(BadString()) == ""
    assert _first(None) is None
    assert _first("abc") == "abc"
    assert _first({"first": "ignored"}) == "first"
    assert _first({}) is None
    assert _first(item for item in ["one"]) == "one"
    assert _first(7) == 7
    assert _first_identifier_value([], "isbn") is None
    assert _identifier_text(None) == ""
    assert _identifier_text(" none ") == ""
    assert _identifier_text(" 0321180607 ") == "0321180607"
    assert _strip_tags("<div>A<br>B</div><li>C</li>") == "A\nB\nC"
    assert _normalize_cover_url("") is None
    assert _normalize_cover_url("data:image/png;base64,AAAA") is None
    assert _normalize_cover_url("//cdn.example/jacket_covers/thumbnail/a.jpg") == (
        "https://cdn.example/jacket_covers/flyout/a.jpg"
    )
    assert _normalize_cover_url("/jacket_covers/medium/a.jpg") == (
        "https://www.edelweiss.plus/jacket_covers/flyout/a.jpg"
    )
    assert _split_csvish("A and B & C, D") == ["A", "B", "C", "D"]

    sanitized = _sanitize_comments_html(
        '<div class="x">Keep <a href="https://example.org" data-x="1">link</a></div>'
        "<script>bad()</script><style>.bad{}</style><noscript>hidden</noscript><!-- drop -->"
    )
    assert sanitized == "<div>Keep <span>link</span></div>"


def test_edelweiss_retry_and_open_text_helpers() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    policy = plugin._retry_policy()
    assert policy.attempts == plugin.HTTP_RETRY_ATTEMPTS
    assert plugin._retry_backoff(1) == plugin.HTTP_RETRY_BASE_SECONDS
    assert plugin._retry_backoff(10) == plugin.HTTP_RETRY_MAX_SECONDS
    assert plugin._wait_for_backoff(Event(), 0) is False
    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0) is True

    browser = _Browser(b"caf\xc3\xa9")
    plugin.browser = lambda: browser
    assert (
        plugin._open_bytes_with_backoff(_Log(), Event(), "https://example.org", 7, "Edelweiss detail")
        == b"caf\xc3\xa9"
    )
    assert browser.requests == [("https://example.org", 7)]

    plugin._open_bytes_with_backoff = lambda **kwargs: b"caf\xc3\xa9"
    assert plugin._open_text_with_backoff(_Log(), Event(), "https://example.org", 7, "Edelweiss detail") == "café"
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    assert plugin._open_text_with_backoff(_Log(), Event(), "https://example.org", 7, "Edelweiss detail") == ""


def test_edelweiss_parse_skus_from_search_payload() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    payload = """
    window.items = ["0321180607", {"sku": "9780312621360"}];
    <div data-sku="1111111111"></div>
    <a href="/ProductDetailPage.aspx?sku=2222222222">x</a>
    """
    skus = plugin._parse_skus_from_search_payload(payload)
    assert skus == ["0321180607", "9780312621360", "2222222222", "1111111111"]


def test_edelweiss_parse_skus_from_malformed_search_payload() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    payload = """
    window.items = [not json];
    {"sku": "JSONISH"}
    <a href="/ProductDetailPage.aspx?sku=QUERY"></a>
    <div id="priority-PRIORITY"></div>
    <div id="title-TITLE"></div>
    """
    assert plugin._parse_skus_from_search_payload(payload) == ["JSONISH", "QUERY", "PRIORITY", "TITLE"]


def test_edelweiss_metadata_from_detail_html_parses_fields_and_caches() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    mi = plugin._metadata_from_detail_html(_detail_html("0321180607"), sku="0321180607", relevance=2)

    assert mi.title == "XQuery from the Experts"
    assert mi.authors == ["Howard Katz", "Don Chamberlin"]
    assert mi.publisher == "Addison-Wesley"
    assert mi.get_identifiers()["edelweiss"] == "0321180607"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert "Computers" in (mi.tags or [])
    assert mi.rating == 9.0
    assert "Résumé" in (mi.comments or "")
    assert "href=" not in (mi.comments or "")
    assert mi.pubdate.year == 2003
    assert plugin.cached_isbn_to_identifier("9780306406157") == "0321180607"
    assert plugin.cached_identifier_to_cover_url("0321180607").endswith("/jacket_covers/flyout/0321180607.jpg")


def test_edelweiss_metadata_from_detail_html_uses_parser_fallbacks() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    html = """
    <html>
      <head>
        <title>Fallback Title - Edelweiss Catalog</title>
        <meta property="og:image" content="//cdn.example/jacket_covers/thumbnail/fallback.jpg" />
      </head>
      <body>
        <span title="Ada Lovelace & Charles Babbage" class="pev_contributor"></span>
        <div class="contributor">Ada Lovelace (Editor), Grace Hopper</div>
        <div class="bisac">&Computers > Programming / History</div>
        <span class="supplier">Publisher: Vintage Press</span>
        <span class="shipDate">Coming in 2024</span>
        <span>4.5 out of 5</span>
        <section id="pd-general-overview-content"><p>Overview <a href="https://example.org">link</a></p></section>
        <section id="pd-general-contributor-content"><p>Contributor bio</p></section>
        <section id="pd-general-quotes-content"><p>Quote</p></section>
      </body>
    </html>
    """
    mi = plugin._metadata_from_detail_html(html, sku="FALLBACK", relevance=1)

    assert mi.title == "Fallback Title"
    assert mi.authors == ["Ada Lovelace", "Charles Babbage", "Grace Hopper"]
    assert mi.tags == ["Computers", "Programming", "History"]
    assert mi.publisher == "Vintage Press"
    assert mi.pubdate.year == 2024
    assert mi.rating == 9.0
    assert "Overview <span>link</span>" in mi.comments
    assert "Contributor bio" in mi.comments
    assert "Quote" in mi.comments
    assert plugin.cached_identifier_to_cover_url("FALLBACK") == "https://cdn.example/jacket_covers/flyout/fallback.jpg"


def test_edelweiss_metadata_from_detail_html_defaults_when_sparse() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    mi = plugin._metadata_from_detail_html("<html><body></body></html>", sku="EMPTY", relevance=0)

    assert mi.title == "Unknown"
    assert mi.authors == ["Unknown"]
    assert mi.get_identifiers() == {"edelweiss": "EMPTY"}
    assert mi.has_cover is False
    assert plugin.cached_identifier_to_cover_url("EMPTY") is None


def test_edelweiss_parser_invalid_and_clamped_values() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    assert plugin._parse_title('<div id="title_SKU"></div>', "SKU") is None
    assert plugin._parse_publisher('<span class="publisher"></span>') is None
    assert plugin._parse_pubdate('<span class="shipDate">not a date</span>') is None
    assert plugin._parse_rating('<span style="width: 1.2.3px; max-width: 4px"></span>') is None
    assert plugin._parse_rating("6 out of 5") == 10.0
    assert plugin._parse_rating("0/5") == 0.0
    assert plugin._parse_cover_url("<img src='data:image/png;base64,AAAA' />") is None


def test_edelweiss_identify_by_sku() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    plugin._open_text_with_backoff = lambda log, abort, url, timeout, context: _detail_html("0321180607")
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"edelweiss": "0321180607"},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["edelweiss"] == "0321180607"
    assert mi.title == "XQuery from the Experts"


def test_edelweiss_identify_search_then_detail() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()

    def _fake_open(log, abort, url, timeout, context):
        del log, abort, timeout
        if "search" in context.lower():
            return 'window.items = ["1111111111", "2222222222"];'
        if "1111111111" in context or "1111111111" in url:
            return _detail_html("1111111111", title="Book One")
        return _detail_html("2222222222", title="Book Two")

    plugin._open_text_with_backoff = _fake_open
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="Book",
        authors=["Author"],
        identifiers={},
    )
    first = out.get_nowait()
    second = out.get_nowait()
    assert first.get_identifiers()["edelweiss"] == "1111111111"
    assert second.get_identifiers()["edelweiss"] == "2222222222"
    assert first.title == "Book One"
    assert second.title == "Book Two"


def test_edelweiss_identify_skus_empty_search_and_isbn_fallback() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    assert plugin._identify_skus(_Log(), Event(), None, None, {}, 1) == []

    plugin._open_text_with_backoff = lambda **kwargs: ""
    assert plugin._identify_skus(_Log(), Event(), "Book", ["Author"], {}, 1) == []

    log = _Log()
    responses = iter(["window.items = [];", 'window.items = ["FALLBACK"];'])

    def _fake_open(**kwargs):
        return next(responses)

    plugin._open_text_with_backoff = _fake_open
    assert plugin._identify_skus(log, Event(), "Book", ["Author"], {"isbn": "9780306406157"}, 1) == ["FALLBACK"]
    assert any("retrying title/author" in str(parts) for level, parts in log.events if level == "info")


def test_edelweiss_identify_dedupes_limits_and_logs_detail_errors() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    plugin._identify_skus = lambda **kwargs: ["A", "", "A", "B", "C", "D", "E", "F"]

    seen_urls = []

    def _fake_open(**kwargs):
        url = kwargs["url"]
        seen_urls.append(url)
        sku = url.split("sku=", 1)[1].split("&", 1)[0]
        if sku == "B":
            return ""
        if sku == "C":
            raise RuntimeError("bad detail")
        return _detail_html(sku, title=f"Title {sku}")

    plugin._open_text_with_backoff = _fake_open
    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), title="Book", authors=["Author"], identifiers={})

    titles = [out.get_nowait().title for _ in range(out.qsize())]
    assert titles == ["Title A", "Title D", "Title E"]
    assert len(seen_urls) == 5
    assert all("F" not in url for url in seen_urls)
    assert any(level == "exception" for level, parts in log.events)


def test_edelweiss_identify_returns_on_abort_and_stops_between_details() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    called = False

    def _should_not_call(**kwargs):
        nonlocal called
        called = True
        return []

    abort = Event()
    abort.set()
    plugin._identify_skus = _should_not_call
    plugin.identify(log=_Log(), result_queue=queue.Queue(), abort=abort, identifiers={})
    assert called is False

    plugin._identify_skus = lambda **kwargs: []
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title="Book", identifiers={})
    assert out.empty()

    plugin._identify_skus = lambda **kwargs: ["A", "B"]
    abort = Event()

    def _fake_open(**kwargs):
        abort.set()
        return _detail_html("A", title="Only A")

    plugin._open_text_with_backoff = _fake_open
    plugin.identify(log=_Log(), result_queue=out, abort=abort, title="Book", identifiers={})
    assert out.get_nowait().title == "Only A"
    assert out.empty()


def test_edelweiss_download_cover_uses_cached_cover_url() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    plugin.cache_identifier_to_cover_url("0321180607", "https://images.example/cover.jpg")
    plugin._open_bytes_with_backoff = lambda log, abort, url, timeout, context: b"cover-bytes"

    out = queue.Queue()
    plugin.download_cover(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"edelweiss": "0321180607"},
    )
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"


def test_edelweiss_download_cover_discovers_cover_via_identify() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()

    def _fake_identify(log, result_queue, abort, title=None, authors=None, identifiers=None, timeout=30):
        del log, abort, title, authors, identifiers, timeout
        result_queue.put(plugin._metadata_from_detail_html(_detail_html("DISCOVERED"), "DISCOVERED", 0))

    plugin.identify = _fake_identify
    plugin._open_bytes_with_backoff = lambda **kwargs: b"discovered-cover"
    out = queue.Queue()
    log = _Log()
    plugin.download_cover(log=log, result_queue=out, abort=Event(), title="Book", identifiers={})

    assert out.get_nowait()[1] == b"discovered-cover"
    assert any("No cached cover found" in str(parts) for level, parts in log.events if level == "info")


def test_edelweiss_download_cover_handles_abort_missing_empty_and_errors() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    out = queue.Queue()
    log = _Log()
    plugin.identify = lambda *args, **kwargs: None
    plugin.download_cover(log=log, result_queue=out, abort=Event(), identifiers={})
    assert out.empty()
    assert any("No cover found" in str(parts) for level, parts in log.events if level == "info")

    plugin = Edelweiss()
    abort = Event()

    def _abort_identify(*args, **kwargs):
        abort.set()

    plugin.identify = _abort_identify
    plugin.download_cover(log=_Log(), result_queue=queue.Queue(), abort=abort, identifiers={})

    plugin = Edelweiss()
    plugin.cache_identifier_to_cover_url("SKU", "https://images.example/cover.jpg")
    abort = Event()
    abort.set()
    plugin._open_bytes_with_backoff = lambda **kwargs: b"unused"
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=abort, identifiers={"edelweiss": "SKU"})
    assert out.empty()

    plugin = Edelweiss()
    plugin.cache_identifier_to_cover_url("SKU", "https://images.example/cover.jpg")
    plugin._open_bytes_with_backoff = lambda **kwargs: (_ for _ in ()).throw(RuntimeError("download failed"))
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"edelweiss": "SKU"})
    assert out.empty()

    plugin = Edelweiss()
    plugin.cache_identifier_to_cover_url("SKU", "https://images.example/cover.jpg")
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"edelweiss": "SKU"})
    assert out.empty()


def test_edelweiss_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("edelweiss")
    assert hasattr(mod, "Edelweiss")
