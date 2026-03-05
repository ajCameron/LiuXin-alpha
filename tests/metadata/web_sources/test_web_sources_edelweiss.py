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
    assert plugin.get_book_url({"edelweiss": {"0321180607"}}) == (
        "edelweiss",
        "0321180607",
        "https://www.edelweiss.plus/#sku=0321180607&page=1",
    )

    plugin.cache_isbn_to_identifier("9780306406157", "0321180607")
    plugin.cache_identifier_to_cover_url("0321180607", "https://images.example/flyout.jpg")
    assert plugin.get_cached_cover_url({"isbn": "9780306406157"}) == "https://images.example/flyout.jpg"


def test_edelweiss_create_query_prefers_isbn_then_title_and_author() -> None:
    from LiuXin_alpha.metadata.web_sources.edelweiss import Edelweiss

    plugin = Edelweiss()
    query = plugin.create_query(log=_Log(), identifiers={"isbn": "9780306406157"})
    assert "q=9780306406157" in query

    query = plugin.create_query(
        log=_Log(),
        title="The Husband's Secret",
        authors=["Liane Moriarty"],
        identifiers={},
    )
    assert "q=Husband%27s+Secret+Liane+Moriarty" in query


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


def test_edelweiss_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("edelweiss")
    assert hasattr(mod, "Edelweiss")
