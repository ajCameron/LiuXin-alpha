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
