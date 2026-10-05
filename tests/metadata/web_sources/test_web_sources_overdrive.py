"""
Verify OverDrive identifiers, search/detail parsing, retries and covers.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources overdrive through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
"""
from __future__ import annotations

import queue
from datetime import datetime
from threading import Event

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


class _Log:
    """
    Provide the Log test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Log through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


def _sample_search_html() -> str:
    """
    Perform the sample search html test-helper operation with deterministic inputs.

    Example:
        Exercise sample search html through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Perform the sample detail html test-helper operation with deterministic inputs.

    Example:
        Exercise sample detail html through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param media_id: Value supplied for media id in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Verify web sources overdrive import smoke.

    Example:
        Exercise test web sources overdrive import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.overdrive as overdrive

    assert overdrive is not None


def test_overdrive_get_book_url_and_id_from_url() -> None:
    """
    Verify overdrive get book url and id from url.

    Example:
        Exercise test overdrive get book url and id from url through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    assert plugin.get_book_url({"overdrive": {"1234567"}}) == (
        "overdrive",
        "1234567",
        "https://www.overdrive.com/media/1234567",
    )
    assert plugin.id_from_url("https://www.overdrive.com/media/1234567") == ("overdrive", "1234567")


def test_overdrive_extract_ids_from_search_html() -> None:
    """
    Verify overdrive extract ids from search html.

    Example:
        Exercise test overdrive extract ids from search html through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.overdrive import _extract_overdrive_ids_from_search_html

    assert _extract_overdrive_ids_from_search_html(_sample_search_html()) == ["1234567", "89ABCDEF"]


def test_overdrive_metadata_from_detail_html_parses_fields() -> None:
    """
    Verify overdrive metadata from detail html parses fields.

    Example:
        Exercise test overdrive metadata from detail html parses fields through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify overdrive identify by id.

    Example:
        Exercise test overdrive identify by id through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify overdrive identify search then details.

    Example:
        Exercise test overdrive identify search then details through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()

    def _open(log, abort, url, timeout, context):
        """
        Perform the open test-helper operation with deterministic inputs.

        Example:
            Exercise test overdrive identify search then details.open through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param context: Value supplied for context in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify overdrive download cover uses cache.

    Example:
        Exercise test overdrive download cover uses cache through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify overdrive open bytes with backoff retries transient.

    Example:
        Exercise test overdrive open bytes with backoff retries transient through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    class _Transient(Exception):
        """
        Provide the Transient test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test overdrive open bytes with backoff retries transient.Transient through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test overdrive open bytes with backoff retries transient.Transient.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 503

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test overdrive open bytes with backoff retries transient.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test overdrive open bytes with backoff retries transient.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"ok"

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test overdrive open bytes with backoff retries transient.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
        """
        def __init__(self):
            """
            Initialize the Browser test double.

            Example:
                Exercise test overdrive open bytes with backoff retries transient.Browser.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


            :return: None; the function records state or raises through its assertions.
            """
            self.calls = 0

        def open_novisit(self, url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test overdrive open bytes with backoff retries transient.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify overdrive import web source module.

    Example:
        Exercise test overdrive import web source module through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("overdrive")
    assert hasattr(mod, "OverDrive")


def test_overdrive_low_level_helpers_handle_odd_inputs() -> None:
    """
    Verify overdrive low level helpers handle odd inputs.

    Example:
        Exercise test overdrive low level helpers handle odd inputs through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.overdrive as overdrive

    class BadText:
        """
        Provide the BadText test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test overdrive low level helpers handle odd inputs.BadText through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py
        """
        def __str__(self):
            """
            Perform the str test-helper operation with deterministic inputs.

            Example:
                Exercise test overdrive low level helpers handle odd inputs.BadText.str through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


            :return: The deterministic value, row, identity or collection described above.
            """
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
    """
    Verify overdrive url query cache and search edge paths.

    Example:
        Exercise test overdrive url query cache and search edge paths through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify overdrive metadata parser uses fallbacks and defaults.

    Example:
        Exercise test overdrive metadata parser uses fallbacks and defaults through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify overdrive metadata parser ignores invalid optional fields.

    Example:
        Exercise test overdrive metadata parser ignores invalid optional fields through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify overdrive identify retry skip and abort paths.

    Example:
        Exercise test overdrive identify retry skip and abort paths through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    log = _Log()
    calls = []

    def fake_open(log, abort, url, timeout, context):
        """
        Perform the fake open test-helper operation with deterministic inputs.

        Example:
            Exercise test overdrive identify retry skip and abort paths.fake open through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param context: Value supplied for context in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
    """
    Verify overdrive download cover discovers from identify and handles failures.

    Example:
        Exercise test overdrive download cover discovers from identify and handles failures through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.overdrive import OverDrive

    plugin = OverDrive()
    log = _Log()
    out = queue.Queue()

    def fake_identify(log, rq, abort, title=None, authors=None, identifiers=None, timeout=30):
        """
        Perform the fake identify test-helper operation with deterministic inputs.

        Example:
            Exercise test overdrive download cover discovers from identify and handles failures.fake identify through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param log: Value supplied for log in the focused test operation.
        :param rq: Value supplied for rq in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
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
        """
        Perform the raise download test-helper operation with deterministic inputs.

        Example:
            Exercise test overdrive download cover discovers from identify and handles failures.raise download through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("download failed")

    plugin._open_bytes_with_backoff = raise_download
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"overdrive": "1234567"})
    assert out.empty()


def test_overdrive_open_text_decodes_and_abort_backoff_returns_empty() -> None:
    """
    Verify overdrive open text decodes and abort backoff returns empty.

    Example:
        Exercise test overdrive open text decodes and abort backoff returns empty through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_overdrive.py


    :return: None; the function records state or raises through its assertions.
    """
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
