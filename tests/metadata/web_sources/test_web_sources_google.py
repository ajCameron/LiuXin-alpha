"""
Verify Google Books query, JSON/feed parsing, caching and covers.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources google through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
"""
from __future__ import annotations

import hashlib
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

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


class _Response:
    """
    Provide the Response test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Response through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
    """
    def __init__(self, payload: bytes) -> None:
        """
        Initialize the Response test double.

        Example:
            Exercise Response.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.payload = payload

    def read(self) -> bytes:
        """
        Perform the read test-helper operation with deterministic inputs.

        Example:
            Exercise Response.read through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.payload


class _Browser:
    """
    Provide the Browser test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Browser through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
    """
    def __init__(self, payloads):
        """
        Initialize the Browser test double.

        Example:
            Exercise Browser.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param payloads: Value supplied for payloads in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.payloads = list(payloads)
        self.requests = []

    def open_novisit(self, url, timeout=30):
        """
        Perform the open novisit test-helper operation with deterministic inputs.

        Example:
            Exercise Browser.open novisit through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.requests.append((url, timeout))
        payload = self.payloads.pop(0)
        if isinstance(payload, Exception):
            raise payload
        return _Response(payload)


def _sample_item(google_id: str = "gid-1") -> dict:
    """
    Perform the sample item test-helper operation with deterministic inputs.

    Example:
        Exercise sample item through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param google_id: Value supplied for google id in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "id": google_id,
        "volumeInfo": {
            "title": "The Title",
            "subtitle": "The Subtitle",
            "authors": ["Alice Example", "Bob Example"],
            "description": "First sentence.Second sentence",
            "language": "en",
            "publisher": "Example House",
            "publishedDate": "2020-02-03",
            "categories": ["Fiction", "Sci,Fi"],
            "industryIdentifiers": [
                {"type": "ISBN_10", "identifier": "0306406152"},
                {"type": "ISBN_13", "identifier": "9780306406157"},
                {"type": "OCLC", "identifier": "123"},
            ],
            "imageLinks": {
                "thumbnail": "https://covers.example/cover.jpg",
            },
        },
    }


def _sample_feed_payload(google_id: str = "gid-feed") -> str:
    """
    Perform the sample feed payload test-helper operation with deterministic inputs.

    Example:
        Exercise sample feed payload through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param google_id: Value supplied for google id in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xmlns:dc="http://purl.org/dc/terms">
  <entry>
    <id>https://books.google.com/books/feeds/volumes/{google_id}</id>
    <title>Atom Title</title>
    <dc:title>Feed Title</dc:title>
    <dc:title>Feed Subtitle</dc:title>
    <dc:creator>Alice Feed</dc:creator>
    <dc:description>Feed description.One more</dc:description>
    <dc:language>en</dc:language>
    <dc:publisher>Feed House</dc:publisher>
    <dc:date>2021-04-05</dc:date>
    <dc:identifier>ISBN: 9780306406157</dc:identifier>
    <dc:identifier>OCLC: 12345</dc:identifier>
    <dc:subject>Fiction / Science, Fiction</dc:subject>
    <link rel="self" href="https://www.google.com/books/feeds/volumes/{google_id}" />
    <link rel="http://schemas.google.com/books/2008/thumbnail" href="https://covers.example/feed.jpg" />
  </entry>
</feed>"""


def test_web_sources_google_import_smoke() -> None:
    """
    Verify web sources google import smoke.

    Example:
        Exercise test web sources google import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google as google

    assert google is not None


def test_google_helper_edges_and_comment_formatting(monkeypatch) -> None:
    """
    Verify google helper edges and comment formatting.

    Example:
        Exercise test google helper edges and comment formatting through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google as google

    class BadString:
        """
        Provide the BadString test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google helper edges and comment formatting.BadString through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
        """
        def __str__(self):
            """
            Perform the str test-helper operation with deterministic inputs.

            Example:
                Exercise test google helper edges and comment formatting.BadString.str through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("broken")

    assert google._as_text(b"hello") == "hello"
    assert google._as_text(BadString()) == ""
    assert google._first(None) is None
    assert google._first("abc") == "abc"
    assert google._first(iter(["one"])) == "one"
    assert google._first([]) is None
    assert google._first(BadString()).__class__ is BadString
    assert google._first_identifier_value([], "isbn") is None
    assert google._clean_identifier_key(" OCLC: Number ") == "oclc_number"
    assert google.pretty_google_books_comments(None) is None
    assert google.pretty_google_books_comments("One sentence.Second sentence") == (
        "<p>One sentence.</p>\n\n<p>Second sentence</p>"
    )

    assert google._safe_isbn({"isbn": "bad", "isbn13": "9780306406157"}) == "9780306406157"
    monkeypatch.setattr(google, "check_isbn", lambda raw: (_ for _ in ()).throw(RuntimeError("bad isbn")))
    assert google._safe_isbn({"isbn": "9780306406157", "isbn13": "9780306406157"}) is None


def test_google_get_book_url_and_id_from_url() -> None:
    """
    Verify google get book url and id from url.

    Example:
        Exercise test google get book url and id from url through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    assert plugin.get_book_url({}) is None
    assert plugin.get_book_url({"google": "  "}) is None
    assert plugin.get_book_url({"google": {"abc123"}}) == (
        "google",
        "abc123",
        "https://books.google.com/books?id=abc123",
    )
    assert plugin.id_from_url("https://books.google.com/books?id=abc123") == ("google", "abc123")
    assert plugin.id_from_url("https://books.google.co.uk/books?id=abc123") == ("google", "abc123")
    assert plugin.id_from_url("https://example.org/books?id=abc123") is None
    assert plugin.id_from_url("https://books.google.com/books") is None


def test_google_id_from_url_handles_parse_errors(monkeypatch) -> None:
    """
    Verify google id from url handles parse errors.

    Example:
        Exercise test google id from url handles parse errors through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google as google

    monkeypatch.setattr(google, "urlparse", lambda raw: (_ for _ in ()).throw(RuntimeError("bad url")))
    assert google.GoogleBooks().id_from_url("https://books.google.com/books?id=abc123") is None


def test_google_create_query_prefers_isbn_and_handles_iterable_identifiers() -> None:
    """
    Verify google create query prefers isbn and handles iterable identifiers.

    Example:
        Exercise test google create query prefers isbn and handles iterable identifiers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    assert plugin.create_query(title="Ignored", authors=["Ignored"], identifiers={"isbn": {"9780306406157"}}) == (
        "isbn:9780306406157"
    )

    query = plugin.create_query(
        title="The Great Gatsby",
        authors=["F. Scott Fitzgerald"],
        identifiers={},
    )
    assert "intitle:Great" in query
    assert "inauthor:Fitzgerald" in query

    assert plugin.create_query(title=None, authors=None, identifiers={}) is None
    assert plugin.create_query(title="Only Title", authors=None, identifiers={}) == "intitle:Only intitle:Title"
    assert plugin.create_query(title=None, authors=["Only Author"], identifiers={}) == (
        "inauthor:Only inauthor:Author"
    )


def test_google_api_url_request_and_retry_helpers(monkeypatch) -> None:
    """
    Verify google api url request and retry helpers.

    Example:
        Exercise test google api url request and retry helpers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google as google

    monkeypatch.setenv("GOOGLE_BOOKS_API_KEY", "KEY")
    plugin = google.GoogleBooks()
    assert plugin.google_api_key == "KEY"
    assert plugin._api_params(q="hello", empty="", missing=None) == {"q": "hello", "key": "KEY"}
    assert plugin._build_api_url(path="/vol/1", q="hello world").endswith(
        "/vol%2F1?q=hello+world&key=KEY"
    )

    browser = _Browser([b'{"ok": true}'])
    monkeypatch.setattr(plugin, "browser", lambda: browser)
    assert plugin._request_json(path="/vol/1", timeout=9, q="hello") == {"ok": True}
    assert browser.requests[0][1] == 9
    assert "/vol%2F1?" in browser.requests[0][0]

    policy = plugin._retry_policy()
    assert policy.attempts == plugin.HTTP_RETRY_ATTEMPTS
    assert plugin._retry_backoff(1) == plugin.HTTP_RETRY_BASE_SECONDS
    assert plugin._retry_backoff(10) == plugin.HTTP_RETRY_MAX_SECONDS
    assert plugin._wait_for_backoff(Event(), 0) is False
    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0) is True


def test_google_item_to_metadata_parses_volume_info(monkeypatch) -> None:
    """
    Verify google item to metadata parses volume info.

    Example:
        Exercise test google item to metadata parses volume info through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google as google

    plugin = google.GoogleBooks()
    monkeypatch.setattr(google, "parse_only_date", lambda raw: datetime(2020, 2, 3))
    mi = plugin._item_to_metadata(_sample_item())

    assert mi.title == "The Title: The Subtitle"
    assert mi.authors == ["Alice Example", "Bob Example"]
    assert mi.publisher == "Example House"
    assert mi.tags == ["Fiction", "Sci;Fi"]
    assert mi.language == "en"
    assert mi.pubdate == datetime(2020, 2, 3)
    assert mi.get_identifiers()["google"] == "gid-1"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert set(mi.all_isbns) == {"0306406152", "9780306406157"}
    assert mi.get_identifiers()["oclc"] == "123"
    assert mi.has_google_cover == "https://covers.example/cover.jpg"


def test_google_item_to_metadata_fallbacks_and_sparse_values(monkeypatch) -> None:
    """
    Verify google item to metadata fallbacks and sparse values.

    Example:
        Exercise test google item to metadata fallbacks and sparse values through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google as google

    plugin = google.GoogleBooks()
    monkeypatch.setattr(google, "parse_only_date", lambda raw: (_ for _ in ()).throw(ValueError("bad date")))
    item = {
        "volumeInfo": {
            "authors": "Solo Author",
            "publishedDate": "not a date",
            "categories": "Fiction, Mystery",
            "industryIdentifiers": {
                "type": "ISBN_13",
                "identifier": "bad",
            },
            "imageLinks": [],
        }
    }
    mi = plugin._item_to_metadata(item)
    assert mi.title == "Unknown"
    assert mi.authors == ["Solo Author"]
    assert mi.tags == ["Fiction; Mystery"]
    assert mi.get_identifiers() == {}
    assert mi.has_google_cover is None
    assert getattr(mi, "pubdate", None) is None

    mi = plugin._item_to_metadata({"volumeInfo": {"industryIdentifiers": ["skip", {"type": "", "identifier": ""}]}})
    assert mi.title == "Unknown"
    assert mi.authors == ["Unknown"]
    assert mi.get_identifiers() == {}
    assert mi.has_google_cover is None


def test_google_legacy_feed_helpers_parse_metadata(monkeypatch) -> None:
    """
    Verify google legacy feed helpers parse metadata.

    Example:
        Exercise test google legacy feed helpers parse metadata through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.google as google

    plugin = google.GoogleBooks()
    monkeypatch.setattr(google, "parse_only_date", lambda raw: datetime(2021, 4, 5))

    entries = plugin._feed_entries_from_payload(_sample_feed_payload("gid-feed"))
    assert len(entries) == 1

    mi = plugin._metadata_from_feed_entry(entries[0])
    assert mi.title == "Feed Title: Feed Subtitle"
    assert mi.authors == ["Alice Feed"]
    assert mi.publisher == "Feed House"
    assert mi.language == "en"
    assert mi.pubdate == datetime(2021, 4, 5)
    assert mi.tags == ["Fiction", "Science; Fiction"]
    assert mi.get_identifiers()["google"] == "gid-feed"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert mi.get_identifiers()["oclc"] == "12345"
    assert mi.has_google_cover == "https://covers.example/feed.jpg"


def test_google_cover_url_priority_and_missing_links() -> None:
    """
    Verify google cover url priority and missing links.

    Example:
        Exercise test google cover url priority and missing links through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    assert plugin._cover_url_from_volume_info({"imageLinks": ["not", "mapping"]}) is None
    assert plugin._cover_url_from_volume_info({"imageLinks": []}) is None
    assert plugin._cover_url_from_volume_info({"imageLinks": {}}) is None
    assert plugin._cover_url_from_volume_info({"imageLinks": {"smallThumbnail": "small.jpg"}}) == "small.jpg"
    assert plugin._cover_url_from_volume_info(
        {"imageLinks": {"thumbnail": "thumb.jpg", "large": "large.jpg"}}
    ) == "large.jpg"


def test_google_postprocess_caches_cover_and_isbn_mappings() -> None:
    """
    Verify google postprocess caches cover and isbn mappings.

    Example:
        Exercise test google postprocess caches cover and isbn mappings through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    mi = calibreMetaInformation("Title", ["Author"])
    mi.comments = "One sentence.Two sentence"
    mi.set_identifier("google", "gid-3")
    mi.all_isbns = ["0306406152", "9780306406157"]
    mi.has_google_cover = "https://covers.example/cover.jpg"

    out = plugin._postprocess_downloaded_google_metadata(mi, relevance=7)
    assert out is mi
    assert out.source_relevance == 7
    assert plugin.cached_isbn_to_identifier("0306406152") == "gid-3"
    assert plugin.cached_identifier_to_cover_url("gid-3") == "https://covers.example/cover.jpg"
    assert "<p>" in out.comments


def test_google_postprocess_handles_none_and_sparse_metadata() -> None:
    """
    Verify google postprocess handles none and sparse metadata.

    Example:
        Exercise test google postprocess handles none and sparse metadata through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    assert plugin._postprocess_downloaded_google_metadata(None) is None

    mi = calibreMetaInformation("Title", ["Author"])
    mi.set_identifier("google", "gid-no-cover")
    out = plugin._postprocess_downloaded_google_metadata(mi, relevance=3)
    assert out is mi
    assert out.source_relevance == 3
    assert plugin.cached_identifier_to_cover_url("gid-no-cover") is None

    mi = calibreMetaInformation("No Google", ["Author"])
    out = plugin._postprocess_downloaded_google_metadata(mi, relevance=4)
    assert out is mi
    assert out.source_relevance == 4


def test_google_identify_uses_google_identifier_lookup(monkeypatch) -> None:
    """
    Verify google identify uses google identifier lookup.

    Example:
        Exercise test google identify uses google identifier lookup through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "_request_json", lambda path="", timeout=30, **params: _sample_item("gid-lookup"))

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"google": "gid-lookup"},
    )

    mi = out.get_nowait()
    assert mi.get_identifiers()["google"] == "gid-lookup"
    assert mi.title.startswith("The Title")


def test_google_identify_guard_and_empty_payload_paths(monkeypatch) -> None:
    """
    Verify google identify guard and empty payload paths.

    Example:
        Exercise test google identify guard and empty payload paths through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    called = {"request": False}

    def _request_json(*args, **kwargs):
        """
        Perform the request json test-helper operation with deterministic inputs.

        Example:
            Exercise test google identify guard and empty payload paths.request json through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param args: Positional values forwarded by the test double.
        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        called["request"] = True
        return {"items": [_sample_item()]}

    monkeypatch.setattr(plugin, "_request_json_with_backoff", _request_json)
    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, title="Title")
    assert called["request"] is False
    assert out.empty()

    plugin = GoogleBooks()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={})
    assert out.empty()

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: None)
    monkeypatch.setattr(plugin, "_request_feed_entries_or_empty", lambda **kwargs: [])
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title="Title")
    assert out.empty()

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: {})
    monkeypatch.setattr(plugin, "_request_feed_entries_or_empty", lambda **kwargs: [])
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={"google": "missing"})
    assert out.empty()


def test_google_identify_retry_query_none_and_parse_failures(monkeypatch) -> None:
    """
    Verify google identify retry query none and parse failures.

    Example:
        Exercise test google identify retry query none and parse failures through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    responses = iter([{"items": []}, None])
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: next(responses))
    monkeypatch.setattr(plugin, "_request_feed_entries_or_empty", lambda **kwargs: [])
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="Fallback",
        authors=["Author"],
        identifiers={"isbn": "9780306406157"},
    )
    assert out.empty()

    plugin = GoogleBooks()
    calls = []

    def _create_query(title=None, authors=None, identifiers=None):
        """
        Perform the create query test-helper operation with deterministic inputs.

        Example:
            Exercise test google identify retry query none and parse failures.create query through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append(dict(identifiers or {}))
        return "isbn:9780306406157" if identifiers else None

    monkeypatch.setattr(plugin, "create_query", _create_query)
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: {"items": []})
    monkeypatch.setattr(plugin, "_request_feed_entries_or_empty", lambda **kwargs: [])
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="Fallback",
        authors=["Author"],
        identifiers={"isbn": "9780306406157"},
    )
    assert out.empty()
    assert calls == [{"isbn": "9780306406157"}, {}]

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: {"items": [{}]})
    monkeypatch.setattr(plugin, "_item_to_metadata", lambda item: (_ for _ in ()).throw(RuntimeError("bad item")))
    log = _Log()
    out = queue.Queue()
    plugin.identify(log=log, result_queue=out, abort=Event(), title="Title")
    assert out.empty()
    assert any(level == "exception" for level, parts in log.events)


def test_google_identify_uses_legacy_feed_after_json_miss(monkeypatch) -> None:
    """
    Verify google identify uses legacy feed after json miss.

    Example:
        Exercise test google identify uses legacy feed after json miss through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    calls = []
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: {"items": []})

    def _feed(**kwargs):
        """
        Perform the feed test-helper operation with deterministic inputs.

        Example:
            Exercise test google identify uses legacy feed after json miss.feed through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append(kwargs)
        return plugin._feed_entries_from_payload(_sample_feed_payload("gid-feed"))

    monkeypatch.setattr(plugin, "_request_feed_entries_or_empty", _feed)

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )

    mi = out.get_nowait()
    assert mi.get_identifiers()["google"] == "gid-feed"
    assert mi.title == "Feed Title: Feed Subtitle"
    assert calls[0]["context"] == "GoogleBooks legacy identify query"
    assert "q=isbn%3A9780306406157" in calls[0]["url"]


def test_google_identify_abort_between_items_and_postprocess_none(monkeypatch) -> None:
    """
    Verify google identify abort between items and postprocess none.

    Example:
        Exercise test google identify abort between items and postprocess none through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    abort = Event()
    items = [_sample_item("gid-one"), _sample_item("gid-two")]

    def _item_to_metadata(item):
        """
        Perform the item to metadata test-helper operation with deterministic inputs.

        Example:
            Exercise test google identify abort between items and postprocess none.item to metadata through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param item: Value supplied for item in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        mi = calibreMetaInformation(item["id"], ["Author"])
        mi.set_identifier("google", item["id"])
        abort.set()
        return mi

    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: {"items": items})
    monkeypatch.setattr(plugin, "_item_to_metadata", _item_to_metadata)
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, title="Title")
    assert out.get_nowait().title == "gid-one"
    assert out.empty()

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: {"items": [_sample_item("gid-none")]})
    monkeypatch.setattr(plugin, "_postprocess_downloaded_google_metadata", lambda mi, relevance=0: None)
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), title="Title")
    assert out.empty()


def test_google_identify_retries_text_query_after_empty_isbn_query(monkeypatch) -> None:
    """
    Verify google identify retries text query after empty isbn query.

    Example:
        Exercise test google identify retries text query after empty isbn query through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    calls = []

    def _fake_request_json(path="", timeout=30, **params):
        """
        Perform the fake request json test-helper operation with deterministic inputs.

        Example:
            Exercise test google identify retries text query after empty isbn query.fake request json through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param path: Value supplied for path in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param params: Value supplied for params in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del timeout
        calls.append((path, params))
        if params.get("q", "").startswith("isbn:"):
            return {"items": []}
        return {"items": [_sample_item("gid-fallback")]}

    monkeypatch.setattr(plugin, "_request_json", _fake_request_json)
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="Fallback Title",
        authors=["Fallback Author"],
        identifiers={"isbn": "9780306406157"},
    )

    mi = out.get_nowait()
    assert mi.get_identifiers()["google"] == "gid-fallback"
    queries = [params.get("q", "") for _path, params in calls if "q" in params]
    assert len(queries) == 2
    assert queries[0].startswith("isbn:")
    assert "intitle:Fallback" in queries[1]


def test_google_identify_retries_text_query_after_request_failure(monkeypatch) -> None:
    """
    Verify google identify retries text query after request failure.

    Example:
        Exercise test google identify retries text query after request failure through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    contexts = []

    def _fake_request(**kwargs):
        """
        Perform the fake request test-helper operation with deterministic inputs.

        Example:
            Exercise test google identify retries text query after request failure.fake request through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        contexts.append(kwargs["context"])
        if kwargs["context"] == "GoogleBooks identify query":
            raise RuntimeError("rate limited")
        return {"items": [_sample_item("gid-fallback-after-error")]}

    monkeypatch.setattr(plugin, "_request_json_with_backoff", _fake_request)
    out = queue.Queue()
    log = _Log()
    plugin.identify(
        log=log,
        result_queue=out,
        abort=Event(),
        title="Fallback Title",
        authors=["Fallback Author"],
        identifiers={"isbn": "9780306406157"},
    )

    assert out.get_nowait().get_identifiers()["google"] == "gid-fallback-after-error"
    assert contexts == ["GoogleBooks identify query", "GoogleBooks identify retry query"]
    assert any(
        level == "warning" and "continuing with fallback paths" in " ".join(map(str, parts))
        for level, parts in log.events
    )


def test_google_get_cached_cover_url_uses_isbn_cache_with_iterables() -> None:
    """
    Verify google get cached cover url uses isbn cache with iterables.

    Example:
        Exercise test google get cached cover url uses isbn cache with iterables through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    plugin.cache_isbn_to_identifier("9780306406157", "gid-cache")
    assert "gid-cache" in plugin.get_cached_cover_url({"isbn": {"978-0-306-40615-7"}})


def test_google_get_cached_cover_url_edges() -> None:
    """
    Verify google get cached cover url edges.

    Example:
        Exercise test google get cached cover url edges through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    assert plugin.get_cached_cover_url({}) is None
    assert plugin.get_cached_cover_url({"google": "   "}) is None
    assert plugin.get_cached_cover_url({"isbn": "9780306406157"}) is None
    assert plugin.get_cached_cover_url({"google": "gid-direct"}).endswith("id=gid-direct&printsec=frontcover&img=1")


def test_google_download_cover_ignores_dummy_and_uses_next_zoom(monkeypatch) -> None:
    """
    Verify google download cover ignores dummy and uses next zoom.

    Example:
        Exercise test google download cover ignores dummy and uses next zoom through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google download cover ignores dummy and uses next zoom.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
        """
        def __init__(self, payload):
            """
            Initialize the Resp test double.

            Example:
                Exercise test google download cover ignores dummy and uses next zoom.Resp.init through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :param payload: Value supplied for payload in the focused test operation.
            :return: None; the function records state or raises through its assertions.
            """
            self.payload = payload

        def read(self):
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test google download cover ignores dummy and uses next zoom.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return self.payload

    payloads = [b"dummy", b"real-cover"]
    seen_urls = []

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google download cover ignores dummy and uses next zoom.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google download cover ignores dummy and uses next zoom.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del timeout
            seen_urls.append(url)
            return _Resp(payloads.pop(0))

    plugin = GoogleBooks()
    plugin.DUMMY_IMAGE_MD5 = frozenset({hashlib.md5(b"dummy").hexdigest()})
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    monkeypatch.setattr(plugin, "get_cached_cover_url", lambda identifiers: "https://covers.example/image.jpg")

    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"google": "gid-1"})

    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"real-cover"
    assert seen_urls[0].endswith("zoom=0")
    assert seen_urls[1].endswith("zoom=1")


def test_google_download_cover_identify_abort_and_multi_result_scan(monkeypatch) -> None:
    """
    Verify google download cover identify abort and multi result scan.

    Example:
        Exercise test google download cover identify abort and multi result scan through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    abort = Event()

    def _abort_identify(log, result_queue, abort, title=None, authors=None, identifiers=None, timeout=30):
        """
        Perform the abort identify test-helper operation with deterministic inputs.

        Example:
            Exercise test google download cover identify abort and multi result scan.abort identify through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Value supplied for log in the focused test operation.
        :param result_queue: Value supplied for result queue in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        abort.set()

    monkeypatch.setattr(plugin, "identify", _abort_identify)
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=abort, identifiers={})
    assert out.empty()

    plugin = GoogleBooks()
    calls = []

    def _identify(log, result_queue, abort, title=None, authors=None, identifiers=None, timeout=30):
        """
        Perform the identify test-helper operation with deterministic inputs.

        Example:
            Exercise test google download cover identify abort and multi result scan.identify through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Value supplied for log in the focused test operation.
        :param result_queue: Value supplied for result queue in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, abort, title, authors, identifiers, timeout
        first = calibreMetaInformation("No Cover", ["Author"])
        first.set_identifier("google", "missing")
        second = calibreMetaInformation("With Cover", ["Author"])
        second.set_identifier("google", "covered")
        result_queue.put(first)
        result_queue.put(second)

    def _cached(identifiers):
        """
        Perform the cached test-helper operation with deterministic inputs.

        Example:
            Exercise test google download cover identify abort and multi result scan.cached through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append(dict(identifiers))
        if identifiers.get("google") == "covered":
            return "https://covers.example/covered.jpg"
        return None

    monkeypatch.setattr(plugin, "identify", _identify)
    monkeypatch.setattr(plugin, "get_cached_cover_url", _cached)
    monkeypatch.setattr(plugin, "_open_with_backoff", lambda **kwargs: b"cover")
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={})
    assert out.get_nowait()[1] == b"cover"
    assert [call.get("google") for call in calls] == [None, "missing", "covered"]


def test_google_download_cover_runs_identify_when_cache_misses(monkeypatch) -> None:
    """
    Verify google download cover runs identify when cache misses.

    Example:
        Exercise test google download cover runs identify when cache misses through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Resp:
        """
        Provide the Resp test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google download cover runs identify when cache misses.Resp through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
        """
        @staticmethod
        def read():
            """
            Perform the read test-helper operation with deterministic inputs.

            Example:
                Exercise test google download cover runs identify when cache misses.Resp.read through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return b"cover-bytes"

    seen = {}

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google download cover runs identify when cache misses.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google download cover runs identify when cache misses.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del timeout
            seen["url"] = url
            return _Resp()

    def _identify(log, result_queue, abort, title=None, authors=None, identifiers=None, timeout=30):
        """
        Perform the identify test-helper operation with deterministic inputs.

        Example:
            Exercise test google download cover runs identify when cache misses.identify through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Value supplied for log in the focused test operation.
        :param result_queue: Value supplied for result queue in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, abort, title, authors, identifiers, timeout
        mi = calibreMetaInformation("Title", ["Author"])
        mi.set_identifier("google", "gid-identify")
        result_queue.put(mi)

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    monkeypatch.setattr(plugin, "identify", _identify)

    out = queue.Queue()
    logger = _Log()
    plugin.download_cover(log=logger, result_queue=out, abort=Event(), identifiers={})

    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"
    assert "gid-identify" in seen["url"]
    assert any(level == "info" for level, _parts in logger.events)


def test_google_download_cover_abort_empty_exception_and_existing_zoom(monkeypatch) -> None:
    """
    Verify google download cover abort empty exception and existing zoom.

    Example:
        Exercise test google download cover abort empty exception and existing zoom through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "get_cached_cover_url", lambda identifiers: "https://covers.example/image.jpg")
    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=abort, identifiers={"google": "gid"})
    assert out.empty()

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "get_cached_cover_url", lambda identifiers: "https://covers.example/image.jpg")
    payloads = iter([RuntimeError("temporary"), b"", b"real"])
    seen = []

    def _open(**kwargs):
        """
        Perform the open test-helper operation with deterministic inputs.

        Example:
            Exercise test google download cover abort empty exception and existing zoom.open through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        seen.append(kwargs["url"])
        payload = next(payloads)
        if isinstance(payload, Exception):
            raise payload
        return payload

    monkeypatch.setattr(plugin, "_open_with_backoff", _open)
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"google": "gid"})
    assert out.empty()
    assert seen == ["https://covers.example/image.jpg&zoom=0", "https://covers.example/image.jpg&zoom=1"]

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "get_cached_cover_url", lambda identifiers: "https://covers.example/image.jpg&zoom=3")
    monkeypatch.setattr(plugin, "_open_with_backoff", lambda **kwargs: b"cover")
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"google": "gid"})
    assert out.get_nowait()[1] == b"cover"


def test_google_download_cover_no_results_logs_and_returns(monkeypatch) -> None:
    """
    Verify google download cover no results logs and returns.

    Example:
        Exercise test google download cover no results logs and returns through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    def _identify(log, result_queue, abort, title=None, authors=None, identifiers=None, timeout=30):
        """
        Perform the identify test-helper operation with deterministic inputs.

        Example:
            Exercise test google download cover no results logs and returns.identify through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param log: Value supplied for log in the focused test operation.
        :param result_queue: Value supplied for result queue in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, result_queue, abort, title, authors, identifiers, timeout

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "identify", _identify)

    out = queue.Queue()
    logger = _Log()
    plugin.download_cover(log=logger, result_queue=out, abort=Event(), identifiers={})

    assert out.empty()
    assert any(level == "info" and "No cover found" in " ".join(map(str, parts)) for level, parts in logger.events)


def test_google_request_json_with_backoff_retries_transient_errors(monkeypatch) -> None:
    """
    Verify google request json with backoff retries transient errors.

    Example:
        Exercise test google request json with backoff retries transient errors through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Transient(Exception):
        """
        Provide the Transient test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google request json with backoff retries transient errors.Transient through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
        """
        @staticmethod
        def getcode():
            """
            Perform the getcode test-helper operation with deterministic inputs.

            Example:
                Exercise test google request json with backoff retries transient errors.Transient.getcode through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :return: The deterministic value, row, identity or collection described above.
            """
            return 503

    plugin = GoogleBooks()
    attempts = {"n": 0}
    delays = []

    def _fake_request_json(path="", timeout=30, **params):
        """
        Perform the fake request json test-helper operation with deterministic inputs.

        Example:
            Exercise test google request json with backoff retries transient errors.fake request json through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


        :param path: Value supplied for path in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param params: Value supplied for params in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del path, timeout, params
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise _Transient("busy")
        return {"items": []}

    monkeypatch.setattr(plugin, "_request_json", _fake_request_json)
    monkeypatch.setattr(plugin, "_wait_for_backoff", lambda abort, delay: delays.append(delay) or False)

    log = _Log()
    out = plugin._request_json_with_backoff(log=log, abort=Event(), context="unit-test", q="hello")
    assert out == {"items": []}
    assert attempts["n"] == 3
    assert len(delays) == 2
    assert any(
        level == "warning" and "retrying with backoff" in " ".join(map(str, parts))
        for level, parts in log.events
    )


def test_google_open_with_backoff_non_retryable_raises_and_logs(monkeypatch) -> None:
    """
    Verify google open with backoff non retryable raises and logs.

    Example:
        Exercise test google open with backoff non retryable raises and logs through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Browser:
        """
        Provide the Browser test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test google open with backoff non retryable raises and logs.Browser through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py
        """
        @staticmethod
        def open_novisit(url, timeout=30):
            """
            Perform the open novisit test-helper operation with deterministic inputs.

            Example:
                Exercise test google open with backoff non retryable raises and logs.Browser.open novisit through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_google.py


            :param url: Value supplied for url in the focused test operation.
            :param timeout: Value supplied for timeout in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
            del url, timeout
            raise ValueError("bad payload")

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "browser", lambda: _Browser())
    log = _Log()

    with pytest.raises(ValueError):
        plugin._open_with_backoff(
            log=log,
            abort=Event(),
            url="https://example.invalid/cover.jpg",
            timeout=12,
            context="unit-test-cover",
        )
    assert any(
        level == "exception" and "Google cover request failed" in " ".join(map(str, parts))
        for level, parts in log.events
    )
