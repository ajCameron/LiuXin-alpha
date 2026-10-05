"""
Verify ISBNDB configured JSON/XML parsing and failures.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources isbndb through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py
"""
from __future__ import annotations

import json
import queue
from threading import Event


class _Response:
    """
    Provide the Response test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Response through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py
    """
    def __init__(self, payload: bytes) -> None:
        """
        Initialize the Response test double.

        Example:
            Exercise Response.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.payload = payload

    def read(self) -> bytes:
        """
        Perform the read test-helper operation with deterministic inputs.

        Example:
            Exercise Response.read through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.payload


class _Browser:
    """
    Provide the Browser test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Browser through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py
    """
    def __init__(self, payload: bytes) -> None:
        """
        Initialize the Browser test double.

        Example:
            Exercise Browser.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.payload = payload
        self.addheaders = []
        self.requests = []

    def open_novisit(self, url, timeout=30):
        """
        Perform the open novisit test-helper operation with deterministic inputs.

        Example:
            Exercise Browser.open novisit through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.requests.append((url, timeout, tuple(self.addheaders)))
        return _Response(self.payload)


class _Log:
    """
    Provide the Log test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Log through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


def _sample_v2_book(isbn13: str = "9780306406157") -> dict:
    """
    Perform the sample v2 book test-helper operation with deterministic inputs.

    Example:
        Exercise sample v2 book through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param isbn13: Value supplied for isbn13 in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "title": "The Great Gatsby",
        "title_long": "The Great Gatsby (Annotated Edition)",
        "authors": ["F. Scott Fitzgerald"],
        "publisher": "Scribner",
        "synopsis": "A classic novel.",
        "date_published": "2004-09-30",
        "isbn13": isbn13,
        "isbn10": "0306406152",
        "language": "en",
    }


def _sample_legacy_xml() -> str:
    """
    Perform the sample legacy xml test-helper operation with deterministic inputs.

    Example:
        Exercise sample legacy xml through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return """<?xml version="1.0" encoding="utf-8"?>
<ISBNdb>
  <BookList total_results="1" page_size="10" shown_results="1">
    <BookData isbn="0306406152" isbn13="9780306406157">
      <Title>The Great Gatsby</Title>
      <Authors>
        <Person>Fitzgerald, F. Scott</Person>
      </Authors>
      <PublisherText>Scribner</PublisherText>
      <Summary>Classic summary</Summary>
    </BookData>
  </BookList>
</ISBNdb>
    """


def _record(title: str, isbn: str = "9780306406157") -> dict:
    """
    Perform the record test-helper operation with deterministic inputs.

    Example:
        Exercise record through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param title: Value supplied for title in the focused test operation.
    :param isbn: Value supplied for isbn in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "title": title,
        "authors": ["Author"],
        "publisher": "Publisher",
        "isbn13": isbn,
    }


def test_web_sources_isbndb_import_smoke() -> None:
    """
    Verify web sources isbndb import smoke.

    Example:
        Exercise test web sources isbndb import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.isbndb as isbndb

    assert isbndb is not None


def test_isbndb_helper_normalization_edges(monkeypatch) -> None:
    """
    Verify isbndb helper normalization edges.

    Example:
        Exercise test isbndb helper normalization edges through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.isbndb as isbndb

    class BadString:
        """
        Provide the BadString test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test isbndb helper normalization edges.BadString through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py
        """
        def __str__(self):
            """
            Perform the str test-helper operation with deterministic inputs.

            Example:
                Exercise test isbndb helper normalization edges.BadString.str through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("broken")

    assert isbndb._as_text(b"caf\xc3\xa9") == "café"
    assert isbndb._as_text(BadString()) == ""
    assert isbndb._first(None) is None
    assert isbndb._first("isbn") == "isbn"
    assert isbndb._first({"first": "ignored"}) == "first"
    assert isbndb._first({}) is None
    assert isbndb._first(item for item in ["one"]) == "one"
    assert isbndb._first(7) == 7
    assert isbndb._first_identifier_value([], "isbn") is None
    assert isbndb._safe_isbn({"isbn13": ["9780306406157"]}) == "9780306406157"
    assert isbndb._safe_isbn({"isbn10": {"0306406152"}}) == "0306406152"
    assert isbndb._safe_isbn({"isbn": "bad"}) is None

    assert isbndb._parse_pubdate("") is None
    assert isbndb._parse_pubdate("2024-05").day == 15
    assert isbndb._parse_pubdate("2024/05").day == 15
    assert isbndb._parse_pubdate("2024.05").day == 15
    assert isbndb._parse_pubdate("2024").year == 2024
    assert isbndb._parse_pubdate("Published in 1999").year == 1999
    assert isbndb._parse_pubdate("not a date") is None

    assert isbndb._ensure_author_list(None) == []
    assert isbndb._ensure_author_list([" A ", "", "B"]) == ["A", "B"]
    assert isbndb._ensure_author_list("A, B") == ["A", "B"]
    assert isbndb._ensure_author_list("Single Author") == ["Single Author"]

    monkeypatch.setattr(isbndb, "ET", None)
    assert isbndb._parse_legacy_xml_books(_sample_legacy_xml()) == []


def test_isbndb_legacy_xml_parser_edges() -> None:
    """
    Verify isbndb legacy xml parser edges.

    Example:
        Exercise test isbndb legacy xml parser edges through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import _parse_legacy_xml_books

    assert _parse_legacy_xml_books("") == []
    assert _parse_legacy_xml_books("<not-xml") == []
    payload = """<?xml version="1.0"?>
    <ISBNdb>
      <BookData isbn="0306406152" isbn13="9780306406157">
        <Title></Title>
      </BookData>
      <BookData isbn="0306406152" isbn13="9780306406157">
        <Title>Valid Title</Title>
        <Authors>
          <Person></Person>
          <Person>Doe, Jane</Person>
          <Person>Direct Name</Person>
        </Authors>
        <PublisherText>Publisher</PublisherText>
        <Summary>Summary</Summary>
      </BookData>
    </ISBNdb>
    """
    books = _parse_legacy_xml_books(payload)
    assert books == [
        {
            "title": "Valid Title",
            "authors": ["Jane Doe", "Direct Name"],
            "publisher": "Publisher",
            "summary": "Summary",
            "isbn10": "0306406152",
            "isbn13": "9780306406157",
        }
    ]


def test_isbndb_api_key_headers_retry_and_open_helpers(monkeypatch) -> None:
    """
    Verify isbndb api key headers retry and open helpers.

    Example:
        Exercise test isbndb api key headers retry and open helpers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.delenv("ISBNDB_API_KEY", raising=False)
    monkeypatch.setattr(plugin.prefs, "get", lambda key, default=None: "" if key == "isbndb_key" else default)
    assert plugin._api_key() is None
    assert plugin.is_configured() is False
    assert plugin._json_headers() == {}

    monkeypatch.setenv("ISBNDB_API_KEY", "ENVKEY")
    assert plugin._api_key() == "ENVKEY"
    assert plugin.is_configured() is True
    assert plugin._json_headers() == {"Authorization": "ENVKEY", "Accept": "application/json"}

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
    payload = plugin._open_bytes_with_backoff(
        _Log(),
        Event(),
        "https://example.org/book",
        9,
        "ISBNDB v2_book",
        headers={"Authorization": "ENVKEY"},
    )
    assert payload == b"caf\xc3\xa9"
    assert browser.requests == [
        ("https://example.org/book", 9, (("Authorization", "ENVKEY"),)),
    ]

    plugin._open_bytes_with_backoff = lambda **kwargs: b"caf\xc3\xa9"
    assert plugin._open_text_with_backoff(_Log(), Event(), "https://example.org", 1, "ctx") == "café"
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    assert plugin._open_text_with_backoff(_Log(), Event(), "https://example.org", 1, "ctx") == ""


def test_isbndb_create_query_prefers_isbn_then_search(monkeypatch) -> None:
    """
    Verify isbndb create query prefers isbn then search.

    Example:
        Exercise test isbndb create query prefers isbn then search through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin.prefs, "get", lambda key, default=None: "KEY" if key == "isbndb_key" else default)

    assert plugin.create_query(identifiers={"isbn": "bad"}) == []
    assert plugin.create_query(title=None, authors=None, identifiers={}) == []

    queries = plugin.create_query(identifiers={"isbn": "9780306406157"})
    assert queries[0][0] == "v2_book"
    assert queries[0][1].endswith("/book/9780306406157")
    assert "index1=isbn" in queries[1][1]

    queries = plugin.create_query(title="Great Gatsby", authors=["Fitzgerald"], identifiers={})
    assert queries[0][0] == "v2_search"
    assert "/books/Great+Gatsby+Fitzgerald" in queries[0][1]
    assert "index1=combined" in queries[1][1]


def test_isbndb_create_query_requires_key(monkeypatch) -> None:
    """
    Verify isbndb create query requires key.

    Example:
        Exercise test isbndb create query requires key through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.delenv("ISBNDB_API_KEY", raising=False)
    monkeypatch.setattr(plugin.prefs, "get", lambda key, default=None: "" if key == "isbndb_key" else default)
    assert plugin.create_query(title="Great Gatsby", authors=["Fitzgerald"], identifiers={}) == []


def test_isbndb_records_from_json_payload_shapes() -> None:
    """
    Verify isbndb records from json payload shapes.

    Example:
        Exercise test isbndb records from json payload shapes through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    payload = json.dumps({"book": _sample_v2_book()})
    records = plugin._records_from_json_payload(payload)
    assert len(records) == 1
    assert records[0]["title"] == "The Great Gatsby"

    payload = json.dumps({"books": [_sample_v2_book(), _sample_v2_book("9780312621360")]})
    records = plugin._records_from_json_payload(payload)
    assert len(records) == 2

    assert plugin._records_from_json_payload("{bad json") == []
    assert plugin._records_from_json_payload(json.dumps("text")) == []
    assert plugin._records_from_json_payload(json.dumps([_sample_v2_book(), "skip"])) == [_sample_v2_book()]
    assert plugin._records_from_json_payload(json.dumps({"data": [_sample_v2_book(), "skip"]})) == [_sample_v2_book()]
    assert plugin._records_from_json_payload(json.dumps({"data": {"not": "a list"}})) == []


def test_isbndb_metadata_from_record_parses_fields() -> None:
    """
    Verify isbndb metadata from record parses fields.

    Example:
        Exercise test isbndb metadata from record parses fields through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    mi = plugin._metadata_from_record(_sample_v2_book(), relevance=2)
    assert mi.title == "The Great Gatsby (Annotated Edition)"
    assert mi.authors == ["F. Scott Fitzgerald"]
    assert mi.publisher == "Scribner"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert mi.pubdate.year == 2004
    assert mi.source_relevance == 2
    assert "classic novel" in (mi.comments or "").lower()


def test_isbndb_metadata_from_record_fallbacks_and_rejections() -> None:
    """
    Verify isbndb metadata from record fallbacks and rejections.

    Example:
        Exercise test isbndb metadata from record fallbacks and rejections through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    assert plugin._metadata_from_record({"title": "Audio", "publisher": "Example Audio"}) is None

    mi = plugin._metadata_from_record(
        {
            "authors": "Ada Lovelace, Charles Babbage",
            "overview": "Overview text",
            "published_date": "2023/04",
            "language_code": "fr",
            "isbn_13": "9780306406157",
            "isbn_10": "0306406152",
            "isbns": ["9780306406157", "9780312621360", "bad"],
        }
    )
    assert mi.title == "Unknown"
    assert mi.authors == ["Ada Lovelace", "Charles Babbage"]
    assert mi.comments == "<p>Overview text</p>"
    assert mi.pubdate.year == 2023
    assert mi.pubdate.day == 15
    assert mi.language == "fr"
    assert mi.get_identifiers()["isbn"] == "9780312621360"
    assert mi.all_isbns == ["9780306406157", "0306406152", "9780312621360"]

    mi = plugin._metadata_from_record({"title": "Sparse"})
    assert mi.title == "Sparse"
    assert mi.authors == ["Unknown"]
    assert mi.get_identifiers() == {}


def test_isbndb_legacy_xml_payload_is_parsed() -> None:
    """
    Verify isbndb legacy xml payload remains parsed.

    Example:
        Exercise test isbndb legacy xml payload is parsed through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    out = plugin._metadata_from_payload(_sample_legacy_xml(), mode="legacy_xml")
    assert len(out) == 1
    mi = out[0]
    assert mi.title == "The Great Gatsby"
    assert mi.authors == ["F. Scott Fitzgerald"]
    assert mi.get_identifiers()["isbn"] == "9780306406157"


def test_isbndb_metadata_from_payload_modes_and_filtered_records() -> None:
    """
    Verify isbndb metadata from payload modes and filtered records.

    Example:
        Exercise test isbndb metadata from payload modes and filtered records through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    assert plugin._metadata_from_payload("", mode="unknown") == []
    assert (
        plugin._metadata_from_payload(json.dumps({"book": {"title": "Audio", "publisher": "Audio"}}), "v2_book")
        == []
    )
    out = plugin._metadata_from_payload(json.dumps({"books": [_record("One"), _record("Two")]}), "v2_search")
    assert [mi.title for mi in out] == ["One", "Two"]
    assert [mi.source_relevance for mi in out] == [0, 1]


def test_isbndb_query_once_uses_headers_and_empty_payload(monkeypatch) -> None:
    """
    Verify isbndb query once uses headers and empty payload.

    Example:
        Exercise test isbndb query once uses headers and empty payload through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setenv("ISBNDB_API_KEY", "KEY")
    calls = []

    def _fake_open(**kwargs):
        """
        Perform the fake open test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb query once uses headers and empty payload.fake open through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param kwargs: Keyword values forwarded by the test double.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append(kwargs)
        return ""

    plugin._open_text_with_backoff = _fake_open
    assert plugin._query_once(_Log(), Event(), "v2_book", "https://example.org/book", 3) == []
    assert calls[0]["headers"] == {"Authorization": "KEY", "Accept": "application/json"}

    plugin._open_text_with_backoff = lambda **kwargs: _sample_legacy_xml()
    out = plugin._query_once(_Log(), Event(), "legacy_xml", "https://example.org/xml", 3)
    assert out[0].title == "The Great Gatsby"


def test_isbndb_identify_uses_v2_payload(monkeypatch) -> None:
    """
    Verify isbndb identify uses v2 payload.

    Example:
        Exercise test isbndb identify uses v2 payload through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: True)
    monkeypatch.setattr(plugin, "create_query", lambda **kwargs: [("v2_book", "https://example.invalid/book")])
    monkeypatch.setattr(
        plugin,
        "_open_text_with_backoff",
        lambda log, abort, url, timeout, context, headers=None: json.dumps({"book": _sample_v2_book()}),
    )

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )
    mi = out.get_nowait()
    assert mi.title.startswith("The Great Gatsby")
    assert mi.get_identifiers()["isbn"] == "9780306406157"


def test_isbndb_identify_configuration_abort_and_insufficient_query(monkeypatch) -> None:
    """
    Verify isbndb identify configuration abort and insufficient query.

    Example:
        Exercise test isbndb identify configuration abort and insufficient query through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    called = False

    def _configured():
        """
        Perform the configured test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb identify configuration abort and insufficient query.configured through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :return: The deterministic value, row, identity or collection described above.
        """
        nonlocal called
        called = True
        return True

    abort = Event()
    abort.set()
    monkeypatch.setattr(plugin, "is_configured", _configured)
    plugin.identify(_Log(), queue.Queue(), abort, identifiers={"isbn": "9780306406157"})
    assert called is False

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: True)
    monkeypatch.setattr(plugin, "create_query", lambda **kwargs: [])
    log = _Log()
    out = queue.Queue()
    plugin.identify(log, out, Event(), identifiers={})
    assert out.empty()
    assert any(level == "error" for level, parts in log.events)


def test_isbndb_identify_skips_failed_queries_and_dedupes(monkeypatch) -> None:
    """
    Verify isbndb identify skips failed queries and dedupes.

    Example:
        Exercise test isbndb identify skips failed queries and dedupes through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: True)
    monkeypatch.setattr(
        plugin,
        "create_query",
        lambda **kwargs: [
            ("v2_book", "https://example.invalid/fail"),
            ("legacy_xml", "https://example.invalid/xml"),
        ],
    )

    def _query_once(log, abort, query_mode, query_url, timeout):
        """
        Perform the query once test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb identify skips failed queries and dedupes.query once through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param query_mode: Value supplied for query mode in the focused test operation.
        :param query_url: Value supplied for query url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, abort, query_mode, timeout
        if "fail" in query_url:
            raise RuntimeError("network")
        return plugin._metadata_from_payload(
            json.dumps({"books": [_record("Same"), _record("Same"), _record("Other", "9780312621360")]}),
            "v2_search",
        )

    monkeypatch.setattr(plugin, "_query_once", _query_once)
    out = queue.Queue()
    plugin.identify(_Log(), out, Event(), title="Same", identifiers={})
    results = [out.get_nowait() for _ in range(out.qsize())]
    assert [mi.title for mi in results] == ["Same", "Other"]


def test_isbndb_identify_aborts_during_query_loops(monkeypatch) -> None:
    """
    Verify isbndb identify aborts during query loops.

    Example:
        Exercise test isbndb identify aborts during query loops through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: True)
    monkeypatch.setattr(
        plugin,
        "create_query",
        lambda **kwargs: [
            ("v2_book", "https://example.invalid/one"),
            ("legacy_xml", "https://example.invalid/two"),
        ],
    )
    abort = Event()
    calls = []

    def _query_once(log, abort, query_mode, query_url, timeout):
        """
        Perform the query once test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb identify aborts during query loops.query once through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param query_mode: Value supplied for query mode in the focused test operation.
        :param query_url: Value supplied for query url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, query_mode, timeout
        calls.append(query_url)
        abort.set()
        return []

    monkeypatch.setattr(plugin, "_query_once", _query_once)
    plugin.identify(_Log(), queue.Queue(), abort, title="Book", identifiers={})
    assert calls == ["https://example.invalid/one"]


def test_isbndb_identify_falls_back_to_title_author(monkeypatch) -> None:
    """
    Verify isbndb identify falls back to title author.

    Example:
        Exercise test isbndb identify falls back to title author through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: True)

    calls = []

    def _create_query(title=None, authors=None, identifiers=None):
        """
        Perform the create query test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb identify falls back to title author.create query through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append((title, tuple(authors or []), dict(identifiers or {})))
        if identifiers and identifiers.get("isbn"):
            return [("v2_book", "https://example.invalid/empty")]
        return [("v2_search", "https://example.invalid/search")]

    def _open_text(log, abort, url, timeout, context, headers=None):
        """
        Perform the open text test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb identify falls back to title author.open text through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param context: Value supplied for context in the focused test operation.
        :param headers: Value supplied for headers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, abort, timeout, context, headers
        if "empty" in url:
            return json.dumps({"books": []})
        return json.dumps({"books": [_sample_v2_book("9780312621360")]})

    monkeypatch.setattr(plugin, "create_query", _create_query)
    monkeypatch.setattr(plugin, "_open_text_with_backoff", _open_text)

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="Great Gatsby",
        authors=["Fitzgerald"],
        identifiers={"isbn": "9780306406157"},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["isbn"] == "9780312621360"
    assert len(calls) >= 2


def test_isbndb_identify_fallback_skips_exceptions_and_stops_on_result(monkeypatch) -> None:
    """
    Verify isbndb identify fallback skips exceptions and stops on result.

    Example:
        Exercise test isbndb identify fallback skips exceptions and stops on result through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: True)
    calls = []

    def _create_query(title=None, authors=None, identifiers=None):
        """
        Perform the create query test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb identify fallback skips exceptions and stops on result.create query through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append(dict(identifiers or {}))
        if identifiers:
            return [("v2_book", "https://example.invalid/empty")]
        return [
            ("v2_search", "https://example.invalid/fail"),
            ("legacy_xml", "https://example.invalid/success"),
        ]

    def _query_once(log, abort, query_mode, query_url, timeout):
        """
        Perform the query once test-helper operation with deterministic inputs.

        Example:
            Exercise test isbndb identify fallback skips exceptions and stops on result.query once through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param query_mode: Value supplied for query mode in the focused test operation.
        :param query_url: Value supplied for query url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        del log, abort, query_mode, timeout
        if query_url.endswith("/fail"):
            raise RuntimeError("network")
        if query_url.endswith("/success"):
            return plugin._metadata_from_payload(json.dumps({"books": [_record("Fallback")]}), "v2_search")
        return []

    monkeypatch.setattr(plugin, "create_query", _create_query)
    monkeypatch.setattr(plugin, "_query_once", _query_once)
    out = queue.Queue()
    log = _Log()
    plugin.identify(log, out, Event(), title="Fallback", authors=["Author"], identifiers={"isbn": "9780306406157"})
    assert out.get_nowait().title == "Fallback"
    assert calls == [{"isbn": "9780306406157"}, {}]
    assert any("retrying with title/author" in str(parts) for level, parts in log.events if level == "info")


def test_isbndb_identify_not_configured_is_noop(monkeypatch) -> None:
    """
    Verify isbndb identify not configured remains noop.

    Example:
        Exercise test isbndb identify not configured is noop through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: False)
    out = queue.Queue()
    log = _Log()

    plugin.identify(
        log=log,
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )
    assert out.empty()
    assert any(level == "warning" for level, _parts in log.events)


def test_isbndb_import_web_source_module() -> None:
    """
    Verify isbndb import web source module.

    Example:
        Exercise test isbndb import web source module through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_isbndb.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("isbndb")
    assert hasattr(mod, "ISBNDB")
