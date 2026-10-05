"""
Verify Internet Archive queries, records, identifiers and covers.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources internet archive through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py
"""
from __future__ import annotations

import queue
from datetime import datetime
from threading import Event


class _Log:
    """
    Provide the Log test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Log through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


def _sample_search_doc(identifier: str = "ia-sample") -> dict:
    """
    Perform the sample search doc test-helper operation with deterministic inputs.

    Example:
        Exercise sample search doc through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param identifier: Value supplied for identifier in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "identifier": identifier,
        "title": "Internet Archive Sample",
        "creator": ["Alice Author", "Bob Author"],
        "description": ["A useful archived text."],
        "publisher": "Example Press",
        "date": "1937-09-21T00:00:00Z",
        "language": "eng",
        "subject": ["Fantasy fiction", "Libraries -- Catalogs"],
        "isbn": ["9780306406157"],
        "external-identifier": [
            "urn:lccn: 2020123456",
            "urn:oclc: 123456789",
            "urn:openlibrary:OL123M",
        ],
    }


def _sample_metadata_payload(identifier: str = "ia-sample") -> dict:
    """
    Perform the sample metadata payload test-helper operation with deterministic inputs.

    Example:
        Exercise sample metadata payload through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param identifier: Value supplied for identifier in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "server": "ia800000.us.archive.org",
        "dir": f"/0/items/{identifier}",
        "files": [
            {"name": "__ia_thumb.jpg", "format": "JPEG Thumb"},
            {"name": f"{identifier}.pdf", "format": "Text PDF"},
        ],
        "metadata": {
            **_sample_search_doc(identifier),
            "external_identifier": ["urn:isbn:9780306406157"],
        },
    }


def test_web_sources_internet_archive_import_smoke() -> None:
    """
    Verify web sources internet archive import smoke.

    Example:
        Exercise test web sources internet archive import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.internet_archive as ia

    assert ia is not None


def test_internet_archive_helper_edges() -> None:
    """
    Verify internet archive helper edges.

    Example:
        Exercise test internet archive helper edges through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.internet_archive as ia

    class BadString:
        """
        Provide the BadString test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test internet archive helper edges.BadString through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py
        """
        def __str__(self):
            """
            Perform the str test-helper operation with deterministic inputs.

            Example:
                Exercise test internet archive helper edges.BadString.str through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("bad")

    assert ia._as_text(b"hello") == "hello"
    assert ia._as_text(None) == ""
    assert ia._as_text(BadString()) == ""
    assert ia._first({"a": "b"}) == "b"
    assert ia._first(iter(["one"])) == "one"
    assert ia._first([]) is None
    assert ia._as_list("x") == ["x"]
    assert ia._as_list({"x": "y"}) == [{"x": "y"}]
    assert ia._safe_isbn({"isbn": "bad", "isbn13": "9780306406157"}) == "9780306406157"
    assert ia._clean_identifier_key(" OpenLibrary Edition ") == "openlibrary_edition"
    assert ia._archive_identifier_from_identifiers({"ia": {"abc123"}}) == "abc123"
    assert ia._escape_lucene_term('The "Book"') == 'The \\"Book\\"'
    assert ia._field_query("title", 'The "Book"') == 'title:"The \\"Book\\""'


def test_internet_archive_external_identifier_normalization() -> None:
    """
    Verify internet archive external identifier normalization.

    Example:
        Exercise test internet archive external identifier normalization through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.internet_archive as ia

    assert ia._normalize_external_identifier("urn:isbn:9780306406157") == ("isbn", "9780306406157")
    assert ia._normalize_external_identifier("urn:lccn: 2020 123456") == ("lccn", "2020123456")
    assert ia._normalize_external_identifier("urn:oclc: 123-456") == ("oclc", "123456")
    assert ia._normalize_external_identifier("urn:openlibrary:OL123M") == ("openlibrary", "OL123M")
    assert ia._normalize_external_identifier("not-a-urn") == (None, None)


def test_internet_archive_get_book_url_id_from_url_and_query() -> None:
    """
    Verify internet archive get book url id from url and query.

    Example:
        Exercise test internet archive get book url id from url and query through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    assert plugin.get_book_url({}) is None
    assert plugin.get_book_url({"internet_archive": "abc123"}) == (
        "internet_archive",
        "abc123",
        "https://archive.org/details/abc123",
    )
    assert plugin.id_from_url("https://archive.org/details/abc123") == ("internet_archive", "abc123")
    assert plugin.id_from_url("https://archive.org/metadata/abc123") == ("internet_archive", "abc123")
    assert plugin.id_from_url("https://archive.org/download/abc123/file.pdf") == ("internet_archive", "abc123")
    assert plugin.id_from_url("https://example.org/details/abc123") is None

    assert plugin.create_query(identifiers={"ia": "abc123"}) == 'identifier:"abc123"'
    assert plugin.create_query(identifiers={"isbn": "9780306406157"}) == (
        '(isbn:9780306406157 OR external-identifier:"urn:isbn:9780306406157")'
    )
    query = plugin.create_query(title="The Great Gatsby", authors=["F. Scott Fitzgerald"], identifiers={})
    assert "mediatype:texts" in query
    assert 'title:"Great Gatsby"' in query
    assert 'creator:"Scott Fitzgerald"' in query
    assert plugin.create_query(title=None, authors=None, identifiers={}) is None


def test_internet_archive_build_urls_and_retry_helpers() -> None:
    """
    Verify internet archive build urls and retry helpers.

    Example:
        Exercise test internet archive build urls and retry helpers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    url = plugin._build_search_url("mediatype:texts", count=3, page=2)
    assert url.startswith("https://archive.org/advancedsearch.php?")
    assert "q=mediatype%3Atexts" in url
    assert "fl%5B%5D=identifier" in url
    assert "rows=3" in url
    assert "page=2" in url
    assert "output=json" in url
    assert plugin._build_metadata_url("abc123") == "https://archive.org/metadata/abc123"

    policy = plugin._retry_policy()
    assert policy.attempts == plugin.HTTP_RETRY_ATTEMPTS
    assert plugin._retry_backoff(1) == plugin.HTTP_RETRY_BASE_SECONDS
    assert plugin._retry_backoff(99) == plugin.HTTP_RETRY_MAX_SECONDS
    assert plugin._wait_for_backoff(Event(), 0) is False
    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0) is True


def test_internet_archive_records_from_payloads() -> None:
    """
    Verify internet archive records from payloads.

    Example:
        Exercise test internet archive records from payloads through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    doc = _sample_search_doc()
    assert plugin._records_from_search_payload({"response": {"docs": ["skip", doc]}}) == [doc]
    assert plugin._records_from_search_payload({"response": {}}) == []
    assert plugin._records_from_search_payload([]) == []

    record = plugin._record_from_metadata_payload(_sample_metadata_payload("ia-meta"))
    assert record["identifier"] == "ia-meta"
    assert record["files"][0]["name"] == "__ia_thumb.jpg"
    assert record["server"] == "ia800000.us.archive.org"
    assert plugin._record_from_metadata_payload({"error": "item not found"}) is None
    assert plugin._record_from_metadata_payload([]) is None


def test_internet_archive_metadata_from_record(monkeypatch) -> None:
    """
    Verify internet archive metadata from record.

    Example:
        Exercise test internet archive metadata from record through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.internet_archive as ia

    plugin = ia.InternetArchive()
    monkeypatch.setattr(ia, "parse_only_date", lambda raw: datetime(1937, 9, 21))
    monkeypatch.setattr(ia, "canonicalize_lang", lambda raw: "en" if str(raw).lower() == "eng" else None)

    mi = plugin._metadata_from_record(_sample_search_doc("ia-doc"), relevance=4)
    assert mi.source_relevance == 4
    assert mi.title == "Internet Archive Sample"
    assert mi.authors == ["Alice Author", "Bob Author"]
    assert mi.comments == "A useful archived text."
    assert mi.publisher == "Example Press"
    assert mi.pubdate == datetime(1937, 9, 21)
    assert mi.language == "en"
    assert mi.tags == ["Fantasy fiction", "Libraries", "Catalogs"]
    assert mi.get_identifiers()["internet_archive"] == "ia-doc"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert mi.get_identifiers()["lccn"] == "2020123456"
    assert mi.get_identifiers()["oclc"] == "123456789"
    assert mi.get_identifiers()["openlibrary"] == "OL123M"
    assert mi.has_internet_archive_cover == "https://archive.org/services/img/ia-doc"


def test_internet_archive_metadata_fallbacks_and_thumbnail_file(monkeypatch) -> None:
    """
    Verify internet archive metadata fallbacks and thumbnail file.

    Example:
        Exercise test internet archive metadata fallbacks and thumbnail file through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.internet_archive as ia

    plugin = ia.InternetArchive()
    monkeypatch.setattr(ia, "parse_only_date", lambda raw: (_ for _ in ()).throw(ValueError("bad date")))

    record = plugin._record_from_metadata_payload(_sample_metadata_payload("ia-meta"))
    record["title"] = ""
    record["creator"] = ""
    record["date"] = "published in 1912"
    mi = plugin._metadata_from_record(record)
    assert mi.title == "Unknown"
    assert mi.authors == ["Unknown"]
    assert mi.pubdate is None
    assert mi.has_internet_archive_cover == "https://archive.org/download/ia-meta/__ia_thumb.jpg"


def test_internet_archive_postprocess_caches_identifiers() -> None:
    """
    Verify internet archive postprocess caches identifiers.

    Example:
        Exercise test internet archive postprocess caches identifiers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    mi = plugin._metadata_from_record(_sample_search_doc("ia-cache"))
    out = plugin._postprocess_downloaded_metadata(mi, relevance=7)

    assert out is mi
    assert out.source_relevance == 7
    assert plugin.cached_isbn_to_identifier("9780306406157") == "ia-cache"
    assert plugin.cached_identifier_to_cover_url("ia-cache") == "https://archive.org/services/img/ia-cache"
    assert plugin.get_cached_cover_url({"isbn": "9780306406157"}) == "https://archive.org/services/img/ia-cache"
    assert plugin.get_cached_cover_url({"internet_archive": "ia-cache"}) == "https://archive.org/services/img/ia-cache"
    assert plugin._postprocess_downloaded_metadata(None) is None


def test_internet_archive_collects_both_external_identifier_spellings() -> None:
    """
    Verify internet archive collects both external identifier spellings.

    Example:
        Exercise test internet archive collects both external identifier spellings through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    record = {
        "identifier": "ia-ids",
        "title": "Identifier Sample",
        "creator": "Alice Author",
        "external-identifier": ["urn:lccn: 2020 123456"],
        "external_identifier": ["urn:isbn:9780306406157", "urn:oclc: 123-456"],
    }

    mi = plugin._metadata_from_record(record)
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert mi.get_identifiers()["lccn"] == "2020123456"
    assert mi.get_identifiers()["oclc"] == "123456"


def test_internet_archive_request_json_failure_is_logged(monkeypatch) -> None:
    """
    Verify internet archive request json failure remains logged.

    Example:
        Exercise test internet archive request json failure is logged through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    monkeypatch.setattr(plugin, "_request_json_with_backoff", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    log = _Log()
    assert plugin._request_json_or_none(log, Event(), "https://archive.org/advancedsearch.php", 1, "test") is None
    assert any(level == "warning" and "failed" in parts[0] for level, parts in log.events)


def test_internet_archive_identify_search_and_metadata_paths(monkeypatch) -> None:
    """
    Verify internet archive identify search and metadata paths.

    Example:
        Exercise test internet archive identify search and metadata paths through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    calls = []

    def _request(log, abort, url, timeout, context):
        """
        Perform the request test-helper operation with deterministic inputs.

        Example:
            Exercise test internet archive identify search and metadata paths.request through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param context: Value supplied for context in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append((url, context))
        return {"response": {"docs": [_sample_search_doc("ia-search")]}}

    monkeypatch.setattr(plugin, "_request_json_or_none", _request)
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["internet_archive"] == "ia-search"
    assert calls[0][1] == "Internet Archive identify query"
    assert "advancedsearch.php" in calls[0][0]

    plugin = InternetArchive()
    calls = []
    monkeypatch.setattr(
        plugin,
        "_request_json_or_none",
        lambda **kwargs: calls.append((kwargs["url"], kwargs["context"])) or _sample_metadata_payload("ia-meta"),
    )
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={"internet_archive": "ia-meta"})
    assert out.get_nowait().get_identifiers()["internet_archive"] == "ia-meta"
    assert calls[0][1] == "Internet Archive metadata lookup"
    assert calls[0][0].endswith("/ia-meta")


def test_internet_archive_identify_empty_abort_and_parse_failure(monkeypatch) -> None:
    """
    Verify internet archive identify empty abort and parse failure.

    Example:
        Exercise test internet archive identify empty abort and parse failure through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    called = {"request": False}
    monkeypatch.setattr(plugin, "_request_json_or_none", lambda **kwargs: called.__setitem__("request", True))
    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, title="Title")
    assert called["request"] is False
    assert out.empty()

    plugin = InternetArchive()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={})
    assert out.empty()

    plugin = InternetArchive()
    monkeypatch.setattr(plugin, "_request_json_or_none", lambda **kwargs: {"response": {"docs": [_sample_search_doc()]}})
    monkeypatch.setattr(plugin, "_metadata_from_record", lambda record, relevance=0: (_ for _ in ()).throw(RuntimeError("bad parse")))
    log = _Log()
    out = queue.Queue()
    plugin.identify(log=log, result_queue=out, abort=Event(), title="Title")
    assert out.empty()
    assert any(level == "exception" for level, _parts in log.events)


def test_internet_archive_download_cover_uses_cached_url(monkeypatch) -> None:
    """
    Verify internet archive download cover uses cached url.

    Example:
        Exercise test internet archive download cover uses cached url through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.internet_archive import InternetArchive

    plugin = InternetArchive()
    plugin.cache_identifier_to_cover_url("ia-cover", "https://archive.org/services/img/ia-cover")
    calls = []
    monkeypatch.setattr(
        plugin,
        "_request_bytes_with_backoff",
        lambda log, abort, url, timeout, context: calls.append((url, context)) or b"cover-bytes",
    )
    out = queue.Queue()
    plugin.download_cover(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"internet_archive": "ia-cover"},
    )
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"
    assert calls == [("https://archive.org/services/img/ia-cover", "Internet Archive cover download")]


def test_internet_archive_imports_from_known_modules() -> None:
    """
    Verify internet archive imports from known modules.

    Example:
        Exercise test internet archive imports from known modules through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_internet_archive.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources import import_web_source_module, iter_known_web_source_modules

    assert "internet_archive" in iter_known_web_source_modules()
    mod = import_web_source_module("internet_archive")
    assert hasattr(mod, "InternetArchive")
