"""
Verify Library of Congress search/item parsing, identifiers and covers.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test web sources library of congress through its owning regression module::

        python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py
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

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py
    """
    def __init__(self) -> None:
        """
        Initialize the Log test double.

        Example:
            Exercise Log.init through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


        :return: None; the function records state or raises through its assertions.
        """
        self.events = []

    def __call__(self, *parts):
        """
        Perform the call test-helper operation with deterministic inputs.

        Example:
            Exercise Log.call through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("call", parts))

    def info(self, *parts):
        """
        Perform the info test-helper operation with deterministic inputs.

        Example:
            Exercise Log.info through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("info", parts))

    def warning(self, *parts):
        """
        Perform the warning test-helper operation with deterministic inputs.

        Example:
            Exercise Log.warning through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("warning", parts))

    def error(self, *parts):
        """
        Perform the error test-helper operation with deterministic inputs.

        Example:
            Exercise Log.error through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("error", parts))

    def exception(self, *parts):
        """
        Perform the exception test-helper operation with deterministic inputs.

        Example:
            Exercise Log.exception through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


        :param parts: Value supplied for parts in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.events.append(("exception", parts))


def _sample_record() -> dict:
    """
    Perform the sample record test-helper operation with deterministic inputs.

    Example:
        Exercise sample record through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "title": "LoC Sample Title",
        "contributor_names": ["Alice Author", "Bob Author"],
        "summary": ["A short summary."],
        "publisher": ["Example House"],
        "date": "2020",
        "language": ["english"],
        "subject": ["Fantasy fiction", "Libraries -- Catalogs"],
        "item_id": "loc.sample.1",
        "url": "https://www.loc.gov/item/loc.sample.1/",
        "number": [
            "isbn 9780306406157",
            "lccn 2020123456",
            "oclc 123456789",
        ],
        "image_url": [
            "https://www.loc.gov/static/images/favicons/open-graph-logo.png",
            "//tile.loc.gov/storage-services/service/gdc/gdcwdl/wdl_00001/_800px.jpg",
            "//tile.loc.gov/storage-services/service/gdc/gdcwdl/wdl_00001/_200px.jpg",
        ],
    }


def test_web_sources_library_of_congress_import_smoke() -> None:
    """
    Verify web sources library of congress import smoke.

    Example:
        Exercise test web sources library of congress import smoke through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.library_of_congress as loc

    assert loc is not None


def test_library_of_congress_helper_edges_and_guard_detection() -> None:
    """
    Verify library of congress helper edges and guard detection.

    Example:
        Exercise test library of congress helper edges and guard detection through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.library_of_congress as loc

    class BadString:
        """
        Provide the BadString test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test library of congress helper edges and guard detection.BadString through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py
        """
        def __str__(self):
            """
            Perform the str test-helper operation with deterministic inputs.

            Example:
                Exercise test library of congress helper edges and guard detection.BadString.str through its owning regression module::

                    python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("bad")

    assert loc._as_text(b"hello") == "hello"
    assert loc._as_text(None) == ""
    assert loc._as_text(BadString()) == ""
    assert loc._first({"a": "b"}) == "b"
    assert loc._first(iter(["one"])) == "one"
    assert loc._first([]) is None
    assert loc._as_list("x") == ["x"]
    assert loc._as_list({"x": "y"}) == [{"x": "y"}]
    assert loc._safe_isbn({"isbn": "bad", "isbn13": "9780306406157"}) == "9780306406157"
    assert loc._compact_lccn(" 2020 123456 ") == "2020123456"
    assert loc._normalize_url("//tile.loc.gov/cover.jpg") == "https://tile.loc.gov/cover.jpg"
    assert loc._normalize_url("/item/abc/") == "https://www.loc.gov/item/abc/"
    assert loc._looks_like_guard_page("<html><title>Just a moment...</title>cf_chl</html>")
    assert not loc._looks_like_guard_page('{"results": []}')


def test_library_of_congress_get_book_url_id_from_url_and_queries() -> None:
    """
    Verify library of congress get book url id from url and queries.

    Example:
        Exercise test library of congress get book url id from url and queries through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress

    plugin = LibraryOfCongress()
    assert plugin.get_book_url({}) is None
    assert plugin.get_book_url({"loc": "loc.sample.1"}) == (
        "loc",
        "loc.sample.1",
        "https://www.loc.gov/item/loc.sample.1/",
    )
    assert plugin.get_book_url({"lccn": " 2020123456 "}) == (
        "lccn",
        "2020123456",
        "https://lccn.loc.gov/2020123456",
    )
    assert plugin.id_from_url("https://www.loc.gov/item/loc.sample.1/") == ("loc", "loc.sample.1")
    assert plugin.id_from_url("https://lccn.loc.gov/2020123456") == ("lccn", "2020123456")
    assert plugin.id_from_url("https://example.org/item/loc.sample.1/") is None

    assert plugin.create_query(identifiers={"loc": "loc.sample.1"}) == "loc.sample.1"
    assert plugin.create_query(identifiers={"lccn": " 2020 123456 "}) == "2020123456"
    assert plugin.create_query(identifiers={"isbn": "9780306406157"}) == "9780306406157"
    query = plugin.create_query(title="The Great Gatsby", authors=["F. Scott Fitzgerald"], identifiers={})
    assert "Great" in query
    assert "Fitzgerald" in query
    assert plugin.create_query(title=None, authors=None, identifiers={}) is None


def test_library_of_congress_build_urls_and_retry_helpers() -> None:
    """
    Verify library of congress build urls and retry helpers.

    Example:
        Exercise test library of congress build urls and retry helpers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress

    plugin = LibraryOfCongress()
    url = plugin._build_search_url("The Hobbit", count=3, page=2)
    assert url.startswith("https://www.loc.gov/books/?")
    assert "fo=json" in url
    assert "q=The+Hobbit" in url
    assert "c=3" in url
    assert "sp=2" in url
    assert plugin._build_item_url("loc.sample.1") == "https://www.loc.gov/item/loc.sample.1/?fo=json"

    policy = plugin._retry_policy()
    assert policy.attempts == plugin.HTTP_RETRY_ATTEMPTS
    assert plugin._retry_backoff(1) == plugin.HTTP_RETRY_BASE_SECONDS
    assert plugin._retry_backoff(99) == plugin.HTTP_RETRY_MAX_SECONDS
    assert plugin._wait_for_backoff(Event(), 0) is False
    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0) is True


def test_library_of_congress_metadata_from_search_record(monkeypatch) -> None:
    """
    Verify library of congress metadata from search record.

    Example:
        Exercise test library of congress metadata from search record through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.library_of_congress as loc

    plugin = loc.LibraryOfCongress()
    monkeypatch.setattr(loc, "parse_only_date", lambda raw: datetime(2020, 6, 15))
    monkeypatch.setattr(loc, "canonicalize_lang", lambda raw: "en" if str(raw).lower() == "english" else None)

    mi = plugin._metadata_from_record(_sample_record(), relevance=4)
    assert mi.source_relevance == 4
    assert mi.title == "LoC Sample Title"
    assert mi.authors == ["Alice Author", "Bob Author"]
    assert mi.comments == "A short summary."
    assert mi.publisher == "Example House"
    assert mi.pubdate == datetime(2020, 6, 15)
    assert mi.language == "en"
    assert mi.tags == ["Fantasy fiction", "Libraries", "Catalogs"]
    assert mi.get_identifiers()["loc"] == "loc.sample.1"
    assert mi.get_identifiers()["isbn"] == "9780306406157"
    assert mi.get_identifiers()["lccn"] == "2020123456"
    assert mi.get_identifiers()["oclc"] == "123456789"
    assert mi.all_isbns == ["9780306406157"]
    assert mi.has_loc_cover == "https://tile.loc.gov/storage-services/service/gdc/gdcwdl/wdl_00001/_800px.jpg"


def test_library_of_congress_metadata_fallbacks_and_item_payload(monkeypatch) -> None:
    """
    Verify library of congress metadata fallbacks and item payload.

    Example:
        Exercise test library of congress metadata fallbacks and item payload through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.web_sources.library_of_congress as loc

    plugin = loc.LibraryOfCongress()
    monkeypatch.setattr(loc, "parse_only_date", lambda raw: (_ for _ in ()).throw(ValueError("bad date")))

    records = plugin._records_from_payload(
        {
            "item": {
                "title": "",
                "contributors": [{"name": "Mapped Author"}],
                "created_published": ["Boston : Created Publisher, 1999."],
                "date": "not a date",
                "number_lccn": [" 2020 123456 "],
            },
            "resources": [{"image": "https://tile.loc.gov/cover_400px.jpg"}],
        }
    )
    assert len(records) == 1
    mi = plugin._metadata_from_record(records[0])
    assert mi.title == "Unknown"
    assert mi.authors == ["Mapped Author"]
    assert mi.publisher == "Created Publisher"
    assert mi.get_identifiers()["lccn"] == "2020123456"
    assert mi.has_loc_cover == "https://tile.loc.gov/cover_400px.jpg"
    assert getattr(mi, "pubdate", None) is None

    assert plugin._records_from_payload({"results": ["skip", _sample_record()]}) == [_sample_record()]
    assert plugin._records_from_payload([]) == []


def test_library_of_congress_postprocess_caches_identifiers() -> None:
    """
    Verify library of congress postprocess caches identifiers.

    Example:
        Exercise test library of congress postprocess caches identifiers through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress

    plugin = LibraryOfCongress()
    mi = plugin._metadata_from_record(_sample_record())
    out = plugin._postprocess_downloaded_metadata(mi, relevance=7)

    assert out is mi
    assert out.source_relevance == 7
    assert plugin.cached_isbn_to_identifier("9780306406157") == "loc.sample.1"
    assert plugin.cached_identifier_to_cover_url("loc.sample.1").endswith("_800px.jpg")
    assert plugin.get_cached_cover_url({"isbn": "9780306406157"}).endswith("_800px.jpg")
    assert plugin.get_cached_cover_url({"loc": "loc.sample.1"}).endswith("_800px.jpg")
    assert plugin._postprocess_downloaded_metadata(None) is None


def test_library_of_congress_request_json_guard_is_logged(monkeypatch) -> None:
    """
    Verify library of congress request json guard remains logged.

    Example:
        Exercise test library of congress request json guard is logged through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress

    plugin = LibraryOfCongress()
    monkeypatch.setattr(
        plugin,
        "_request_bytes",
        lambda url, timeout=30: b"<html><title>Just a moment...</title>cf_chl</html>",
    )
    log = _Log()
    assert plugin._request_json_or_none(log, Event(), "https://www.loc.gov/books/?fo=json", 1, "test") is None
    assert any(level == "warning" and "blocked" in parts[0] for level, parts in log.events)


def test_library_of_congress_identify_search_and_item_paths(monkeypatch) -> None:
    """
    Verify library of congress identify search and item paths.

    Example:
        Exercise test library of congress identify search and item paths through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress

    plugin = LibraryOfCongress()
    calls = []

    def _request(log, abort, url, timeout, context):
        """
        Perform the request test-helper operation with deterministic inputs.

        Example:
            Exercise test library of congress identify search and item paths.request through its owning regression module::

                python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


        :param log: Value supplied for log in the focused test operation.
        :param abort: Value supplied for abort in the focused test operation.
        :param url: Value supplied for url in the focused test operation.
        :param timeout: Value supplied for timeout in the focused test operation.
        :param context: Value supplied for context in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append((url, context))
        return {"results": [_sample_record()]}

    monkeypatch.setattr(plugin, "_request_json_or_none", _request)
    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9780306406157"},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["loc"] == "loc.sample.1"
    assert calls[0][1] == "Library of Congress identify query"
    assert "q=9780306406157" in calls[0][0]

    plugin = LibraryOfCongress()
    calls = []
    monkeypatch.setattr(
        plugin,
        "_request_json_or_none",
        lambda **kwargs: calls.append((kwargs["url"], kwargs["context"])) or {"item": _sample_record()},
    )
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={"loc": "loc.sample.1"})
    assert out.get_nowait().get_identifiers()["loc"] == "loc.sample.1"
    assert calls[0][1] == "Library of Congress item lookup"
    assert calls[0][0].endswith("/loc.sample.1/?fo=json")


def test_library_of_congress_identify_empty_abort_and_parse_failure(monkeypatch) -> None:
    """
    Verify library of congress identify empty abort and parse failure.

    Example:
        Exercise test library of congress identify empty abort and parse failure through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress

    plugin = LibraryOfCongress()
    called = {"request": False}
    monkeypatch.setattr(plugin, "_request_json_or_none", lambda **kwargs: called.__setitem__("request", True))
    abort = Event()
    abort.set()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, title="Title")
    assert called["request"] is False
    assert out.empty()

    plugin = LibraryOfCongress()
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=Event(), identifiers={})
    assert out.empty()

    plugin = LibraryOfCongress()
    monkeypatch.setattr(plugin, "_request_json_or_none", lambda **kwargs: {"results": [_sample_record()]})
    monkeypatch.setattr(plugin, "_metadata_from_record", lambda record, relevance=0: (_ for _ in ()).throw(RuntimeError("bad parse")))
    log = _Log()
    out = queue.Queue()
    plugin.identify(log=log, result_queue=out, abort=Event(), title="Title")
    assert out.empty()
    assert any(level == "exception" for level, _parts in log.events)


def test_library_of_congress_download_cover_uses_cached_url(monkeypatch) -> None:
    """
    Verify library of congress download cover uses cached url.

    Example:
        Exercise test library of congress download cover uses cached url through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources.library_of_congress import LibraryOfCongress

    plugin = LibraryOfCongress()
    plugin.cache_identifier_to_cover_url("loc.sample.1", "https://tile.loc.gov/cover_800px.jpg")
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
        identifiers={"loc": "loc.sample.1"},
    )
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"
    assert calls == [("https://tile.loc.gov/cover_800px.jpg", "Library of Congress cover download")]


def test_library_of_congress_imports_from_known_modules() -> None:
    """
    Verify library of congress imports from known modules.

    Example:
        Exercise test library of congress imports from known modules through its owning regression module::

            python -m pytest -q tests/metadata/web_sources/test_web_sources_library_of_congress.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.web_sources import import_web_source_module, iter_known_web_source_modules

    assert "library_of_congress" in iter_known_web_source_modules()
    mod = import_web_source_module("library_of_congress")
    assert hasattr(mod, "LibraryOfCongress")
