from __future__ import annotations

import hashlib
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


def _sample_item(google_id: str = "gid-1") -> dict:
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


def test_web_sources_google_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.google as google

    assert google is not None


def test_google_get_book_url_and_id_from_url() -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    assert plugin.get_book_url({"google": {"abc123"}}) == (
        "google",
        "abc123",
        "https://books.google.com/books?id=abc123",
    )
    assert plugin.id_from_url("https://books.google.com/books?id=abc123") == ("google", "abc123")
    assert plugin.id_from_url("https://books.google.co.uk/books?id=abc123") == ("google", "abc123")


def test_google_create_query_prefers_isbn_and_handles_iterable_identifiers() -> None:
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


def test_google_item_to_metadata_parses_volume_info(monkeypatch) -> None:
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


def test_google_postprocess_caches_cover_and_isbn_mappings() -> None:
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


def test_google_identify_uses_google_identifier_lookup(monkeypatch) -> None:
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


def test_google_identify_retries_text_query_after_empty_isbn_query(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    calls = []

    def _fake_request_json(path="", timeout=30, **params):
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


def test_google_get_cached_cover_url_uses_isbn_cache_with_iterables() -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    plugin = GoogleBooks()
    plugin.cache_isbn_to_identifier("9780306406157", "gid-cache")
    assert "gid-cache" in plugin.get_cached_cover_url({"isbn": {"978-0-306-40615-7"}})


def test_google_download_cover_ignores_dummy_and_uses_next_zoom(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Resp:
        def __init__(self, payload):
            self.payload = payload

        def read(self):
            return self.payload

    payloads = [b"dummy", b"real-cover"]
    seen_urls = []

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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


def test_google_download_cover_runs_identify_when_cache_misses(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Resp:
        @staticmethod
        def read():
            return b"cover-bytes"

    seen = {}

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
            del timeout
            seen["url"] = url
            return _Resp()

    def _identify(log, result_queue, abort, title=None, authors=None, identifiers=None, timeout=30):
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


def test_google_download_cover_no_results_logs_and_returns(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    def _identify(log, result_queue, abort, title=None, authors=None, identifiers=None, timeout=30):
        del log, result_queue, abort, title, authors, identifiers, timeout

    plugin = GoogleBooks()
    monkeypatch.setattr(plugin, "identify", _identify)

    out = queue.Queue()
    logger = _Log()
    plugin.download_cover(log=logger, result_queue=out, abort=Event(), identifiers={})

    assert out.empty()
    assert any(level == "info" and "No cover found" in " ".join(map(str, parts)) for level, parts in logger.events)


def test_google_request_json_with_backoff_retries_transient_errors(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Transient(Exception):
        @staticmethod
        def getcode():
            return 503

    plugin = GoogleBooks()
    attempts = {"n": 0}
    delays = []

    def _fake_request_json(path="", timeout=30, **params):
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
    assert any(level == "warning" and "retrying with backoff" in " ".join(map(str, parts)) for level, parts in log.events)


def test_google_open_with_backoff_non_retryable_raises_and_logs(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.google import GoogleBooks

    class _Browser:
        @staticmethod
        def open_novisit(url, timeout=30):
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
    assert any(level == "exception" and "Google cover request failed" in " ".join(map(str, parts)) for level, parts in log.events)
