from __future__ import annotations

from datetime import datetime
from threading import Event

from LiuXin_alpha.metadata.utils import calibreMetaInformation


class _Plugin:
    def __init__(self, name: str, results, *, has_html_comments: bool = False):
        self.name = name
        self.version = (1, 0, 0)
        self._results = list(results)
        self.has_html_comments = has_html_comments
        self.cached_cover_url_is_reliable = True
        self.prefer_results_with_isbn = True
        self.prefs = {"ignore_fields": []}
        self.touched_fields = frozenset()

    @staticmethod
    def is_configured() -> bool:
        return True

    def identify(self, log, result_queue, abort, **kwargs):
        del log, kwargs
        if abort.is_set():
            return
        for result in self._results:
            result_queue.put(result)

    @staticmethod
    def identify_results_keygen(**kwargs):
        del kwargs
        return lambda mi: getattr(mi, "source_relevance", 0)

    @staticmethod
    def get_cached_cover_url(_identifiers):
        return "https://covers.example/cover.jpg"

    @staticmethod
    def browser():
        return type("B", (), {"addheaders": [("User-Agent", "test")]})()


def _mi(title: str, authors: list[str], *, isbn: str | None = None, comments: str = "", pubyear: int | None = None):
    mi = calibreMetaInformation(title, authors)
    if isbn:
        mi.set_identifier("isbn", isbn)
    if comments:
        mi.comments = comments
    if pubyear is not None:
        mi.pubdate = datetime(pubyear, 1, 2)
    return mi


def _prefs(**overrides):
    prefs = {
        "wait_after_first_identify_result": 0.1,
        "txt_comments": False,
        "max_tags": 20,
        "swap_author_names": False,
        "find_first_edition_date": False,
        "fewer_tags": True,
        "id_link_rules": {},
        "keep_dups": False,
    }
    prefs.update(overrides)
    return prefs


def _ranked_result(mi, plugin, relevance: int):
    mi.identify_plugin = plugin
    mi.relevance_in_source = relevance
    mi.average_source_relevance = relevance
    return mi


def test_web_sources_identify_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    assert identify is not None


def test_urls_from_identifiers_default_links() -> None:
    from LiuXin_alpha.metadata.web_sources.identify import urls_from_identifiers

    urls = urls_from_identifiers(
        {
            "isbn": "9780306406157",
            "doi": "10.5555/12345678",
            "arxiv": "2401.01234",
            "oclc": "123456",
            "issn": "2049-3630",
        }
    )
    got = {(id_type, value, url) for _name, id_type, value, url in urls}

    assert ("isbn", "9780306406157", "https://www.worldcat.org/isbn/9780306406157") in got
    assert ("doi", "10.5555/12345678", "https://dx.doi.org/10.5555/12345678") in got
    assert ("arxiv", "2401.01234", "https://arxiv.org/abs/2401.01234") in got
    assert ("oclc", "123456", "https://www.worldcat.org/oclc/123456") in got
    assert ("issn", "20493630", "https://www.worldcat.org/issn/20493630") in got


def test_urls_from_identifiers_uses_plugin_links(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    class _Plugin:
        name = "PluginX"

        @staticmethod
        def get_book_urls(_identifiers):
            return (("plugin-id", "abc", "https://example.invalid/books/abc"),)

        @staticmethod
        def get_book_url_name(id_type, id_val, _url):
            return f"{id_type}:{id_val}"

    monkeypatch.setattr(identify, "_iter_all_metadata_plugins", lambda: [_Plugin()])

    urls = identify.urls_from_identifiers({"isbn": "9780306406157"})
    assert ("plugin-id:abc", "plugin-id", "abc", "https://example.invalid/books/abc") in urls


def test_urls_from_identifiers_accepts_uri_fields_and_sorting() -> None:
    from LiuXin_alpha.metadata.web_sources.identify import urls_from_identifiers

    urls = urls_from_identifiers(
        {
            "uri1": "https://example.invalid/path",
            "url": "https://another.invalid/item",
        },
        sort_results=True,
    )
    names = [name for name, *_rest in urls]

    assert "example.invalid" in names
    assert "another.invalid" in names
    assert names == sorted(names, key=str.casefold)


def test_identify_respects_allowed_plugins_and_html_to_text(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    a = _Plugin(
        "A",
        [_mi("Caf\u0065\u0301", ["Jane Doe"], comments="<p>Hello <b>world</b></p>")],
        has_html_comments=True,
    )
    b = _Plugin("B", [_mi("Ignored", ["Ignored"])], has_html_comments=False)

    monkeypatch.setattr(identify, "_iter_metadata_plugins", lambda _caps: [a, b])
    monkeypatch.setattr(
        identify,
        "msprefs",
        {
            "wait_after_first_identify_result": 0.1,
            "txt_comments": True,
            "max_tags": 20,
            "swap_author_names": False,
            "find_first_edition_date": False,
            "fewer_tags": True,
            "id_link_rules": {},
            "keep_dups": False,
        },
    )

    merged = identify.identify(log=lambda *a: None, abort=Event(), identifiers={}, allowed_plugins={"A"})
    assert len(merged) == 1
    assert merged[0].title == "Caf\u00e9"
    assert "<" not in (merged[0].comments or "")


def test_identify_swaps_author_names_when_enabled(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    p = _Plugin("A", [_mi("Title", ["Jane Mary Doe"])])
    monkeypatch.setattr(identify, "_iter_metadata_plugins", lambda _caps: [p])
    monkeypatch.setattr(
        identify,
        "msprefs",
        {
            "wait_after_first_identify_result": 0.1,
            "txt_comments": False,
            "max_tags": 20,
            "swap_author_names": True,
            "find_first_edition_date": False,
            "fewer_tags": True,
            "id_link_rules": {},
            "keep_dups": False,
        },
    )

    merged = identify.identify(log=lambda *a: None, abort=Event(), identifiers={})
    assert merged[0].authors == ["Doe, Jane Mary"]


def test_isbn_merge_respects_keep_dups(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    plugin = _Plugin("A", [])
    r1 = _mi("Title One", ["Author One"])
    r2 = _mi("Title Two", ["Author Two"])
    r1.identify_plugin = plugin
    r2.identify_plugin = plugin
    r1.relevance_in_source = 0
    r2.relevance_in_source = 1

    monkeypatch.setattr(identify, "msprefs", {"keep_dups": False, "fewer_tags": True})
    merger = identify.ISBNMerge(log=lambda *a: None)
    merger.add_result(r1)
    merger.add_result(r2)
    out = merger.finalize()
    assert len(out) == 1

    monkeypatch.setattr(identify, "msprefs", {"keep_dups": True, "fewer_tags": True})
    merger = identify.ISBNMerge(log=lambda *a: None)
    merger.add_result(r1)
    merger.add_result(r2)
    out = merger.finalize()
    assert len(out) == 2


def test_urls_from_identifiers_normalizes_iterables_and_pipe_urls(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    monkeypatch.setattr(identify, "_iter_all_metadata_plugins", lambda: [])
    urls = identify.urls_from_identifiers(
        {
            "ISBN": [b"9780306406157"],
            "uri1": "https|//example.invalid/path|with|pipes",
        }
    )
    got = {(id_type, value, url) for _name, id_type, value, url in urls}
    assert ("isbn", "9780306406157", "https://www.worldcat.org/isbn/9780306406157") in got
    assert (
        "uri1",
        "https://example.invalid/path,with,pipes",
        "https://example.invalid/path,with,pipes",
    ) in got


def test_urls_from_identifiers_uses_id_link_rules(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    monkeypatch.setattr(identify, "_iter_all_metadata_plugins", lambda: [])
    monkeypatch.setattr(
        identify,
        "msprefs",
        {
            "id_link_rules": {
                "isbn": [
                    ("Custom ISBN", "https://id.example/books/{id_unquoted}"),
                ]
            }
        },
    )
    urls = identify.urls_from_identifiers({"isbn": "9780306406157"})
    assert ("Custom ISBN", "isbn", "9780306406157", "https://id.example/books/9780306406157") in urls


def test_worker_records_plugin_failures() -> None:
    from LiuXin_alpha.metadata.web_sources.identify import Worker

    class BadPlugin:
        name = "BadPlugin"

        def identify(self, log, result_queue, abort, **kwargs):
            del log, result_queue, abort, kwargs
            raise RuntimeError("offline failure")

    plugin = BadPlugin()
    worker = Worker(plugin, {}, Event())

    worker.run()

    assert plugin.dl_time_spent >= 0
    log_text = worker.buf.getvalue()
    assert "BadPlugin" in log_text
    assert "offline failure" in log_text


def test_xisbn_thread_records_service_exceptions(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    class BrokenXISBN:
        service_available = True

        @staticmethod
        def get_isbn_pool(isbn):
            raise ValueError(f"bad isbn lookup: {isbn}")

    monkeypatch.setattr(identify, "xisbn", BrokenXISBN())

    worker = identify.xISBN("9780306406157")
    worker.run()

    assert isinstance(worker.exception, ValueError)
    assert worker.tb
    assert any("bad isbn lookup" in line for line in worker.tb)


def test_isbn_merge_uses_xisbn_pool_and_first_edition_date(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    class FakeXISBN:
        service_available = True

        @staticmethod
        def get_isbn_pool(isbn):
            assert isbn in {"9780000000001", "9780000000002"}
            return frozenset({"9780000000001", "9780000000002"}), 1999

    plugin_a = _Plugin("A", [])
    plugin_b = _Plugin("B", [])
    first = _ranked_result(
        _mi("Longer Shared Title", ["Jane Example"], isbn="9780000000001", pubyear=2005),
        plugin_a,
        0,
    )
    first.publisher = "Short"
    first.tags = ["space opera", "award winner", "translation"]
    first.series = "Sequence"
    first.series_index = 4
    first.rating = 4
    second = _ranked_result(
        _mi("Shared Title", ["Jane Example", "Janet Example"], isbn="9780000000002", pubyear=1999),
        plugin_b,
        2,
    )
    second.publisher = "Longer Publisher"
    second.tags = ["space opera"]
    second.series = "Sequence"
    second.series_index = 1
    second.rating = 5

    monkeypatch.setattr(identify, "xisbn", FakeXISBN())
    monkeypatch.setattr(identify, "msprefs", _prefs(find_first_edition_date=True, fewer_tags=True))

    merger = identify.ISBNMerge(log=lambda *parts: None)
    merger.add_result(first)
    merger.add_result(second)
    merged = merger.finalize()

    assert len(merged) == 1
    result = merged[0]
    assert result.title == "Shared Title"
    assert result.authors == ["Jane Example", "Janet Example"]
    assert result.publisher == "Short"
    assert result.tags == ["space opera"]
    assert result.series == "Sequence"
    assert result.series_index == 4
    assert result.rating == 4
    assert result.pubdate.year == 1999
    assert result.average_source_relevance == 1
    assert result.get_identifiers()["isbn"] == "9780000000002"


def test_isbn_merge_drops_isbnless_duplicates_from_preferred_sources(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    plugin = _Plugin("A", [])
    isbn_result = _ranked_result(_mi("ISBN Result", ["Author"], isbn="9780306406157"), plugin, 0)
    isbnless = _ranked_result(_mi("ISBN-less Result", ["Author"]), plugin, 1)

    monkeypatch.setattr(identify, "msprefs", _prefs())
    merger = identify.ISBNMerge(log=lambda *parts: None)
    merger.add_result(isbn_result)
    merger.add_result(isbnless)

    assert [r.title for r in merger.finalize()] == ["ISBN Result"]

    plugin.prefer_results_with_isbn = False
    merger = identify.ISBNMerge(log=lambda *parts: None)
    merger.add_result(isbn_result)
    merger.add_result(isbnless)

    assert [r.title for r in merger.finalize()] == ["ISBN Result", "ISBN-less Result"]


def test_merge_metadata_results_can_join_overlapping_identifiers(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    plugin_a = _Plugin("A", [])
    plugin_b = _Plugin("B", [])
    plugin_c = _Plugin("C", [])
    left = _ranked_result(_mi("Left Title", ["Author"]), plugin_a, 0)
    left.set_identifier("doi", "10/example")
    right = _ranked_result(_mi("Right", ["Author"]), plugin_b, 2)
    right.set_identifier("doi", "10/example")
    right.set_identifier("oclc", "123")
    empty = _ranked_result(_mi("No IDs", ["Other"]), plugin_c, 1)

    monkeypatch.setattr(identify, "msprefs", _prefs())
    merger = identify.ISBNMerge(log=lambda *parts: None)
    merger.results = [left, right, empty]

    merger.merge_metadata_results(merge_on_identifiers=True)

    assert len(merger.results) == 2
    merged = next(r for r in merger.results if r.title == "Right")
    assert merged.average_source_relevance == 1
    assert merged.get_identifiers()["doi"] == "10/example"
    assert merged.get_identifiers()["oclc"] == "123"
    assert any(r.title == "No IDs" for r in merger.results)


def test_identify_applies_plugin_ignore_fields_cover_errors_and_output_normalization(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify
    from LiuXin_alpha.utils.date import UNDEFINED_DATE

    class InfoLog:
        def __init__(self):
            self.messages = []

        def info(self, message):
            self.messages.append(message)

    class NoisyResult:
        def __str__(self):
            raise TypeError("not printable")

    class NoisyPlugin(_Plugin):
        name = "Noisy"

        def __init__(self, results):
            super().__init__("Noisy", results, has_html_comments=True)
            self.prefs = {"ignore_fields": ["series", "unknown_field"]}
            self.cached_cover_url_is_reliable = True
            self.touched_fields = frozenset({"publisher"})

        @staticmethod
        def get_cached_cover_url(_identifiers):
            raise RuntimeError("cover cache unavailable")

        @staticmethod
        def browser():
            raise RuntimeError("browser unavailable")

    result = _mi("Cafe\u0301", ["Jose\u0301 Example"], comments="<p>One <b>Two</b></p>")
    result.tags = ["tag-%02d" % i for i in range(5)]
    result.publisher = "Publishe\u0301r"
    result.series = "Should be ignored"
    result.series_index = 9
    result.pubdate = UNDEFINED_DATE
    result.__class__ = type("PrintableMetadata", (result.__class__, NoisyResult), {})
    plugin = NoisyPlugin([result, result])
    log = InfoLog()

    monkeypatch.setattr(identify, "_iter_metadata_plugins", lambda _caps: [plugin])
    monkeypatch.setattr(identify, "msprefs", _prefs(txt_comments=True, max_tags=2))

    merged = identify.identify(log=log, abort=Event(), title="Unknown", authors=["Unknown"], identifiers={})

    assert len(merged) == 1
    out = merged[0]
    assert out.title == "Caf\u00e9"
    assert out.authors == ["Jos\u00e9 Example"]
    assert out.publisher == "Publish\u00e9r"
    assert "<" not in out.comments
    assert out.tags == ["tag-00", "tag-01"]
    assert out.pubdate is None
    assert out.series is None
    assert out.has_cached_cover_url is False
    assert any("Request extra headers:" in msg for msg in log.messages)


def test_urls_from_identifiers_falls_back_when_formatter_or_plugins_fail(monkeypatch) -> None:
    import LiuXin_alpha.metadata.web_sources.identify as identify

    class SingleUrlPlugin:
        name = "Single"

        @staticmethod
        def get_book_url(_identifiers):
            return ("single", "abc", "https://single.example/abc")

        @staticmethod
        def get_book_url_name(id_type, id_val, _url):
            return f"{id_type}:{id_val}"

    class BrokenPlugin:
        @staticmethod
        def get_book_urls(_identifiers):
            raise RuntimeError("provider failed")

    monkeypatch.setattr(identify, "_iter_all_metadata_plugins", lambda: [SingleUrlPlugin(), BrokenPlugin()])
    monkeypatch.setattr(identify, "msprefs", _prefs(id_link_rules={"doi": [("Broken", "https://{missing}")]}))

    urls = identify.urls_from_identifiers(
        {
            None: "ignored",
            "doi": "10.5555/abc",
            "empty": ["", None],
            "custom": "https|//custom.example/path|with|pipes",
            "file": "file:///tmp/book.opf",
        }
    )

    assert ("single:abc", "single", "abc", "https://single.example/abc") in urls
    assert ("DOI", "doi", "10.5555/abc", "https://dx.doi.org/10.5555/abc") in urls
    assert ("custom.example", "custom", "https://custom.example/path,with,pipes", "https://custom.example/path,with,pipes") in urls
    assert ("/tmp/book.opf", "file", "file:///tmp/book.opf", "file:///tmp/book.opf") in urls
