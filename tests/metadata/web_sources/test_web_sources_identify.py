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
