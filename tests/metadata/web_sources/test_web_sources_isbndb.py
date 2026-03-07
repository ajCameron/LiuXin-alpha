from __future__ import annotations

import json
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


def _sample_v2_book(isbn13: str = "9780306406157") -> dict:
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


def test_web_sources_isbndb_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.isbndb as isbndb

    assert isbndb is not None


def test_isbndb_create_query_prefers_isbn_then_search(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin.prefs, "get", lambda key, default=None: "KEY" if key == "isbndb_key" else default)

    queries = plugin.create_query(identifiers={"isbn": "9780306406157"})
    assert queries[0][0] == "v2_book"
    assert queries[0][1].endswith("/book/9780306406157")
    assert "index1=isbn" in queries[1][1]

    queries = plugin.create_query(title="Great Gatsby", authors=["Fitzgerald"], identifiers={})
    assert queries[0][0] == "v2_search"
    assert "/books/Great+Gatsby+Fitzgerald" in queries[0][1]
    assert "index1=combined" in queries[1][1]


def test_isbndb_records_from_json_payload_shapes() -> None:
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    payload = json.dumps({"book": _sample_v2_book()})
    records = plugin._records_from_json_payload(payload)
    assert len(records) == 1
    assert records[0]["title"] == "The Great Gatsby"

    payload = json.dumps({"books": [_sample_v2_book(), _sample_v2_book("9780312621360")]})
    records = plugin._records_from_json_payload(payload)
    assert len(records) == 2


def test_isbndb_metadata_from_record_parses_fields() -> None:
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


def test_isbndb_legacy_xml_payload_is_parsed() -> None:
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    out = plugin._metadata_from_payload(_sample_legacy_xml(), mode="legacy_xml")
    assert len(out) == 1
    mi = out[0]
    assert mi.title == "The Great Gatsby"
    assert mi.authors == ["F. Scott Fitzgerald"]
    assert mi.get_identifiers()["isbn"] == "9780306406157"


def test_isbndb_identify_uses_v2_payload(monkeypatch) -> None:
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


def test_isbndb_identify_falls_back_to_title_author(monkeypatch) -> None:
    from LiuXin_alpha.metadata.web_sources.isbndb import ISBNDB

    plugin = ISBNDB()
    monkeypatch.setattr(plugin, "is_configured", lambda: True)

    calls = []

    def _create_query(title=None, authors=None, identifiers=None):
        calls.append((title, tuple(authors or []), dict(identifiers or {})))
        if identifiers and identifiers.get("isbn"):
            return [("v2_book", "https://example.invalid/empty")]
        return [("v2_search", "https://example.invalid/search")]

    def _open_text(log, abort, url, timeout, context, headers=None):
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


def test_isbndb_identify_not_configured_is_noop(monkeypatch) -> None:
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
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("isbndb")
    assert hasattr(mod, "ISBNDB")
