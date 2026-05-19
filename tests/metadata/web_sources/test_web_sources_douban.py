from __future__ import annotations

import queue
from datetime import datetime
from xml.etree import ElementTree as ET
from threading import Event

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


def _sample_json_book(book_id: str = "1234567") -> dict:
    return {
        "id": book_id,
        "title": "三体",
        "subtitle": "地球往事",
        "author": ["刘慈欣"],
        "publisher": "重庆出版社",
        "summary": "科幻经典。",
        "pubdate": "2008-01",
        "isbn13": "9787536692930",
        "rating": {"average": "8.8"},
        "tags": [{"name": "科幻"}, {"name": "中国文学"}],
        "images": {"large": "https://img.example/large.jpg"},
    }


def _sample_atom_feed() -> str:
    return """<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"
      xmlns:db="http://www.douban.com/xmlns/"
      xmlns:gd="http://schemas.google.com/g/2005">
  <entry>
    <id>https://api.douban.com/book/subject/7654321</id>
    <title>活着</title>
    <summary>中文简介。</summary>
    <db:attribute name="author">余华</db:attribute>
    <db:attribute name="publisher">作家出版社</db:attribute>
    <db:attribute name="pubdate">1998-01-01</db:attribute>
    <db:attribute name="isbn13">9787506365437</db:attribute>
    <db:tag name="文学" />
    <gd:rating average="9.1" />
    <link rel="image" href="https://img3.douban.com/spic/s1.jpg" />
  </entry>
</feed>
"""


def test_web_sources_douban_import_smoke() -> None:
    import LiuXin_alpha.metadata.web_sources.douban as douban

    assert douban is not None


def test_douban_get_book_url_and_create_query() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    assert plugin.get_book_url({"douban": "1234567"}) == (
        "douban",
        "1234567",
        "https://book.douban.com/subject/1234567/",
    )

    urls, qtype = plugin.create_query(identifiers={"isbn": "9787536692930"})
    assert qtype == "isbn"
    assert any("/v2/book/isbn/9787536692930" in u for u in urls)

    urls, qtype = plugin.create_query(title="三体", authors=["刘慈欣"], identifiers={})
    assert qtype == "search"
    assert any("/v2/book/search?" in u for u in urls)


def test_douban_metadata_from_json_record_parses_fields() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    mi = plugin._metadata_from_json_record(_sample_json_book(), relevance=4)
    assert mi.title == "三体: 地球往事"
    assert mi.authors == ["刘慈欣"]
    assert mi.publisher == "重庆出版社"
    assert mi.get_identifiers()["douban"] == "1234567"
    assert mi.get_identifiers()["isbn"] == "9787536692930"
    assert mi.rating == 8.8
    assert "科幻" in (mi.tags or [])
    assert mi.has_douban_cover == "https://img.example/large.jpg"
    assert mi.source_relevance == 4


def test_douban_identify_handles_json_payload() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    payload = '{"books": [%s]}' % __import__("json").dumps(_sample_json_book("998877"))
    plugin._open_text_with_backoff = lambda log, abort, url, timeout, context: payload

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"isbn": "9787536692930"},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["douban"] == "998877"
    assert plugin.cached_isbn_to_identifier("9787536692930") == "998877"
    assert plugin.cached_identifier_to_cover_url("998877") == "https://img.example/large.jpg"


def test_douban_identify_handles_xml_atom_payload() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    plugin._open_text_with_backoff = lambda log, abort, url, timeout, context: _sample_atom_feed()

    out = queue.Queue()
    plugin.identify(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        title="活着",
        authors=["余华"],
        identifiers={},
    )
    mi = out.get_nowait()
    assert mi.get_identifiers()["douban"] == "7654321"
    assert mi.get_identifiers()["isbn"] == "9787506365437"
    assert mi.publisher == "作家出版社"
    assert mi.has_douban_cover == "https://img3.douban.com/lpic/s1.jpg"
    assert plugin.cached_identifier_to_cover_url("7654321") == "https://img3.douban.com/lpic/s1.jpg"


def test_douban_download_cover_uses_cache() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    plugin.cache_identifier_to_cover_url("1112223", "https://img.example/cover.jpg")
    plugin._open_bytes_with_backoff = lambda log, abort, url, timeout, context: b"cover-bytes"

    out = queue.Queue()
    plugin.download_cover(
        log=_Log(),
        result_queue=out,
        abort=Event(),
        identifiers={"douban": "1112223"},
    )
    source, payload = out.get_nowait()
    assert source is plugin
    assert payload == b"cover-bytes"


def test_douban_import_web_source_module() -> None:
    from LiuXin_alpha.metadata.web_sources import import_web_source_module

    mod = import_web_source_module("douban")
    assert hasattr(mod, "Douban")


def test_douban_low_level_helpers_handle_odd_inputs() -> None:
    import LiuXin_alpha.metadata.web_sources.douban as douban

    class BadText:
        def __str__(self):
            raise RuntimeError("cannot stringify")

    assert douban._as_text(b"\xe4\xb8\x89\xe4\xbd\x93") == "三体"
    assert douban._as_text(BadText()) == ""
    assert douban._first({"first": "ignored"}) == "first"
    assert douban._first(item for item in ["value"]) == "value"
    assert douban._first_identifier_value([], "douban") is None
    assert douban._safe_isbn({"isbn": ["bad", "9787536692930"]}) is None
    assert douban._safe_isbn({"isbn13": b"9787536692930"}) == "9787536692930"
    assert douban._parse_pubdate("") is None
    assert douban._parse_pubdate("2008-01").day == 15
    assert douban._parse_pubdate("2008").month == 6
    assert douban._parse_pubdate("January 2nd, 2008") == datetime(2008, 1, 2)
    assert douban._parse_pubdate("First published in 2008").year == 2008
    assert douban._parse_pubdate("not a date") is None
    assert douban._safe_float("8,8") == 8.8
    assert douban._safe_float("not a number") is None
    assert douban._extract_douban_id("") is None
    assert douban._extract_douban_id("1234567") == "1234567"
    assert douban._extract_douban_id("https://book.douban.com/subject/1234567/") == "1234567"
    assert douban._extract_douban_id("/book/subject/7654321") == "7654321"
    assert douban._extract_douban_id("/not-a-number") is None


def test_douban_query_cache_and_payload_parsing_edges() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    plugin.douban_api_key = ""
    assert plugin._append_api_key("https://api.example/path") == "https://api.example/path"
    plugin.douban_api_key = "key-1"
    assert plugin._append_api_key("https://api.example/path?x=1").endswith("&apikey=key-1")

    assert plugin.get_book_url({"douban": "bad"}) is None
    plugin.cache_isbn_to_identifier("9787536692930", "1234567")
    plugin.cache_identifier_to_cover_url("1234567", "https://img.example/cover.jpg")
    assert plugin.get_cached_cover_url({"isbn": "9787536692930"}) == "https://img.example/cover.jpg"
    assert plugin.get_cached_cover_url({}) is None

    urls, qtype = plugin.create_query(identifiers={"douban": "1234567"})
    assert qtype == "subject"
    assert any("/v2/book/1234567" in url for url in urls)
    assert plugin.create_query(title=None, authors=None, identifiers={}) == (None, None)

    assert plugin._json_records_from_payload("{bad}") is None
    assert plugin._json_records_from_payload("[1, 2]") == [1, 2]
    assert plugin._json_records_from_payload('"text"') == []
    assert plugin._json_records_from_payload('{"title": "三体"}') == [{"title": "三体"}]
    assert plugin._xml_entries_from_payload("<bad") is None

    atom_entry = """<entry xmlns="http://www.w3.org/2005/Atom"><title>Only Entry</title></entry>"""
    entries = plugin._xml_entries_from_payload(atom_entry)
    assert len(entries) == 1
    assert plugin._xml_text(entries[0], "atom:title") == "Only Entry"
    assert plugin._parse_metadata_payload("not json or xml") == []


def test_douban_json_metadata_parser_uses_fallbacks_and_optional_fields() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    record = {
        "alt": "https://book.douban.com/subject/7654321/",
        "title": "主标题",
        "subtitle": "",
        "author": "单作者",
        "publisher": "",
        "summary": "",
        "pubdate": "2008",
        "tags": "科幻,经典",
        "rating": "12.5",
        "isbn": ["bad", "9787536692930"],
        "image": "https://img.example/image.jpg",
    }

    mi = plugin._metadata_from_json_record(record, relevance=2)

    assert mi.title == "主标题"
    assert mi.authors == ["单作者"]
    assert mi.get_identifiers()["douban"] == "7654321"
    assert mi.pubdate.year == 2008
    assert mi.tags == ["科幻;经典"]
    assert mi.rating == 10.0
    assert mi.get_identifiers()["isbn"] == "9787536692930"
    assert mi.has_douban_cover == "https://img.example/image.jpg"

    sparse = plugin._metadata_from_json_record({"title": "", "author": [], "rating": "bad"}, relevance=0)
    assert sparse.title == "Unknown"
    assert sparse.authors == ["Unknown"]
    assert getattr(sparse, "has_douban_cover", "unset") is None
    assert plugin._metadata_from_json_record(["not", "a", "mapping"]) is None
    assert plugin._cover_url_from_json_record("not a mapping") is None
    assert plugin._cover_url_from_json_record({"images": {}, "cover": "https://img.example/c.jpg"}) == (
        "https://img.example/c.jpg"
    )


def test_douban_xml_metadata_parser_uses_fallbacks_and_default_cover_rejection() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban, NAMESPACES

    plugin = Douban()
    entry = ET.fromstring(
        """<entry xmlns="http://www.w3.org/2005/Atom"
                  xmlns:db="http://www.douban.com/xmlns/"
                  xmlns:gd="http://schemas.google.com/g/2005">
             <id>https://api.douban.com/book/subject/7654321</id>
             <title>XML Title</title>
             <summary>XML summary</summary>
             <author><name>Atom Author</name></author>
             <db:attribute name="publisher">XML Publisher</db:attribute>
             <db:attribute name="pubdate">1999</db:attribute>
             <db:attribute name="isbn13">9787506365437</db:attribute>
             <db:tag name="文学,经典" />
             <gd:rating average="11.5" />
             <link rel="image" href="https://img3.douban.com/spic/book-default.jpg" />
           </entry>"""
    )

    mi = plugin._metadata_from_xml_entry(entry, relevance=1)

    assert mi.title == "XML Title"
    assert mi.authors == ["Atom Author"]
    assert mi.publisher == "XML Publisher"
    assert mi.pubdate.year == 1999
    assert mi.tags == ["文学;经典"]
    assert mi.rating == 10.0
    assert mi.get_identifiers()["isbn"] == "9787506365437"
    assert mi.has_douban_cover is None
    assert plugin._xml_text(entry, "atom:missing") is None

    no_author = ET.fromstring(
        """<entry xmlns="http://www.w3.org/2005/Atom"
                  xmlns:db="http://www.douban.com/xmlns/">
             <title>No Author</title>
           </entry>"""
    )
    assert plugin._metadata_from_xml_entry(no_author).authors == ["Unknown"]
    assert no_author.findall("db:attribute[@name='author']", NAMESPACES) == []


def test_douban_identify_retry_abort_parse_error_and_empty_paths() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    log = _Log()
    calls = []
    payload = '{"books": [%s]}' % __import__("json").dumps(_sample_json_book("998877"))

    def fake_open(log, abort, url, timeout, context):
        del log, abort, timeout
        calls.append((context, url))
        if context == "Douban identify query":
            return ""
        if context == "Douban identify retry query":
            return payload
        raise AssertionError(context)

    plugin._open_text_with_backoff = fake_open
    out = queue.Queue()
    plugin.identify(
        log=log,
        result_queue=out,
        abort=Event(),
        title="三体",
        authors=["刘慈欣"],
        identifiers={"douban": "1234567"},
    )
    assert out.get_nowait().get_identifiers()["douban"] == "998877"
    assert any(context == "Douban identify retry query" for context, _url in calls)

    abort = Event()
    abort.set()
    plugin._open_text_with_backoff = lambda **kwargs: payload
    out = queue.Queue()
    plugin.identify(log=_Log(), result_queue=out, abort=abort, title="三体", authors=["刘慈欣"], identifiers={"douban": "1"})
    assert out.empty()

    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), title=None, authors=None, identifiers={})
    assert out.empty()
    assert any(level == "error" for level, _parts in log.events)

    plugin._parse_metadata_payload = lambda _payload: [("json", ["bad"])]
    plugin._open_text_with_backoff = lambda **kwargs: "[]"
    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), identifiers={"douban": "1234567"})
    assert out.empty()

    def raise_parse(_item, relevance=0):
        raise RuntimeError("bad item")

    plugin._parse_metadata_payload = lambda _payload: [("json", {})]
    plugin._metadata_from_json_record = raise_parse
    out = queue.Queue()
    log = _Log()
    plugin.identify(log=log, result_queue=out, abort=Event(), identifiers={"douban": "1234567"})
    assert out.empty()
    assert any(level == "exception" for level, _parts in log.events)


def test_douban_download_cover_discovers_from_identify_and_handles_failures() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    log = _Log()
    out = queue.Queue()

    def fake_identify(log, rq, abort, title=None, authors=None, identifiers=None, timeout=30):
        del log, abort, title, authors, identifiers, timeout
        mi = calibreMetaInformation("Cover Book", ["Author"])
        mi.set_identifier("douban", "1234567")
        plugin.cache_identifier_to_cover_url("1234567", "https://img.example/discovered.jpg")
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

    plugin.cache_identifier_to_cover_url("1234567", "https://img.example/empty.jpg")
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"douban": "1234567"})
    assert out.empty()

    def raise_download(**kwargs):
        raise OSError("download failed")

    plugin._open_bytes_with_backoff = raise_download
    out = queue.Queue()
    plugin.download_cover(log=_Log(), result_queue=out, abort=Event(), identifiers={"douban": "1234567"})
    assert out.empty()


def test_douban_open_text_decodes_and_abort_backoff_returns_empty() -> None:
    from LiuXin_alpha.metadata.web_sources.douban import Douban

    plugin = Douban()
    plugin._open_bytes_with_backoff = lambda **kwargs: b"\xe4\xb8\x89\xe4\xbd\x93"
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == (
        "三体"
    )
    plugin._open_bytes_with_backoff = lambda **kwargs: b""
    assert plugin._open_text_with_backoff(log=_Log(), abort=Event(), url="https://example.invalid", timeout=1, context="x") == ""

    abort = Event()
    abort.set()
    assert plugin._wait_for_backoff(abort, 0.01) is True
