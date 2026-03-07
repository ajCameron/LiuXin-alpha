from __future__ import annotations

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
