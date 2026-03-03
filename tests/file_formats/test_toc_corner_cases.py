from __future__ import annotations

from io import BytesIO

import pytest

from LiuXin_alpha.file_formats.toc import TOC
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _opf_reader_with_spine_toc(toc_name: str):
    class _Manifest(list):
        def item(self, _name):  # pragma: no cover - explicit API stub
            return None

    class _Soup:
        def find(self, name, **kwargs):
            if name == "spine" and kwargs.get("toc") is True:
                return {"toc": toc_name}
            return None

    class _Reader:
        def __init__(self):
            self.soup = _Soup()
            self.manifest = _Manifest()

    return _Reader()


def test_read_ncx_toc_skips_missing_src_and_keeps_children(tmp_path) -> None:
    xml = f"""\
    <ncx xmlns="http://www.daisy.org/z3986/2005/ncx/">
      <navMap>
        <navPoint playOrder="9">
          <navLabel><text> Parent </text></navLabel>
          <content />
          <navPoint playOrder="2">
            <navLabel><text> Child   Title </text></navLabel>
            <content src="chapter.xhtml#sec%201" />
          </navPoint>
        </navPoint>
      </navMap>
    </ncx>
    """
    root = etree.fromstring(xml.encode("utf-8"))
    toc = TOC()
    toc.read_ncx_toc(str(tmp_path / "toc.ncx"), root=root)

    assert len(toc) == 1
    assert toc[0].text == "Child Title"
    assert toc[0].href == "chapter.xhtml"
    assert toc[0].fragment == "sec 1"
    assert toc[0].play_order == 2


def test_read_ncx_toc_invalid_play_order_defaults_to_one(tmp_path) -> None:
    xml = """\
    <ncx xmlns="http://www.daisy.org/z3986/2005/ncx/">
      <navMap>
        <navPoint playOrder="not-an-int">
          <navLabel><text>One</text></navLabel>
          <content src="one.xhtml" />
        </navPoint>
      </navMap>
    </ncx>
    """
    root = etree.fromstring(xml.encode("utf-8"))
    toc = TOC()
    toc.read_ncx_toc(str(tmp_path / "toc.ncx"), root=root)
    assert toc[0].play_order == 1


def test_read_ncx_toc_requires_navmap(tmp_path) -> None:
    xml = """<ncx xmlns="http://www.daisy.org/z3986/2005/ncx/"></ncx>"""
    root = etree.fromstring(xml.encode("utf-8"))
    toc = TOC()
    with pytest.raises(ValueError, match="navmap"):
        toc.read_ncx_toc(str(tmp_path / "toc.ncx"), root=root)


def test_read_html_toc_deduplicates_and_normalizes_text(tmp_path) -> None:
    toc_html = tmp_path / "toc.html"
    toc_html.write_text(
        (
            "<html><body>"
            '<a href="chap.xhtml#one"> First <b>Chapter</b> </a>'
            '<a href="chap.xhtml#one">Duplicate</a>'
            '<a href="  ">Ignored empty href</a>'
            "<a>No href</a>"
            '<a href="#local"> Local   Link </a>'
            "</body></html>"
        ),
        encoding="utf-8",
    )

    toc = TOC()
    toc.read_html_toc(str(toc_html))

    assert [(x.href, x.fragment, x.text) for x in toc] == [
        ("chap.xhtml", "one", "First Chapter"),
        ("", "local", "Local Link"),
    ]


def test_read_from_opf_uses_baen_top_to_toc_fallback(tmp_path) -> None:
    (tmp_path / "book_toc.htm").write_text(
        '<html><body><a href="chapter.xhtml#start">Chapter</a></body></html>',
        encoding="utf-8",
    )
    opf = _opf_reader_with_spine_toc("book_top.htm")
    toc = TOC(base_path=str(tmp_path))
    toc.read_from_opf(opf)

    assert len(toc) == 1
    assert toc[0].href == "chapter.xhtml"
    assert toc[0].fragment == "start"


def test_render_handles_none_href_without_literal_none_text() -> None:
    toc = TOC()
    node = toc.add_item(None, None, "  T \n i  ")
    node.author = "A U"
    node.description = "D E"
    node.toc_thumbnail = "thumb.png"

    stream = BytesIO()
    toc.render(stream, uid="id-1")
    raw = stream.getvalue()

    assert b'src="None"' not in raw
    assert b"toc_thumbnail" in raw
    assert b"author" in raw
    assert b"description" in raw


def test_ebook_toc_module_is_compatibility_alias() -> None:
    from LiuXin_alpha.file_formats import ebook_toc
    from LiuXin_alpha.file_formats import toc

    assert ebook_toc.TOC is toc.TOC
    assert ebook_toc.NCX_NS == toc.NCX_NS


def test_toc_render_is_deterministic() -> None:
    toc = TOC()
    a = toc.add_item("a.xhtml", "intro", "A")
    b = a.add_item("b.xhtml", "sec", "B")
    b.author = "Author"

    one = BytesIO()
    two = BytesIO()
    toc.render(one, uid="uid-1")
    toc.render(two, uid="uid-1")

    assert one.getvalue() == two.getvalue()
