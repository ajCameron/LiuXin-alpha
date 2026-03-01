from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats.oeb.polish.container import OPF_NAMESPACES
from LiuXin_alpha.file_formats.oeb.polish.opf import get_book_language, set_guide_item
from LiuXin_alpha.file_formats.oeb.polish.parsing import parse, parse_html5
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


class _OpfContainer:
    opf_name = "content.opf"

    def __init__(self, opf_xml: str):
        self.opf = etree.fromstring(opf_xml.encode("utf-8"))
        self.dirty_calls = []

    def opf_xpath(self, expr):
        return self.opf.xpath(expr, namespaces=OPF_NAMESPACES)

    def insert_into_xml(self, parent, elem, index=None):
        if index is None:
            parent.append(elem)
        else:
            parent.insert(index, elem)

    def remove_from_xml(self, elem):
        parent = elem.getparent()
        if parent is not None:
            parent.remove(elem)

    def dirty(self, name):
        self.dirty_calls.append(name)


def test_get_book_language_skips_bad_entries(monkeypatch) -> None:
    xml = """
    <package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/">
      <metadata>
        <dc:language>bad-lang</dc:language>
        <dc:language>en-US</dc:language>
      </metadata>
    </package>
    """
    c = _OpfContainer(xml)

    def _canon(code: str):
        if code == "bad-lang":
            raise ValueError("bad")
        return "en"

    import LiuXin_alpha.file_formats.oeb.polish.opf as opf_mod

    monkeypatch.setattr(opf_mod, "canonicalize_lang", _canon)
    assert get_book_language(c) == "en"


def test_set_guide_item_removes_existing_match_when_href_is_invalid() -> None:
    xml = """
    <package xmlns="http://www.idpf.org/2007/opf">
      <guide>
        <reference type="cover" title="Old" href="old.xhtml" />
      </guide>
    </package>
    """
    c = _OpfContainer(xml)

    def _bad_name_to_href(name, base):
        raise ValueError("invalid path")

    c.name_to_href = _bad_name_to_href  # type: ignore[attr-defined]
    set_guide_item(c, "cover", None, "C:/outside.xhtml")
    assert c.opf_xpath('//opf:guide/opf:reference[@type="cover"]') == []


def test_set_guide_item_creates_guide_and_reference_without_title_when_none() -> None:
    xml = """
    <package xmlns="http://www.idpf.org/2007/opf">
      <metadata />
      <manifest />
      <spine />
    </package>
    """
    c = _OpfContainer(xml)
    c.name_to_href = lambda name, base: name  # type: ignore[attr-defined]

    set_guide_item(c, "cover", None, "images/cover.jpg", frag="top")

    refs = c.opf_xpath('//opf:guide/opf:reference[@type="cover"]')
    assert len(refs) == 1
    ref = refs[0]
    assert ref.get("href") == "images/cover.jpg#top"
    assert ref.get("title") is None
    assert c.dirty_calls == ["content.opf"]


def test_parse_html5_rejects_none_input_cleanly() -> None:
    with pytest.raises(ValueError, match="raw input is None"):
        parse_html5(None)  # type: ignore[arg-type]


def test_parse_rejects_none_input_cleanly() -> None:
    with pytest.raises(ValueError, match="raw input is None"):
        parse(None)  # type: ignore[arg-type]
