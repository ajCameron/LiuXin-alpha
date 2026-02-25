"""
Smoke tests for opf2.OPF object.

Uses the legacy alias shim so import succeeds even before cleanup.
"""

from __future__ import annotations

import importlib
import pytest

pytest.importorskip("lxml")
from lxml import etree

DC_NS = "http://purl.org/dc/elements/1.1/"

OPF2_MINIMAL = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>Parser Smoke</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">Ada Example</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:12121212-3434-5656-7878-909090909090</dc:identifier>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
    <item id="ncx" href="toc.ncx" media-type="application/x-dtbncx+xml"/>
  </manifest>
  <spine toc="ncx">
    <itemref idref="chap1"/>
  </spine>
</package>
"""


def _dc_text(root: etree._Element, tag: str) -> str | None:
    el = root.find(f".//{{{DC_NS}}}{tag}")
    return el.text if el is not None else None


def test_opf2_construct_render_and_to_book_metadata_smoke(legacy_liuxin_alias) -> None:
    opf2 = importlib.import_module("LiuXin_alpha.file_formats.opf.opf2")
    OPF = getattr(opf2, "OPF")
    pretty_print = getattr(opf2, "pretty_print")

    root = etree.fromstring(OPF2_MINIMAL)
    obj = OPF(None, preparsed_opf=root, read_toc=False)

    rendered = obj.render()
    root2 = etree.fromstring(bytes(rendered))
    assert root2.tag.endswith("package")
    assert _dc_text(root2, "title") == "Parser Smoke"

    mi = obj.to_book_metadata()
    assert getattr(mi, "title", None) == "Parser Smoke"

    with pretty_print:
        rendered2 = obj.render()
    assert isinstance(rendered2, (bytes, bytearray))
