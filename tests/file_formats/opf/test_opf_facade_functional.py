"""
Functional tests for LiuXin_alpha.file_formats.opf.opf (façade).

These run with the legacy alias shim enabled, so they can exercise deeper behaviour
even if the package import path still references `LiuXin.*`.
"""

from __future__ import annotations

import importlib
import io
from typing import Any, Optional, Union

import pytest
from lxml import etree

DC_NS = "http://purl.org/dc/elements/1.1/"
OPF_NS = "http://www.idpf.org/2007/opf"

OPF2_MINIMAL = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>Test Book</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">Alice Example</dc:creator>
    <dc:language>en</dc:language>
    <dc:identifier id="BookId">urn:uuid:11111111-2222-3333-4444-555555555555</dc:identifier>
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

OPF3_MINIMAL = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="3.0">
  <metadata>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <dc:title>Test Book 3</dc:title>
    <dc:creator id="creator">Bob Example</dc:creator>
    <dc:language>en</dc:language>
    <meta property="dcterms:modified">2020-01-01T00:00:00Z</meta>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
    <item id="nav" href="nav.xhtml" media-type="application/xhtml+xml" properties="nav"/>
  </manifest>
  <spine>
    <itemref idref="chap1"/>
  </spine>
</package>
"""


@pytest.fixture()
def opf_mod(legacy_liuxin_alias):
    return importlib.import_module("LiuXin_alpha.file_formats.opf.opf")


def _title(mi: Any) -> Optional[str]:
    t = getattr(mi, "title", None)
    return None if t is None else str(t)


def _authors(mi: Any) -> list[str]:
    a = getattr(mi, "authors", None)
    if a is None:
        return []
    if isinstance(a, (list, tuple)):
        return [str(x) for x in a]
    if isinstance(a, dict):
        return [str(x) for x in a.keys()]
    return [str(a)]


def _dc_text(root: etree._Element, tag: str) -> Optional[str]:
    el = root.find(f".//{{{DC_NS}}}{tag}")
    return el.text if el is not None else None


def _parse_xml(data: Union[bytes, bytearray, str]) -> etree._Element:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return etree.fromstring(bytes(data))


@pytest.mark.parametrize("input_obj", [OPF2_MINIMAL, io.BytesIO(OPF2_MINIMAL)])
def test_get_metadata_opf2_parses_title_and_author(opf_mod: Any, input_obj: Any) -> None:
    mi, ver, cover, first = opf_mod.get_metadata(input_obj)
    assert getattr(ver, "major", None) == 2
    assert _title(mi) == "Test Book"
    assert any("Alice" in a for a in _authors(mi))


@pytest.mark.parametrize("input_obj", [OPF3_MINIMAL, io.BytesIO(OPF3_MINIMAL)])
def test_get_metadata_opf3_parses_title_and_author(opf_mod: Any, input_obj: Any) -> None:
    mi, ver, cover, first = opf_mod.get_metadata(input_obj)
    assert getattr(ver, "major", None) == 3
    assert _title(mi) == "Test Book 3"
    assert any("Bob" in a for a in _authors(mi))


def test_set_metadata_opf2_updates_title_and_xml_is_parseable(opf_mod: Any) -> None:
    mi, ver, cover, first = opf_mod.get_metadata(OPF2_MINIMAL)
    mi.title = "Test Book (Updated)"
    opf_bytes, ver2, raster_cover = opf_mod.set_metadata(OPF2_MINIMAL, mi)

    root = _parse_xml(opf_bytes)
    assert root.tag.endswith("package")
    assert _dc_text(root, "title") == "Test Book (Updated)"
    assert getattr(ver2, "major", None) == 2


def test_set_metadata_opf3_updates_title_and_xml_is_parseable(opf_mod: Any) -> None:
    mi, ver, cover, first = opf_mod.get_metadata(OPF3_MINIMAL)
    mi.title = "Test Book 3 (Updated)"
    opf_bytes, ver2, raster_cover = opf_mod.set_metadata(OPF3_MINIMAL, mi)

    root = _parse_xml(opf_bytes)
    assert root.tag.endswith("package")
    assert _dc_text(root, "title") == "Test Book 3 (Updated)"
    assert getattr(ver2, "major", None) == 3


def test_set_metadata_rejects_non_calibre_metadata(opf_mod: Any) -> None:
    with pytest.raises(AssertionError):
        opf_mod.set_metadata(OPF2_MINIMAL, {"title": "Nope"})  # type: ignore[arg-type]


def test_set_metadata_opf2_can_add_missing_cover_hint_smoke(opf_mod: Any) -> None:
    opf2_no_cover = b"""<?xml version='1.0' encoding='utf-8'?>
    <package xmlns="http://www.idpf.org/2007/opf"
             xmlns:dc="http://purl.org/dc/elements/1.1/"
             unique-identifier="BookId"
             version="2.0">
      <metadata>
        <dc:title>No Cover</dc:title>
        <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">Nora Example</dc:creator>
        <dc:language>en</dc:language>
        <dc:identifier id="BookId">urn:uuid:99999999-8888-7777-6666-555555555555</dc:identifier>
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
    mi, ver, cover, first = opf_mod.get_metadata(opf2_no_cover)
    opf_bytes, ver2, raster_cover = opf_mod.set_metadata(
        opf2_no_cover,
        mi,
        cover_prefix="",
        cover_data=b"fakejpegbytes",
        add_missing_cover=True,
    )

    assert raster_cover is not None

    root = _parse_xml(opf_bytes)
    manifest_items = root.findall(f".//{{{OPF_NS}}}manifest/{{{OPF_NS}}}item")
    hrefs = {i.get("href") for i in manifest_items if i.get("href")}
    assert any("cover" in (h or "").lower() for h in hrefs) or b"cover" in opf_bytes
