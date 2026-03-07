from __future__ import annotations

import io
import importlib
import unicodedata
from pathlib import Path

import pytest

pytest.importorskip("lxml")
from lxml import etree

DC_NS = "http://purl.org/dc/elements/1.1/"

OPF2_UNICODE = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>Unicode OEB \xe2\x80\x94 caf\xc3\xa9 \xce\xba\xe1\xbd\xb9\xcf\x83\xce\xbc\xce\xb5 \xf0\x9f\x98\x80</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">\xe4\xbd\x9c\xe8\x80\x85 \xe2\x80\x94 \xc3\x86gir</dc:creator>
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

OPF3_UNICODE = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         unique-identifier="BookId"
         version="3.0">
  <metadata>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <dc:title>OPF3 \xe2\x80\x94 \xe6\x97\xa5\xe6\x9c\xac\xe8\xaa\x9e \xd8\xa7\xd9\x84\xd8\xb9\xd8\xb1\xd8\xa8\xd9\x8a\xd8\xa9 \xf0\x9f\x91\xa9\xe2\x80\x8d\xf0\x9f\x92\xbb</dc:title>
    <dc:creator id="creator">\xce\x94\xce\xb7\xce\xbc\xce\xb9\xce\xbf\xcf\x85\xcf\x81\xce\xb3\xcf\x8c\xcf\x82</dc:creator>
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


def _dc_text(root: etree._Element, tag: str) -> str | None:
    el = root.find(f".//{{{DC_NS}}}{tag}")
    return el.text if el is not None else None


def test_get_metadata_accepts_pathlike_and_byteslike(opf_mod, tmp_path: Path) -> None:
    p = tmp_path / "unicode.opf"
    p.write_bytes(OPF2_UNICODE)

    mi_path, ver_path, *_ = opf_mod.get_metadata(p)
    assert ver_path.major == 2
    assert "Unicode OEB" in str(mi_path.title)

    mi_ba, ver_ba, *_ = opf_mod.get_metadata(bytearray(OPF2_UNICODE))
    assert ver_ba.major == 2
    assert "café" in str(mi_ba.title)

    mi_mv, ver_mv, *_ = opf_mod.get_metadata(memoryview(OPF2_UNICODE))
    assert ver_mv.major == 2
    assert "κοσμε" in unicodedata.normalize("NFD", str(mi_mv.title)).replace("\u0301", "")


def test_get_metadata_restores_stream_position(opf_mod) -> None:
    stream = io.BytesIO(OPF2_UNICODE)
    stream.seek(11)
    pos = stream.tell()
    mi, ver, *_ = opf_mod.get_metadata(stream)
    assert ver.major == 2
    assert stream.tell() == pos
    assert "Unicode OEB" in str(mi.title)


def test_set_metadata_accepts_memoryview_and_preserves_unicode(opf_mod) -> None:
    mi, ver, *_ = opf_mod.get_metadata(OPF2_UNICODE)
    mi.title = "Updated — café κόσμε 😀"
    opf_bytes, ver2, _ = opf_mod.set_metadata(memoryview(OPF2_UNICODE), mi)
    assert ver2.major == 2

    root = etree.fromstring(bytes(opf_bytes))
    assert _dc_text(root, "title") == "Updated — café κόσμε 😀"


def test_set_metadata_restores_stream_position(opf_mod) -> None:
    stream = io.BytesIO(OPF2_UNICODE)
    stream.seek(7)
    pos = stream.tell()
    mi, *_ = opf_mod.get_metadata(OPF2_UNICODE)
    opf_mod.set_metadata(stream, mi)
    assert stream.tell() == pos


def test_set_metadata_opf3_with_pathlike_unicode_title(opf_mod, tmp_path: Path) -> None:
    p = tmp_path / "unicode3.opf"
    p.write_bytes(OPF3_UNICODE)

    mi, ver, *_ = opf_mod.get_metadata(p)
    assert ver.major == 3
    mi.title = "OPF3 updated — हिन्दी 中文 日本語 👩‍💻"

    opf_bytes, ver2, _ = opf_mod.set_metadata(p, mi)
    assert ver2.major == 3
    root = etree.fromstring(bytes(opf_bytes))
    assert _dc_text(root, "title") == "OPF3 updated — हिन्दी 中文 日本語 👩‍💻"
