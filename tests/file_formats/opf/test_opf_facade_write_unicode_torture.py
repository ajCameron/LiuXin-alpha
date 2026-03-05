from __future__ import annotations

import importlib
from pathlib import Path

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
    <dc:title>Seed</dc:title>
    <dc:creator xmlns:opf="http://www.idpf.org/2007/opf" opf:role="aut">Seed Author</dc:creator>
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
    <dc:title>Seed 3</dc:title>
    <dc:creator id="creator">Seed Author 3</dc:creator>
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

UNICODE_TORTURE_TITLE = (
    "Title — cafe\u0301 / café / Ångström / Ελληνικά / العربية / עברית / "
    "हिन्दी / বাংলা / தமிழ் / 日本語 / 한글 / 汉字 / 😀👩🏽‍🔬🧪 / 𐍈"
)

UNICODE_TORTURE_AUTHORS = [
    "Renée Faßbinder",
    "李白",
    "Мария Иванова",
    "أحمد بن سينا",
    "A\u030angstro\u0308m Tester",
    "👩🏽‍🔬 Unicode",
]

UNICODE_TORTURE_TAGS = [
    "タグ",
    "Κατηγορία",
    "العربية",
    "हिंदी",
    "emoji😀",
    "cafe\u0301",
]

UNICODE_TORTURE_COMMENTS = (
    "Line 1 — comment café\n"
    "Line 2 — RTL العربية עברית\n"
    "Line 3 — Indic हिन्दी বাংলা தமிழ்\n"
    "Line 4 — emoji 👩🏽‍🔬😀"
)


@pytest.fixture()
def opf_mod(legacy_liuxin_alias):
    return importlib.import_module("LiuXin_alpha.file_formats.opf.opf")


def _dc_text(root: etree._Element, tag: str) -> str | None:
    el = root.find(f".//{{{DC_NS}}}{tag}")
    return el.text if el is not None else None


def _dc_texts(root: etree._Element, tag: str) -> list[str]:
    nodes = root.findall(f".//{{{DC_NS}}}{tag}")
    return [n.text for n in nodes if n is not None and n.text]


def _contains_forbidden_xml_char(text: str) -> bool:
    for ch in text:
        cp = ord(ch)
        if cp == 0x7F:
            return True
        if cp in (0x9, 0xA, 0xD):
            continue
        if 0x20 <= cp <= 0xD7FF:
            continue
        if 0xE000 <= cp <= 0xFFFD:
            continue
        if 0x10000 <= cp <= 0x10FFFF:
            continue
        return True
    return False


@pytest.mark.parametrize("payload", [OPF2_MINIMAL, OPF3_MINIMAL])
def test_set_metadata_unicode_torture_roundtrip_and_deterministic(opf_mod, payload: bytes) -> None:
    mi, ver, *_ = opf_mod.get_metadata(payload)
    mi.title = UNICODE_TORTURE_TITLE
    mi.authors = list(UNICODE_TORTURE_AUTHORS)
    mi.publisher = "Éditions Δ / 出版社 / دار نشر"
    mi.tags = list(UNICODE_TORTURE_TAGS)
    mi.comments = UNICODE_TORTURE_COMMENTS
    mi.title_sort = "Sort Ω — Ångström"
    mi.series = "シリーズΩ"
    mi.series_index = 12.75
    mi.set_identifier("custom", "urn:unicode:δοκιμή:測試")

    out1, ver1, _ = opf_mod.set_metadata(payload, mi)
    out2, ver2, _ = opf_mod.set_metadata(payload, mi)
    assert ver1.major == ver2.major == ver.major
    assert bytes(out1) == bytes(out2)

    xml_text = bytes(out1).decode("utf-8")
    assert not _contains_forbidden_xml_char(xml_text)

    root = etree.fromstring(bytes(out1))
    assert _dc_text(root, "title") == UNICODE_TORTURE_TITLE
    creators = _dc_texts(root, "creator")
    for expected in UNICODE_TORTURE_AUTHORS:
        assert expected in creators

    subjects = set(_dc_texts(root, "subject"))
    assert set(UNICODE_TORTURE_TAGS).issubset(subjects)
    assert "café" in (_dc_text(root, "title") or "")


@pytest.mark.parametrize("payload", [OPF2_MINIMAL, OPF3_MINIMAL])
def test_set_metadata_strips_invalid_control_chars_without_crashing(opf_mod, payload: bytes) -> None:
    mi, *_ = opf_mod.get_metadata(payload)
    bad_title = "Bad\x00Title\x1f\x7f 😀"
    bad_comment = "Comment\x00 with\x01 bad controls"
    bad_author = "Name\x00 One"
    bad_tag = "Tag\x00One"
    bad_identifier = "urn:test:bad\x00id"

    mi.title = bad_title
    mi.comments = bad_comment
    mi.authors = [bad_author]
    mi.tags = [bad_tag]
    mi.set_identifier("custom", bad_identifier)

    out, _ver, _ = opf_mod.set_metadata(payload, mi)

    # Input object should remain unchanged; writer sanitizes a safe copy.
    assert mi.title == bad_title
    assert mi.comments == bad_comment
    assert mi.authors == [bad_author]

    xml_text = bytes(out).decode("utf-8")
    assert "\x00" not in xml_text
    assert "\x01" not in xml_text
    assert "\x1f" not in xml_text
    assert "\x7f" not in xml_text
    assert not _contains_forbidden_xml_char(xml_text)

    root = etree.fromstring(bytes(out))
    title = _dc_text(root, "title") or ""
    assert "BadTitle" in title
    assert "😀" in title


def test_set_metadata_accepts_pathlike_payload_for_unicode_torture(opf_mod, tmp_path: Path) -> None:
    p = tmp_path / "unicode_torture.opf"
    p.write_bytes(OPF2_MINIMAL)

    mi, *_ = opf_mod.get_metadata(p)
    mi.title = UNICODE_TORTURE_TITLE
    mi.authors = list(UNICODE_TORTURE_AUTHORS)
    out, ver, _ = opf_mod.set_metadata(p, mi)

    assert ver.major == 2
    root = etree.fromstring(bytes(out))
    assert _dc_text(root, "title") == UNICODE_TORTURE_TITLE
