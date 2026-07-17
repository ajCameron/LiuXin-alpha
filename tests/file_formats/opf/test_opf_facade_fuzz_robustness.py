from __future__ import annotations

import importlib
import random

import pytest

import LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver.utils

pytest.importorskip("lxml")
from lxml import etree

from LiuXin_alpha.utils.libraries.cleantext import clean_xml_chars

OPF_NS = "http://www.idpf.org/2007/opf"
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

SAFE_CHARS = (
    "abcXYZ0123456789 -_./"
    "cafe\u0301 Ångström Ελληνικά العربية हिन्दी 日本語 한글 汉字 😀"
)
BAD_CHARS = "\x00\x01\x02\x1f\x7f"


@pytest.fixture()
def opf_mod(legacy_liuxin_alias):
    return importlib.import_module("LiuXin_alpha.file_formats.opf.opf")


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


def _rand_text(rng: random.Random, *, max_len: int = 40, with_bad_controls: bool = False) -> str:
    chars = SAFE_CHARS + (BAD_CHARS if with_bad_controls else "")
    length = rng.randint(1, max_len)
    return "".join(rng.choice(chars) for _ in range(length)).strip() or "seed"


def _mutate_parseable_payload(rng: random.Random, payload: bytes) -> bytes:
    root = etree.fromstring(payload)
    metadata = root.xpath("./opf:metadata", namespaces={"opf": OPF_NS})[0]

    # Random extra unknown metadata nodes.
    for _ in range(rng.randint(0, 4)):
        m = etree.SubElement(metadata, f"{{{OPF_NS}}}meta")
        m.set("property", f"x-test:{rng.randint(1, 999)}")
        m.text = clean_xml_chars(_rand_text(rng))

    # Randomly add/duplicate creators and title variants.
    if rng.random() < 0.7:
        c = etree.SubElement(metadata, f"{{{DC_NS}}}creator")
        c.text = clean_xml_chars(_rand_text(rng, max_len=24))
    if rng.random() < 0.5:
        t = etree.SubElement(metadata, f"{{{DC_NS}}}title")
        t.text = clean_xml_chars(_rand_text(rng, max_len=28))

    # OPF3 collection noise.
    version = root.get("version") or ""
    if version.startswith("3"):
        col = etree.SubElement(metadata, f"{{{OPF_NS}}}meta")
        col_id = f"rand-col-{rng.randint(1, 9999)}"
        col.set("id", col_id)
        col.set("property", "belongs-to-collection")
        col.text = clean_xml_chars(_rand_text(rng, max_len=20))
        r1 = etree.SubElement(metadata, f"{{{OPF_NS}}}meta")
        r1.set("refines", "#" + col_id)
        r1.set("property", "collection-type")
        r1.text = "set" if rng.random() < 0.5 else "series"
        r2 = etree.SubElement(metadata, f"{{{OPF_NS}}}meta")
        r2.set("refines", "#" + col_id)
        r2.set("property", "group-position")
        r2.text = str(rng.randint(1, 20))

    return etree.tostring(root, encoding="utf-8")


@pytest.mark.parametrize("base_payload", [OPF2_MINIMAL, OPF3_MINIMAL])
def test_randomized_parseable_inputs_roundtrip_without_xml_breakage(opf_mod, base_payload: bytes) -> None:
    for seed in range(35):
        rng = random.Random(1000 + seed)
        payload = _mutate_parseable_payload(rng, base_payload)

        mi, ver, *_ = opf_mod.get_metadata(payload)
        assert ver.major in {2, 3}

        mi.title = _rand_text(rng, with_bad_controls=True)
        mi.authors = [
            _rand_text(rng, max_len=30, with_bad_controls=True),
            _rand_text(rng, max_len=30),
        ]
        mi.comments = _rand_text(rng, max_len=60, with_bad_controls=True)
        mi.publisher = _rand_text(rng, max_len=24, with_bad_controls=True)
        mi.tags = [_rand_text(rng, max_len=16, with_bad_controls=True) for _ in range(3)]
        mi.title_sort = _rand_text(rng, max_len=24, with_bad_controls=True)
        mi.series = _rand_text(rng, max_len=20, with_bad_controls=True)
        mi.series_index = float(rng.randint(1, 99))
        mi.set_identifier("custom", _rand_text(rng, max_len=30, with_bad_controls=True))

        out1, ver1, _ = opf_mod.set_metadata(payload, mi)
        out2, ver2, _ = opf_mod.set_metadata(payload, mi)
        assert ver1.major == ver2.major == ver.major
        assert bytes(out1) == bytes(out2)

        text = bytes(out1).decode("utf-8")
        assert not _contains_forbidden_xml_char(text)
        etree.fromstring(bytes(out1))
