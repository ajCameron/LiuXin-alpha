"""Regression coverage for the format-owned OPF utility operations."""

from __future__ import annotations

import io
from xml.etree import ElementTree

import pytest

from LiuXin_alpha.file_formats.oeb.base import OPF
from LiuXin_alpha.file_formats.opf import utils
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def test_version_parse_manifest_and_language_helpers(tmp_path) -> None:
    """Keep parsing, language normalization, and manifest mutation behavior stable."""
    assert utils.parse_opf_version("3.0") == utils.OPFVersion(3, 0, 0)
    assert utils.parse_opf_version("bad") == utils.OPFVersion(2, 0, 0)
    assert utils.parse_opf_version("4.bad") == utils.OPFVersion(4, 0, 0)

    raw = b"""<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0">
  <manifest><item id="chap" href="chapter.xhtml"/></manifest>
</package>
"""
    path = tmp_path / "metadata.opf"
    path.write_bytes(raw)

    assert utils.parse_opf(path).tag == OPF("package")
    assert utils.parse_opf(io.BytesIO(raw)).tag == OPF("package")
    assert utils.parse_opf(raw.decode("utf-8")).tag == OPF("package")
    with pytest.raises(ValueError, match="Empty file"):
        utils.parse_opf(b"")

    assert utils.normalize_languages(
        ["en-US", "zho-TW"],
        ["eng", "zho", "qaa-x-private"],
    ) == ["en-US", "zh-TW", "qaa-X"]
    assert utils.normalize_languages(["zh-Hant-TW"], ["zho"]) == ["zh-TW"]
    assert utils.normalize_languages(["", "-US"], ["", None]) == []
    assert utils.ensure_unique("cover.jpg", {"cover.jpg", "cover-1.jpg"}) == "cover-2.jpg"

    root = etree.Element(OPF("package"))
    etree.SubElement(root, OPF("manifest"))
    item = utils.create_manifest_item(root, "cover.jpg", "cover", media_type=None)
    assert item.get("href") == "cover.jpg"
    assert item.get("id") == "cover"
    assert item.get("media-type") == "image/jpeg"

    stdlib_root = ElementTree.Element(OPF("package"))
    ElementTree.SubElement(stdlib_root, OPF("manifest"))
    existing = ElementTree.SubElement(stdlib_root.find(OPF("manifest")), OPF("item"))
    existing.set("id", "cover")
    existing.set("href", "cover.jpg")

    item = utils.create_manifest_item(stdlib_root, "cover.jpg", "cover")
    assert item.get("href") == "cover-1.jpg"
    assert item.get("id") == "cover-1"
    assert item.get("media-type") == "image/jpeg"

    no_manifest = etree.Element(OPF("package"))
    assert utils.create_manifest_item(no_manifest, "cover.bin", "cover") is None
    etree.SubElement(no_manifest, OPF("manifest"))
    item = utils.create_manifest_item(
        no_manifest,
        "payload.unknownext",
        "payload",
        media_type=None,
    )
    assert item.get("media-type") == "application/octet-stream"


def test_unicode_language_and_xml_round_trip() -> None:
    """Retain multilingual OPF normalization and parsing behavior."""
    assert utils.normalize_languages(
        ["ja-JP", "ara-EG", "he-IL", "zh-Hans-CN"],
        ["jpn", "ara", "heb", "zho", "kor"],
    ) == ["ja-JP", "ar-EG", "he-IL", "zh-CN", "ko"]

    raw = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="3.0">
  <metadata>
    <dc:title xmlns:dc="http://purl.org/dc/elements/1.1/">雪国 — 😀</dc:title>
    <dc:language xmlns:dc="http://purl.org/dc/elements/1.1/">ja-JP</dc:language>
  </metadata>
  <manifest><item id="章" href="text/第1章.xhtml"/></manifest>
</package>
"""
    assert utils.parse_opf(raw).tag == OPF("package")
