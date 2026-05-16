from __future__ import annotations

import io
import shutil
import zipfile
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation

pytest.importorskip("lxml")


def _field_values(raw):
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    return list(raw)


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


def test_docx_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.docx as docx_md

    assert docx_md is not None


def test_docx_metadata_reads_known_fixture(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata

    fixture = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    metadata = get_metadata(fixture)

    assert metadata.title == "DOCX Demo"
    assert _field_values(metadata.authors) == ["Kovid Goyal"]
    assert "Demonstration of DOCX support in calibre" in _field_values(metadata.comments)


def test_docx_metadata_reads_stream_and_rewinds(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata

    fixture = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    with fixture.open("rb") as stream:
        metadata = get_metadata(stream)
        assert stream.tell() == 0

    assert metadata.title == "DOCX Demo"


def test_docx_reader_plugin_is_available(md_test_fixture) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    fixture = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)

    plugins = get_metadata_reader_plugins()
    docx_cls = next((p for p in plugins if p.__name__ == "DocXMetadataReader"), None)
    assert docx_cls is not None

    reader = docx_cls(None)
    with fixture.open("rb") as stream:
        metadata = reader.get_metadata(stream=stream, ftype="docx")
    inplace_metadata = reader.get_metadata_inplace(file_path=str(fixture), ftype="docx")

    assert metadata.title == "DOCX Demo"
    assert inplace_metadata.title == "DOCX Demo"


def test_docx_set_metadata_roundtrip_path(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata, set_metadata

    source = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    target = tmp_path / "roundtrip.docx"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Unicode title — 測試", ["Alice Example", "Bob Writer"])
    updated.tags = ["fantasy", "unicode test"]
    updated.comments = "Docx metadata write smoke"
    updated.publisher = "Alpha Press"

    set_metadata(target, updated)
    metadata = get_metadata(target)

    assert metadata.title == "Unicode title — 測試"
    assert _field_values(metadata.authors) == ["Alice Example", "Bob Writer"]
    assert _field_values(metadata.tags) == ["fantasy", "unicode test"]
    assert _field_values(metadata.comments) == ["Docx metadata write smoke"]
    assert _field_values(metadata.publisher) == ["Alpha Press"]


def test_docx_set_metadata_roundtrip_stream(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata, set_metadata

    source = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    target = tmp_path / "roundtrip_stream.docx"
    shutil.copy2(source, target)

    updated = calibreMetaInformation("Stream title", ["Stream Author"])
    updated.publisher = "Stream Publisher"

    with target.open("r+b") as stream:
        set_metadata(stream, updated)
        assert stream.tell() == 0

    metadata = get_metadata(target)
    assert metadata.title == "Stream title"
    assert _field_values(metadata.authors) == ["Stream Author"]
    assert _field_values(metadata.publisher) == ["Stream Publisher"]


def test_docx_set_metadata_unicode_torture_roundtrip(tmp_path: Path, md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata, set_metadata

    source = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    target = tmp_path / "roundtrip_unicode_torture.docx"
    shutil.copy2(source, target)

    updated = calibreMetaInformation(
        "Unicode Torture — Καλημέρα мир مرحبا हिन्दी 中文 日本語 😀",
        ["Renée Δ", "李白", "مريم", "Иван", "Zoë 👩🏽‍💻"],
    )
    updated.tags = ["café", "καλημέρα", "emoji😀", "漢字", "e\u0301-vs-é"]
    updated.comments = "Combining: e\u0301 a\u0308 n\u0303; RTL: العربية; ZWJ: 👨‍👩‍👧‍👦"
    updated.publisher = "出版者 / الناشر / Издатель"

    set_metadata(target, updated)
    metadata = get_metadata(target)

    assert metadata.title == updated.title
    assert _field_values(metadata.authors) == _field_values(updated.authors)
    assert _field_values(metadata.tags) == _field_values(updated.tags)
    assert _field_values(metadata.comments) == _field_values(updated.comments)
    assert _field_values(metadata.publisher) == _field_values(updated.publisher)


def test_docx_set_metadata_preserves_zip_members_and_sanitizes_hostile_xml(
    tmp_path: Path,
    md_test_fixture,
) -> None:
    from LiuXin_alpha.metadata.file_sources.docx import get_metadata, set_metadata

    source = md_test_fixture(file_ext="docx", file_num=1, verify_hash=True)
    target = tmp_path / "roundtrip_container_contract.docx"
    shutil.copy2(source, target)

    with zipfile.ZipFile(target, "r") as zf:
        before = {name: zf.read(name) for name in zf.namelist()}

    title = "DOCX\x00Title\ud800 😀"
    authors = ["Alice\x01 One", "Bob\udfff Two"]
    updated = calibreMetaInformation(title, authors)
    updated.tags = ["tag\x02one", "emoji 😀"]
    updated.comments = "Comment\x03 with <xml> & emoji 😀"
    updated.publisher = "Pub\x04lisher"

    set_metadata(target, updated)

    with zipfile.ZipFile(target, "r") as zf:
        assert zf.testzip() is None
        assert set(zf.namelist()) == set(before)
        for name, payload in before.items():
            if name in {"docProps/core.xml", "docProps/app.xml"}:
                continue
            assert zf.read(name) == payload
        core_xml = zf.read("docProps/core.xml")
        app_xml = zf.read("docProps/app.xml")

    ET.fromstring(core_xml)
    ET.fromstring(app_xml)
    combined_xml = (core_xml + app_xml).decode("utf-8")
    assert not _contains_forbidden_xml_char(combined_xml)
    assert "DOCXTitle" in combined_xml
    assert "Publisher" in combined_xml

    metadata = get_metadata(target)
    assert metadata.title == "DOCXTitle 😀"
    assert _field_values(metadata.authors) == ["Alice One", "Bob Two"]
    assert _field_values(metadata.tags) == ["tagone", "emoji 😀"]
    assert _field_values(metadata.comments) == ["Comment with <xml> & emoji 😀"]
    assert _field_values(metadata.publisher) == ["Publisher"]
    assert updated.title == title
    assert updated.authors == authors


def test_docx_set_metadata_invalid_zip_raises_clean_error() -> None:
    from LiuXin_alpha.utils.libraries.calibre_zipfile import BadZipfile
    from zipfile import BadZipFile

    from LiuXin_alpha.metadata.file_sources.docx import set_metadata

    stream = io.BytesIO(b"not-a-zip")
    mi = calibreMetaInformation("x", ["y"])
    with pytest.raises((BadZipFile, BadZipfile)):
        set_metadata(stream, mi)
