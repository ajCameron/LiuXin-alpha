from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from tests.support.file_format_epub import (
    EPUB_AUTHORS,
    EPUB_DESCRIPTION,
    EPUB_IMAGE_MEMBER,
    EPUB_PUBLISHER,
    EPUB_TITLE,
    NullLog,
    build_unicode_epub,
    read_container_opf_path,
    read_epub_member,
    rewrite_epub_zip,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars


def test_epub_fixture_builds_valid_container_shape_and_unicode_payload(tmp_path: Path) -> None:
    fixture = build_unicode_epub(tmp_path / "container_Καλημέρα_世界.epub", include_image=True)

    with zipfile.ZipFile(fixture.path, "r") as zf:
        infos = zf.infolist()
        assert infos[0].filename == "mimetype"
        assert infos[0].compress_type == zipfile.ZIP_STORED
        assert zf.read("mimetype") == b"application/epub+zip"
        members = {info.filename for info in infos}

    assert "META-INF/container.xml" in members
    assert fixture.opf_path in members
    assert set(fixture.chapter_members).issubset(members)
    assert set(fixture.asset_members).issubset(members)
    assert EPUB_IMAGE_MEMBER in members
    assert read_container_opf_path(fixture.path) == fixture.opf_path

    ET.fromstring(read_epub_member(fixture.path, "META-INF/container.xml"))
    ET.fromstring(read_epub_member(fixture.path, fixture.opf_path))

    opf_text = read_epub_member(fixture.path, fixture.opf_path).decode("utf-8")
    assert EPUB_TITLE in opf_text
    assert EPUB_AUTHORS[0] in opf_text
    assert EPUB_AUTHORS[1] in opf_text
    assert EPUB_DESCRIPTION in opf_text
    assert EPUB_PUBLISHER in opf_text

    chapter_text = read_epub_member(fixture.path, fixture.chapter_members[0]).decode("utf-8")
    assert_fragments_present(chapter_text, fixture.text_fragments, context="EPUB XHTML fixture")
    assert_no_replacement_chars(chapter_text, context="EPUB XHTML fixture")

    assert read_epub_member(fixture.path, EPUB_IMAGE_MEMBER).startswith(b"\x89PNG")


def test_epub_fixture_rewrite_helper_removes_replaces_and_adds_members(tmp_path: Path) -> None:
    fixture = build_unicode_epub(tmp_path / "base.epub")
    rewritten = tmp_path / "rewritten.epub"
    replacement_chapter = (
        b'<?xml version="1.0"?><html xmlns="http://www.w3.org/1999/xhtml">'
        b"<head><title>Replacement</title></head><body><p>Replacement</p></body></html>"
    )

    rewrite_epub_zip(
        fixture.path,
        rewritten,
        remove=("OPS/styles/main.css",),
        replace={fixture.chapter_members[0]: replacement_chapter},
        add={"OPS/text/extra_世界.xhtml": b"<html/>"},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    with zipfile.ZipFile(rewritten, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        extra_info = zf.getinfo("OPS/text/extra_世界.xhtml")
        assert "OPS/styles/main.css" not in members
        assert "OPS/text/extra_世界.xhtml" in members
        assert extra_info.compress_type == zipfile.ZIP_DEFLATED
        assert zf.read(fixture.chapter_members[0]) == replacement_chapter


def test_epub_input_plugin_accepts_fixture_and_preserves_unicode_workdir(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.epub_input import EPUBInput

    fixture = build_unicode_epub(tmp_path / "plugin_container.epub")
    workdir = tmp_path / "plugin_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with fixture.path.open("rb") as stream:
        opf_path = Path(EPUBInput(None).convert(stream, SimpleNamespace(), "epub", NullLog(), {}))

    assert opf_path.is_absolute()
    assert opf_path.name == "content.opf"
    assert opf_path.parent == workdir
    assert (workdir / "OPS" / "text" / "chapter_Καλημέρα.xhtml").exists()
    assert (workdir / "OPS" / "images" / "深" / "cover_世界.png").exists()

    opf_text = opf_path.read_text("utf-8", "replace")
    assert EPUB_TITLE in opf_text
    assert EPUB_AUTHORS[0] in opf_text
    assert EPUB_AUTHORS[1] in opf_text
    assert_no_replacement_chars(opf_text, context="EPUBInput generated OPF")
