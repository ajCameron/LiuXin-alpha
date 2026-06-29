from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from tests.support.file_format_docx import (
    DOCX_AUTHORS,
    DOCX_DESCRIPTION,
    DOCX_IMAGE_MEMBER,
    DOCX_KEYWORDS,
    DOCX_PUBLISHER,
    DOCX_SUBJECT,
    DOCX_TITLE,
    NullLog,
    build_unicode_docx,
    document_text,
    read_docx_member,
    rewrite_docx_zip,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars


def test_docx_fixture_builds_valid_container_shape_and_unicode_payload(tmp_path: Path) -> None:
    fixture = build_unicode_docx(tmp_path / "container_Καλημέρα_世界.docx", include_image=True)

    with zipfile.ZipFile(fixture.path, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        assert all(info.compress_type == zipfile.ZIP_DEFLATED for info in zf.infolist())

    assert "[Content_Types].xml" in members
    assert "_rels/.rels" in members
    assert fixture.document_member in members
    assert "word/_rels/document.xml.rels" in members
    assert fixture.styles_member in members
    assert set(fixture.metadata_members).issubset(members)
    assert set(fixture.media_members).issubset(members)
    assert DOCX_IMAGE_MEMBER in members

    for member_name in (
        "[Content_Types].xml",
        "_rels/.rels",
        fixture.document_member,
        "word/_rels/document.xml.rels",
        fixture.styles_member,
        *fixture.metadata_members,
    ):
        ET.fromstring(read_docx_member(fixture.path, member_name))

    content_types = read_docx_member(fixture.path, "[Content_Types].xml").decode("utf-8")
    assert "/word/document.xml" in content_types
    assert "/docProps/core.xml" in content_types
    assert "/docProps/app.xml" in content_types
    assert 'Extension="png"' in content_types

    package_rels = read_docx_member(fixture.path, "_rels/.rels").decode("utf-8")
    assert 'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument"' in package_rels
    assert 'Target="word/document.xml"' in package_rels
    assert 'Target="docProps/core.xml"' in package_rels
    assert 'Target="docProps/app.xml"' in package_rels

    document_rels = read_docx_member(fixture.path, "word/_rels/document.xml.rels").decode("utf-8")
    assert 'Target="styles.xml"' in document_rels
    assert 'Target="media/深/cover_世界.png"' in document_rels

    core_props = read_docx_member(fixture.path, "docProps/core.xml").decode("utf-8")
    assert DOCX_TITLE in core_props
    assert DOCX_AUTHORS[0] in core_props
    assert DOCX_AUTHORS[1] in core_props
    assert DOCX_SUBJECT in core_props
    assert DOCX_DESCRIPTION in core_props
    assert DOCX_KEYWORDS in core_props

    app_props = read_docx_member(fixture.path, "docProps/app.xml").decode("utf-8")
    assert DOCX_PUBLISHER in app_props

    rendered_text = document_text(fixture.path, fixture.document_member)
    assert DOCX_TITLE in rendered_text
    assert "Archive image holder 画像" in rendered_text
    assert_fragments_present(rendered_text, fixture.text_fragments, context="DOCX document.xml fixture")
    assert_no_replacement_chars(rendered_text, context="DOCX document.xml fixture")

    assert read_docx_member(fixture.path, DOCX_IMAGE_MEMBER).startswith(b"\x89PNG")


def test_docx_fixture_supports_optional_extra_assets(tmp_path: Path) -> None:
    extra_assets = {
        "word/media/audio مرحبا.bin": ("application/octet-stream", b"audio"),
        "word/theme/深/theme_世界.xml": ("application/xml", b"<theme/>"),
    }
    fixture = build_unicode_docx(
        tmp_path / "extra_assets.docx",
        include_image=False,
        extra_assets=extra_assets,
    )

    with zipfile.ZipFile(fixture.path, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        content_types = zf.read("[Content_Types].xml").decode("utf-8")
        assert DOCX_IMAGE_MEMBER not in members
        assert set(extra_assets).issubset(members)
        for member_name, (_content_type, payload) in extra_assets.items():
            assert zf.read(member_name) == payload
            assert f'PartName="/{member_name}"' in content_types


def test_docx_fixture_rewrite_helper_removes_replaces_and_adds_members(tmp_path: Path) -> None:
    fixture = build_unicode_docx(tmp_path / "base.docx")
    rewritten = tmp_path / "rewritten.docx"
    replacement_document = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        b"<w:body><w:p><w:r><w:t>Replacement</w:t></w:r></w:p></w:body></w:document>"
    )

    rewrite_docx_zip(
        fixture.path,
        rewritten,
        remove=("word/styles.xml",),
        replace={fixture.document_member: replacement_document},
        add={"word/media/extra_世界.bin": b"extra"},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    with zipfile.ZipFile(rewritten, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        extra_info = zf.getinfo("word/media/extra_世界.bin")
        assert "word/styles.xml" not in members
        assert "word/media/extra_世界.bin" in members
        assert extra_info.compress_type == zipfile.ZIP_DEFLATED
        assert zf.read(fixture.document_member) == replacement_document


def test_docx_convert_preserves_unicode_body_metadata_and_nested_media(tmp_path: Path) -> None:
    import LiuXin_alpha.file_formats.docx.to_html as to_html_mod

    fixture = build_unicode_docx(tmp_path / "convert_Καλημέρα_世界.docx", include_image=True)
    out_dir = tmp_path / "convert_out"
    out_dir.mkdir()

    result = Path(to_html_mod.Convert(str(fixture.path), dest_dir=str(out_dir), log=NullLog())())

    assert result == out_dir / "metadata.opf"
    assert (out_dir / "index.html").exists()
    assert (out_dir / "docx.css").exists()
    assert (out_dir / "metadata.opf").exists()
    ET.parse(result)

    html = (out_dir / "index.html").read_text("utf-8", "replace")
    assert DOCX_TITLE in html
    assert "Archive image holder 画像" in html
    assert "<img" in html
    assert 'alt="封面 世界"' in html
    assert_fragments_present(html, fixture.text_fragments, context="DOCX Convert index.html")
    assert_no_replacement_chars(html, context="DOCX Convert index.html")

    copied_images = sorted((out_dir / "images").glob("*.png"))
    assert copied_images
    assert copied_images[0].read_bytes().startswith(b"\x89PNG")

    opf_text = result.read_text("utf-8", "replace")
    assert DOCX_TITLE in opf_text
    assert DOCX_AUTHORS[0] in opf_text
    assert DOCX_AUTHORS[1] in opf_text
    assert DOCX_DESCRIPTION in opf_text
    assert DOCX_PUBLISHER in opf_text
    assert DOCX_SUBJECT.replace(",", "_") in opf_text
    assert "Κατηγορία" in opf_text
    assert "cafe\u0301" in opf_text
    assert "index.html" in opf_text
    assert "docx.css" in opf_text
    assert "images/" in opf_text
    assert_no_replacement_chars(opf_text, context="DOCX Convert metadata.opf")


def test_docx_input_plugin_uses_workdir_and_preserves_unicode_conversion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.docx_input import DOCXInput

    fixture = build_unicode_docx(tmp_path / "plugin_Καλημέρα_世界.docx", include_image=True)
    workdir = tmp_path / "plugin_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with fixture.path.open("rb") as stream:
        opf_path = Path(
            DOCXInput(None).convert(
                stream,
                SimpleNamespace(docx_no_cover=False),
                "docx",
                NullLog(),
                {},
            )
        )

    assert opf_path == workdir / "metadata.opf"
    assert (workdir / "index.html").exists()
    assert (workdir / "docx.css").exists()
    assert (workdir / "images").is_dir()

    html = (workdir / "index.html").read_text("utf-8", "replace")
    assert DOCX_TITLE in html
    assert_fragments_present(html, fixture.text_fragments, context="DOCXInput index.html")
    assert_no_replacement_chars(html, context="DOCXInput index.html")

    opf_text = opf_path.read_text("utf-8", "replace")
    assert DOCX_TITLE in opf_text
    assert DOCX_AUTHORS[0] in opf_text
    assert DOCX_AUTHORS[1] in opf_text
    assert_no_replacement_chars(opf_text, context="DOCXInput metadata.opf")
