from __future__ import annotations

import zipfile
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from tests.support.file_format_htmlz import (
    HTMLZ_AUTHORS,
    HTMLZ_CSS_MEMBER,
    HTMLZ_DESCRIPTION,
    HTMLZ_IMAGE_MEMBER,
    HTMLZ_OPF_MEMBER,
    HTMLZ_PUBLISHER,
    HTMLZ_SUBJECT,
    HTMLZ_TITLE,
    NullLog,
    build_unicode_htmlz,
    install_htmlz_input_pipeline_stubs,
    read_htmlz_member,
    rewrite_htmlz_zip,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars


def test_htmlz_fixture_builds_valid_container_shape_and_unicode_payload(tmp_path: Path) -> None:
    fixture = build_unicode_htmlz(tmp_path / "container_Καλημέρα_世界.htmlz", include_image=True)

    with zipfile.ZipFile(fixture.path, "r") as zf:
        infos = zf.infolist()
        members = {info.filename for info in infos}
        assert all(info.compress_type == zipfile.ZIP_DEFLATED for info in infos)

    assert fixture.html_member in members
    assert fixture.opf_member == HTMLZ_OPF_MEMBER
    assert fixture.opf_member in members
    assert set(fixture.css_members).issubset(members)
    assert set(fixture.asset_members).issubset(members)
    assert HTMLZ_CSS_MEMBER in members
    assert HTMLZ_IMAGE_MEMBER in members

    ET.fromstring(read_htmlz_member(fixture.path, fixture.html_member))
    ET.fromstring(read_htmlz_member(fixture.path, fixture.opf_member))

    html_text = read_htmlz_member(fixture.path, fixture.html_member).decode("utf-8")
    assert HTMLZ_TITLE in html_text
    assert HTMLZ_CSS_MEMBER in html_text
    assert HTMLZ_IMAGE_MEMBER in html_text
    assert_fragments_present(html_text, fixture.text_fragments, context="HTMLZ HTML fixture")
    assert_no_replacement_chars(html_text, context="HTMLZ HTML fixture")

    opf_text = read_htmlz_member(fixture.path, fixture.opf_member).decode("utf-8")
    assert HTMLZ_TITLE in opf_text
    assert HTMLZ_AUTHORS[0] in opf_text
    assert HTMLZ_AUTHORS[1] in opf_text
    assert HTMLZ_DESCRIPTION in opf_text
    assert HTMLZ_PUBLISHER in opf_text
    assert HTMLZ_SUBJECT in opf_text
    assert HTMLZ_IMAGE_MEMBER in opf_text

    assert read_htmlz_member(fixture.path, HTMLZ_IMAGE_MEMBER).startswith(b"\x89PNG")


def test_htmlz_fixture_supports_optional_metadata_css_image_and_extra_assets(tmp_path: Path) -> None:
    extra_assets = {
        "assets/audio مرحبا.bin": ("application/octet-stream", b"audio"),
        "assets/深/theme_世界.css": ("text/css", b"body { color: #123456; }"),
    }
    fixture = build_unicode_htmlz(
        tmp_path / "extra_assets.htmlz",
        opf_member=None,
        include_css=False,
        include_image=False,
        extra_assets=extra_assets,
    )

    with zipfile.ZipFile(fixture.path, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        assert fixture.html_member in members
        assert HTMLZ_OPF_MEMBER not in members
        assert HTMLZ_CSS_MEMBER not in members
        assert HTMLZ_IMAGE_MEMBER not in members
        assert set(extra_assets).issubset(members)
        for member_name, (_media_type, payload) in extra_assets.items():
            assert zf.read(member_name) == payload

    assert fixture.opf_member is None
    assert fixture.css_members == ()
    assert fixture.asset_members == tuple(extra_assets)


def test_htmlz_fixture_rewrite_helper_removes_replaces_and_adds_members(tmp_path: Path) -> None:
    fixture = build_unicode_htmlz(tmp_path / "base.htmlz")
    rewritten = tmp_path / "rewritten.htmlz"
    replacement_html = (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<html xmlns="http://www.w3.org/1999/xhtml"><body><p>Replacement</p></body></html>'
    )

    rewrite_htmlz_zip(
        fixture.path,
        rewritten,
        remove=(HTMLZ_CSS_MEMBER,),
        replace={fixture.html_member: replacement_html},
        add={"assets/extra_世界.bin": b"extra"},
        add_compression=zipfile.ZIP_DEFLATED,
    )

    with zipfile.ZipFile(rewritten, "r") as zf:
        members = {info.filename for info in zf.infolist()}
        extra_info = zf.getinfo("assets/extra_世界.bin")
        assert HTMLZ_CSS_MEMBER not in members
        assert "assets/extra_世界.bin" in members
        assert extra_info.compress_type == zipfile.ZIP_DEFLATED
        assert zf.read(fixture.html_member) == replacement_html


def test_htmlz_input_accepts_fixture_preserves_unicode_metadata_and_cover(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    fixture = build_unicode_htmlz(tmp_path / "plugin_Καλημέρα_世界.htmlz", include_image=True)
    workdir = tmp_path / "plugin_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    recorder = install_htmlz_input_pipeline_stubs(monkeypatch)

    options = SimpleNamespace(input_encoding=None, debug_pipeline="keep")
    with fixture.path.open("rb") as stream:
        out = HTMLZInput(None).convert(stream, options, "htmlz", NullLog(), {})

    assert out is recorder.oeb
    assert options.input_encoding == "utf-8"
    assert options.debug_pipeline == "keep"
    assert options.breadth_first is False
    assert options.max_levels == 5
    assert options.dont_package is False

    assert len(recorder.html_input.calls) == 1
    html_call = recorder.html_input.calls[0]
    assert Path(html_call.name).name == "index1.html"
    assert html_call.file_ext == "html"
    assert html_call.accelerators == {}
    html_text = html_call.payload.decode("utf-8")
    assert HTMLZ_TITLE in html_text
    assert HTMLZ_CSS_MEMBER in html_text
    assert HTMLZ_IMAGE_MEMBER in html_text
    assert_fragments_present(html_text, fixture.text_fragments, context="HTMLZInput handoff")
    assert_no_replacement_chars(html_text, context="HTMLZInput handoff")

    assert [(call.file_ext, call.position) for call in recorder.metadata_calls] == [("htmlz", 0)]
    assert len(recorder.metadata_transform_calls) == 1
    assert recorder.metadata_transform_calls[0].metadata_info is recorder.metadata_info
    assert recorder.metadata_transform_calls[0].metadata is recorder.oeb.metadata

    assert (workdir / fixture.html_member).exists()
    assert (workdir / HTMLZ_CSS_MEMBER).exists()
    assert (workdir / HTMLZ_IMAGE_MEMBER).exists()

    assert recorder.oeb.manifest.generated == [("cover", "cover_世界.png")]
    assert len(recorder.oeb.manifest.added) == 1
    cover_item = recorder.oeb.manifest.added[0]
    assert cover_item.id == "cover"
    assert cover_item.href == "cover_世界.png"
    assert cover_item.media_type == "image/png"
    assert cover_item.data.startswith(b"\x89PNG")
    assert recorder.oeb.guide.added == [("cover", "Cover", "cover_世界.png")]


def test_htmlz_input_accepts_non_index_top_level_xhtml(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    fixture = build_unicode_htmlz(
        tmp_path / "top_level_xhtml.htmlz",
        html_member="book_Καλημέρα.xhtml",
        opf_member=None,
        include_css=False,
        include_image=False,
    )
    workdir = tmp_path / "xhtml_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    recorder = install_htmlz_input_pipeline_stubs(monkeypatch)

    options = SimpleNamespace(input_encoding=None, debug_pipeline=None)
    with fixture.path.open("rb") as stream:
        out = HTMLZInput(None).convert(stream, options, "htmlz", NullLog(), {})

    assert out is recorder.oeb
    assert len(recorder.html_input.calls) == 1
    html_call = recorder.html_input.calls[0]
    assert Path(html_call.name).name == "index.html"
    html_text = html_call.payload.decode("utf-8")
    assert HTMLZ_TITLE in html_text
    assert_fragments_present(html_text, fixture.text_fragments, context="HTMLZInput top-level XHTML")
    assert recorder.oeb.manifest.added == []
    assert recorder.oeb.guide.added == []


def test_htmlz_input_warns_and_prefers_index_when_multiple_top_level_html_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.htmlz_input import HTMLZInput

    fixture = build_unicode_htmlz(tmp_path / "base.htmlz", include_image=False)
    archive = tmp_path / "multi_html.htmlz"
    rewrite_htmlz_zip(
        fixture.path,
        archive,
        add={"chapter_世界.xhtml": b"<html><body><p>Secondary chapter</p></body></html>"},
        add_compression=zipfile.ZIP_DEFLATED,
    )
    workdir = tmp_path / "multi_work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    recorder = install_htmlz_input_pipeline_stubs(monkeypatch)
    log = NullLog()

    options = SimpleNamespace(input_encoding=None, debug_pipeline=None)
    with archive.open("rb") as stream:
        out = HTMLZInput(None).convert(stream, options, "htmlz", log, {})

    assert out is recorder.oeb
    assert len(recorder.html_input.calls) == 1
    html_text = recorder.html_input.calls[0].payload.decode("utf-8")
    assert HTMLZ_TITLE in html_text
    assert "Secondary chapter" not in html_text
    assert any(
        "Multiple HTML files found in the archive. Only index.html will be used." in msg
        for msg in log.messages
    )
