from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

from tests.support.file_format_fb2 import (
    FB2_AUTHORS,
    FB2_COVER_ID,
    FB2_DESCRIPTION,
    FB2_EXTRA_BINARY_ID,
    FB2_KEYWORDS,
    FB2_NS,
    FB2_PUBLISHER,
    FB2_TITLE,
    NullLog,
    XLINK_NS,
    build_unicode_fb2,
    fb2_body_text,
    parse_fb2,
    png_bytes,
    read_fb2_binary,
    rewrite_fb2_text,
)
from tests.support.file_format_unicode import assert_fragments_present, assert_no_replacement_chars


def test_fb2_fixture_builds_valid_document_shape_and_unicode_payload(tmp_path: Path) -> None:
    fixture = build_unicode_fb2(tmp_path / "fixture_Καλημέρα_世界.fb2")
    root = parse_fb2(fixture.path)

    assert root.tag == f"{{{FB2_NS}}}FictionBook"
    assert root.attrib == {}

    title_info = root.find(f"{{{FB2_NS}}}description/{{{FB2_NS}}}title-info")
    assert title_info is not None
    title_text = title_info.findtext(f"{{{FB2_NS}}}book-title")
    assert title_text == FB2_TITLE
    assert title_info.findtext(f"{{{FB2_NS}}}keywords") == FB2_KEYWORDS
    assert title_info.findtext(f"{{{FB2_NS}}}annotation/{{{FB2_NS}}}p") == FB2_DESCRIPTION

    authors = title_info.findall(f"{{{FB2_NS}}}author")
    assert len(authors) == len(FB2_AUTHORS)
    assert authors[0].findtext(f"{{{FB2_NS}}}first-name") == FB2_AUTHORS[0][0]
    assert authors[0].findtext(f"{{{FB2_NS}}}middle-name") == FB2_AUTHORS[0][1]
    assert authors[0].findtext(f"{{{FB2_NS}}}last-name") == FB2_AUTHORS[0][2]
    assert authors[1].findtext(f"{{{FB2_NS}}}last-name") == FB2_AUTHORS[1][2]

    publisher = root.findtext(f"{{{FB2_NS}}}description/{{{FB2_NS}}}publish-info/{{{FB2_NS}}}publisher")
    assert publisher == FB2_PUBLISHER

    cover_image = title_info.find(f"{{{FB2_NS}}}coverpage/{{{FB2_NS}}}image")
    assert cover_image is not None
    assert cover_image.attrib[f"{{{XLINK_NS}}}href"] == f"#{FB2_COVER_ID}"

    body_text = fb2_body_text(fixture.path)
    assert FB2_TITLE in body_text
    assert_fragments_present(body_text, fixture.text_fragments, context="FB2 body fixture")
    assert_no_replacement_chars(body_text, context="FB2 body fixture")

    assert read_fb2_binary(fixture.path, FB2_COVER_ID).startswith(b"\x89PNG")


def test_fb2_fixture_supports_optional_extra_binaries(tmp_path: Path) -> None:
    extra_png = png_bytes(width=8, height=8, rgb=(12, 40, 90))
    fixture = build_unicode_fb2(
        tmp_path / "extra_binaries.fb2",
        include_cover=False,
        extra_binaries={
            FB2_EXTRA_BINARY_ID: ("image/png", extra_png),
            "notes_مرحبا": ("text/plain", "plain embedded note".encode("utf-8")),
        },
    )
    root = parse_fb2(fixture.path)

    assert fixture.cover_id is None
    assert fixture.binary_ids == (FB2_EXTRA_BINARY_ID, "notes_مرحبا")
    assert root.find(f"{{{FB2_NS}}}description/{{{FB2_NS}}}title-info/{{{FB2_NS}}}coverpage") is None
    assert read_fb2_binary(fixture.path, FB2_EXTRA_BINARY_ID) == extra_png
    assert read_fb2_binary(fixture.path, "notes_مرحبا") == "plain embedded note".encode("utf-8")


def test_fb2_fixture_can_emit_utf16_xml_for_encoding_cases(tmp_path: Path) -> None:
    fixture = build_unicode_fb2(tmp_path / "utf16.fb2", encoding="utf-16")

    assert fixture.path.read_bytes().startswith((b"\xff\xfe", b"\xfe\xff"))
    root = parse_fb2(fixture.path)
    assert root.findtext(f"{{{FB2_NS}}}description/{{{FB2_NS}}}title-info/{{{FB2_NS}}}book-title") == FB2_TITLE

    body_text = fb2_body_text(fixture.path)
    assert_fragments_present(body_text, fixture.text_fragments, context="UTF-16 FB2 body fixture")
    assert_no_replacement_chars(body_text, context="UTF-16 FB2 body fixture")


def test_fb2_fixture_rewrite_helper_removes_replaces_and_appends_text(tmp_path: Path) -> None:
    fixture = build_unicode_fb2(tmp_path / "base.fb2")
    rewritten = tmp_path / "rewritten.fb2"

    payload = rewrite_fb2_text(
        fixture.path,
        rewritten,
        remove=("<keywords>" + FB2_KEYWORDS + "</keywords>",),
        replace={FB2_TITLE: "Replacement Καλημέρα 世界"},
        append="\n<!-- trailing malformed-test marker -->\n",
    )

    text = payload.decode("utf-8")
    assert FB2_KEYWORDS not in text
    assert "Replacement Καλημέρα 世界" in text
    assert text.endswith("<!-- trailing malformed-test marker -->\n")
    with rewritten.open("rb") as stream:
        assert ET.parse(stream).getroot().tag == f"{{{FB2_NS}}}FictionBook"


def test_fb2_fixture_binary_payload_can_feed_existing_extraction_helper(tmp_path: Path, monkeypatch) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree

    fixture = build_unicode_fb2(tmp_path / "extract.fb2", cover_id=FB2_COVER_ID)
    out_dir = tmp_path / "extract-out"
    out_dir.mkdir()
    monkeypatch.chdir(out_dir)

    plugin = FB2Input(None)
    plugin.log = type("Log", (), {"exception": lambda self, *args: None})()
    plugin.extract_embedded_content(etree.fromstring(fixture.path.read_bytes()))

    extracted = out_dir / f"{FB2_COVER_ID}.png"
    assert extracted.read_bytes().startswith(b"\x89PNG")
    assert plugin.binary_map[FB2_COVER_ID] == f"{FB2_COVER_ID}.png"


def test_fb2_input_convert_preserves_unicode_body_metadata_css_and_binaries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    cover_id = "cover_世界.png"
    extra_id = "illustration_cafe\u0301.png"
    fixture = build_unicode_fb2(
        tmp_path / "convert_Καλημέρα_世界.fb2",
        cover_id=cover_id,
        extra_binaries={extra_id: ("image/png", png_bytes(width=10, height=12))},
    )
    workdir = tmp_path / "convert-work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with fixture.path.open("rb") as stream:
        opf_path = Path(
            FB2Input(None).convert(
                stream,
                SimpleNamespace(no_inline_fb2_toc=False),
                "fb2",
                NullLog(),
                {},
            )
        )

    assert opf_path == workdir / "metadata.opf"
    assert (workdir / "index.xhtml").exists()
    assert (workdir / "inline-styles.css").exists()
    ET.parse(opf_path)

    html = (workdir / "index.xhtml").read_text("utf-8", "replace")
    assert FB2_TITLE in html
    assert '<img src="cover_世界.png"' in html
    assert_fragments_present(html, fixture.text_fragments, context="FB2Input index.xhtml")
    assert_no_replacement_chars(html, context="FB2Input index.xhtml")

    css = (workdir / "inline-styles.css").read_text("utf-8", "replace")
    assert "font-family" in css
    assert_no_replacement_chars(css, context="FB2Input inline-styles.css")

    opf = opf_path.read_text("utf-8", "replace")
    assert FB2_TITLE in opf
    assert "José" in opf
    assert "Niño" in opf
    assert "Иван" in opf
    assert "Петров" in opf
    assert FB2_DESCRIPTION in opf
    assert FB2_PUBLISHER in opf
    assert "index.xhtml" in opf
    assert "inline-styles.css" in opf
    assert cover_id in opf
    assert extra_id in opf
    assert_no_replacement_chars(opf, context="FB2Input metadata.opf")

    assert (workdir / cover_id).read_bytes().startswith(b"\x89PNG")
    assert (workdir / extra_id).read_bytes().startswith(b"\x89PNG")


def test_fb2_input_convert_accepts_utf16_fixture_without_replacement_chars(
    tmp_path: Path,
    monkeypatch,
) -> None:
    from LiuXin_alpha.file_formats.conversion.plugins.fb2_input import FB2Input

    fixture = build_unicode_fb2(tmp_path / "convert_utf16.fb2", encoding="utf-16", cover_id="cover_utf16.png")
    workdir = tmp_path / "utf16-work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)

    with fixture.path.open("rb") as stream:
        opf_path = Path(
            FB2Input(None).convert(
                stream,
                SimpleNamespace(no_inline_fb2_toc=False),
                "fb2",
                NullLog(),
                {},
            )
        )

    html = (opf_path.parent / "index.xhtml").read_text("utf-8", "replace")
    assert_fragments_present(html, fixture.text_fragments, context="UTF-16 FB2Input index.xhtml")
    assert_no_replacement_chars(html, context="UTF-16 FB2Input index.xhtml")

    opf = opf_path.read_text("utf-8", "replace")
    assert FB2_TITLE in opf
    assert_no_replacement_chars(opf, context="UTF-16 FB2Input metadata.opf")
