from __future__ import annotations

import importlib
import types

from lxml import etree

from tests.support.file_format_lit import (
    LitLog,
    lit_options,
    read_manifest_from_payload,
    render_unbinary_html,
)
from tests.support.file_format_oeb import (
    MinimalManifest,
    NullStylizer,
    build_rich_oeb_output_book,
    text_output_options,
)
from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    MULTISCRIPT_TEXT,
    assert_fragments_present,
    assert_no_replacement_chars,
)


def _lit_writer(monkeypatch):
    writer = importlib.import_module("LiuXin_alpha.file_formats.lit.writer")
    monkeypatch.setattr(writer, "Stylizer", NullStylizer)
    return writer


def _lit_output_book(*, include_image: bool = False):
    book = build_rich_oeb_output_book(include_image=include_image)
    book.logger = LitLog()

    chapter = book.spine[0]
    chapter.href = "chapters/κόσμε_世界.xhtml"
    chapter.id = "chapter_世界"

    if include_image:
        image = book.manifest[1]
        image.href = "images/封面_世界.png"
        image.id = "cover_世界"

    book.manifest = MinimalManifest(book.manifest)
    spine_ids = {id(item) for item in book.spine}
    for index, item in enumerate(book.spine):
        item.spine_position = index
        item.linear = True
        item.page_breaks = []
    for item in book.manifest:
        if id(item) not in spine_ids:
            item.spine_position = None
            item.linear = True
            item.page_breaks = []
    return book


def _extract_directory_payload(writer, entry_name: str) -> bytes:
    entry = next(entry for entry in writer._directory if entry.name == entry_name)
    section = writer._sections[entry.section].getvalue()
    return section[entry.offset : entry.offset + entry.size]


def test_lit_rebinary_output_preserves_multiscript_xhtml(monkeypatch) -> None:
    writer = _lit_writer(monkeypatch)
    book = _lit_output_book(include_image=False)
    item = book.spine[0]

    rebin = writer.ReBinary(item.data, item, book, text_output_options(), map=writer.HTML_MAP)
    rendered = render_unbinary_html(rebin.content, path=item.href)
    root = etree.fromstring(rendered.encode("utf-8"))
    text = "".join(root.itertext())

    assert isinstance(rebin.content, bytes)
    assert isinstance(rebin.ahc, bytes)
    assert b"intro" in rebin.ahc
    assert "bold Ω" in text
    assert "italic שלום" in text
    assert_fragments_present(text, COMMON_TEXT_FRAGMENTS, context="LIT ReBinary output")
    assert_no_replacement_chars(rendered, context="LIT ReBinary output")


def test_lit_writer_manifest_preserves_unicode_ids_and_paths(monkeypatch) -> None:
    writer = _lit_writer(monkeypatch)
    book = _lit_output_book(include_image=True)

    for item in book.manifest:
        if hasattr(item.data, "xpath"):
            rebin = writer.ReBinary(item.data, item, book, text_output_options(), map=writer.HTML_MAP)
            item.size = len(rebin.content)
        elif isinstance(item.data, bytes):
            item.size = len(item.data)
        else:
            item.size = len(str(item.data).encode("utf-8"))

    lit_writer = writer.LitWriter(text_output_options())
    lit_writer._oeb = book
    lit_writer._logger = book.logger
    lit_writer._sections = [writer.six_cStringIO() for _ in range(4)]
    lit_writer._directory = []
    lit_writer._build_manifest()

    parsed = read_manifest_from_payload(_extract_directory_payload(lit_writer, "/manifest"))
    chapter = parsed.manifest["chapter_世界"]
    image = parsed.manifest["cover_世界"]
    combined = "\n".join(
        value
        for item in (chapter, image)
        for value in (item.internal, item.original, item.mime_type)
    )

    assert chapter.original == "chapters/κόσμε_世界.xhtml"
    assert chapter.mime_type == "application/xhtml+xml"
    assert chapter.state == "spine"
    assert image.original == "images/封面_世界.png"
    assert image.mime_type == "image/png"
    assert image.state == "images"
    assert_no_replacement_chars(combined, context="LIT writer manifest")


def test_lit_input_postprocess_preserves_multiscript_pre_text() -> None:
    lit_input_mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.lit_input")
    from LiuXin_alpha.file_formats.oeb.base import XHTML
    from LiuXin_alpha.utils.libraries.liuxin_etree import etree as liuxin_etree

    root = liuxin_etree.fromstring(
        b"<html xmlns='http://www.w3.org/1999/xhtml'><body><pre /></body></html>"
    )
    body = root.find(XHTML("body"))
    pre = body[0]
    pre.text = MULTISCRIPT_TEXT + "\nReserved <tag> & café"
    oeb = types.SimpleNamespace(spine=[types.SimpleNamespace(data=root)])

    lit_input_mod.LITInput(None).postprocess_book(oeb, lit_options(), LitLog())
    rendered = liuxin_etree.tostring(body[0], encoding="unicode")
    text = "".join(body[0].itertext())

    assert body[0].tag == XHTML("div")
    assert "Reserved <tag> & café" in text
    assert_fragments_present(text, COMMON_TEXT_FRAGMENTS, context="LITInput.postprocess_book")
    assert_no_replacement_chars(rendered, context="LITInput.postprocess_book")
