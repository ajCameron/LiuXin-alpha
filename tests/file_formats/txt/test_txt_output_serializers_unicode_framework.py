from __future__ import annotations

import io
import importlib

import pytest

from tests.support.file_format_unicode import (
    COMMON_TEXT_FRAGMENTS,
    assert_fragments_present,
    assert_no_replacement_chars,
)
from tests.support.file_format_oeb import (
    build_text_output_book,
    install_minimal_stylizers,
    null_log,
    text_output_options,
)


def test_txtmlizer_extracts_shared_unicode_corpus_from_oeb_spine(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    txtml = importlib.import_module("LiuXin_alpha.file_formats.txt.txtml")
    options = text_output_options(inline_toc=True)

    rendered_a = txtml.TXTMLizer(null_log()).extract_content(build_text_output_book(), options)
    rendered_b = txtml.TXTMLizer(null_log()).extract_content(build_text_output_book(), options)

    assert rendered_a == rendered_b
    assert "Table of Contents" in rendered_a
    assert "Shared Καλημέρα 世界 👩🏽‍💻" in rendered_a
    assert "bold Ω" in rendered_a
    assert "italic שלום" in rendered_a
    assert_fragments_present(rendered_a, COMMON_TEXT_FRAGMENTS, context="TXTMLizer")
    assert_no_replacement_chars(rendered_a, context="TXTMLizer")


def test_markdownmlizer_keeps_links_images_and_shared_unicode(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    markdownml = importlib.import_module("LiuXin_alpha.file_formats.txt.markdownml")
    options = text_output_options(keep_links=True, keep_image_references=True)

    rendered_a = markdownml.MarkdownMLizer(null_log()).extract_content(build_text_output_book(), options)
    rendered_b = markdownml.MarkdownMLizer(null_log()).extract_content(build_text_output_book(), options)

    assert rendered_a == rendered_b
    assert "# Shared Καλημέρα 世界 👩🏽‍💻" in rendered_a
    assert "**bold Ω**" in rendered_a
    assert "*italic שלום*" in rendered_a
    assert "[参照](https://example.com/路径?鍵=值" in rendered_a
    assert "![封面 世界](images/000000.png)" in rendered_a
    assert "+ First नमस्ते" in rendered_a
    assert_fragments_present(rendered_a, COMMON_TEXT_FRAGMENTS, context="MarkdownMLizer")
    assert_no_replacement_chars(rendered_a, context="MarkdownMLizer")


def test_textilemlizer_keeps_links_images_and_shared_unicode(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    textileml = importlib.import_module("LiuXin_alpha.file_formats.txt.textileml")
    options = text_output_options(keep_links=True, keep_image_references=True)

    rendered_a = textileml.TextileMLizer(null_log()).extract_content(build_text_output_book(), options)
    rendered_b = textileml.TextileMLizer(null_log()).extract_content(build_text_output_book(), options)

    assert rendered_a == rendered_b
    assert "h1. Shared Καλημέρα 世界 👩🏽‍💻" in rendered_a
    assert "[*bold Ω*]" in rendered_a
    assert "[_italic שלום_]" in rendered_a
    assert "参照" in rendered_a
    assert "https://example.com/路径?鍵=值" in rendered_a
    assert "!images/000000.png(封面 世界)!" in rendered_a
    assert "* First नमस्ते" in rendered_a
    assert_fragments_present(rendered_a, COMMON_TEXT_FRAGMENTS, context="TextileMLizer")
    assert_no_replacement_chars(rendered_a, context="TextileMLizer")


@pytest.mark.parametrize(
    ("formatting", "expected_fragments"),
    (
        ("plain", ("Shared Καλημέρα 世界 👩🏽‍💻", "bold Ω", "italic שלום")),
        ("markdown", ("# Shared Καλημέρα 世界 👩🏽‍💻", "**bold Ω**", "![封面 世界](images/000000.png)")),
        ("textile", ("h1. Shared Καλημέρα 世界 👩🏽‍💻", "[*bold Ω*]", "!images/000000.png(封面 世界)!")),
    ),
)
def test_txt_output_uses_real_serializers_with_shared_unicode_oeb(
    monkeypatch,
    formatting: str,
    expected_fragments: tuple[str, ...],
) -> None:
    install_minimal_stylizers(monkeypatch)
    txt_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")
    options = text_output_options(txt_output_formatting=formatting)
    out = io.BytesIO()

    txt_output.TXTOutput(None).convert(build_text_output_book(), out, None, options, null_log())
    rendered = out.getvalue().decode("utf-8", "strict")

    for fragment in expected_fragments:
        assert fragment in rendered
    assert_fragments_present(rendered, COMMON_TEXT_FRAGMENTS, context=f"TXTOutput {formatting}")
    assert_no_replacement_chars(rendered, context=f"TXTOutput {formatting}")
    assert options.conversion_report.loss_events == []


def test_txt_output_reports_output_encoding_character_replacement(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    txt_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")
    options = text_output_options(txt_output_encoding="ascii")
    out = io.BytesIO()

    txt_output.TXTOutput(None).convert(build_text_output_book(), out, None, options, null_log())
    rendered = out.getvalue().decode("ascii", "strict")

    assert "?" in rendered
    report = options.conversion_report
    events = [event for event in report.loss_events if event.code == "output-encoding-character-replacement"]
    assert len(events) == 1
    event = events[0]
    assert event.phase == "txt-output"
    assert event.source_format == "oeb"
    assert event.target_format == "txt"
    assert event.edge_name == "oeb-to-txt"
    assert event.recoverable is True
    assert event.count > 0
    assert event.details["encoding"] == "ascii"
    assert event.details["replacement"] == "?"
    assert event.details["unique_characters"] >= 1
    assert any("U+039A" in sample.codepoints for sample in event.samples)

    payload = report.to_mapping()
    assert payload["loss_event_count"] == 1
    assert payload["loss_events"][0]["code"] == "output-encoding-character-replacement"


def test_txt_output_report_uses_explicit_conversion_edge(monkeypatch) -> None:
    install_minimal_stylizers(monkeypatch)
    edges = importlib.import_module("LiuXin_alpha.file_formats.conversion.edges")
    txt_output = importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.txt_output")
    options = text_output_options(
        txt_output_encoding="ascii",
        conversion_edge=edges.legacy_oeb_edge("html", "txt"),
    )
    out = io.BytesIO()

    txt_output.TXTOutput(None).convert(build_text_output_book(), out, None, options, null_log())

    event = options.conversion_report.loss_events[0]
    assert event.source_format == "html"
    assert event.target_format == "txt"
    assert event.edge_name == "legacy-oeb:html->txt"
