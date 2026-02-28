from __future__ import annotations

from pathlib import Path

import pytest


def test_markdown_unicode_torture_end_to_end() -> None:
    from LiuXin_alpha.file_formats import markdown

    source = (
        "# T\u00eftle \U0001f9ea\n\n"
        "Meta line: caf\u00e9 | \u0395\u03bb\u03bb\u03b7\u03bd\u03b9\u03ba\u03ac | \u0939\u093f\u0928\u094d\u0926\u0940 | \u65e5\u672c\u8a9e | \u0639\u0631\u0628\u064a | e\u0301 [^n]\n\n"
        "| k | v |\n"
        "| - | - |\n"
        "| \U0001f642 | \u6f22\u5b57 |\n\n"
        "[^n]: Footnote \u03a9\n"
    )

    html = markdown.markdown(source, extensions=["tables", "footnotes", "toc", "headerid"])

    assert '<h1 id="title">T\u00eftle \U0001f9ea</h1>' in html
    assert "<table>" in html
    assert "\u6f22\u5b57" in html
    assert "footnote" in html.lower()


def test_markdown_deterministic_output_same_input() -> None:
    from LiuXin_alpha.file_formats import markdown

    source = "# Same\n\nParagraph with [^1].\n\n[^1]: deterministic"
    exts = ["footnotes", "toc", "headerid"]

    out1 = markdown.markdown(source, extensions=exts)
    out2 = markdown.markdown(source, extensions=exts)

    assert out1 == out2


def test_markdown_from_file_end_to_end_with_invalid_utf8(tmp_path: Path) -> None:
    from LiuXin_alpha.file_formats.markdown import markdownFromFile

    src = tmp_path / "input.md"
    dst = tmp_path / "output.html"
    src.write_bytes(b"# Titl\xffe\n\nBody caf\xc3\xa9 with bad seq \xe2(\xa1\n")

    markdownFromFile(input=str(src), output=str(dst), encoding="utf-8")

    rendered = dst.read_text(encoding="utf-8")
    assert "Titl\ufffde" in rendered
    assert "caf\u00e9" in rendered
    assert "\ufffd" in rendered


@pytest.mark.parametrize(
    "payload",
    [
        b"# Byte text\n\nplain",
        bytearray(b"# Bytearray text\n\nplain"),
        memoryview(b"# Memoryview text\n\nplain"),
    ],
)
def test_markdown_convert_accepts_bytes_like_inputs(payload: bytes | bytearray | memoryview) -> None:
    from LiuXin_alpha.file_formats.markdown import Markdown

    html = Markdown().convert(payload)

    assert "<h1>" in html
    assert "text" in html.lower()


def test_txt_processor_convert_markdown_integration_unicode() -> None:
    from LiuXin_alpha.file_formats.txt.processor import convert_markdown

    source = (
        "# Smoke\n\n"
        "Body with caf\u00e9 and \u6f22\u5b57 and [^1].\n\n"
        "| a | b |\n"
        "| - | - |\n"
        "| 1 | 2 |\n\n"
        "[^1]: Footnote \u03a9\n"
    )

    html = convert_markdown(
        source,
        title="Markdown E2E",
        extensions=("footnotes", "tables", "toc", "not_a_real_extension"),
    )

    assert "<html>" in html
    assert "<title>Markdown E2E" in html
    assert '<h1 id="smoke">Smoke</h1>' in html
    assert "<table>" in html
    assert "\u6f22\u5b57" in html
