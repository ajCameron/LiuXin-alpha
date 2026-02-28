from __future__ import annotations

import importlib


def test_markdown_modules_import_smoke() -> None:
    importlib.import_module("LiuXin_alpha.file_formats.markdown")
    importlib.import_module("LiuXin_alpha.file_formats.markdown.__main__")


def test_markdown_render_smoke() -> None:
    from LiuXin_alpha.file_formats import markdown

    html = markdown.markdown("# Smoke\n\nParagraph with **bold**.")

    assert "<h1>Smoke</h1>" in html
    assert "<strong>bold</strong>" in html
