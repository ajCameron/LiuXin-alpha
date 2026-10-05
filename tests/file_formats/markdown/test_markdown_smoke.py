"""
Provide test markdown smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test markdown smoke through a consuming regression::

        python -m pytest -q tests/file_formats/markdown/test_markdown_smoke.py
"""
from __future__ import annotations

import importlib


def test_markdown_modules_import_smoke() -> None:
    """
    Perform the test markdown modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module("LiuXin_alpha.file_formats.markdown")
    importlib.import_module("LiuXin_alpha.file_formats.markdown.__main__")


def test_markdown_render_smoke() -> None:
    """
    Perform the test markdown render smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test markdown render smoke through a consuming regression::

            python -m pytest -q tests/file_formats/markdown/test_markdown_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats import markdown

    html = markdown.markdown("# Smoke\n\nParagraph with **bold**.")

    assert "<h1>Smoke</h1>" in html
    assert "<strong>bold</strong>" in html
