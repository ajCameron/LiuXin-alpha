from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "LiuXin_alpha.file_formats",
        "LiuXin_alpha.file_formats.api",
        "LiuXin_alpha.file_formats.chardet",
        "LiuXin_alpha.file_formats.constants",
        "LiuXin_alpha.file_formats.covers",
        "LiuXin_alpha.file_formats.ebook_toc",
        "LiuXin_alpha.file_formats.html_entities",
        "LiuXin_alpha.file_formats.hyphenate",
        "LiuXin_alpha.file_formats.markupbase",
        "LiuXin_alpha.file_formats.sgmllib",
        "LiuXin_alpha.file_formats.toc",
        "LiuXin_alpha.file_formats.tweak",
        "LiuXin_alpha.file_formats.utils",
    ],
)
def test_top_level_file_formats_modules_import(module_name: str) -> None:
    importlib.import_module(module_name)

