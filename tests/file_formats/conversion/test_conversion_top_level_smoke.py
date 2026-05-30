from __future__ import annotations

import importlib


def test_conversion_top_level_modules_import_smoke() -> None:
    modules = (
        "LiuXin_alpha.file_formats.conversion",
        "LiuXin_alpha.file_formats.conversion.cli",
        "LiuXin_alpha.file_formats.conversion.config",
        "LiuXin_alpha.file_formats.conversion.edges",
        "LiuXin_alpha.file_formats.conversion.plumber",
        "LiuXin_alpha.file_formats.conversion.preprocess",
        "LiuXin_alpha.file_formats.conversion.report",
        "LiuXin_alpha.file_formats.conversion.utils",
    )
    for module_name in modules:
        importlib.import_module(module_name)


def test_conversion_heuristic_word_count_smoke() -> None:
    mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.utils")
    processor = mod.HeuristicProcessor()
    words = processor.get_word_count("<html><body><p>Hello world</p></body></html>")
    assert isinstance(words, int)
    assert words >= 2
