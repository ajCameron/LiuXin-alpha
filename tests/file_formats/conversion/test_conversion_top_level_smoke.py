"""
Provide test conversion top level smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test conversion top level smoke through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
"""
from __future__ import annotations

import importlib


def test_conversion_top_level_modules_import_smoke() -> None:
    """
    Perform the test conversion top level modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test conversion top level modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test conversion heuristic word count smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test conversion heuristic word count smoke through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    mod = importlib.import_module("LiuXin_alpha.file_formats.conversion.utils")
    processor = mod.HeuristicProcessor()
    words = processor.get_word_count("<html><body><p>Hello world</p></body></html>")
    assert isinstance(words, int)
    assert words >= 2
