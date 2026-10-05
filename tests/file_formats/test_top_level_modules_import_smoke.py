"""
Provide test top level modules import smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test top level modules import smoke through a consuming regression::

        python -m pytest -q tests/file_formats/test_top_level_modules_import_smoke.py
"""
from __future__ import annotations

import importlib

import pytest


@pytest.mark.parametrize(
    "module_name",
    [
        "LiuXin_alpha.file_formats",
        "LiuXin_alpha.file_formats.api",
        "LiuXin_alpha.utils.libraries.calibre_chardet",
        "LiuXin_alpha.file_formats.constants",
        "LiuXin_alpha.file_formats.covers",
        "LiuXin_alpha.file_formats.toc",
        "LiuXin_alpha.file_formats.html_entities",
        "LiuXin_alpha.file_formats.hyphenate",
        "LiuXin_alpha.file_formats.markupbase",
        "LiuXin_alpha.file_formats.sgmllib",
        "LiuXin_alpha.file_formats.tweak",
        "LiuXin_alpha.file_formats.utils",
    ],
)
def test_top_level_file_formats_modules_import(module_name: str) -> None:
    """
    Perform the test top level file formats modules import operation under explicit file-format and conversion rules.

    Example:
        Exercise test top level file formats modules import through a consuming regression::

            python -m pytest -q tests/file_formats/test_top_level_modules_import_smoke.py


    :param module_name: Value supplied for module name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module(module_name)

