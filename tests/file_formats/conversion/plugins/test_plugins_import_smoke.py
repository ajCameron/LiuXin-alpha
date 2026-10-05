"""
Provide test plugins import smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test plugins import smoke through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_import_smoke.py
"""
from __future__ import annotations

import importlib
import pkgutil
import traceback

import pytest


def test_import_all_conversion_plugins_smoke() -> None:
    """
    Perform the test import all conversion plugins smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test import all conversion plugins smoke through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/plugins/test_plugins_import_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    package_name = "LiuXin_alpha.file_formats.conversion.plugins"
    package = importlib.import_module(package_name)
    failures: list[str] = []

    for module_info in pkgutil.iter_modules(package.__path__):
        module_name = f"{package_name}.{module_info.name}"
        try:
            importlib.import_module(module_name)
        except Exception as exc:
            tb = traceback.format_exc()
            failures.append(f"{module_name}: {exc!r}\n{tb}")

    if failures:
        pytest.fail("Conversion plugin import smoke failures:\n\n" + "\n\n".join(failures))
