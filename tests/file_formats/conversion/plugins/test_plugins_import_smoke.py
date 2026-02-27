from __future__ import annotations

import importlib
import pkgutil
import traceback

import pytest


def test_import_all_conversion_plugins_smoke() -> None:
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
