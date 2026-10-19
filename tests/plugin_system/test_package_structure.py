"""Enforce retirement of the old namespace in source and installed distributions."""

from __future__ import annotations

import ast
import importlib.util
import tokenize
import zipfile
from pathlib import Path

import pytest

from scripts.verify_wheel_install import WheelVerificationError, inspect_wheel

SOURCE = Path(__file__).resolve().parents[2] / "src/LiuXin_alpha"


def test_customize_namespace_is_absent() -> None:
    assert not (SOURCE / "customize").exists()
    assert importlib.util.find_spec("LiuXin_alpha.customize") is None


def test_production_imports_use_the_unified_namespace() -> None:
    violations = []
    for path in SOURCE.rglob("*.py"):
        with tokenize.open(path) as stream:
            source = stream.read()
        if "customize" not in source:
            continue
        for node in ast.walk(ast.parse(source)):
            modules = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                modules = [node.module or ""]
                if node.module == "LiuXin_alpha":
                    modules.extend("LiuXin_alpha." + alias.name for alias in node.names)
            if any(
                module == "LiuXin_alpha.customize"
                or module.startswith("LiuXin_alpha.customize.")
                for module in modules
            ):
                violations.append(f"{path.relative_to(SOURCE)}:{node.lineno}")
    assert not violations, violations


def test_wheel_rejects_a_stale_customize_package(tmp_path: Path) -> None:
    wheel = tmp_path / "stale.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("LiuXin_alpha/customize/__init__.py", "")
    with pytest.raises(WheelVerificationError, match="retired modules"):
        inspect_wheel(wheel)


def test_retained_cache_callers_use_the_relocated_contracts() -> None:
    """Check live class identity without requiring the obsolete cache schema."""
    from LiuXin_alpha.library.caches.calibre.cache import CalibreCache
    from LiuXin_alpha.plugin_system.cache.base_cache import BaseCache
    from LiuXin_alpha.plugin_system.cache.base_field import BaseField
    from LiuXin_alpha.plugin_system.cache.base_tables import BaseTable

    assert issubclass(CalibreCache, BaseCache)
    assert BaseField.__module__ == "LiuXin_alpha.plugin_system.cache.base_field"
    assert BaseTable.__module__ == "LiuXin_alpha.plugin_system.cache.base_tables"
