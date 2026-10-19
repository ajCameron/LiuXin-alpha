"""Enforce retired plugin namespaces in source and installed distributions."""

from __future__ import annotations

import ast
import importlib.util
import tokenize
import zipfile
from pathlib import Path

import pytest

from scripts.verify_wheel import verify_wheel
from scripts.verify_wheel_install import WheelVerificationError, inspect_wheel

SOURCE = Path(__file__).resolve().parents[2] / "src/LiuXin_alpha"
RETIRED_NAMES = ("customize", "plugin_system")


@pytest.mark.parametrize("name", RETIRED_NAMES)
def test_retired_namespace_is_absent(name: str) -> None:
    assert not (SOURCE / name).exists()
    assert importlib.util.find_spec(f"LiuXin_alpha.{name}") is None


def test_production_imports_use_the_unified_namespace() -> None:
    violations = []
    for path in SOURCE.rglob("*.py"):
        with tokenize.open(path) as stream:
            source = stream.read()
        if not any(name in source for name in RETIRED_NAMES):
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
                module == f"LiuXin_alpha.{name}"
                or module.startswith(f"LiuXin_alpha.{name}.")
                for module in modules
                for name in RETIRED_NAMES
            ):
                violations.append(f"{path.relative_to(SOURCE)}:{node.lineno}")
    assert not violations, violations


@pytest.mark.parametrize("name", RETIRED_NAMES)
def test_wheel_rejects_a_stale_plugin_package(tmp_path: Path, name: str) -> None:
    wheel = tmp_path / "stale.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(f"LiuXin_alpha/{name}/__init__.py", "")
    with pytest.raises(WheelVerificationError, match="retired modules"):
        inspect_wheel(wheel)
    with pytest.raises(ValueError, match="retired plugin package"):
        verify_wheel(wheel)


def test_retained_cache_callers_use_the_relocated_contracts() -> None:
    """Check live class identity without requiring the obsolete cache schema."""
    from LiuXin_alpha.library.caches.calibre.cache import CalibreCache
    from LiuXin_alpha.plugins.cache.base_cache import BaseCache
    from LiuXin_alpha.plugins.cache.base_field import BaseField
    from LiuXin_alpha.plugins.cache.base_tables import BaseTable

    assert issubclass(CalibreCache, BaseCache)
    assert BaseField.__module__ == "LiuXin_alpha.plugins.cache.base_field"
    assert BaseTable.__module__ == "LiuXin_alpha.plugins.cache.base_tables"
