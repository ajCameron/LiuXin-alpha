"""Regression contracts for explicit wheel discovery and runtime assets."""

from __future__ import annotations

import importlib.util
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "verify_wheel_install.py"
SPEC = importlib.util.spec_from_file_location("verify_wheel_install", SCRIPT_PATH)
assert SPEC is not None
verify_wheel_install = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = verify_wheel_install
SPEC.loader.exec_module(verify_wheel_install)


def test_pyproject_owns_production_discovery_and_runtime_data() -> None:
    configuration = tomllib.loads(
        (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
    )
    setuptools = configuration["tool"]["setuptools"]
    discovery = setuptools["packages"]["find"]

    assert setuptools["include-package-data"] is False
    assert discovery["where"] == ["src"]
    assert discovery["include"] == ["LiuXin_alpha*", "past*"]
    assert discovery["exclude"] == [
        "LiuXin_tests*",
        "LiuXin_alpha.file_formats.lrf.html.demo*",
        "LiuXin_alpha.file_formats.oeb.display.test-cfi*",
        "LiuXin_alpha.file_formats.oeb.polish.tests*",
        "LiuXin_alpha.utils.decompression.rarfile.test*",
        "LiuXin_alpha.working-memory*",
    ]
    assert setuptools["package-data"][
        "LiuXin_alpha.databases.database_driver_plugins.SQL.database_generator_frbr"
    ] == [
        "aggregate_sql/*.sql",
        "*.toml",
        "table_sql/*/*.sql",
        "trigger_sql/*/*.sql",
    ]


def test_wheel_inspection_rejects_test_package_leaks(tmp_path: Path) -> None:
    wheel = tmp_path / "bad.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        for name in verify_wheel_install.expected_runtime_assets(REPO_ROOT):
            archive.writestr(name, b"runtime")
        archive.writestr("past/__init__.py", b"")
        archive.writestr("past/builtins.py", b"")
        archive.writestr("LiuXin_tests/test_leak.py", b"")

    with pytest.raises(
        verify_wheel_install.WheelVerificationError,
        match="Development-only test or demo",
    ):
        verify_wheel_install.inspect_wheel(wheel, REPO_ROOT)


def test_wheel_inspection_rejects_missing_schema_asset(tmp_path: Path) -> None:
    wheel = tmp_path / "bad.whl"
    assets = set(verify_wheel_install.expected_runtime_assets(REPO_ROOT))
    assets.remove(
        "LiuXin_alpha/databases/database_driver_plugins/SQL/"
        "database_generator_frbr/interlink_table_requests.toml"
    )
    with zipfile.ZipFile(wheel, "w") as archive:
        for name in assets:
            archive.writestr(name, b"runtime")
        archive.writestr("past/__init__.py", b"")
        archive.writestr("past/builtins.py", b"")

    with pytest.raises(
        verify_wheel_install.WheelVerificationError,
        match="interlink_table_requests.toml",
    ):
        verify_wheel_install.inspect_wheel(wheel, REPO_ROOT)
