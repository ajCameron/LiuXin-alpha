"""Protect metadata ownership of the two distinct legacy identifier write contracts."""

from __future__ import annotations

import ast
from pathlib import Path

LEGACY_LIBRARY_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "LiuXin_alpha"
    / "library"
    / "legacy.py"
)


def _library_database_method(name: str) -> ast.FunctionDef:
    """Return one method node without importing the dependency-heavy legacy module."""

    tree = ast.parse(LEGACY_LIBRARY_PATH.read_text(encoding="utf-8"))
    library_database = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "LibraryDatabase"
    )
    return next(
        node
        for node in library_database.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _called_names(method: ast.FunctionDef) -> set[str]:
    """Collect direct function names called within one parsed method."""

    return {
        node.func.id
        for node in ast.walk(method)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }


def test_legacy_identifier_writers_delegate_to_separate_metadata_contracts() -> None:
    """Keep single validation and bulk cleaning delegated without homogenizing them."""

    single_calls = _called_names(_library_database_method("set_identifier"))
    bulk_preflight_calls = _called_names(
        _library_database_method("_id_val_dict_preflight")
    )

    assert "prepare_legacy_single_identifier" in single_calls
    assert "prepare_legacy_identifier_mapping" in bulk_preflight_calls
    assert "clean_identifier" not in single_calls | bulk_preflight_calls
    assert "validate_identifier" not in single_calls | bulk_preflight_calls
