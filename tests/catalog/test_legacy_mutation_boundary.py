"""Keep legacy catalog mutation code frozen while its caller graph shrinks."""

from __future__ import annotations

import ast
from functools import cache
from pathlib import Path
import tokenize


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "src" / "LiuXin_alpha"
LEGACY_MODULES = (
    "LiuXin_alpha.catalog.catalog_macros",
    "LiuXin_alpha.catalog.metadata_tools",
)
REFERENCE_PATHS = (
    SOURCE_ROOT / "catalog" / "catalog_macros.py",
    SOURCE_ROOT / "catalog" / "metadata_tools" / "__init__.py",
)
ALLOWED_PRODUCTION_IMPORTS: set[tuple[str, str]] = {
    ("catalog/catalog.py", "LiuXin_alpha.catalog.metadata_tools"),
}
ALLOWED_INDIRECT_FACADE_REFERENCES: set[tuple[str, int, str]] = set()


def _legacy_root(module: str) -> str | None:
    return next(
        (
            legacy
            for legacy in LEGACY_MODULES
            if module == legacy or module.startswith(legacy + ".")
        ),
        None,
    )


@cache
def _production_dependencies() -> tuple[
    frozenset[tuple[str, str]],
    frozenset[tuple[str, int, str]],
]:
    """Scan production once for direct and indirect legacy dependencies."""

    imports: set[tuple[str, str]] = set()
    references: set[tuple[str, int, str]] = set()
    helper_names = {"add", "ensure", "apply", "intralink"}
    for path in SOURCE_ROOT.rglob("*.py"):
        relative = path.relative_to(SOURCE_ROOT).as_posix()
        if relative == "catalog/catalog_macros.py" or relative.startswith(
            "catalog/metadata_tools/"
        ):
            continue
        with tokenize.open(path) as source:
            tree = ast.parse(source.read(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    legacy = _legacy_root(alias.name)
                    if legacy is not None:
                        imports.add((relative, legacy))
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                legacy = _legacy_root(module)
                if legacy is not None:
                    imports.add((relative, legacy))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Attribute)
                and node.attr in helper_names
                and _is_database_attribute(node.value)
            ):
                references.add((relative, node.lineno, node.attr))
    return frozenset(imports), frozenset(references)


def _production_imports() -> set[tuple[str, str]]:
    return set(_production_dependencies()[0])


def _is_database_attribute(node: ast.expr) -> bool:
    """Return whether ``node`` names a conventional database attribute."""

    return isinstance(node, ast.Name) and node.id == "db" or (
        isinstance(node, ast.Attribute) and node.attr == "db"
    )


def _indirect_facade_references() -> set[tuple[str, int, str]]:
    """Find production access to the formerly injected mutation facades."""

    return set(_production_dependencies()[1])


def test_legacy_mutation_reference_sources_are_preserved() -> None:
    """The migration must retain its direct-SQL reference implementations."""

    assert all(path.is_file() for path in REFERENCE_PATHS)


def test_legacy_mutation_production_import_allowlist_only_changes_deliberately() -> None:
    """Permit only the Catalog composition root to import metadata helpers."""

    assert _production_imports() == ALLOWED_PRODUCTION_IMPORTS


def test_legacy_mutation_facades_are_not_obtained_indirectly() -> None:
    """Reject calls through the old database-injected helper attributes."""

    assert _indirect_facade_references() == ALLOWED_INDIRECT_FACADE_REFERENCES
