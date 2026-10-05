"""
Verify test legacy mutation boundary behavior against the public catalog contracts.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test legacy mutation boundary through its owning regression module::

        python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py
"""

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
    """
    Perform the legacy root test-helper operation with deterministic inputs.

    Example:
        Exercise legacy root through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :param module: Value supplied for module under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Scan production once for direct and indirect legacy dependencies.

    Example:
        Exercise production dependencies through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :return: The deterministic value, row, identity or collection described above.
    """

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
    """
    Perform the production imports test-helper operation with deterministic inputs.

    Example:
        Exercise production imports through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return set(_production_dependencies()[0])


def _is_database_attribute(node: ast.expr) -> bool:
    """
    Return whether ``node`` names a conventional database attribute.

    Example:
        Exercise is database attribute through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :param node: Value supplied for node under the catalog contract.
    :return: The deterministic value, row, identity or collection described above.
    """

    return isinstance(node, ast.Name) and node.id == "db" or (
        isinstance(node, ast.Attribute) and node.attr == "db"
    )


def _indirect_facade_references() -> set[tuple[str, int, str]]:
    """
    Find production access to the formerly injected mutation facades.

    Example:
        Exercise indirect facade references through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :return: The deterministic value, row, identity or collection described above.
    """

    return set(_production_dependencies()[1])


def test_legacy_mutation_reference_sources_are_preserved() -> None:
    """
    The migration must retain its direct-SQL reference implementations.

    Example:
        Exercise test legacy mutation reference sources are preserved through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :return: None; the function records state or raises through its assertions.
    """

    assert all(path.is_file() for path in REFERENCE_PATHS)


def test_legacy_mutation_production_import_allowlist_only_changes_deliberately() -> None:
    """
    Permit only the Catalog composition root to import metadata helpers.

    Example:
        Exercise test legacy mutation production import allowlist only changes deliberately through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :return: None; the function records state or raises through its assertions.
    """

    assert _production_imports() == ALLOWED_PRODUCTION_IMPORTS


def test_legacy_mutation_facades_are_not_obtained_indirectly() -> None:
    """
    Reject calls through the old database-injected helper attributes.

    Example:
        Exercise test legacy mutation facades are not obtained indirectly through its owning regression module::

            python -m pytest -q tests/catalog/test_legacy_mutation_boundary.py


    :return: None; the function records state or raises through its assertions.
    """

    assert _indirect_facade_references() == ALLOWED_INDIRECT_FACADE_REFERENCES
