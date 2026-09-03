"""
Documentation contracts for LiuXin's reviewed first-party boundaries.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = REPOSITORY_ROOT / "src" / "LiuXin_alpha"
MODERN_PACKAGES = (
    "caches",
    "catalog",
    "core",
    "databases",
    "ingest",
    "jobs",
    "storage",
    "surfaces",
)
METADATA_BOUNDARY_ROOTS = (
    SOURCE_ROOT
    / "metadata"
    / "api"
    / "containers_api"
    / "main_table_containers_api",
    SOURCE_ROOT
    / "metadata"
    / "api"
    / "containers_api"
    / "wemi_containers_api",
    SOURCE_ROOT
    / "metadata"
    / "containers"
    / "metadata_containers"
    / "non_wemi_containers",
    SOURCE_ROOT
    / "metadata"
    / "containers"
    / "metadata_containers"
    / "wemi_containers",
)
PLACEHOLDER_FRAGMENTS = (
    "implement the corresponding",
    "provide the public `",
    "represents a tree container in the",
    "state shared across the public boundary",
    "todo: document",
)


def _modern_source_paths() -> tuple[Path, ...]:
    """
    Return the production paths included in the modern documentation ratchet.


    :return:
    """

    paths = {
        path
        for package in MODERN_PACKAGES
        for path in (SOURCE_ROOT / package).rglob("*.py")
    }
    return tuple(sorted(paths))


def _maintained_script_paths() -> tuple[Path, ...]:
    """
    Return maintained top-level Python tools while excluding generated caches.


    :return:
    """

    return tuple(sorted((REPOSITORY_ROOT / "scripts").glob("*.py")))


def _metadata_boundary_paths() -> tuple[Path, ...]:
    """
    Return the reviewed metadata row, relation, and WEMI container modules.


    :return:
    """

    paths = {
        path
        for root in METADATA_BOUNDARY_ROOTS
        for path in root.rglob("*.py")
    }
    return tuple(sorted(paths))


def _parse(path: Path) -> ast.Module:
    """
    Parse one repository source file using its real path in diagnostics.


    :param path:
    :return:
    """

    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _location(path: Path, node: ast.AST) -> str:
    """
    Return a concise repository-relative definition location.


    :param path:
    :param node:
    :return:
    """

    name = getattr(node, "name", "<module>")
    line = getattr(node, "lineno", 1)
    return f"{path.relative_to(REPOSITORY_ROOT)}:{line}:{name}"


def _literal_exports(tree: ast.Module) -> tuple[str, ...]:
    """
    Return names from a literal module ``__all__`` declaration.


    :param tree:
    :return:
    """

    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(target, ast.Name) and target.id == "__all__"
            for target in node.targets
        ):
            continue
        try:
            value = ast.literal_eval(node.value)
        except (TypeError, ValueError):
            return ()
        if isinstance(value, (list, tuple)):
            return tuple(name for name in value if isinstance(name, str))
        return ()
    return ()


def _guarded_nodes(path: Path) -> Iterator[ast.AST]:
    """
    Yield reviewed module, public-class, and explicit-export boundaries.


    :param path:
    :return:
    """

    tree = _parse(path)
    yield tree
    definitions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }
    yielded_names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and not node.name.startswith("_"):
            yielded_names.add(node.name)
            yield node
    for name in _literal_exports(tree):
        node = definitions.get(name)
        if node is not None and name not in yielded_names:
            yielded_names.add(name)
            yield node


def test_modern_public_boundaries_have_docstrings() -> None:
    """
    Keep modern architecture and explicitly exported definitions documented.


    :return:
    """

    missing = [
        _location(path, node)
        for path in _modern_source_paths()
        for node in _guarded_nodes(path)
        if ast.get_docstring(node, clean=False) is None
    ]

    assert not missing, "undocumented modern public boundaries:\n" + "\n".join(
        missing
    )


def test_metadata_row_and_container_boundaries_have_docstrings() -> None:
    """
    Keep public metadata row, relation, and WEMI container families documented.


    :return:
    """

    missing = [
        _location(path, node)
        for path in _metadata_boundary_paths()
        for node in _guarded_nodes(path)
        if ast.get_docstring(node, clean=False) is None
    ]

    assert not missing, "undocumented metadata public boundaries:\n" + "\n".join(
        missing
    )


def test_maintained_script_modules_and_public_classes_have_docstrings() -> None:
    """
    Keep operator tooling discoverable without documenting trivial helpers.


    :return:
    """

    missing = []
    for path in _maintained_script_paths():
        tree = _parse(path)
        nodes = [
            tree,
            *(
                node
                for node in tree.body
                if isinstance(node, ast.ClassDef) and not node.name.startswith("_")
            ),
        ]
        missing.extend(
            _location(path, node)
            for node in nodes
            if ast.get_docstring(node, clean=False) is None
        )

    assert not missing, "undocumented maintained-script boundaries:\n" + "\n".join(
        missing
    )


def test_reviewed_boundaries_have_no_known_placeholder_prose() -> None:
    """
    Reject generic summaries that conceal rather than explain responsibility.


    :return:
    """

    placeholders = []
    reviewed_paths = (*_modern_source_paths(), *_metadata_boundary_paths())
    for path in reviewed_paths:
        for node in _guarded_nodes(path):
            docstring = ast.get_docstring(node, clean=False)
            if docstring is None:
                continue
            lowered = docstring.casefold()
            if any(fragment in lowered for fragment in PLACEHOLDER_FRAGMENTS):
                placeholders.append(_location(path, node))

    assert not placeholders, "placeholder public-boundary docstrings:\n" + "\n".join(
        placeholders
    )
