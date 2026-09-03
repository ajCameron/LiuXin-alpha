"""
Documentation-quality contracts for the composed storage-manager surface.
"""

from __future__ import annotations

import ast
from collections.abc import Iterator
from pathlib import Path

_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
_STORAGE_ROOT = _REPOSITORY_ROOT / "src" / "LiuXin_alpha" / "storage"
_SOURCE_ROOTS = (
    _STORAGE_ROOT / "api" / "storage_manager_api",
    _STORAGE_ROOT / "storage_manager",
)
_SOURCE_FILES = (_STORAGE_ROOT / "store_manager.py",)
_DOCUMENTABLE_NODES = (
    ast.Module,
    ast.ClassDef,
    ast.FunctionDef,
    ast.AsyncFunctionDef,
)
_PLACEHOLDER_FRAGMENTS = (
    "implement the corresponding storage-manager responsibility",
    "todo: document",
)


def _source_paths() -> tuple[Path, ...]:
    """
    Return the stable, de-duplicated source scope guarded by this test.


    :return:
    """

    paths = set(_SOURCE_FILES)
    for root in _SOURCE_ROOTS:
        paths.update(root.rglob("*.py"))
    return tuple(sorted(paths))


def _documentable_nodes(path: Path) -> Iterator[ast.AST]:
    """
    Yield every module, class, and callable definition in one source file.


    :param path:
    :return:
    """

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    yield from (
        node for node in ast.walk(tree) if isinstance(node, _DOCUMENTABLE_NODES)
    )


def _location(path: Path, node: ast.AST) -> str:
    """
    Return a concise repository-relative definition location.


    :param path:
    :param node:
    :return:
    """

    name = getattr(node, "name", "<module>")
    line = getattr(node, "lineno", 1)
    return f"{path.relative_to(_REPOSITORY_ROOT)}:{line}:{name}"


def test_storage_manager_definitions_have_docstrings() -> None:
    """
    Keep every definition in the reviewed storage-manager scope documented.


    :return:
    """

    missing = [
        _location(path, node)
        for path in _source_paths()
        for node in _documentable_nodes(path)
        if ast.get_docstring(node, clean=False) is None
    ]

    assert not missing, "undocumented storage-manager definitions:\n" + "\n".join(
        missing
    )


def test_storage_manager_docstrings_have_no_known_placeholders() -> None:
    """
    Reject generic prose that does not explain a definition's responsibility.


    :return:
    """

    placeholders = []
    for path in _source_paths():
        for node in _documentable_nodes(path):
            docstring = ast.get_docstring(node, clean=False)
            if docstring is None:
                continue
            lowered = docstring.casefold()
            if any(fragment in lowered for fragment in _PLACEHOLDER_FRAGMENTS):
                placeholders.append(_location(path, node))

    assert not placeholders, "placeholder storage-manager docstrings:\n" + "\n".join(
        placeholders
    )


def test_public_storage_manager_function_docstrings_have_conventional_fields() -> None:
    """
    Keep public parameter and return fields aligned with callable signatures.

    Private implementation helpers still require meaningful docstrings, but
    their type-annotated signatures are the parameter contract. Requiring
    Sphinx fields there as well would duplicate implementation detail and work
    against the module-size maintainability guardrail.


    :return:
    """

    invalid_fields = []
    for path in _source_paths():
        for node in _documentable_nodes(path):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if node.name.startswith("_") or node.col_offset > 4:
                continue
            docstring = ast.get_docstring(node, clean=False)
            if docstring is None:
                continue
            parameters = (
                *node.args.posonlyargs,
                *node.args.args,
                *node.args.kwonlyargs,
            )
            expected = {
                argument.arg
                for argument in parameters
                if argument.arg not in {"self", "cls"}
            }
            if node.args.vararg is not None:
                expected.add(node.args.vararg.arg)
            if node.args.kwarg is not None:
                expected.add(node.args.kwarg.arg)
            actual = {
                line.split(":", 2)[1].removeprefix("param ").strip()
                for line in docstring.splitlines()
                if line.lstrip().startswith(":param ")
            }
            if actual != expected or not any(
                line.lstrip().startswith((":return:", ":returns:"))
                for line in docstring.splitlines()
            ):
                invalid_fields.append(_location(path, node))

    assert not invalid_fields, "non-conventional storage doc fields:\n" + "\n".join(
        invalid_fields
    )
