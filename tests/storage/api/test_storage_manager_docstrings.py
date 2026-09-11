"""
Guard basic documentation presence and field shape in the composed manager source.

The scope is the manager API tree, implementation tree, and application-manager
module. These AST-only checks do not import production modules or prove complete
descriptive reST documentation; the whole-project audit and source review enforce
the broader documentation target separately.
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
    Collect explicit manager files and recursively discovered Python files from the guarded roots.

    Paths are deduplicated and sorted lexically. Discovery uses filesystem rglob rather than Git
    ownership, and explicitly listed files are retained without an existence check.

    Example:
        >>> paths = _source_paths()
        >>> _STORAGE_ROOT / "store_manager.py" in paths
        True


    :return: Sorted tuple of Path values for the bounded manager documentation guard.
    """

    paths = set(_SOURCE_FILES)
    for root in _SOURCE_ROOTS:
        paths.update(root.rglob("*.py"))
    return tuple(sorted(paths))


def _documentable_nodes(path: Path) -> Iterator[ast.AST]:
    """
    Lazily parse one UTF-8 source file and yield modules, classes, and named function definitions.

    ast.walk includes private, nested, and async definitions in its traversal order. Lambda
    expressions and code embedded inside string fixtures are not yielded as definitions. File and
    parse errors propagate on iteration; the module is not imported.

    Example:
        >>> nodes = _documentable_nodes(_SOURCE_FILES[0])
        >>> isinstance(next(nodes), ast.Module)
        True
        >>> nodes.close()


    :param path: Python source file to read as UTF-8 and parse without execution.
    :return: Generator over the parsed AST nodes selected by _DOCUMENTABLE_NODES.
    """

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    yield from (
        node for node in ast.walk(tree) if isinstance(node, _DOCUMENTABLE_NODES)
    )


def _location(path: Path, node: ast.AST) -> str:
    """
    Format a repository-relative path, source line, and node name for assertion diagnostics.

    Nodes without a name or line use <module> and line 1. The path must lie below the repository
    root for relative_to to succeed; no filesystem lookup is performed.

    Example:
        >>> _location(_SOURCE_FILES[0], ast.parse("pass")).endswith(":1:<module>")
        True


    :param path: Source path beneath _REPOSITORY_ROOT.
    :param node: AST node whose optional lineno and name attributes identify the definition.
    :return: Colon-separated repository path, line number, and definition name.
    """

    name = getattr(node, "name", "<module>")
    line = getattr(node, "lineno", 1)
    return f"{path.relative_to(_REPOSITORY_ROOT)}:{line}:{name}"


def test_storage_manager_definitions_have_docstrings() -> None:
    """
    Reject definitions with no leading literal docstring in the bounded manager source scope.

    The test checks for None, so blank strings and incomplete prose can pass. It does not establish
    descriptive quality, examples, field completeness, or the whole-project reviewed-file status.

    Example:
        >>> test_storage_manager_definitions_have_docstrings()  # doctest: +SKIP


    :return: None after every discovered definition has a literal docstring; missing entries fail with source locations.
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
    Reject either configured placeholder fragment in existing manager docstrings, ignoring case.

    Missing docstrings are skipped by this test and handled by the presence check. This finite
    substring guard does not detect all generic or inaccurate descriptions.

    Example:
        >>> test_storage_manager_docstrings_have_no_known_placeholders()  # doctest: +SKIP


    :return: None when neither known fragment is found; offending definitions fail with source locations.
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
    Check parameter-name sets and presence of a return field on shallow nonprivate function
    definitions.

    The selection skips names beginning with an underscore and definitions indented more than four
    columns. Expected names include positional-only, ordinary, keyword-only, and variadic
    parameters, while literal self/cls names are omitted. Missing docstrings are left to the
    presence test.

    Sets make this a name-coverage check rather than an order or uniqueness check. Empty field
    descriptions and either return spelling pass. Private and deeper definitions remain subject to
    the broader documentation goal even though this older bounded guard excludes their fields.

    Example:
        >>> test_public_storage_manager_function_docstrings_have_conventional_fields()  # doctest: +SKIP


    :return: None when selected functions have the expected parameter-name set and a return field; failures list their source locations.
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
