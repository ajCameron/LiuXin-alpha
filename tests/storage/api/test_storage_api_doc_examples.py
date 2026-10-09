"""
Ratchet reviewed Example sections throughout the public storage API.

The regression parses source without importing it and includes private/nested/async
definitions. It checks only docstring presence and the exact Example: marker, not
parameter coverage, prose accuracy, executable examples, or module introductions.

Example:
    >>> test_storage_api_docstrings_add_no_new_example_debt()
"""

from __future__ import annotations

import ast
from pathlib import Path


STORAGE_API_ROOT = Path(__file__).parents[3] / "src" / "LiuXin_alpha" / "storage" / "api"
EXAMPLE_DEBT_BASELINE = Path(__file__).with_name("storage_api_example_debt.txt")
DOCUMENTABLE_NODES = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def _documentable_nodes(
    nodes: list[ast.stmt],
    *,
    prefix: str = "",
):
    """Yield each declaration with a stable lexical name rather than a brittle line number."""

    for node in nodes:
        if not isinstance(node, DOCUMENTABLE_NODES):
            continue
        qualified_name = f"{prefix}.{node.name}" if prefix else node.name
        yield node, qualified_name
        yield from _documentable_nodes(node.body, prefix=qualified_name)


def test_storage_api_docstrings_add_no_new_example_debt() -> None:
    """
    Parse every Python file below storage/api and reject missing Example markers not present in the
    explicit debt baseline.

    Collect relative paths, source lines, and declaration names before asserting. Syntax/read
    failures propagate. Embedded source strings and anonymous lambdas are not declarations in this
    AST scan.

    Example:
        >>> test_storage_api_docstrings_add_no_new_example_debt()


    :return: None when no declaration adds new example debt; otherwise raise AssertionError.
    """

    missing_examples: set[str] = set()

    for source_path in sorted(STORAGE_API_ROOT.rglob("*.py")):
        module = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        for node, qualified_name in _documentable_nodes(module.body):
            docstring = ast.get_docstring(node, clean=False)
            if docstring is not None and "Example:" in docstring:
                continue
            relative_path = source_path.relative_to(STORAGE_API_ROOT)
            missing_examples.add(f"{relative_path}::{qualified_name}")

    accepted_debt = {
        line
        for line in EXAMPLE_DEBT_BASELINE.read_text(encoding="utf-8").splitlines()
        if line and not line.startswith("#")
    }
    regressions = missing_examples - accepted_debt
    assert regressions == set(), (
        "New storage API docstrings without examples:\n" + "\n".join(sorted(regressions))
    )
