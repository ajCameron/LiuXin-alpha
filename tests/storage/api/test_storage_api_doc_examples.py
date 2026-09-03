"""
Regression coverage for examples on the public storage API source tree.
"""

from __future__ import annotations

import ast
from pathlib import Path


STORAGE_API_ROOT = Path(__file__).parents[3] / "src" / "LiuXin_alpha" / "storage" / "api"
DOCUMENTABLE_NODES = (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)


def test_every_storage_api_docstring_has_an_examples_section() -> None:
    """
    Require the project-wide singular example section on every API definition.

    Example:
        >>> test_every_storage_api_docstring_has_an_examples_section()


    :return:
    """

    missing_examples: list[str] = []

    for source_path in sorted(STORAGE_API_ROOT.rglob("*.py")):
        module = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        for node in ast.walk(module):
            if not isinstance(node, DOCUMENTABLE_NODES):
                continue
            docstring = ast.get_docstring(node, clean=False)
            if docstring is not None and "Example:" in docstring:
                continue
            relative_path = source_path.relative_to(STORAGE_API_ROOT)
            object_name = getattr(node, "name", "<module>")
            line_number = getattr(node, "lineno", 1)
            missing_examples.append(f"{relative_path}:{line_number}: {object_name}")

    assert missing_examples == [], (
        "Storage API docstrings without examples:\n" + "\n".join(missing_examples)
    )
