from __future__ import annotations

import ast
import re
from pathlib import Path


CATALOG_ROOT = Path(__file__).resolve().parents[2] / "src" / "LiuXin_alpha" / "catalog"
SQL_PATTERN = re.compile(
    r"\bSELECT\b.+\bFROM\b|"
    r"\bINSERT\s+INTO\b|"
    r"\bUPDATE\b.+\bSET\b|"
    r"\bDELETE\s+FROM\b|"
    r"\bCREATE\s+(TABLE|INDEX|VIEW|TRIGGER|VIRTUAL)\b|"
    r"\bDROP\s+(TABLE|INDEX|VIEW|TRIGGER)\b|"
    r"\bJOIN\b.+\bON\b",
    re.IGNORECASE | re.DOTALL,
)


def _docstring_nodes(tree: ast.AST) -> set[ast.AST]:
    nodes: set[ast.AST] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = getattr(node, "body", ())
        if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
            if isinstance(body[0].value.value, str):
                nodes.add(body[0].value)
    return nodes


def test_catalog_contains_no_executable_sql_literals() -> None:
    offenders: list[str] = []
    for path in sorted(CATALOG_ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text(), filename=str(path))
        docstrings = _docstring_nodes(tree)
        for node in ast.walk(tree):
            if node in docstrings:
                continue
            if isinstance(node, ast.Constant) and isinstance(node.value, str) and SQL_PATTERN.search(node.value):
                rel_path = path.relative_to(CATALOG_ROOT.parents[1])
                offenders.append(f"{rel_path}:{node.lineno}")

    assert not offenders, "Executable SQL belongs under databases, not catalog: " + ", ".join(offenders)
