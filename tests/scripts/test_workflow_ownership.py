"""
Provide test workflow ownership utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test workflow ownership through a consuming regression::

        python -m pytest -q tests/scripts/test_workflow_ownership.py
"""

import ast
from pathlib import Path

import pytest

from scripts.check_modern_import_cycles import (
    build_graph,
    strongly_connected_components,
)
from tests.support.docstring_ownership import SourceMetrics, body_without_docstring

ROOT = Path(__file__).resolve().parents[2]
SERVICE_ROOTS = ("core/program_services", "surfaces/cli/storage_commands")


@pytest.mark.parametrize("relative", SERVICE_ROOTS)
def test_workflow_owners_remain_bounded(relative: str) -> None:
    """
    Perform the test workflow owners remain bounded operation under explicit file-format and conversion rules.

    Example:
        Exercise test workflow owners remain bounded through a consuming regression::

            python -m pytest -q tests/scripts/test_workflow_ownership.py


    :param relative: Value supplied for relative under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for path in (ROOT / "src/LiuXin_alpha" / relative).glob("*.py"):
        metrics = SourceMetrics(path.read_text())
        assert metrics.line_count() <= 450, path
        for node in ast.walk(metrics.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.end_lineno is not None
                assert metrics.line_count(node) <= 160, (path, node.name)


@pytest.mark.parametrize("relative", ("core/program_api.py", "surfaces/cli/storage.py"))
def test_compatibility_facades_do_not_reaccumulate_workflows(relative: str) -> None:
    """
    Perform the test compatibility facades do not reaccumulate workflows operation under explicit file-format and conversion rules.

    Example:
        Exercise test compatibility facades do not reaccumulate workflows through a consuming regression::

            python -m pytest -q tests/scripts/test_workflow_ownership.py


    :param relative: Value supplied for relative under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path = ROOT / "src/LiuXin_alpha" / relative
    metrics = SourceMetrics(path.read_text())
    assert metrics.line_count() <= 250
    for node in ast.walk(metrics.tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            assert node.end_lineno is not None and metrics.line_count(node) - 1 < 10
            if node.name not in {"install", "install_program_api"}:
                # Historical instance methods retain their unbound signature
                # through a single explicit delegate, never a workflow body.
                body = body_without_docstring(node)
                assert len(body) == 1
                assert isinstance(body[0], ast.Return)
                assert isinstance(body[0].value, ast.Call)


def test_workflow_owners_have_no_cycles_or_back_imports_to_facades() -> None:
    """
    Perform the test workflow owners have no cycles or back imports to facades operation under explicit file-format and conversion rules.

    Example:
        Exercise test workflow owners have no cycles or back imports to facades through a consuming regression::

            python -m pytest -q tests/scripts/test_workflow_ownership.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    prefixes = tuple(
        "LiuXin_alpha." + value.replace("/", ".") for value in SERVICE_ROOTS
    )
    forbidden = {"LiuXin_alpha.core.program_api", "LiuXin_alpha.surfaces.cli.storage"}
    graph = build_graph(ROOT / "src", protected_prefixes=(*prefixes, *forbidden))
    assert strongly_connected_components(graph) == ()
    for owner, dependencies in graph.items():
        if owner not in forbidden:
            assert not forbidden.intersection(dependencies), owner
