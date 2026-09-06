"""Keep extracted implementations bounded and outside compatibility facades."""

import ast
from pathlib import Path

import pytest

from scripts.check_modern_import_cycles import (
    build_graph,
    strongly_connected_components,
)

ROOT = Path(__file__).resolve().parents[2]
SERVICE_ROOTS = ("core/program_services", "surfaces/cli/storage_commands")


@pytest.mark.parametrize("relative", SERVICE_ROOTS)
def test_workflow_owners_remain_bounded(relative: str) -> None:
    for path in (ROOT / "src/LiuXin_alpha" / relative).glob("*.py"):
        source = path.read_text()
        assert len(source.splitlines()) <= 450, path
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.end_lineno is not None
                assert node.end_lineno - node.lineno + 1 <= 160, (path, node.name)


@pytest.mark.parametrize("relative", ("core/program_api.py", "surfaces/cli/storage.py"))
def test_compatibility_facades_do_not_reaccumulate_workflows(relative: str) -> None:
    path = ROOT / "src/LiuXin_alpha" / relative
    source = path.read_text()
    assert len(source.splitlines()) <= 250
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            assert node.end_lineno is not None and node.end_lineno - node.lineno < 10
            if node.name not in {"install", "install_program_api"}:
                # Historical instance methods retain their unbound signature
                # through a single explicit delegate, never a workflow body.
                assert len(node.body) == 1
                assert isinstance(node.body[0], ast.Return)
                assert isinstance(node.body[0].value, ast.Call)


def test_workflow_owners_have_no_cycles_or_back_imports_to_facades() -> None:
    prefixes = tuple(
        "LiuXin_alpha." + value.replace("/", ".") for value in SERVICE_ROOTS
    )
    forbidden = {"LiuXin_alpha.core.program_api", "LiuXin_alpha.surfaces.cli.storage"}
    graph = build_graph(ROOT / "src", protected_prefixes=(*prefixes, *forbidden))
    assert strongly_connected_components(graph) == ()
    for owner, dependencies in graph.items():
        if owner not in forbidden:
            assert not forbidden.intersection(dependencies), owner
