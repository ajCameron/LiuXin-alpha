"""Keep terminal behavior in bounded owners with checked composition contracts."""

from __future__ import annotations

import ast
import inspect
import tomllib
from pathlib import Path

import pytest

from LiuXin_alpha.surfaces.terminal.browser import TextDatabaseBrowser
from LiuXin_alpha.surfaces.terminal.browser_components.contracts import BrowserState
from LiuXin_alpha.surfaces.terminal.windowed_components.contracts import WindowedState
from LiuXin_alpha.surfaces.terminal.windowed_ui import _CursesUiDriver

ROOT = Path(__file__).resolve().parents[2]
TERMINAL = ROOT / "src/LiuXin_alpha/surfaces/terminal"
COMPONENTS = ("browser_components", "windowed_components")


@pytest.mark.parametrize("directory", COMPONENTS)
def test_component_owners_and_functions_remain_bounded(directory: str) -> None:
    paths = sorted((TERMINAL / directory).rglob("*.py"))
    assert paths
    for path in paths:
        source = path.read_text()
        assert len(source.splitlines()) <= 450, path
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.end_lineno is not None
                assert node.end_lineno - node.lineno + 1 <= 160, (path, node.name)


@pytest.mark.parametrize(
    ("filename", "owner", "allowed"),
    [
        ("browser.py", "TextDatabaseBrowser", {"__init__", "_extension_host"}),
        ("windowed_ui.py", "_CursesUiDriver", {"__init__", "bind_browser"}),
    ],
)
def test_roots_only_initialize_and_bind_components(
    filename: str, owner: str, allowed: set[str]
) -> None:
    source = (TERMINAL / filename).read_text()
    assert len(source.splitlines()) <= 250
    node = next(
        node
        for node in ast.parse(source).body
        if isinstance(node, ast.ClassDef) and node.name == owner
    )
    assert {n.name for n in node.body if isinstance(n, ast.FunctionDef)} == allowed


@pytest.mark.parametrize(
    ("owner", "state"),
    [(TextDatabaseBrowser, BrowserState), (_CursesUiDriver, WindowedState)],
)
def test_all_required_owner_methods_are_concretely_composed(owner, state) -> None:
    assert inspect.isabstract(state)
    assert not inspect.isabstract(owner)
    for name in state.__abstractmethods__:
        implementation = inspect.getattr_static(owner, name)
        assert not getattr(implementation, "__isabstractmethod__", False), name


@pytest.mark.parametrize(
    ("owner", "method", "module"),
    [
        (TextDatabaseBrowser, "run", "browser_components.session"),
        (TextDatabaseBrowser, "register_command", "browser_components.registry"),
        (
            TextDatabaseBrowser,
            "_rewrite_show_legacy_group_args",
            "browser_components.legacy",
        ),
        (TextDatabaseBrowser, "_print_help", "browser_components.help"),
        (
            TextDatabaseBrowser,
            "command_completion_candidates",
            "browser_components.completion",
        ),
        (
            TextDatabaseBrowser,
            "_row_id_completion_candidates",
            "browser_components.completion_sources",
        ),
        (TextDatabaseBrowser, "resolve_table_column", "browser_components.catalog"),
        (TextDatabaseBrowser, "render_row_details", "browser_components.rows"),
        (TextDatabaseBrowser, "execute_core_query", "browser_components.host"),
        (TextDatabaseBrowser, "_cmd_search", "browser_components.browsing"),
        (_CursesUiDriver, "append_output", "windowed_components.console"),
        (_CursesUiDriver, "_get_job", "windowed_components.job_output"),
        (_CursesUiDriver, "_build_telemetry_lines", "windowed_components.telemetry"),
        (_CursesUiDriver, "_build_status_lines", "windowed_components.status"),
        (_CursesUiDriver, "_complete_current_input", "windowed_components.completion"),
        (_CursesUiDriver, "read_line", "windowed_components.input"),
        (_CursesUiDriver, "_render", "windowed_components.layout"),
        (_CursesUiDriver, "_wrap_lines_for_width", "windowed_components.presentation"),
    ],
)
def test_real_composition_resolves_methods_to_their_owners(
    owner, method, module
) -> None:
    assert (
        getattr(owner, method).__module__ == f"LiuXin_alpha.surfaces.terminal.{module}"
    )


def test_complete_component_trees_enter_all_quality_scopes() -> None:
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())
    runner = (ROOT / "scripts/run_type_checks.sh").read_text()
    for name in (*COMPONENTS, "browser.py", "windowed_ui.py"):
        path = f"src/LiuXin_alpha/surfaces/terminal/{name}"
        assert path in config["tool"]["liuxin"]["format"]["paths"]
        assert path in config["tool"]["basedpyright"]["include"]
        assert path in config["tool"]["mypy"]["files"]
        for start, end in (
            ("RUFF_CMD=(", "MODERN_COMPLEXITY_CMD=("),
            ("MODERN_COMPLEXITY_CMD=(", "STORAGE_MANAGER_COMPLEXITY_CMD=("),
        ):
            section = runner.split(start, 1)[1].split(end, 1)[0]
            assert f'"{path}"' in section
    strict = config["tool"]["basedpyright"]["strict"]
    for directory in COMPONENTS:
        assert f"src/LiuXin_alpha/surfaces/terminal/{directory}/models.py" in strict
