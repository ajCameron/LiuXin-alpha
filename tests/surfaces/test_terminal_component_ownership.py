"""
Check terminal owner sizes, composition responsibilities, abstract-method resolution, and quality-scope coverage.

The size guards exclude documentation-only lines while retaining code, comments,
and other blank lines. Function spans retain their AST boundaries. Numeric limits,
composition rules, and quality-scope selection remain unchanged.
"""

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
from tests.support.docstring_ownership import SourceMetrics

ROOT = Path(__file__).resolve().parents[2]
TERMINAL = ROOT / "src/LiuXin_alpha/surfaces/terminal"
COMPONENTS = ("browser_components", "windowed_components")


@pytest.mark.parametrize("directory", COMPONENTS)
def test_component_owners_and_functions_remain_bounded(directory: str) -> None:
    """
    Enforce nonempty component discovery and the existing 450-file/160-function physical-line ceilings.

    Recognized docstrings are excluded only on documentation-only lines, including
    nested owners. Comments, other blank lines, and lines shared with code count.

    Example:
        >>> test_component_owners_and_functions_remain_bounded("browser_components")  # doctest: +SKIP


    :param directory: Parametrized browser_components or windowed_components directory beneath the terminal package.
    :return: None if every discovered Python file and named function meets the unchanged limits after documentation-only lines are excluded.
    """
    paths = sorted((TERMINAL / directory).rglob("*.py"))
    assert paths
    for path in paths:
        metrics = SourceMetrics(path.read_text())
        assert metrics.line_count() <= 450, path
        for node in ast.walk(metrics.tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                assert node.end_lineno is not None
                assert metrics.line_count(node) <= 160, (path, node.name)


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
    """
    Enforce the root file's 250-line ceiling and exact directly defined method-name set on its composition class.

    Other classes in the same file are not inspected for method membership, but
    all file lines except documentation-only lines still count. This check does
    not inspect allowed method bodies.

    Example:
        >>> test_roots_only_initialize_and_bind_components("browser.py", "TextDatabaseBrowser", {"__init__", "_extension_host"})  # doctest: +SKIP


    :param filename: Root module filename relative to the terminal package.
    :param owner: Top-level composition class to select from the parsed module.
    :param allowed: Exact set of directly declared function names permitted on that class.
    :return: None after the unchanged file limit and owner-method set assertions succeed.
    """
    metrics = SourceMetrics((TERMINAL / filename).read_text())
    assert metrics.line_count() <= 250
    node = next(
        node
        for node in metrics.tree.body
        if isinstance(node, ast.ClassDef) and node.name == owner
    )
    assert {n.name for n in node.body if isinstance(n, ast.FunctionDef)} == allowed


@pytest.mark.parametrize(
    ("owner", "state"),
    [(TextDatabaseBrowser, BrowserState), (_CursesUiDriver, WindowedState)],
)
def test_all_required_owner_methods_are_concretely_composed(owner, state) -> None:
    """
    Require an abstract shared contract, a concrete composition, and concrete static lookup of every required method.

    Example:
        >>> test_all_required_owner_methods_are_concretely_composed(TextDatabaseBrowser, BrowserState)


    :param owner: Concrete browser or curses-driver composition class to inspect without constructing it.
    :param state: Corresponding ABC whose abstract-method names define the required surface.
    :return: None after abstractness and resolved descriptor flags are checked for all required methods.
    """
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
    """
    Check that a representative composed method still originates from its designated responsibility module.

    Example:
        >>> test_real_composition_resolves_methods_to_their_owners(TextDatabaseBrowser, "run", "browser_components.session")


    :param owner: Public browser or internal driver class whose inherited method is inspected.
    :param method: Method name resolved normally through the composition's MRO.
    :param module: Expected module suffix relative to LiuXin_alpha.surfaces.terminal.
    :return: None after the resolved method's __module__ matches its declared owner.
    """
    assert (
        getattr(owner, method).__module__ == f"LiuXin_alpha.surfaces.terminal.{module}"
    )


def test_complete_component_trees_enter_all_quality_scopes() -> None:
    """
    Check explicit terminal-tree entries in formatting, both typing scopes, lint/complexity commands, and strict model typing.

    This reads configuration and shell-command sections; it does not execute the
    quality tools or prove that every source file passes them.

    Example:
        >>> test_complete_component_trees_enter_all_quality_scopes()


    :return: None after all named root/tree paths and strict model entries are present in their required declarations.
    """
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
