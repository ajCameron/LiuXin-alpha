#!/usr/bin/env python3
"""
Enforce protected-module dependency direction.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise check modern import cycles through a consuming regression::

        python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
"""

from __future__ import annotations

import argparse
import ast
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path

SHARED_SURFACE_PREFIXES = (
    "LiuXin_alpha.surfaces.api",
    "LiuXin_alpha.surfaces.core",
    "LiuXin_alpha.surfaces.acquisition",
    "LiuXin_alpha.surfaces.acquisition_types",
    "LiuXin_alpha.surfaces.catalog",
    "LiuXin_alpha.surfaces.images",
    "LiuXin_alpha.surfaces.opds",
    "LiuXin_alpha.surfaces.presentation",
    "LiuXin_alpha.surfaces.read_model",
    "LiuXin_alpha.surfaces.renderers",
)
WEB_APPLICATION_PREFIXES = (
    "LiuXin_alpha.surfaces.web_readonly",
    "LiuXin_alpha.surfaces.web_readwrite",
    "LiuXin_alpha.surfaces.web_calibre_readonly",
    "LiuXin_alpha.surfaces.api_readonly",
    "LiuXin_alpha.surfaces.opds_readonly",
)
CLI_PREFIX = "LiuXin_alpha.surfaces.cli"
CLI_ENTRY_POINTS = frozenset((CLI_PREFIX, f"{CLI_PREFIX}.__main__"))
CLI_COMPATIBILITY_TARGETS = frozenset((CLI_PREFIX, f"{CLI_PREFIX}.app"))
TERMINAL_PREFIX = "LiuXin_alpha.surfaces.terminal"
TERMINAL_ENTRY_POINTS = frozenset((TERMINAL_PREFIX, f"{TERMINAL_PREFIX}.__main__"))
TERMINAL_COMPATIBILITY_TARGETS = frozenset(
    (*TERMINAL_ENTRY_POINTS, f"{TERMINAL_PREFIX}.app")
)
TERMINAL_LEAVES = frozenset(
    f"{TERMINAL_PREFIX}.{owner}"
    for owner in ("presentation", "commands.base", "plugins.base")
)
TERMINAL_COMPONENT_ROOTS = (
    f"{TERMINAL_PREFIX}.browser_components",
    f"{TERMINAL_PREFIX}.windowed_components",
)
TERMINAL_COMPONENT_CONTRACTS = frozenset(
    f"{root}.{owner}"
    for root in TERMINAL_COMPONENT_ROOTS
    for owner in ("models", "contracts")
)
PROTECTED_PREFIXES = (
    "LiuXin_alpha.catalog.api",
    "LiuXin_alpha.catalog.write",
    "LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api",
    "LiuXin_alpha.caches.write",
    *SHARED_SURFACE_PREFIXES,
    *WEB_APPLICATION_PREFIXES,
    CLI_PREFIX,
    TERMINAL_PREFIX,
)


class ImportKind(StrEnum):
    """
    Execution context of an explicit import, not a runtime cycle verdict.

    Example:
        Exercise ImportKind through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    IMPORT_TIME = "import-time"
    DEFERRED = "deferred"
    TYPE_ONLY = "type-only"


@dataclass(frozen=True, order=True)
class ImportEdge:
    """
    An explicit dependency candidate with its source line and context.

    Example:
        Exercise ImportEdge through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    source: str
    target: str
    line: int
    kind: ImportKind


def _within(name: str, prefixes: Iterable[str]) -> bool:
    """
    Perform the within operation under explicit file-format and conversion rules.

    Example:
        Exercise  within through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param prefixes: Value supplied for prefixes under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes)


def _module_name(source_root: Path, path: Path) -> str:
    """
    Perform the module name operation under explicit file-format and conversion rules.

    Example:
        Exercise  module name through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param source_root: Value supplied for source root under the utility contract.
    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    relative = path.relative_to(source_root)
    parts = list(relative.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_from(module_name: str, is_package: bool, node: ast.ImportFrom) -> str:
    """
    Perform the resolve from operation under explicit file-format and conversion rules.

    Example:
        Exercise  resolve from through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param module_name: Value supplied for module name under the utility contract.
    :param is_package: Value supplied for is package under the utility contract.
    :param node: Value supplied for node under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if node.level == 0:
        return str(node.module or "")
    package_parts = (
        module_name.split(".") if is_package else module_name.split(".")[:-1]
    )
    keep = max(0, len(package_parts) - (node.level - 1))
    resolved = package_parts[:keep]
    if node.module:
        resolved.extend(node.module.split("."))
    return ".".join(resolved)


class _ImportCollector(ast.NodeVisitor):
    """
    Provide the importcollector contract for validated ebook processing.

    Example:
        Exercise  ImportCollector through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    def __init__(
        self, name: str, is_package: bool, tree: ast.AST, modules: Mapping[str, Path]
    ) -> None:
        """
        Initialize and validate the importcollector state.

        Example:
            Exercise  ImportCollector.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param is_package: Value supplied for is package under the utility contract.
        :param tree: Value supplied for tree under the utility contract.
        :param modules: Value supplied for modules under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.name = name
        self.is_package = is_package
        self.modules = modules
        self.kind = ImportKind.IMPORT_TIME
        self.edges: set[ImportEdge] = set()
        self.guard_names: set[str] = set()
        self.typing_names: set[str] = set()
        # Alias recognition is syntactic, not a general symbol resolver. Both
        # branches and every context remain in the enforced dependency graph.
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.ImportFrom)
                and node.module == "typing"
                and not node.level
            ):
                self.guard_names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name == "TYPE_CHECKING"
                )
            elif isinstance(node, ast.Import):
                self.typing_names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name == "typing"
                )

    def _guard(self, node: ast.expr) -> bool | None:
        """
        Perform the guard operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector. guard through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param node: Value supplied for node under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if isinstance(node, ast.Name) and node.id in self.guard_names:
            return True
        if (
            isinstance(node, ast.Attribute)
            and node.attr == "TYPE_CHECKING"
            and isinstance(node.value, ast.Name)
            and node.value.id in self.typing_names
        ):
            return True
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            guard = self._guard(node.operand)
            return None if guard is None else not guard
        return None

    def _visit_body(self, body: Iterable[ast.stmt], kind: ImportKind) -> None:
        """
        Perform the visit body operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector. visit body through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param body: Value supplied for body under the utility contract.
        :param kind: Value supplied for kind under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        previous = self.kind
        self.kind = ImportKind.TYPE_ONLY if previous == ImportKind.TYPE_ONLY else kind
        for child in body:
            self.visit(child)
        self.kind = previous

    def visit_If(self, node: ast.If) -> None:
        """
        Perform the visit If operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector.visit If through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        guard = self._guard(node.test)
        self._visit_body(
            node.body, ImportKind.TYPE_ONLY if guard is True else self.kind
        )
        self._visit_body(
            node.orelse, ImportKind.TYPE_ONLY if guard is False else self.kind
        )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """
        Perform the visit FunctionDef operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector.visit FunctionDef through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._visit_body(node.body, ImportKind.DEFERRED)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """
        Perform the visit AsyncFunctionDef operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector.visit AsyncFunctionDef through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self._visit_body(node.body, ImportKind.DEFERRED)

    def _add(self, target: str, line: int) -> None:
        """
        Perform the add operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector. add through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param target: Value supplied for target under the utility contract.
        :param line: Value supplied for line under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if target and target != self.name:
            self.edges.add(ImportEdge(self.name, target, line, self.kind))

    def visit_Import(self, node: ast.Import) -> None:
        """
        Perform the visit Import operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector.visit Import through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for alias in node.names:
            self._add(alias.name, node.lineno)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """
        Perform the visit ImportFrom operation under explicit file-format and conversion rules.

        Example:
            Exercise  ImportCollector.visit ImportFrom through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        base = _resolve_from(self.name, self.is_package, node)
        self._add(base, node.lineno)
        for alias in node.names:
            if alias.name != "*":
                candidate = f"{base}.{alias.name}" if base else alias.name
                if candidate in self.modules:
                    self._add(candidate, node.lineno)


@dataclass(frozen=True)
class ImportInventory:
    """
    Protected source modules and all their explicit dependency candidates.

    Example:
        Exercise ImportInventory through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    modules: Mapping[str, Path]
    edges: tuple[ImportEdge, ...]

    def graph(
        self, kinds: Iterable[ImportKind] = tuple(ImportKind)
    ) -> dict[str, set[str]]:
        """
        Project selected contexts onto known protected modules.

        Example:
            Exercise ImportInventory.graph through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param kinds: Value supplied for kinds under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        included = frozenset(kinds)
        graph: dict[str, set[str]] = {name: set() for name in self.modules}
        for edge in self.edges:
            if edge.kind in included and edge.target in graph:
                graph[edge.source].add(edge.target)
        return graph


def collect_imports(
    source_root: Path,
    *,
    protected_prefixes: Iterable[str] = PROTECTED_PREFIXES,
) -> ImportInventory:
    """
    Classify explicit imports, including both branches and deferred bodies.

    Example:
        Exercise collect imports through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param source_root: Value supplied for source root under the utility contract.
    :param protected_prefixes: Value supplied for protected prefixes under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    prefixes = tuple(protected_prefixes)
    all_modules = {
        _module_name(source_root, path): path
        for path in sorted(source_root.rglob("*.py"))
    }
    modules = {
        name: path for name, path in all_modules.items() if _within(name, prefixes)
    }
    edges: set[ImportEdge] = set()
    for name, path in modules.items():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        collector = _ImportCollector(
            name, path.name == "__init__.py", tree, all_modules
        )
        collector.visit(tree)
        edges.update(collector.edges)
    return ImportInventory(modules, tuple(sorted(edges)))


def build_graph(
    source_root: Path,
    *,
    protected_prefixes: Iterable[str] = PROTECTED_PREFIXES,
) -> dict[str, set[str]]:
    """
    Build the combined graph; type-only and deferred imports remain protected.

    Example:
        Exercise build graph through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param source_root: Value supplied for source root under the utility contract.
    :param protected_prefixes: Value supplied for protected prefixes under the utility
        contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return collect_imports(source_root, protected_prefixes=protected_prefixes).graph()


def _forbidden_terminal_dependency(edge: ImportEdge) -> str | None:
    """
    Keep terminal composition above its reusable implementations and contracts.

    Example:
        Exercise  forbidden terminal dependency through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param edge: Value supplied for edge under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if _within(edge.source, TERMINAL_COMPONENT_ROOTS) and edge.target in {
        f"{TERMINAL_PREFIX}.browser",
        f"{TERMINAL_PREFIX}.windowed_ui",
    }:
        return "terminal components must not import concrete composition roots"
    if _within(edge.source, (TERMINAL_COMPONENT_ROOTS[0],)) and _within(
        edge.target, (TERMINAL_COMPONENT_ROOTS[1],)
    ):
        return "terminal browser components must not depend on curses components"
    if (
        edge.source in TERMINAL_COMPONENT_CONTRACTS
        and _within(edge.target, TERMINAL_COMPONENT_ROOTS)
        and edge.target not in TERMINAL_COMPONENT_CONTRACTS
    ):
        return "terminal contracts must not depend on component implementations"
    if (
        _within(edge.source, (TERMINAL_PREFIX,))
        and edge.source not in TERMINAL_ENTRY_POINTS
        and edge.target in TERMINAL_COMPATIBILITY_TARGETS
    ):
        return "terminal implementations must not import application entry points"
    if (
        edge.source == f"{TERMINAL_PREFIX}.browser"
        and edge.target == f"{TERMINAL_PREFIX}.windowed_ui"
    ):
        return "terminal browser execution must not select or import its curses adapter"
    if edge.source in TERMINAL_LEAVES and _within(edge.target, ("LiuXin_alpha",)):
        return "terminal presentation and extension APIs must remain independent leaves"
    return None


def forbidden_dependency(edge: ImportEdge) -> str | None:
    """
    Reject backward ownership even when it does not close a cycle.

    Example:
        Exercise forbidden dependency through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param edge: Value supplied for edge under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    terminal_error = _forbidden_terminal_dependency(edge)
    if terminal_error is not None:
        return terminal_error
    if (
        _within(edge.source, (CLI_PREFIX,))
        and edge.source not in CLI_ENTRY_POINTS
        and edge.target in CLI_COMPATIBILITY_TARGETS
    ):
        return "CLI implementations must not import application entry points"
    if (
        edge.source == f"{CLI_PREFIX}.parsers"
        and edge.target == f"{CLI_PREFIX}.completion"
    ):
        return "CLI parser construction must receive completion registration, not import its command"
    if edge.source == f"{CLI_PREFIX}.parser_types" and _within(
        edge.target, ("LiuXin_alpha",)
    ):
        return "CLI parser contracts must remain independent leaves"
    if (
        edge.source.startswith("LiuXin_alpha.caches.write.")
        and edge.target == "LiuXin_alpha.caches.write"
    ):
        return "cache writers must import implementation owners, not their assembling package"
    if _within(edge.source, SHARED_SURFACE_PREFIXES) and _within(
        edge.target, WEB_APPLICATION_PREFIXES
    ):
        return "shared surface backends must not depend on web applications"
    if edge.source in {
        "LiuXin_alpha.surfaces.presentation",
        "LiuXin_alpha.surfaces.acquisition_types",
    } and _within(edge.target, ("LiuXin_alpha",)):
        return (
            "shared presentation and acquisition types must remain independent leaves"
        )
    return None


@dataclass
class _ComponentSearch:
    """
    Provide the componentsearch contract for validated ebook processing.

    Example:
        Exercise  ComponentSearch through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py
    """

    graph: Mapping[str, set[str]]
    index: int = 0
    indices: dict[str, int] = field(default_factory=dict)
    lowlinks: dict[str, int] = field(default_factory=dict)
    stack: list[str] = field(default_factory=list)
    active: set[str] = field(default_factory=set)
    components: list[tuple[str, ...]] = field(default_factory=list)

    def visit(self, node: str) -> None:
        """
        Perform the visit operation under explicit file-format and conversion rules.

        Example:
            Exercise  ComponentSearch.visit through a consuming regression::

                python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


        :param node: Value supplied for node under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.indices[node] = self.index
        self.lowlinks[node] = self.index
        self.index += 1
        self.stack.append(node)
        self.active.add(node)
        for target in sorted(self.graph.get(node, ())):
            if target not in self.indices:
                self.visit(target)
                self.lowlinks[node] = min(self.lowlinks[node], self.lowlinks[target])
            elif target in self.active:
                self.lowlinks[node] = min(self.lowlinks[node], self.indices[target])
        if self.lowlinks[node] != self.indices[node]:
            return
        component: list[str] = []
        while self.stack:
            member = self.stack.pop()
            self.active.remove(member)
            component.append(member)
            if member == node:
                break
        if len(component) > 1:
            self.components.append(tuple(sorted(component)))


def strongly_connected_components(
    graph: Mapping[str, set[str]],
) -> tuple[tuple[str, ...], ...]:
    """
    Return multi-module strongly connected components in stable order.

    Example:
        Exercise strongly connected components through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param graph: Value supplied for graph under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """

    search = _ComponentSearch(graph)
    for node in sorted(graph):
        if node not in search.indices:
            search.visit(node)
    return tuple(sorted(search.components))


def main(argv: list[str] | None = None) -> int:
    """
    Enforce the combined graph and explain the contexts of failing edges.

    Example:
        Exercise main through a consuming regression::

            python -m pytest -q tests/scripts/test_ci_workflow_contracts.py


    :param argv: Value supplied for argv under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "src",
    )
    args = parser.parse_args(argv)
    inventory = collect_imports(args.source_root)
    if not inventory.modules:
        print(
            f"Modern import-cycle check failed: no protected modules under {args.source_root}."
        )
        return 1
    graph = inventory.graph()
    cycles = strongly_connected_components(graph)
    violations = [
        (edge, reason)
        for edge in inventory.edges
        if (reason := forbidden_dependency(edge))
    ]
    if not cycles and not violations:
        print(
            f"Modern import-cycle check passed ({len(graph)} protected modules; "
            "import-time, deferred, and type-only dependencies; direction rules enforced)."
        )
        return 0
    print("Modern import-cycle check failed:")
    for component in cycles:
        # A sorted SCC is not necessarily a traversal path. Print actual edges
        # instead of joining its members with misleading cycle arrows.
        print("  Dependency component: " + ", ".join(component))
        for edge in inventory.edges:
            if edge.source in component and edge.target in component:
                print(
                    f"    {inventory.modules[edge.source]}:{edge.line} [{edge.kind}] -> {edge.target}"
                )
    for edge, reason in violations:
        print(
            f"  {inventory.modules[edge.source]}:{edge.line} [{edge.kind}] -> {edge.target}: {reason}"
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
