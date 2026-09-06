#!/usr/bin/env python3
"""Reject import cycles inside LiuXin's protected modern dependency seams."""

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
PROTECTED_PREFIXES = (
    "LiuXin_alpha.catalog.api",
    "LiuXin_alpha.catalog.write",
    "LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api",
    "LiuXin_alpha.caches.write",
    *SHARED_SURFACE_PREFIXES,
    *WEB_APPLICATION_PREFIXES,
)


class ImportKind(StrEnum):
    """Execution context of an explicit import, not a runtime cycle verdict."""

    IMPORT_TIME = "import-time"
    DEFERRED = "deferred"
    TYPE_ONLY = "type-only"


@dataclass(frozen=True, order=True)
class ImportEdge:
    """An explicit dependency candidate with its source line and context."""

    source: str
    target: str
    line: int
    kind: ImportKind


def _within(name: str, prefixes: Iterable[str]) -> bool:
    return any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes)


def _module_name(source_root: Path, path: Path) -> str:
    relative = path.relative_to(source_root)
    parts = list(relative.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_from(module_name: str, is_package: bool, node: ast.ImportFrom) -> str:
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
    def __init__(
        self, name: str, is_package: bool, tree: ast.AST, modules: Mapping[str, Path]
    ) -> None:
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
        previous = self.kind
        self.kind = ImportKind.TYPE_ONLY if previous == ImportKind.TYPE_ONLY else kind
        for child in body:
            self.visit(child)
        self.kind = previous

    def visit_If(self, node: ast.If) -> None:
        guard = self._guard(node.test)
        self._visit_body(
            node.body, ImportKind.TYPE_ONLY if guard is True else self.kind
        )
        self._visit_body(
            node.orelse, ImportKind.TYPE_ONLY if guard is False else self.kind
        )

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_body(node.body, ImportKind.DEFERRED)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_body(node.body, ImportKind.DEFERRED)

    def _add(self, target: str, line: int) -> None:
        if target and target != self.name:
            self.edges.add(ImportEdge(self.name, target, line, self.kind))

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self._add(alias.name, node.lineno)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        base = _resolve_from(self.name, self.is_package, node)
        self._add(base, node.lineno)
        for alias in node.names:
            if alias.name != "*":
                candidate = f"{base}.{alias.name}" if base else alias.name
                if candidate in self.modules:
                    self._add(candidate, node.lineno)


@dataclass(frozen=True)
class ImportInventory:
    """Protected source modules and all their explicit dependency candidates."""

    modules: Mapping[str, Path]
    edges: tuple[ImportEdge, ...]

    def graph(
        self, kinds: Iterable[ImportKind] = tuple(ImportKind)
    ) -> dict[str, set[str]]:
        """Project selected contexts onto known protected modules."""
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
    """Classify explicit imports, including both branches and deferred bodies.

    Dynamic imports and implicit parent-package initialization are not modeled.
    TYPE_CHECKING recognition covers direct/negated names and typing aliases;
    unfamiliar conditions remain conservatively runtime dependencies.
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
    """Build the combined graph; type-only and deferred imports remain protected."""
    return collect_imports(source_root, protected_prefixes=protected_prefixes).graph()


def forbidden_dependency(edge: ImportEdge) -> str | None:
    """Reject backward ownership even when it does not close a cycle."""
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
    graph: Mapping[str, set[str]]
    index: int = 0
    indices: dict[str, int] = field(default_factory=dict)
    lowlinks: dict[str, int] = field(default_factory=dict)
    stack: list[str] = field(default_factory=list)
    active: set[str] = field(default_factory=set)
    components: list[tuple[str, ...]] = field(default_factory=list)

    def visit(self, node: str) -> None:
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
    """Return multi-module strongly connected components in stable order."""

    search = _ComponentSearch(graph)
    for node in sorted(graph):
        if node not in search.indices:
            search.visit(node)
    return tuple(sorted(search.components))


def main(argv: list[str] | None = None) -> int:
    """Enforce the combined graph and explain the contexts of failing edges."""
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
