#!/usr/bin/env python3
"""Reject import cycles inside LiuXin's protected modern dependency seams."""

from __future__ import annotations

import argparse
import ast
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

PROTECTED_PREFIXES = (
    "LiuXin_alpha.catalog.api",
    "LiuXin_alpha.catalog.write",
    "LiuXin_alpha.metadata.api.containers_api.calibre_metadata_api",
)


def _module_name(source_root: Path, path: Path) -> str:
    relative = path.relative_to(source_root)
    parts = list(relative.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve_from(module_name: str, is_package: bool, node: ast.ImportFrom) -> str:
    if node.level == 0:
        return str(node.module or "")
    package_parts = module_name.split(".") if is_package else module_name.split(".")[:-1]
    keep = max(0, len(package_parts) - (node.level - 1))
    resolved = package_parts[:keep]
    if node.module:
        resolved.extend(node.module.split("."))
    return ".".join(resolved)


def build_graph(
    source_root: Path,
    *,
    protected_prefixes: Iterable[str] = PROTECTED_PREFIXES,
) -> dict[str, set[str]]:
    """Build imports among modules belonging to the protected prefixes."""

    prefixes = tuple(protected_prefixes)
    paths = tuple(sorted(source_root.rglob("*.py")))
    modules = {_module_name(source_root, path): path for path in paths}
    protected = {
        name
        for name in modules
        if any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes)
    }
    graph = {name: set() for name in protected}
    for name in sorted(protected):
        path = modules[name]
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        is_package = path.name == "__init__.py"
        for node in ast.walk(tree):
            candidates: list[str] = []
            if isinstance(node, ast.Import):
                candidates.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                base = _resolve_from(name, is_package, node)
                candidates.append(base)
                candidates.extend(
                    f"{base}.{alias.name}" if base else alias.name
                    for alias in node.names
                    if alias.name != "*"
                )
            else:
                continue
            for candidate in candidates:
                if candidate in protected and candidate != name:
                    graph[name].add(candidate)
    return graph


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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-root",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "src",
    )
    args = parser.parse_args(argv)
    graph = build_graph(args.source_root)
    cycles = strongly_connected_components(graph)
    if not cycles:
        print(
            f"Modern import-cycle check passed ({len(graph)} protected modules)."
        )
        return 0
    print("Modern import-cycle check failed:")
    for component in cycles:
        print("  - " + " -> ".join(component))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
