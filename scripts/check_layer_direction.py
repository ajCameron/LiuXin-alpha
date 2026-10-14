#!/usr/bin/env python3
"""Ratchet whole-package dependency direction without hiding existing debt.

The checker scans every LiuXin Python module, applies named package-direction
rules, and compares existing backward edges, unresolved dynamic imports, and
cycle measurements with a reviewed baseline. It is deterministic and read-only;
``--print-baseline`` emits candidate JSON to stdout for explicit review.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from scripts.check_modern_import_cycles import (
        DynamicImport,
        ImportEdge,
        ImportInventory,
        ImportKind,
        collect_imports,
        strongly_connected_components,
    )
except ModuleNotFoundError:  # Direct ``python scripts/...`` execution.
    from check_modern_import_cycles import (  # type: ignore[no-redef]
        DynamicImport,
        ImportEdge,
        ImportInventory,
        ImportKind,
        collect_imports,
        strongly_connected_components,
    )

PACKAGE_ROOT = "LiuXin_alpha"
DOMAIN_PACKAGES = frozenset({"caches", "catalog", "databases", "metadata", "surfaces"})
RUNTIME_KINDS = (ImportKind.IMPORT_TIME, ImportKind.DEFERRED)

# Cache direction remains deliberately undecided. Do not add a cache source rule
# until its architectural role has been agreed explicitly.
FORBIDDEN_TARGETS: Mapping[str, frozenset[str]] = {
    "file_formats": frozenset(
        {
            "caches",
            "catalog",
            "core",
            "customize",
            "databases",
            "ingest",
            "jobs",
            "library",
            "storage",
            "surfaces",
        }
    ),
    "plugin_system": frozenset(
        {
            "caches",
            "catalog",
            "core",
            "customize",
            "databases",
            "file_formats",
            "ingest",
            "jobs",
            "library",
            "metadata",
            "storage",
            "surfaces",
        }
    ),
    "metadata": frozenset(
        {"caches", "catalog", "core", "databases", "library", "storage", "surfaces"}
    ),
    "databases": frozenset(
        {"caches", "catalog", "core", "library", "storage", "surfaces"}
    ),
    "storage": frozenset(
        {"caches", "catalog", "core", "library", "metadata", "surfaces"}
    ),
    "catalog": frozenset({"core", "library", "storage", "surfaces"}),
    "library": frozenset({"core", "surfaces"}),
    "core": frozenset({"surfaces"}),
}


@dataclass(frozen=True, order=True)
class DirectionDebt:
    """Stable baseline identity for one known backward dependency edge."""

    source: str
    target: str
    kind: str


def _package(module: str) -> str | None:
    """Return the first package below LiuXin_alpha, if present."""

    parts = module.split(".")
    if len(parts) < 2 or parts[0] != PACKAGE_ROOT:
        return None
    return parts[1]


def direction_reason(edge: ImportEdge) -> str | None:
    """Explain a named backward package edge, or return None when allowed."""

    source_package = _package(edge.source)
    target_package = _package(edge.target)
    if source_package is None or target_package is None:
        return None
    if target_package in FORBIDDEN_TARGETS.get(source_package, ()):
        return f"{source_package} must not depend on {target_package}"
    return None


def _largest_component(
    graph: Mapping[str, set[str]], *, packages: Iterable[str] | None = None
) -> tuple[str, ...]:
    """Return the stable largest multi-module SCC for the selected packages."""

    selected = set(graph)
    if packages is not None:
        accepted = frozenset(packages)
        selected = {name for name in graph if _package(name) in accepted}
    projected = {
        source: {target for target in graph[source] if target in selected}
        for source in selected
    }
    components = strongly_connected_components(projected)
    return max(components, key=lambda value: (len(value), value), default=())


def _largest_cross_package_component(
    graph: Mapping[str, set[str]], *, required_packages: Iterable[str]
) -> tuple[str, ...]:
    """Return the largest SCC containing every requested top-level package."""
    required = frozenset(required_packages)
    candidates = (
        component
        for component in strongly_connected_components(graph)
        if required <= {_package(module) for module in component}
    )
    return max(candidates, key=lambda value: (len(value), value), default=())


def measurements(inventory: ImportInventory) -> dict[str, int]:
    """Measure the domain, whole-runtime, and whole-combined SCC ceilings."""

    runtime_graph = inventory.graph(RUNTIME_KINDS)
    combined_graph = inventory.graph()
    return {
        "domain_component": len(
            _largest_component(runtime_graph, packages=DOMAIN_PACKAGES)
        ),
        "catalog_cache_runtime_component": len(
            _largest_cross_package_component(
                runtime_graph,
                required_packages=("catalog", "caches"),
            )
        ),
        "catalog_cache_combined_component": len(
            _largest_cross_package_component(
                combined_graph,
                required_packages=("catalog", "caches"),
            )
        ),
        "whole_runtime_component": len(_largest_component(runtime_graph)),
        "whole_combined_component": len(_largest_component(combined_graph)),
    }


def _direction_debt(inventory: ImportInventory) -> tuple[DirectionDebt, ...]:
    """Return exact identities of all current named-direction violations."""

    return tuple(
        sorted(
            {
                DirectionDebt(edge.source, edge.target, edge.kind.value)
                for edge in inventory.edges
                if direction_reason(edge) is not None
            }
        )
    )


def _dynamic_record(entry: DynamicImport) -> dict[str, Any]:
    """Serialize an unresolved dynamic import without losing its source context."""

    return {
        "source": entry.source,
        "line": entry.line,
        "kind": entry.kind.value,
        "expression": entry.expression,
    }


def baseline_payload(inventory: ImportInventory) -> dict[str, Any]:
    """Build deterministic candidate baseline data for explicit human review."""

    return {
        "version": 1,
        "direction_edges": [
            {"source": edge.source, "target": edge.target, "kind": edge.kind}
            for edge in _direction_debt(inventory)
        ],
        "unresolved_dynamic": [
            _dynamic_record(entry) for entry in inventory.unresolved_dynamic
        ],
        "measurements": measurements(inventory),
    }


def _load_baseline(path: Path) -> dict[str, Any]:
    """Load and minimally validate the reviewed baseline document."""

    with path.open(encoding="utf-8") as baseline_file:
        payload = json.load(baseline_file)
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise ValueError(f"unsupported layer-direction baseline: {path}")
    return payload


def _debt_from_payload(payload: Mapping[str, Any]) -> tuple[DirectionDebt, ...]:
    """Normalize baseline direction entries for exact set comparison."""

    return tuple(
        sorted(
            DirectionDebt(str(item["source"]), str(item["target"]), str(item["kind"]))
            for item in payload.get("direction_edges", ())
        )
    )


def _dynamic_from_payload(payload: Mapping[str, Any]) -> tuple[tuple[Any, ...], ...]:
    """Normalize baseline unresolved calls for exact comparison."""

    return tuple(
        sorted(
            (
                str(item["source"]),
                int(item["line"]),
                str(item["kind"]),
                str(item["expression"]),
            )
            for item in payload.get("unresolved_dynamic", ())
        )
    )


def _current_dynamic(inventory: ImportInventory) -> tuple[tuple[Any, ...], ...]:
    """Normalize scanned unresolved calls for exact comparison."""

    return tuple(
        (
            item.source,
            item.line,
            item.kind.value,
            item.expression,
        )
        for item in inventory.unresolved_dynamic
    )


def _edge_lookup(inventory: ImportInventory) -> dict[DirectionDebt, list[ImportEdge]]:
    """Index concrete edges so diagnostics retain file paths and line numbers."""

    result: dict[DirectionDebt, list[ImportEdge]] = {}
    for edge in inventory.edges:
        debt = DirectionDebt(edge.source, edge.target, edge.kind.value)
        result.setdefault(debt, []).append(edge)
    return result


def check(inventory: ImportInventory, baseline: Mapping[str, Any]) -> list[str]:
    """Return actionable deviations from the exact reviewed baseline."""

    errors: list[str] = []
    current_debt = _direction_debt(inventory)
    expected_debt = _debt_from_payload(baseline)
    edge_lookup = _edge_lookup(inventory)
    for debt in sorted(set(current_debt) - set(expected_debt)):
        edges = edge_lookup.get(debt, ())
        reason = direction_reason(edges[0]) if edges else "backward dependency"
        for edge in edges or (None,):
            location = debt.source
            if edge is not None:
                location = f"{inventory.modules[edge.source]}:{edge.line}"
            errors.append(
                f"new direction debt: {location} [{debt.kind}] -> {debt.target}: {reason}"
            )
    for debt in sorted(set(expected_debt) - set(current_debt)):
        errors.append(
            "stale direction allowance (record the reduction): "
            f"{debt.source} [{debt.kind}] -> {debt.target}"
        )

    current_dynamic = _current_dynamic(inventory)
    expected_dynamic = _dynamic_from_payload(baseline)
    for entry in sorted(set(current_dynamic) - set(expected_dynamic)):
        source, line, kind, expression = entry
        path = inventory.modules.get(source, Path(source))
        errors.append(
            f"new unresolved dynamic import: {path}:{line} [{kind}] {expression}"
        )
    for entry in sorted(set(expected_dynamic) - set(current_dynamic)):
        source, line, kind, expression = entry
        errors.append(
            "stale unresolved-dynamic allowance (record the reduction): "
            f"{source}:{line} [{kind}] {expression}"
        )

    current_measurements = measurements(inventory)
    expected_measurements = baseline.get("measurements", {})
    for name, value in current_measurements.items():
        expected = expected_measurements.get(name)
        if value == expected:
            continue
        direction = (
            "grew" if isinstance(expected, int) and value > expected else "changed"
        )
        errors.append(
            f"measurement {name} {direction}: baseline={expected!r}, current={value}; "
            "review the graph and update the baseline only for an intentional reduction"
        )
    return errors


def main(argv: list[str] | None = None) -> int:
    """Run the whole-package ratchet or print a candidate reviewed baseline."""

    repo_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=repo_root / "src")
    parser.add_argument(
        "--baseline",
        type=Path,
        default=repo_root / "scripts" / "layer_direction_baseline.json",
    )
    parser.add_argument("--print-baseline", action="store_true")
    args = parser.parse_args(argv)

    inventory = collect_imports(args.source_root, protected_prefixes=(PACKAGE_ROOT,))
    if not inventory.modules:
        print(
            f"Layer-direction check failed: no LiuXin modules under {args.source_root}."
        )
        return 1
    if args.print_baseline:
        print(json.dumps(baseline_payload(inventory), indent=2, sort_keys=True))
        return 0
    try:
        baseline = _load_baseline(args.baseline)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Layer-direction check failed: {exc}")
        return 1
    errors = check(inventory, baseline)
    if errors:
        print("Layer-direction check failed:")
        for error in errors:
            print(f"  {error}")
        return 1
    print(
        "Layer-direction check passed "
        f"({len(inventory.modules)} modules; "
        f"{len(_direction_debt(inventory))} reviewed backward edges; "
        f"{len(inventory.unresolved_dynamic)} unresolved dynamic imports)."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
