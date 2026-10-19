"""Verify the whole-package dependency-direction ratchet."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.check_layer_direction import (
    baseline_payload,
    check,
    direction_reason,
    main,
    measurements,
)
from scripts.check_modern_import_cycles import ImportKind, collect_imports


def _write(path: Path, source: str) -> None:
    """Write one UTF-8 fixture module."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_repository_matches_reviewed_layer_baseline() -> None:
    """The checked-in graph has no unreviewed debt or metric drift."""

    assert main([]) == 0


@pytest.mark.parametrize("context", tuple(ImportKind))
def test_production_callers_cannot_depend_on_the_legacy_plugin_ui(
    tmp_path: Path, context: ImportKind
) -> None:
    """Compatibility exports cannot grow back into the application owner."""
    statement = "from LiuXin_alpha.customize.ui import initialized_plugins\n"
    if context == ImportKind.DEFERRED:
        statement = "def load():\n    " + statement
    elif context == ImportKind.TYPE_ONLY:
        statement = (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    " + statement
        )
    _write(tmp_path / "LiuXin_alpha/customize/ui.py", "")
    _write(tmp_path / "LiuXin_alpha/core/service.py", statement)
    inventory = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))
    edge = next(
        edge for edge in inventory.edges if edge.target.endswith("customize.ui")
    )
    assert edge.kind == context
    assert direction_reason(edge) == (
        "production callers must use customize.application or file_formats.plugin_runtime"
    )


@pytest.mark.parametrize("context", tuple(ImportKind))
@pytest.mark.parametrize("source", ("metadata", "file_formats", "plugin_system"))
def test_new_backward_edge_fails_with_context(
    tmp_path: Path, context: ImportKind, source: str
) -> None:
    """Named rules cover import-time, deferred, and type-only edges."""

    statement = "from LiuXin_alpha.databases import row\n"
    if context == ImportKind.DEFERRED:
        statement = "def load():\n    " + statement
    elif context == ImportKind.TYPE_ONLY:
        statement = (
            "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    " + statement
        )
    owner = tmp_path / f"LiuXin_alpha/{source}/owner.py"
    _write(owner, "")
    _write(tmp_path / "LiuXin_alpha/databases/row.py", "")
    clean = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))
    baseline = baseline_payload(clean)

    _write(owner, statement)
    changed = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))
    errors = check(changed, baseline)

    assert any("new direction debt" in error for error in errors)
    edge = next(
        edge
        for edge in changed.edges
        if edge.target.startswith("LiuXin_alpha.databases")
    )
    assert edge.kind == context
    assert direction_reason(edge) == f"{source} must not depend on databases"


def test_removed_debt_and_reduced_measurement_require_baseline_refresh(
    tmp_path: Path,
) -> None:
    """Improvements cannot be silently reversed under an older, looser baseline."""

    owner = tmp_path / "LiuXin_alpha/metadata/owner.py"
    _write(owner, "from LiuXin_alpha.databases import row\n")
    _write(tmp_path / "LiuXin_alpha/databases/row.py", "")
    indebted = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))
    baseline = baseline_payload(indebted)

    _write(owner, "")
    reduced = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))
    errors = check(reduced, baseline)

    assert any("stale direction allowance" in error for error in errors)


def test_new_unresolved_dynamic_import_fails_exact_baseline(tmp_path: Path) -> None:
    """A variable dynamic import is visible debt rather than an omitted edge."""

    owner = tmp_path / "LiuXin_alpha/catalog/owner.py"
    _write(owner, "")
    clean = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))
    baseline = baseline_payload(clean)

    _write(owner, "def load(name):\n    return __import__(name)\n")
    changed = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))

    assert any(
        "new unresolved dynamic import" in error for error in check(changed, baseline)
    )


def test_catalog_cache_cycle_is_measured_across_the_whole_graph(
    tmp_path: Path,
) -> None:
    """A Catalog/cache SCC cannot hide outside the stricter protected-module scope."""
    _write(
        tmp_path / "LiuXin_alpha/catalog/owner.py",
        "from LiuXin_alpha.caches import owner\n",
    )
    _write(
        tmp_path / "LiuXin_alpha/caches/owner.py",
        "from LiuXin_alpha.catalog import owner\n",
    )

    inventory = collect_imports(tmp_path, protected_prefixes=("LiuXin_alpha",))

    assert measurements(inventory)["catalog_cache_runtime_component"] == 2
    assert measurements(inventory)["catalog_cache_combined_component"] == 2
