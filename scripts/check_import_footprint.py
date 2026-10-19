#!/usr/bin/env python3
"""Enforce cold-process import budgets for reusable package boundaries.

Each public boundary is imported in its own interpreter. The checker limits the
number of newly loaded LiuXin modules and rejects named cross-domain imports even
when the numerical budget still has room. This complements the static layer ratchet:
it measures what Python actually executes while resolving package initializers.
"""

from __future__ import annotations

import argparse
import importlib
import json
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    from scripts.check_layer_direction import PLUGIN_HOST_PREFIXES
except ModuleNotFoundError:
    from check_layer_direction import PLUGIN_HOST_PREFIXES  # type: ignore[no-redef]

PACKAGE_ROOT = "LiuXin_alpha"
REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src"


@dataclass(frozen=True)
class ImportBudget:
    """Describe one cold-import ceiling and its forbidden project domains."""

    module: str
    max_project_modules: int
    forbidden_domains: frozenset[str] = frozenset()
    forbidden_modules: tuple[str, ...] = ()


ISOLATED_DOMAINS = frozenset(
    {"caches", "catalog", "core", "file_formats", "library", "metadata", "storage"}
)
PLUGIN_NAMESPACE_FORBIDDEN_DOMAINS = frozenset(
    {
        "caches",
        "catalog",
        "core",
        "databases",
        "file_formats",
        "ingest",
        "jobs",
        "library",
        "metadata",
        "storage",
        "surfaces",
    }
)
PLUGIN_CONTRACT_FORBIDDEN_DOMAINS = frozenset(
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
)
CONVERSION_CONTRACT_FORBIDDEN_DOMAINS = PLUGIN_CONTRACT_FORBIDDEN_DOMAINS - {
    "file_formats"
}

IMPORT_BUDGETS = (
    ImportBudget("LiuXin_alpha", 1),
    # Pure configuration leaves add one module to constants consumers; constrain
    # the leaves themselves rather than allowing eager preference/store loading.
    ImportBudget("LiuXin_alpha.configuration", 2, ISOLATED_DOMAINS),
    ImportBudget("LiuXin_alpha.configuration_defaults", 2, ISOLATED_DOMAINS),
    ImportBudget(
        "LiuXin_alpha.plugin_system",
        2,
        PLUGIN_NAMESPACE_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.plugin_system.base",
        30,
        PLUGIN_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.plugin_system.file_types",
        30,
        PLUGIN_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.plugin_system.configuration",
        31,
        PLUGIN_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.file_formats.conversion.profiles",
        31,
        CONVERSION_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.file_formats.plugin_runtime",
        37,
        CONVERSION_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.file_formats.rendering",
        16,
        CONVERSION_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.file_formats.conversion.plugin",
        31,
        CONVERSION_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget(
        "LiuXin_alpha.plugin_system.contracts",
        35,
        PLUGIN_NAMESPACE_FORBIDDEN_DOMAINS,
    ),
    ImportBudget(
        "LiuXin_alpha.plugin_system.builtins",
        3,
        PLUGIN_NAMESPACE_FORBIDDEN_DOMAINS,
    ),
    ImportBudget(
        "LiuXin_alpha.plugin_system.application",
        44,
        PLUGIN_NAMESPACE_FORBIDDEN_DOMAINS - {"file_formats"},
    ),
    ImportBudget(
        "LiuXin_alpha.plugin_system.loading",
        30,
        PLUGIN_CONTRACT_FORBIDDEN_DOMAINS,
        PLUGIN_HOST_PREFIXES,
    ),
    ImportBudget("LiuXin_alpha.metadata", 2),
    ImportBudget(
        "LiuXin_alpha.metadata.identifiers",
        25,
        ISOLATED_DOMAINS - {"metadata"},
    ),
    ImportBudget(
        "LiuXin_alpha.metadata.utils",
        30,
        (ISOLATED_DOMAINS - {"metadata"}) | {"databases", "surfaces"},
    ),
    ImportBudget(
        "LiuXin_alpha.metadata.facade",
        25,
        ISOLATED_DOMAINS - {"metadata"},
    ),
    ImportBudget("LiuXin_alpha.catalog", 2),
    ImportBudget(
        "LiuXin_alpha.catalog.api",
        70,
        ISOLATED_DOMAINS - {"catalog"},
    ),
    ImportBudget("LiuXin_alpha.caches", 2),
    ImportBudget(
        "LiuXin_alpha.caches.api",
        80,
        ISOLATED_DOMAINS - {"caches"},
    ),
    ImportBudget("LiuXin_alpha.storage", 2),
    ImportBudget(
        "LiuXin_alpha.storage.api",
        80,
        ISOLATED_DOMAINS - {"storage"},
    ),
)


def _domain(module: str) -> str | None:
    """Return the first package below LiuXin_alpha for one module name."""

    parts = module.split(".")
    if len(parts) < 2 or parts[0] != PACKAGE_ROOT:
        return None
    return parts[1]


def probe(module: str) -> dict[str, Any]:
    """Import one module and report newly loaded project modules."""

    source = str(SOURCE_ROOT)
    if source not in sys.path:
        sys.path.insert(0, source)
    before = set(sys.modules)
    importlib.import_module(module)
    loaded = sorted(
        name
        for name in set(sys.modules) - before
        if name == PACKAGE_ROOT or name.startswith(PACKAGE_ROOT + ".")
    )
    return {"module": module, "project_modules": loaded}


def run_probe(module: str) -> dict[str, Any]:
    """Run one probe in a fresh interpreter and decode its JSON report."""

    completed = subprocess.run(
        [sys.executable, str(Path(__file__).resolve()), "--probe", module],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(f"cold import failed for {module}: {detail}")
    return dict(json.loads(completed.stdout))


def check_report(budget: ImportBudget, report: dict[str, Any]) -> list[str]:
    """Return stable budget and domain violations for one probe report."""

    loaded = tuple(str(name) for name in report["project_modules"])
    errors: list[str] = []
    if len(loaded) > budget.max_project_modules:
        errors.append(
            f"{budget.module} loaded {len(loaded)} LiuXin modules; "
            f"budget is {budget.max_project_modules}"
        )
    forbidden = sorted(
        name for name in loaded if _domain(name) in budget.forbidden_domains
    )
    if forbidden:
        domains = sorted(
            {domain for name in forbidden if (domain := _domain(name)) is not None}
        )
        errors.append(
            f"{budget.module} crossed forbidden domains {domains}: "
            + ", ".join(forbidden)
        )
    forbidden_modules = [
        name
        for name in loaded
        if any(
            name == prefix or name.startswith(prefix + ".")
            for prefix in budget.forbidden_modules
        )
    ]
    if forbidden_modules:
        errors.append(
            f"{budget.module} loaded forbidden modules: " + ", ".join(forbidden_modules)
        )
    return errors


def check_all(budgets: Sequence[ImportBudget] = IMPORT_BUDGETS) -> list[str]:
    """Probe every configured boundary and return all violations."""

    errors: list[str] = []
    for budget in budgets:
        try:
            report = run_probe(budget.module)
        except (RuntimeError, json.JSONDecodeError) as exc:
            errors.append(str(exc))
            continue
        errors.extend(check_report(budget, report))
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    """Run a child probe or enforce all repository import budgets."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--probe", metavar="MODULE")
    args = parser.parse_args(argv)
    if args.probe:
        print(json.dumps(probe(args.probe), sort_keys=True))
        return 0

    errors = check_all()
    if errors:
        for error in errors:
            print(f"import-footprint: {error}", file=sys.stderr)
        return 1
    print(f"import-footprint: {len(IMPORT_BUDGETS)} cold-import budgets passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
