"""Verify fresh-process import-footprint budgets and diagnostics."""

from __future__ import annotations

from scripts.check_import_footprint import (
    IMPORT_BUDGETS,
    ImportBudget,
    check_report,
    main,
)


def test_repository_cold_import_budgets_pass() -> None:
    """The checked-in reusable boundaries stay within their runtime budgets."""

    assert main([]) == 0


def test_plugin_namespace_boundaries_are_budgeted() -> None:
    """Keep generic plugin imports separate from plugin composition."""

    modules = {budget.module for budget in IMPORT_BUDGETS}

    assert "LiuXin_alpha.plugins.contracts" in modules
    assert "LiuXin_alpha.plugins.builtins" in modules
    assert "LiuXin_alpha.plugins.application" in modules
    assert "LiuXin_alpha.plugins.loading" in modules


def test_plugin_contract_boundaries_are_budgeted() -> None:
    """Keep reusable plugin contracts below composition and application domains."""
    budgets = {budget.module: budget for budget in IMPORT_BUDGETS}

    generic_budget = budgets["LiuXin_alpha.plugins.base"]
    conversion_budget = budgets["LiuXin_alpha.file_formats.conversion.plugin"]

    assert "customize" in generic_budget.forbidden_domains
    assert "file_formats" in generic_budget.forbidden_domains
    assert "customize" in conversion_budget.forbidden_domains
    assert "file_formats" not in conversion_budget.forbidden_domains
    assert generic_budget.max_project_modules == 30
    # Conversion configuration also imports the shared, resource-free path owner.
    assert conversion_budget.max_project_modules == 31


def test_metadata_utility_boundary_is_budgeted() -> None:
    """Keep generic metadata helpers independent of file-format implementations."""
    budgets = {budget.module: budget for budget in IMPORT_BUDGETS}

    utility_budget = budgets["LiuXin_alpha.metadata.utils"]
    assert "file_formats" in utility_budget.forbidden_domains
    assert utility_budget.max_project_modules == 30


def test_standalone_file_plugin_boundaries_are_budgeted() -> None:
    """Loading the runtime or its contracts cannot assemble application services."""
    budgets = {budget.module: budget for budget in IMPORT_BUDGETS}
    for module in (
        "plugins.file_types",
        "plugins.configuration",
        "file_formats.conversion.profiles",
        "file_formats.plugin_runtime",
        "file_formats.rendering",
    ):
        assert {"customize", "catalog", "core", "surfaces"} <= budgets[
            "LiuXin_alpha." + module
        ].forbidden_domains


def test_module_count_budget_reports_growth() -> None:
    """A footprint larger than its ceiling produces an actionable failure."""

    budget = ImportBudget("LiuXin_alpha.sample.api", 2)
    report = {
        "module": budget.module,
        "project_modules": [
            "LiuXin_alpha",
            "LiuXin_alpha.sample",
            "LiuXin_alpha.sample.api",
        ],
    }

    assert check_report(budget, report) == [
        "LiuXin_alpha.sample.api loaded 3 LiuXin modules; budget is 2"
    ]


def test_forbidden_domain_fails_below_numerical_budget() -> None:
    """Cross-domain execution cannot hide beneath a generous count ceiling."""

    budget = ImportBudget(
        "LiuXin_alpha.metadata.identifiers",
        100,
        frozenset({"catalog"}),
    )
    report = {
        "module": budget.module,
        "project_modules": [
            "LiuXin_alpha",
            "LiuXin_alpha.metadata",
            "LiuXin_alpha.metadata.identifiers",
            "LiuXin_alpha.catalog",
            "LiuXin_alpha.catalog.api",
        ],
    }

    errors = check_report(budget, report)

    assert len(errors) == 1
    assert "forbidden domains ['catalog']" in errors[0]
    assert "LiuXin_alpha.catalog.api" in errors[0]


def test_same_package_application_imports_are_rejected_below_count_budget() -> None:
    """A package merger must not hide host imports behind the plugins prefix."""
    budgets = {budget.module: budget for budget in IMPORT_BUDGETS}
    budget = budgets["LiuXin_alpha.file_formats.plugin_runtime"]
    report = {
        "module": budget.module,
        "project_modules": [
            budget.module,
            "LiuXin_alpha.plugins.builtins.registry",
        ],
    }
    errors = check_report(budget, report)
    assert len(errors) == 1
    assert "loaded forbidden modules" in errors[0]
    assert "plugins.builtins.registry" in errors[0]
