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


def test_customize_namespace_boundaries_are_budgeted() -> None:
    """Keep generic customization imports separate from plugin composition."""

    modules = {budget.module for budget in IMPORT_BUDGETS}

    assert "LiuXin_alpha.customize" in modules
    assert "LiuXin_alpha.customize.builtins" in modules


def test_plugin_contract_boundaries_are_budgeted() -> None:
    """Keep reusable plugin contracts below composition and application domains."""
    budgets = {budget.module: budget for budget in IMPORT_BUDGETS}

    generic_budget = budgets["LiuXin_alpha.plugin_system.base"]
    conversion_budget = budgets["LiuXin_alpha.file_formats.conversion.plugin"]

    assert "customize" in generic_budget.forbidden_domains
    assert "file_formats" in generic_budget.forbidden_domains
    assert "customize" in conversion_budget.forbidden_domains
    assert "file_formats" not in conversion_budget.forbidden_domains
    assert generic_budget.max_project_modules == 30
    assert conversion_budget.max_project_modules == 30


def test_metadata_utility_boundary_is_budgeted() -> None:
    """Keep generic metadata helpers independent of file-format implementations."""
    budgets = {budget.module: budget for budget in IMPORT_BUDGETS}

    utility_budget = budgets["LiuXin_alpha.metadata.utils"]
    assert "file_formats" in utility_budget.forbidden_domains
    assert utility_budget.max_project_modules == 30


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
