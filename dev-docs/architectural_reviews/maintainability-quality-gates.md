# Maintainability quality gates

Reviewed **2026-10-20**, against the working tree based on `fc5de7af`.
Status: **enforced, scoped ratchets; inherited debt remains outside some gates**.

This guide describes the local quality runner and its relationship to CI. The
[CI guide](../hacking/testing/continuous-integration.md) owns workflow scheduling;
the [ownership guide](<../04 - Seperation of Concerns.md>) owns architectural policy.
Configuration and scripts, linked below, are the authority for exact file lists.
A green scoped gate is not a whole-project typing, behavioural or security claim.

## Purpose

The modern application spine has a zero-error gate that can grow as inherited
code is repaired. Basedpyright uses standard mode for selected orchestration and
strict mode for named leaves; mypy uses strict checking for its selected files,
with `follow_imports = "skip"` for other imported implementation. These are
separate scopes. Formatting, lint, complexity, typing and dependency coverage
must not be inferred from one another.

Run the configured checks from the repository root:

```bash
bash scripts/run_type_checks.sh
```

The runner is offline by default and checks source without rewriting it. Type
checkers may write caches. `--install` explicitly installs the typing extra;
`--create-venv` and `--new-venv` are environment setup options, not prerequisites
for every invocation. Use a prepared environment for routine checks.

## Runner order and scope

[`run_type_checks.sh`](../../scripts/run_type_checks.sh) executes these stages:

| Order | Stage | Authoritative scope / behaviour |
| --- | --- | --- |
| 1 | Ruff formatting check | `[tool.liuxin.format].paths` in [pyproject.toml](../../pyproject.toml), expanded by [run_format_checks.py](../../scripts/run_format_checks.py). |
| 2 | File-format callable annotations | [annotate_file_formats.py](../../scripts/annotate_file_formats.py) in `--check` mode. Annotation presence does not establish type correctness. |
| 3 | Protected dependency graph | [check_modern_import_cycles.py](../../scripts/check_modern_import_cycles.py): combined-graph cycles and named directions. |
| 4 | Whole-package layer debt | [check_layer_direction.py](../../scripts/check_layer_direction.py) against its reviewed baseline. |
| 5 | Fresh-process imports | [check_import_footprint.py](../../scripts/check_import_footprint.py): module counts and forbidden dependencies. |
| 6 | Ruff lint | Explicit paths in the shell runner; not the formatter or type-checker list. |
| 7 | Modern orchestration complexity | Separate Ruff C901 pass, maximum 10. |
| 8 | Composed storage-manager complexity | Manager and implementation mixins, maximum 15. This legacy ceiling is a ratchet, not a target for new functions. |
| 9 | Basedpyright and internal contracts | Production check first; contract fixtures run only if that check succeeds. |
| 10 | Mypy and internal contracts | Same production-then-fixture rule, with its own configured scope. |

Common stages stop on failure. The selected type-checker results are collected
for the final status. `--basedpyright` or `--mypy` selects a checker but still runs
all common stages. Additional checker arguments require selecting one checker.
`--dry-run` prints commands and does not execute the gates.

Expanded scopes measured on **2026-10-20**:

| Measurement | Count | Limit of the measurement |
| --- | ---: | --- |
| Formatter files | 176 | Python files after directory expansion and deduplication. |
| Mypy files | 191 | Selected production files, not all transitively imported code. |
| Basedpyright include files | 202 | Selected files; the checker may analyse dependencies too. |
| Basedpyright strict files | 27 | Strict subset, not an additional 27 files. |
| Protected graph modules | 243 | Zero-cycle scope; whole-package graph measured separately. |
| Whole-package source modules | 1,792 | Named direction rules, with 15 reviewed backward edges and 28 unresolved dynamic imports. |
| Cold-import budgets | 25 | Independent fresh-process boundaries. |

Counts change when directory scopes gain files. Read the configuration before
using a number as an acceptance criterion. The October 20 documentation review
ran the three architecture checkers and inspected the runner with `--dry-run`;
it did **not** rerun the full formatter/lint/type gate.

## Ratchet rules

1. Keep production checks at zero errors. Do not replace them with an error-count
   allowance or blanket suppressions.
2. Introduce new leaf protocols and application services in the appropriate
   strict scope. Move an existing owner only after it passes and callers remain
   compatible; a directory move alone does not make inherited code strictly typed.
3. Prefer dependencies on defining modules and leaf contracts over assembling
   package facades. Check deferred and type-only imports as well as eager ones.
4. Surfaces use Core or an explicit application service. New command families
   do not receive file-wide boundary exceptions.
5. Extend named callable contracts when components gain shared behaviour. Do not
   conceal missing conformance behind catch-all attribute lookup or broad casts.
6. Extract policy or presentation helpers when an owner exceeds its complexity
   budget. New or materially rewritten methods should aim at 10 or less even
   inside the storage manager's legacy ceiling of 15.
7. Widen positive formatting, lint, typing and documentation scopes deliberately,
   after reviewing the change and running relevant behaviour checks.
8. Tighten reviewed dependency allowances when debt is removed. A baseline update
   must explain the architectural change; it is not a way to silence a failure.

## Incremental formatting

The formatter helper expands repository-relative files and directories, rejects
missing/empty/escaping scopes and deduplicates overlaps. Nested configuration or
Git ignore rules cannot silently exclude explicitly selected files. The helper
uses the repository configuration and disables Ruff's formatter cache.

```bash
.venv/bin/python scripts/run_format_checks.py
.venv/bin/python scripts/run_format_checks.py --dry-run
.venv/bin/python scripts/run_format_checks.py --write
```

Only `--write` rewrites source. Review that diff and run affected tests before
expanding a scope. Ruff is pinned to **0.16.5** in both the typing extra and
`required-version`; Python 3.12, 88 columns and LF endings are the current format
policy. Upgrade both pins together in a reviewed change.
[Formatter regressions](../../tests/scripts/test_run_format_checks.py) exercise
real bounded writes and checks; a dry run does not substitute for them.

## Current ownership seams

| Owner | Current responsibility |
| --- | --- |
| `core/program_endpoints`, `core/program_services` | Endpoint descriptions/registration and execution respectively; `CoreProgramAPI` composes them. |
| `surfaces/cli/parsers`, `surfaces/cli/storage_commands` | Grammar and named storage command families; `app` dispatches. Completion uses an explicit registrar. |
| `ingest/mixed_application.py` | Mixed-ingest coordination over an injected open Library. The CLI/Core boundary owns database and storage lifecycle. |
| `catalog/write/host_api.py` | Leaf host contract for Catalog writers, below the assembling API. |
| `catalog/api/metadata_tools_api/facades.py` | Co-located mutually referring tool protocols; callers should use their defining owner. |
| `caches/write` | Writer assembly; implementations import defining base modules directly. |
| `surfaces/presentation.py`, `surfaces/acquisition_types.py` | Shared presentation and acquisition contracts without other LiuXin imports. |
| `storage/storage_manager/mixins` | API-shaped implementation components, with private state, wire types and cross-component contracts. |

See [Core workflows](../core/core-program-workflows.md),
[CLI composition](../surfaces/cli/cli-composition.md) and
[terminal composition](../surfaces/terminal/terminal-composition.md).
The runtime ownership contract remains distinct from the set of files a checker
currently covers.

## Internal callable contracts

[`check_internal_type_contracts.py`](../../scripts/check_internal_type_contracts.py)
checks valid and deliberately invalid examples in
[the static fixture](../../tests/typing/internal_contracts.py). It verifies the
expected diagnostic at each marked location; unrelated import failures cannot
satisfy an expected type error. The fixture is never executed.

```bash
.venv/bin/python scripts/check_internal_type_contracts.py --checker basedpyright
.venv/bin/python scripts/check_internal_type_contracts.py --checker mypy
```

Keep an `expect-error` marker on the precise line reported by each checker.
Different locations can use separate markers with `-` in the non-applicable
checker slot. Unmarked diagnostics fail. Production checks still require zero
errors; negative examples run separately after production succeeds.

## Workflow implementation ratchet

[Workflow ownership tests](../../tests/scripts/test_workflow_ownership.py) enforce
composition roots, dependency directions and growth ceilings: 450 lines per
implementation module, 160 per function and 250 for the two facade roots. The
shared [SourceMetrics](../../tests/support/docstring_ownership.py) excludes
**docstring-only lines**; these are not raw `wc -l` limits. Documentation additions
must not be mistaken for new executable workflow bodies.

The Core and CLI implementation trees have lint/typing and complexity coverage.
Extracted strict policy leaves and standard-mode orchestration remain distinct.

## Dependency direction and import contexts

Three checks answer different questions:

- The protected graph rejects combined-graph cycles and named bad directions
  within its selected modern modules.
- The [whole-package ratchet](layer-direction-ratchet.md) records existing
  backward edges, unresolved dynamic imports and component-size ceilings. Its
  passing result still contains cycles and reviewed exceptions.
- The [cold-import ratchet](import-footprint-ratchet.md) observes actual imports
  in fresh interpreters, including parent package initialization.

The static scanners classify import-time, deferred and recognized type-only
imports, and resolve supported **literal** dynamic import calls. Non-literal
calls remain a separately reported inventory. Classification is syntactic: a
function may execute during initialization, guards are not a general symbolic
interpreter, and implicit parent initialization is not a static edge expansion.
No spelling of a deferred or type-only edge exempts it from the combined rules.

## Cold-import footprint

The 25 configured probes enforce module-count ceilings and forbidden domains or
module prefixes independently. They count newly loaded `LiuXin_alpha` modules,
not memory, elapsed time or every external dependency. A budget passing does not
prove absence of every import side effect. See the
[dedicated guide](import-footprint-ratchet.md) for diagnostic commands and changes.

## CLI composition and entry-point boundaries

Parser composition does not import completion; completion supplies its registrar.
The installed command targets `LiuXin_alpha.surfaces.cli.app:main`. The complete
CLI package participates in the protected graph, while selected owners enter
formatting, lint and typing. Package-wide graph coverage is not package-wide
strict typing. [CLI contract tests](../../tests/surfaces/test_cli_dependency_contracts.py)
cover imports, registration, completion and dispatch behaviour.

## Terminal composition and extension boundaries

Startup, browser execution and curses presentation have separate owners.
`browser_components` and `windowed_components` contain the implementations;
roots compose them. Extension contracts avoid imports back to concrete roots.
The protected graph covers the complete terminal package; ownership tests apply
component ceilings using the same docstring-excluding source metrics. See the
[terminal guide](../surfaces/terminal/terminal-composition.md) for exact contracts.

## Read-model failure visibility

Successful empty results and explicit missing-capability fallbacks are distinct
from failed reads. Read-model/image backends and outer API/OPDS adapters must not
hide query failures in catch-all handlers. WSGI owns a generic HTTP 500 response
and server-side traceback logging. The
[read-model failure contract](../surfaces/read-model-failures.md) owns the exact
fallback rules. This does not settle every catalogue projection difference.

## Repository-wide documentation audit

The broad docstring campaign **completed on 2026-10-05**, with 1,372 units,
2,671 files and 42,166 declarations at that checkpoint. See the
[completion record](../../working-memory/project-docstrings-l001-l037-2026-10-05.md)
and [campaign plan](../hacking/project-docstrings-completion-plan.md). The older
D095 pause and partial inventories are history, not today's work queue.

[`audit_project_docstrings.py`](../../scripts/audit_project_docstrings.py) audits
the whole project without positional paths; `--output` writes a report and
`--check` fails on structural gaps. Explicit file arguments create a batch view.
The [style guide](<../hacking/Style Guide.md>) requires meaningful parameter and
return descriptions. Empty fields are not sufficient documentation.

[`normalize_docstrings.py`](../../scripts/normalize_docstrings.py) normalizes safe
existing literals and checks executable syntax; ambiguous literals and unmatched
fields require review. Neither tool proves prose accuracy. Help output consuming
`__doc__` still needs behavioural checks. Completion of the dated inventory does
not certify all later code or remove the need to review public examples.

### Earlier public-boundary audit

The public-boundary ratchet remains narrower than the completed campaign. It
checks modules/public classes in eight modern packages, locally defined exports
listed in literal `__all__`, maintained top-level tools, and named metadata row
and WEMI container boundaries. The
[test source](../../tests/scripts/test_public_documentation_boundaries.py) is the
exact scope. It rejects missing/placeholder documentation, not every inaccurate
sentence. Documentation-link tests check tracked local targets and reachability;
renderer-specific fragments and external URLs need separate review.

## Whole-project baseline and priority order

The August/September measurements motivated these gates: the former broad type
configuration had 2,639 basedpyright and 498 mypy errors on August 31. The
September 2 review recorded large orchestration modules and modern cycles. Those
counts and subjective scores describe their old source, not the current tree.

The September tranches established internal contracts, extracted workflows and
CLI/terminal owners, exposed read failures and consolidated CI ownership. The
October work added lifecycle composition, whole-package direction and cold-import
ratchets, Catalog hydration/write consolidation, application settings and unified
plugins. Detailed remaining findings belong in the
[dated architecture review](architecture-review-2026-09-26.md) and
[cross-project work list](../global_todo.md), rather than a second queue here.

For a failed gate, identify its exact scope and diagnostic, fix the owner or
caller, then run that gate and affected behaviour tests. If scope or dependencies
change, run the complete configured runner. Installed-artifact checks remain
separate: see [packaging](../hacking/packaging.md). The runner does not invoke
pytest; CI explicitly selects documentation, architecture and surface regression
tests alongside it, with the full suite owned by the general tests workflow.
