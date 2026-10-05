# Catalog exact-entity and note repository documentation — 2026-09-12

Completed source work from before the token-cost pause is now reconciled. All
existing validation runs finished successfully. This adds 12 reviewed modules,
18 classes, and 50 functions (80 declarations), supplying 12 missing function
docstrings. The manifest advances from 650 to **662 files**, covering 869 classes
and 7,146 functions (8,677 declarations). No source was edited during P00.

## Scope and contracts

Full files: `src/LiuXin_alpha/catalog/repositories/{exact,entities,notes,__init__}.py`,
`src/LiuXin_alpha/catalog/api/repositories/{exact_entity,comments,annotations,synopses,notes,__init__}.py`,
and `tests/catalog/{test_catalog_convenience_api,test_semantic_catalog}.py`.
Catalog coverage is 39/113 source files and 7/19 test files. Repository coverage
is 9/13 concrete modules and 11/15 API modules.

- Exact matching documents identity/scope requirements, scalar lookup, normalization,
  ambiguity, and mutation preflight. Language rejects reuse before lookup;
  Comment/Annotation reject match-or-create while retaining read matching and CRUD.
- Normalization derives only missing normalized fields from supplied non-None text,
  preserves explicit None/custom values, and does not fetch omitted old values.
  Update validation order differs from the base repository.
- Comment/Synopsis add operations create and link transactionally. Note add preflights
  owners/schema but creation precedes a separate link transaction and can leave an
  unattached row. Replacement validates input, creates fresh rows, and replaces
  links; it does not explicitly delete the previously linked records.
- Annotation listing requires the Item first, validates user IDs without querying
  user existence, trims kind without case folding, and returns plain rows.
- Exports/composition document the fixed repository attributes and actual aliases.
  Tests describe the assertions present, including title clearing and selected
  rollback checks, without claiming concurrency or complete protocol conformance.

## Verification

Executable ASTs match `9790d020` after stripping leading literal docstrings.
Ruff remains **4 to 4**, with no added findings; tokenized comments and interpreter
headers are preserved. The AST/Ruff/header comparison was observed again during
P00. Strict audit and normalizer passed for this batch and the full 662-file set.
No new runtime-doc exception was introduced.

- Catalog regression: **511 passed**, 142.31s.
- Doctest/test selection: **27 passed, 52 skipped**, 58.74s; eleven ordinary tests
  overlap the Catalog regression. Skips require an existing Catalog/database.
- Documentation contracts: **38 passed**, 44.40s.
- Full quality runner: passed formatting (159 files), annotations (456 modules),
  dependencies (221 modules), lint/complexity, both production type checkers,
  strict mypy (188 files), and both sets of 37 rejected invalid-call examples.

All four `.done` records were read with terminal exit zero; no completed broad run
was repeated. Evidence, source hashes, audit summaries, and completion records:
[observations](test-results/docstrings-catalog-exact-repositories-2026-09-12-observations.json).
Matching log/done files use that prefix with `regression`, `doctests`, `contracts`,
and `quality` suffixes.

Whole audit: 2,730 modules, 4,400 classes, 35,119 functions, no parse failures.
Missing/blank docs: 1,065 modules + 1,894 classes + 20,044 functions = **23,003**.
Other overlapping findings: 3,288 delimiter layout, 9,492 examples, 2,735 parameter
fields, 3,590 return fields, 3,151 empty parameter descriptions, 4,119 empty return
descriptions, and 28 summaries. These are not additive undocumented totals.

## Resume

Use [P00](project-docstrings-p00-2026-09-12.md) and the
[bounded plan](../dev-docs/project-docstrings-completion-plan.md). G01 is next;
stop after each requested module. Preserve the three runtime-doc exceptions and
all seven unresolved ownership-guard failures. Their separate repair is approved
but remains queued. The whole-project goal is unfinished; no commit or push.
