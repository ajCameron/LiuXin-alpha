# Catalog specialized matcher documentation — 2026-09-12

## Scope and checkpoint

Continues the whole-project goal after the
[matching policy pass](project-docstrings-catalog-matching-policy-2026-09-12.md).
Branch remains `codex/project-docstrings` at `9790d020`. No commit or push; existing
example, Catalog, working-memory, and data-submodule changes remain.

Read and documented all four concrete matchers in `catalog/matching/` and their
four corresponding `catalog/api/matching_api/` contracts: `work_matcher.py`,
`agent_matcher.py`, `identifier_matcher.py`, and `item_identifier_matcher.py`.
Each file was unchanged from HEAD before editing.

Batch: **8 modules, 8 classes, 41 functions = 57 declarations**. Includes all
private helpers and constructors. Thirteen previously missing private-function
docs are supplied; other edits replace incomplete or inaccurate descriptions.

Cumulative [reviewed set](project-docstrings-reviewed-files.txt): **639 modules,
841 classes, 7,019 functions = 8,499 declarations**. All **8 matching source modules**
and **6 matching API modules** are now complete: together 14 classes and 76
functions. Catalog source coverage is **19 of 113 modules**, 33 of 144 classes,
and 115 of 860 functions. Catalog tests remain **4 of 19 modules**; this batch
changes no tests. Dependency excerpts are not completed-file claims.

## Contracts clarified

- Work and Agent matchers retain database/repository/policy references without
  querying or rebinding them at construction. Matching reads repositories and
  does not persist entities. Identifier ownership is resolved before row scans.
  Multi-owner hints are ambiguous before cross-hint singleton conflicts are
  considered; hints are not intersected to rescue an ambiguous owner set.
- Work descriptive matching picks the first non-None candidate title, including
  blank values, and the closest stored title variant. Approximate titles need
  exact corroboration. Agent-credit evidence uses canonical names without role
  filtering or aliases. Field disagreements reduce confidence but do not alone
  reject a descriptive candidate. Qualification precedes final acceptance.
- Agent descriptive matching accepts normalized exact canonical/sort/alias names;
  it has no policy switch enabling approximate name-only reuse. Supplied type
  disagreement rejects the row. JSON alias lists preserve string entries, blanks,
  and duplicates; delimited fallback strips and drops blank pieces.
- Identifier-backed Work/Agent decisions bypass final confidence acceptance.
  Specialized contradiction cutoffs still apply. Executable examples demonstrate
  a match with confidence 0.8125 and individual conflict evidence. Work's identifier
  path ignores Agent hints; Agent's compatible type contributes no evidence on
  that path. Candidate displays suppress terminal conflict/ambiguity; best keeps
  those decisions. A zero display limit still normalizes and evaluates input.
- Curated identifier matching checks normalized scheme/value without owner
  existence or scope checks. It selects the lowest stored identifier row ID,
  rather than resolving bibliographic ownership or duplicate ambiguity. Stored
  IDs are accessed directly without integer/positivity validation. Malformed
  stored identifier values raising TypeError/ValueError are skipped.
- Observed Item identifiers use the same normalized comparison with optional
  direct-equality Item filtering. Their integer row-ID check admits booleans;
  limit validation also admits booleans. Duplicate observations remain separate
  records; best picks the first sorted observation globally or within scope.
  Pure examples verify deterministic curated selection and scoped observation
  selection with in-memory repositories.
- Existing API mismatch retained and made explicit: WorkMatcherAPI.exact uses
  `cand_str`, while WorkMatcher.exact uses `candidate_str`. Positional calls are
  portable; a separate behavior/API change should reconcile keyword names.
  No signature, validation, constant, matching decision, or assertion changed.

## Verification

- Eight executable ASTs match `9790d020` after stripping only leading literal
  docstrings. Ruff remains **5 to 5**, with no added finding. Existing module
  headers/comments remain unchanged. No new runtime-doc exception was needed.
- Strict audit and normalizer pass for this batch and the entire **639-file**
  reviewed set. Independent AST recount confirms the totals and coverage above.
- Full `tests/catalog`: **511 passed**, 183.18s.
- Eight-file doctests: **19 passed, 32 skipped**, 14.52s. Skips show calls requiring
  existing Catalog/matcher objects; they are not executed integration evidence.
- Migration/public-documentation/developer-link contracts: **38 passed**, 44.13s.
- Full quality runner passed: 159 formatted files, 456 annotated modules, 221
  protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and both sets of 37 rejected invalid-call examples.
- All four test/quality terminal exits were observed as zero. Durable log/done
  pairs are in `working-memory/test-results/`, prefix
  `docstrings-catalog-specialized-matchers-2026-09-12-`, suffixes `regression`,
  `doctests`, `contracts`, and `quality`. The matching `observations.json` saves
  source hashes, AST/Ruff evidence, audit summaries, and completion records.
- Root/data-submodule diffs are whitespace-clean; handoff links resolve.

Static reports: `/tmp/liuxin-catalog-specialized-matchers-ast-lint-2026-09-12.json`,
`/tmp/liuxin-docstring-catalog-specialized-matchers-batch-2026-09-12.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-12.json`, and
`/tmp/liuxin-docstring-catalog-specialized-matchers-2026-09-12.json`.

Whole-project scope remains **2,730 modules, 4,400 classes, 35,119 functions**,
without parse failures. Missing/blank docs: **1,065 modules, 1,894 classes,
20,070 functions = 23,029 declarations**, down 13. Other overlapping findings:
3,415 delimiter layout, 9,614 examples, 2,775 parameter fields, 3,629 return fields,
3,151 empty parameter descriptions, 4,119 empty return descriptions, 28 summaries.
Do not add these categories to the missing-docstring total.

## Next

The [base/WEMI repository continuation](project-docstrings-catalog-wemi-repositories-2026-09-12.md)
now completes those five concrete repositories, their five API modules, and the
full repository-invariant test module. Counts above retain this earlier batch's
checkpoint. Continue remaining repositories/contracts, other Catalog owners/tests,
and the wider project backlog from the newer note. All specialized matching files
and both matching regression modules are complete; do not repeat their prose pass.
Other dependency excerpts remain read-ahead unless explicitly listed as reviewed.

Preserve the three narrow runtime-doc exceptions, all seven unresolved docstring-
sensitive ownership-guard failures, and the legacy FRBR fingerprint follow-up in
the [main ledger](project-docstrings-2026-09-08.md). Those guard tests were not rerun
or changed here. The whole-project goal remains active.
