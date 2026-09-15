# Catalog foundation documentation — 2026-09-12

## Scope and checkpoint

Continues the unfinished whole-project goal after the
[example completion](project-docstrings-example-completion-2026-09-12.md).
Branch remains `codex/project-docstrings` at `9790d020`; no commit or push.
Earlier example, working-memory, and data-submodule changes are preserved.

Read and documented these complete files, each unchanged from HEAD before editing:

- `src/LiuXin_alpha/catalog/__init__.py`
- `src/LiuXin_alpha/catalog/api/__init__.py`
- `src/LiuXin_alpha/catalog/api/common.py`
- `src/LiuXin_alpha/catalog/api/catalog.py`
- `src/LiuXin_alpha/catalog/catalog.py`
- `tests/catalog/test_catalog_imports.py`
- `tests/catalog/test_catalog_api_documentation.py`

The batch adds **7 modules, 20 classes, 50 functions = 77 declarations** to the
[reviewed manifest](project-docstrings-reviewed-files.txt). This includes all
nineteen repository shortcuts, private constructors/post-init methods, the small
database protocol, error classes, and every named helper/test in the two suites.
Cumulative: **623 modules, 827 classes, 6,916 functions = 8,366 declarations**.

Catalog source coverage is now **5 of 113 modules**; Catalog tests **2 of 19**.
Earlier Catalog examples remain separate reviewed files, not implementation proof.

## Contracts clarified

- Replaced the repetitive package introduction and hypothetical API calls with
  current repository, matching, bundle, writer, and compatibility entry points.
  The package exports Catalog; public candidate/result contracts live under api.
- Catalog borrows its database and has no close/context-manager API. Construction
  checks MatchingPolicy first, then builds and binds owners without a general
  database capability check or cleanup wrapper. Grouped repositories are mutable;
  the nineteen shortcut properties return their current members without queries.
  for_name accepts only registry names after whitespace/case/hyphen normalization
  and explicit singular aliases.
- Writer selection uses exact schema table/column names, source-column precedence,
  and a unique linked destination. Cardinality alone does not imply ownership.
  Generic writes preserve concrete writer arguments, results, and failure behavior.
  Normalized updates own their execution through the database or portable macros;
  the facade adds no transaction. Column-update results are the stored instruction
  values, not database readback. Clearing an owned-row value unlinks it but leaves
  the destination row for explicit cleanup.
- Candidate and WEMI records do not normalize, validate, or copy supplied data.
  Frozen fields do not freeze nested mappings. Identifier normalization belongs to
  the consuming owner. Result containers do not establish transaction success,
  path consistency, or exhaustive retrieval merely by being constructed.
- MatchEvidence validates selected fields and converts numeric values to floats:
  score rejects nonfinite values, but weight currently accepts NaN and positive
  infinity. MatchResult checks confidence, decision/ID consistency, and integer
  alternatives; it does not validate selected-ID type, positivity, uniqueness,
  evidence consistency, or database existence. Predicates classify stored fields
  without rescoring. Ambiguity/conflict do not authorize automatic creation.
- WemiGraph truncation marks possible downstream incompleteness when an upstream
  limit cuts traversal; a marked level need not have reached its own limit.
  Matching errors retain the same result object without validating its decision.
  CatalogMutationError itself guarantees no rollback.
- Import tests document their attribute-free database double and structural-only
  evidence. Documentation tests distinguish text regexes, AST word counts,
  runtime/inherited doc inspection, and syntax-only guide examples from semantic
  review or executable integration proof. Their guards and assertions are unchanged.

## Verification

- Final seven-file executable AST comparison against `9790d020`: identical after
  removing only leading literal docstrings. Ruff remains **49 to 49**, with no
  introduced finding. Existing source headers are unchanged. No runtime-doc
  exception is added; the three narrow earlier exceptions remain unchanged.
- Final seven-file strict audit and normalizer passed; both also pass for the
  complete **623-file** reviewed set, including the final regex-scope wording fix.
- Full `tests/catalog`: **511 passed**, 232.78s. Includes matching, repositories,
  aggregate mutations, retrieval, writer routes/updates, Unicode, search,
  compatibility boundaries, imports, and API documentation contracts.
- Production foundation doctests: **24 passed, 39 skipped**, 19.33s. Skips are
  explicit examples needing an existing Catalog/database or normalized update.
- The two newly documented test modules, with doctests: **12 passed, 11 skipped**,
  21.29s. The eleven normal tests overlap the full Catalog suite; these totals are
  separate execution evidence rather than additional unique regression cases.
- Migration/public-documentation/developer-link contracts: **38 passed**, 44.51s.
- Full quality runner passed: 159 formatted files, 456 annotated modules, 221
  protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and both sets of 37 rejected invalid-call examples.
- All five pytest/quality process terminal exits were observed as zero. Results
  are durable log/done pairs under `working-memory/test-results/` with prefix
  `docstrings-catalog-foundation-2026-09-12-` and suffixes `regression`, `doctests`,
  `test-docs`, `contracts`, and `quality`.

Static reports: `/tmp/liuxin-catalog-foundation-ast-lint-2026-09-12.json`,
`/tmp/liuxin-docstring-catalog-foundation-batch-2026-09-12.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-12.json`, and
`/tmp/liuxin-docstring-catalog-foundation-final-2026-09-12.json`.

The whole-project audit still covers **2,730 modules, 4,400 classes, 35,119
functions**, with no parse failures. Missing/blank docs remain **1,065 modules,
1,894 classes, 20,125 functions = 23,084 declarations**, down 13. Most source
work in this batch replaced incomplete existing documentation. Other overlapping
findings: 3,493 delimiter layout, 9,669 examples, 2,775 parameter fields, 3,630
return fields, 3,151 empty parameter descriptions, 4,119 empty return descriptions,
and 28 missing summaries.

## Next

The [matching-policy continuation](project-docstrings-catalog-matching-policy-2026-09-12.md)
has completed the shared/exact matching owners, specifications, group composition,
two API modules, and both main matching test modules. The reviewed set now contains
631 files. The following was this foundation batch's original handoff.

Continue into matching policies, exact-entity specifications, matcher composition,
and concrete matchers with their API contracts and adjacent regression helpers.
The matching tree has **8 modules, 8 classes, 60 functions**; none is reviewed yet.
`policy.py` and `exact_matcher.py` need a full source read before documenting their
normalization and decision boundaries. Read-ahead excerpts of matching, retrieval,
writer factory/update, and repository contracts do not count as completed files.

Then continue other Catalog owners/APIs/tests and the wider source/test/script/
inherited/data-submodule backlog. Preserve all seven unresolved ownership-guard
failures, the three narrow runtime-doc exceptions, and the legacy FRBR fingerprint
follow-up in the [main ledger](project-docstrings-2026-09-08.md). Those seven guards
were not rerun or modified in this batch. The whole-project goal stays active.
