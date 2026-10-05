# Catalog base and WEMI repository documentation — 2026-09-12

## Scope and checkpoint

Continues the whole-project goal after the
[specialized matcher pass](project-docstrings-catalog-specialized-matchers-2026-09-12.md).
Branch remains `codex/project-docstrings` at `9790d020`. No commit or push; earlier
example, Catalog, working-memory, and data-submodule changes remain local.

Read and documented these complete files, each unchanged from HEAD before editing:

- `catalog/repositories/{base,works,expressions,manifestations,items}.py`
- `catalog/api/repositories/{base,works,expressions,manifestations,items}.py`
- `tests/catalog/test_catalog_repository_invariants.py`

The first two groups are under `src/LiuXin_alpha/`. Batch: **11 modules, 10 classes,
77 functions = 98 declarations**, including all private helpers/properties and
both nested failure seams. Fourteen previously missing function docs are supplied.

Cumulative [reviewed set](project-docstrings-reviewed-files.txt): **650 modules,
851 classes, 7,096 functions = 8,597 declarations**. Independent AST recount
confirms unique manifest paths and these totals. Catalog source now covers **29 of
113 modules**, 43 of 144 classes, and 177 of 860 functions. Its repository tree
covers **5 of 13 modules**, its repository API tree **5 of 15**, and Catalog tests
**5 of 19**. All matching implementation/API files remain complete.

## Contracts clarified

- BaseRepository retains borrowed dependencies without eager capability checks.
  Repository and matching-policy bindings are independent and replace references;
  missing policy falls back lazily to the shared default. Existing matchers are not
  updated by later rebinding. None, rather than general falsehood, triggers fallback.
- CRUD validates nonnegative integer IDs and rejects booleans. Normalization reads
  the live schema even for empty payloads, maps aliases once, rejects unequal alias
  collisions, protects the known ID column even when ignoring unknown fields, and
  returns a new dictionary retaining value references. Allowing IDs does not validate
  their values. The base implementation has no additional per-column writability or
  value-type validation beyond these rules; subclass/backend constraints still apply.
- Mapping annotations do not enforce immutable rows: reads produce shallow plain
  dictionaries without write-through. get treats false-valued rows as absent. list
  validates both arguments, returns early for zero, otherwise fetches/copies the
  full ID-ordered table before slicing. require is an existence check without a lock.
- Creation checks returned-ID type after insertion and does not undo insertion if
  that check fails; it rejects bool IDs but does not check their sign. Update checks
  existence before normalization, even for an empty payload. Generic read/write
  pairs have no enclosing repository transaction; base delete adds no ownership scan.
- Link upserts validate both endpoints in a macro transaction. Omitted priority on
  ordered links preserves an existing matching link's priority, including None,
  otherwise uses int(max(numeric non-boolean priorities, default=0)) + 1 across
  types. Type participates in identity only when declared by the spec. Nonfinite
  priorities have no extra repository guard. Link traversal preserves macro order
  and duplicate destinations, attaches copied _catalog_link metadata, and raises
  for a missing destination rather than dropping it. Explicit None filters null
  types; the sentinel leaves types unfiltered. Reads do not form a snapshot here.
- Work title lookup uses NFKC/case-folding/collapsed whitespace while preserving
  punctuation, considers preferred/canonical titles rather than sort titles, and
  applies the limit after full matching. Work matching constructs a fresh matcher
  from the currently bound group and policy. Reuse leaves metadata unchanged;
  candidate source/hints are not separately persisted by match_or_create.
- Expression, Manifestation, and Item matching first reads an explicit parent scope,
  then compares the documented identity fields (weight five) and corroboration
  (weight one). Supporting fields alone do not qualify; field disagreements can
  reduce confidence. These contextual matchers return match/no_match/ambiguous,
  not terminal conflict. Parent input determines scope independently of candidate
  fields. Matching may ignore fields later rejected by creation.
- Corrected existing transaction promises: Expression and Manifestation creation
  precedes the link helper's transaction. Without an outer transaction, link failure
  can leave the new row unlinked. These conveniences also have no uniqueness lock.
  Their parent-field aliases are removed from a shallow copy before insertion.
- Item ownership is a direct item_manifestation_id column, not a traversed link row;
  Item/Manifestation reads add no _catalog_link metadata. Creation overwrites the
  canonical parent key but leaves a separate manifestation_id alias, so a disagreeing
  alias can fail normalization. Non-integer stored references return None; boolean
  and negative references pass the first int check but fail strict ID validation.
- Item bundles use a fresh retriever and the bound group, choosing the first upward
  Expression/Work path and collecting attachments without a snapshot transaction.
  Missing relationships differ from dangling references. Bundle, macro, schema,
  and policy source read-ahead supports these descriptions but is not added to the
  reviewed manifest by this batch.
- The invariant test module now documents real identifier replacement/rollback,
  owner isolation, storage/cache alignment, sequential Unicode Work reuse, alias
  rejection, priority preservation, and the simulated dangling read. Descriptions
  distinguish actual row-count/snapshot assertions from absent concurrency proof.
  No validation, signature, executable constant, fixture, or assertion changed.

## Verification

- All eleven executable ASTs match `9790d020` after stripping only leading literal
  docstrings. Ruff remains **20 to 20**, with no added finding. Tokenized comments
  match HEAD, and existing module headers remain unchanged. No new runtime-doc
  exception is needed. Relevant runtime consumers are documentation contract tests.
- Strict audit and normalizer pass for the batch and the complete **650-file**
  reviewed set. A final two-class wording edit expands Item aliases to their exact
  public names; the whole batch then passes another audit/normalizer and AST/Ruff
  comparison. Unchanged reviewed files retain the full-set verification.
- Full `tests/catalog`: **511 passed**, 190.80s.
- Eleven-file doctest/test selection: **26 passed, 70 skipped**, 54.66s. This includes
  the eleven normal invariant tests, which overlap the full Catalog regression run.
  Skips illustrate calls needing an existing Catalog/database; pure examples exercise
  actual normalization, dependency binding, row copying, link metadata, and lookup.
- Migration/public-documentation/developer-link contracts: **38 passed**, 43.45s.
- Full quality runner passed: 159 formatted files, 456 annotated modules, 221 protected
  dependency modules, lint/complexity, both production type checkers, 188 strict-mypy
  files, and both sets of 37 rejected invalid-call examples.

All four test/quality process terminal exits were observed as zero; no run was
restarted because of a lost observation. Durable log/done pairs are under
`working-memory/test-results/` with prefix
`docstrings-catalog-wemi-repositories-2026-09-12-` and suffixes `regression`,
`doctests`, `contracts`, and `quality`. The matching `observations.json` retains
final source hashes, AST/Ruff comparisons, audit counts, and completion records.
Root/data-submodule diffs are whitespace-clean, and local handoff links resolve.

Whole-project audit scope remains **2,730 modules, 4,400 classes, 35,119 functions**,
with no parse failures. Missing/blank documentation: **1,065 modules, 1,894 classes,
20,056 functions = 23,015 declarations**, down 14. Other overlapping findings:
3,348 delimiter layout, 9,546 examples, 2,754 parameter fields, 3,608 return fields,
3,151 empty parameter descriptions, 4,119 empty return descriptions, 28 summaries.
These categories are not additive undocumented-declaration totals.

Static reports: `/tmp/liuxin-catalog-wemi-repositories-ast-lint-2026-09-12.json`,
`/tmp/liuxin-docstring-catalog-wemi-repositories-batch-2026-09-12.json`,
`/tmp/liuxin-docstring-reviewed-2026-09-12.json`, and
`/tmp/liuxin-docstring-catalog-wemi-repositories-2026-09-12.json`.

## Next

Superseded scheduling: the [exact/note batch](project-docstrings-catalog-exact-repositories-2026-09-12.md)
is now verified, advancing the manifest to 662 files. Use the
[bounded plan](../dev-docs/project-docstrings-completion-plan.md) for subsequent
work; the following paragraph preserves the earlier checkpoint's backlog.

Continue remaining repositories and their contracts: exact-entity mechanics and
configured entities, Agents, curated/observed identifiers, titles, notes, and package
exports/composition. Then remaining Catalog retrieval, mutation/writers, search,
compatibility owners, APIs/tests, and the wider whole-project backlog. The complete
invariant test module is now reviewed; earlier partial dependency reads are not.

Preserve the three narrow runtime-doc exceptions, all seven unresolved docstring-
sensitive ownership-guard failures, the Work exact keyword mismatch, and the legacy
FRBR fingerprint follow-up in the [main ledger](project-docstrings-2026-09-08.md).
Those guard tests were not rerun or modified. The whole-project goal remains active.
