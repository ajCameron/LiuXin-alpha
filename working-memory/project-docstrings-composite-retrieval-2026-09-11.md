# Composite, Item-link, and retrieval contracts — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[Asset/Replica checkpoint](project-docstrings-asset-replica-2026-09-11.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, without a commit
or push. Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/api/storage_manager_api/models/composites.py` and `resolutions.py`.
- The `composites_api.py`, `item_links_api.py`, and `retrieval_api.py` contracts.
- `storage/storage_manager/mixins/composites.py`, `item_links.py`, and `retrieval.py`.
- Complete `tests/storage/api/test_storage_manager_composition.py` and
  `test_store_container_db_roundtrip.py` regression modules.

Newly completed: **10 modules, 13 classes, 51 functions = 74 declarations**.
Cumulative: **492 modules, 686 classes, 5,837 functions = 7,015 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 492 unique
paths. All ten targets matched HEAD before editing. The prior complete model
read-ahead was revalidated; shared-helper/mini-database excerpts remain dependency
evidence rather than completed-file claims.

## Contracts clarified

- Composite membership checks selected ID/position comparisons, nonblank optional
  labels, and NUL exclusion. It does not validate safe export paths. Position
  validation permits an unsorted supplied sequence and repeated atomic Asset IDs;
  the member sequence is retained rather than normalized. Declaration names are
  checked while record names are not, and extension attributes remain unchecked.
- Availability values compare supplied counts and diagnostics without validating
  their consistency. Atomic/member resolutions check Asset-ID agreement rather
  than current Replica health. Item resolutions compare whole membership sets for
  coverage, allowing duplicate resolutions to survive into ordered Locations.
- Composite declaration always allocates a new identity, including for equivalent
  declarations. Declaration/replacement validate all member Assets before the
  metadata transaction. Replacement and forgetting use optional revision checks.
  Waiving the unlink requirement skips both Item and derivation-source checks
  without cascading reference or member deletion.
- Composite resolution follows stored order, attempts optional members, and catches
  only DigitalAssetNotFound/NoReadableReplica for member unavailability. Required
  failures accumulate before CompositeDigitalAssetIncomplete is raised. Assessment
  ignores optional members, counts required occurrences, deduplicates missing IDs
  only, and can report zero-required-member readability.
- Item linking validates target existence before the shared setter checks the Item
  ID and role. Exact role spelling is retained, and linking replaces any previous
  atomic/Composite target for that pair. Unlinking does no corresponding ID/role
  validation. This layer does not look up a separate Item catalogue record.
- Replica selection ranks Store preference before VERIFIED/PRESENT/UNVERIFIED state
  and integer Replica ID. It checks current stat size, suppresses stat StorageError
  failures while trying alternatives, and does not compare digests, open readers,
  impose read versions, or update observations. require_verified checks the recorded
  VERIFIED enum identity; same-size changed bytes can still pass selection.
- Asset resolution reads the Asset before the selector reads it again. Exact Replica
  Location lookup also works for tombstones. Item target and Composite/member reads
  occur separately; result construction can expose inconsistent concurrent changes.
- Materialization without a cache returns a selected source without making it local
  or recomputing verification. A matching requested-cache hit precedes source-ID or
  source-mode validation. New cache publication allows an unverified source and
  delegates resulting verification to replication without an added health check or
  rollback. Exact source selection ignores source modes; mode-based selection
  eagerly normalizes the entire iterable before trying distinct modes in order.
- Tests distinguish imported-class ownership/MRO checks, physical-line ceilings,
  abstract-helper failures, in-memory codec identity, and an actual temporary SQLite
  configuration round trip. The latter does not publish Asset bytes or exercise a
  complete application restart.

## Verification

- Strict structural audit and normalizer pass for all ten files and the full
  492-file reviewed set. Counts were recounted from source ASTs.
- Every executable AST matches HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, runtime strings, codec identities,
  and limits are unchanged; no runtime-doc exception was added. Final per-file
  Ruff result multisets match their baselines. A final plain-language summary
  refinement left examples and line counts unchanged; static checks were repeated.
- Selected source/test doctests and Store/API/manager/ingest/filesystem/archive
  regressions, including composition and SQLite/database reload checks:
  **445 passed, 300 explicitly skipped integration examples**, 160.69s. Skips are
  not executed integration proof.
- Full quality runner passed: 159 formatted files, annotation coverage across
  456 modules, 221 protected dependency modules, selected lint/complexity, both
  production type checkers, 188 strict-mypy files, and 37 rejected invalid examples
  per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 24.62s.
- **31 isolated observations** passed for model validation/ordering, selection
  ranking and error boundaries, stale verification evidence, no-copy/cache paths,
  source-mode evaluation, Composite optional/repeated members, exact role keys,
  link replacement/forgetting, and tombstone routing. These used transient-manager
  operations over memory Store bytes and isolated failure/iteration hooks, without
  claiming durable or remote-service coverage. Manager cleanup ran in finally.
- The six known docstring-sensitive ownership-test failures remain outstanding;
  those unrelated guards were neither rerun nor weakened. The separate manager
  composition size guard passes with its original 120/900-line limits. Shared
  `_support.py` remains 891 lines and `_policy_support.py` 810 before their full
  documentation review; their size limits still count docstrings.
- Root and data-submodule whitespace checks passed. Every verification process
  completed with an observed exit code. Existing data-generator edits and bytecode
  artifacts remain retained.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-composite-retrieval-2026-09-11-{regression,quality,contracts}.{log,done}`.
Observations:
`working-memory/test-results/docstrings-composite-retrieval-2026-09-11-observations.json`.
Static reports: `/tmp/liuxin-docstring-composite-retrieval-batch-2026-09-11.json`,
`/tmp/liuxin-composite-retrieval-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with no
parse failures. Missing/blank docs: **1,077 modules, 1,908 classes, 20,583 functions
= 23,568 declarations**. Overlapping findings: 3,782 delimiter layout, 10,150
missing examples, 2,884 parameter fields, 3,793 return fields, 3,482 empty parameter
descriptions, 4,559 empty return descriptions, and 28 missing summaries. Structural
counts alone do not establish descriptive completeness.

## Next

Storage still has **24 unreviewed API modules** and **15 unreviewed
storage_manager implementation modules**, plus the application manager, registries,
repositories, and workflows outside that package. Continue with policy models/API
contracts and their implementation/regressions, then derivation, reconciliation,
ingest, persistence, and remaining shared composition/support owners.

The Composite/resolution read-ahead is now complete and counted; do not repeat it
as unfinished work. Shared helper and miniature-database excerpts, the application
manager, and database reload tests used for verification are not fully reviewed.
The remaining metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python stay in scope. Preserve the
runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
