# Workflow API contracts and values — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[manager convenience checkpoint](project-docstrings-manager-convenience-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.
Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented all **12 modules** under
`src/LiuXin_alpha/storage/api/workflow_api`: package/base/shared models;
all six backup API modules; and all three sealed-artifact API modules.
Every file was read in full and had no diff from HEAD before editing.
Their combined source grew from 1,692 to 2,154 lines through docstrings only.

Newly completed: **12 modules, 20 classes, 47 functions = 79 declarations**.
Cumulative: **532 modules, 772 classes, 6,331 functions = 7,635 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 532 unique
paths. All **65 storage API modules** are now complete, including the previously
completed 29 manager API modules. All 22 storage_manager implementation modules
remain complete. Implementation and regression dependency reads are not newly
completed-file claims.

## Contracts clarified

- Generic workflow values distinguish intent, current checkpoint, one-unit work,
  completion, and cancellation. The ABC does not persist checkpoints or open
  transactions. Its terminal property calls progress once. FAILED is both
  terminal and resumable; runtime protocol membership checks member presence,
  not the status type or lifecycle consistency.
- SquashFS reconstruction retains FAILED. run_to_completion immediately returns
  that failure until run_next explicitly retries. Cancellation stops future work
  without removing staging; COMPLETE/CANCELLED checkpoints are not resumable.
  One finalization unit can contain multiple physical operations.
- Source declaration validation requires enum identity and the corresponding
  identifier type, infers/cross-checks the Store UUID, and rejects negative sizes.
  It neither reads bytes nor resolves provenance IDs. Member normalization strips
  leading separators and dot/empty components and rejects parent traversal, but
  retains NUL and other backend-sensitive spellings. A missing local file may be
  designated without Asset association and fail at staging.
- Frozen backup values enforce selected comparisons rather than strict type or
  complete evidence validation. Sources/reports are not deeply frozen or copied.
  Options alone are stringified/sorted, with collisions checked after conversion.
  Checkpoint counters are bounded but not reconciled with source reports; COMPLETE
  does not require checkpoint output. A successful result requires a non-None
  reference, so an empty output string passes; final_checkpoint is not cross-checked.
- Planning may read source bytes to compute missing digests. The concrete planner
  sorts normalized member names, carries known non-deleted Replica identity, and
  groups uncompressed size estimates. An oversized source gets its own pack.
  Empty supplied extension filters select nothing. Planning does not publish or
  reserve output, pin inventory versions, or enforce sealed-image size/capacity.
- Repository writes do not establish a multi-row transaction. Checkpoint state is
  written before workflow status; a later status failure can leave state visible.
  Missing state inherits the workflow row's status rather than always DRAFT.
  updated_at does not round-trip through the current adapter. Replacing intent
  retains prior checkpoint/output rows, requiring caller coordination.
- Result recording prefers a supplied final checkpoint without reconciling it
  with the result fields. Presence links deduplicate by Store and exact member
  path, without replacing existing source/protection metadata or reading bytes.
  Deleting a workflow is subject to schema constraints and leaves artifact bytes.
- Artifact registry associates an image with a configured Store. It reuses an
  existing registration before checking current artifact existence or applying
  new name/link options. New registration can leave metadata and links before
  later manager attachment fails. It does not prove archive member contents.
- Sealed-image cataloguing is separate from archive construction and Store
  registration. It refreshes atomic member records, records ordered path/identity
  evidence, adopts a Location or ingests a local image, and records provenance.
  Bytes can be committed before final recipe/declaration validation fails.
  Format/reproducibility/completeness are recorded claims, not executed replay or
  membership verification. Location adoption cannot be redirected to another Store.
- Backup-to-provenance adaptation supports SquashFS and requires catalogue source
  IDs or a complete override with matching paths/order. It records the supplied
  executor and captured settings, and uses backup:<id> as workflow_reference,
  leaving catalogue workflow_id unset. Registration values check only matching
  output Asset identity and recipe equality; shortcuts retain existing records.

## Verification

- Strict structural audit and normalizer passed for the whole 12-module batch
  and all **532 reviewed modules**. Independent AST recount confirms 772 classes,
  6,331 functions, manifest uniqueness, and complete storage API coverage.
- All twelve executable ASTs match HEAD after stripping only leading literal
  docstrings. Signatures, annotations, abstract methods, exports, runtime literals,
  assertions, and guard limits are unchanged. No runtime-doc exception was added.
  Existing examples were retained. Ruff remains **17 to 17**, with no new finding.
- Workflow API doctests and adjacent workflow/backup/manager/archive regressions:
  **175 passed, 124 explicitly skipped integration examples, 1 failed**, 92.45s.
  The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  `_policy_support.py` 1,213, `_support.py` 1,139, and `_contracts.py` 902 exceed
  the unchanged 900-line ceiling. No newly edited file is an offender.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid examples rejected per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 28.26s.
- **24 isolated observations** passed on their first run. These include state/type
  boundaries, shallow collection retention, inconsistent evidence accepted by value
  constructors, real local staging and explicit failure retry, SQLite checkpoint
  partial writes/replacement behavior, registry reuse, estimated pack sizing, and
  actual manager ingest preceding late provenance validation failure. Temporary
  directories, database connections, and the manager were cleaned up. No archive
  tool or live remote service was invoked by these observations.
- All verification processes completed with observed terminal exits. The **seven**
  known docstring-sensitive pytest guard failures remain unresolved: the six earlier
  Core/CLI/terminal failures and the manager-composition failure above. The earlier
  six were not rerun. No guard was weakened or deselected.
- Root and data-submodule whitespace checks passed. All 119 local link targets
  across the index, main ledger, and this handoff resolve; all three notes are free
  of trailing whitespace. The existing data-generator edit and untracked submodule
  bytecode directory remain intact.

Durable results:
`working-memory/test-results/docstrings-workflow-api-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-workflow-api-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-workflow-api-batch-2026-09-11.json`,
`/tmp/liuxin-workflow-api-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank documentation remains **1,076 modules, 1,908 classes,
20,579 functions = 23,563 declarations**. This batch completed existing incomplete
documentation. Overlapping findings: 3,662 delimiter layout, 9,820 examples,
2,805 parameter fields, 3,692 return fields, 3,196 empty parameter descriptions,
4,174 empty return descriptions, and 28 missing summaries.

## Next

Storage has **17 unreviewed source modules** outside its completed API and manager
implementation packages: six backup modules; two workflow modules; backend_registry,
location, migrations, single_file, storage_types, store_container, store_factory,
store_manager, and store_spec_utils. Continue with the concrete backup/workflow
owners and their full regression modules, then the remaining application-manager,
registry, and compatibility owners.

Dependency reads this turn covered the complete StoreBackupPlanner,
BackupArtifactRegistry, SquashfsBackupWorkflow, and SealedArtifactWorkflow files;
repository operations through line 425; the full adjacent planner/repository/
registry/SquashFS/sealed-workflow tests; and the workflow API tests from line 249.
These files still need their own complete docstring passes before manifest entry.
The prototype pipeline and remaining repository/test helpers were not read in full.

The broader metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python remain in scope. Preserve the
runtime-doc consumer cautions and three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
