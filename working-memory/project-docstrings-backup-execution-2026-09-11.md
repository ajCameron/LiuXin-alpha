# Backup execution and sealed-image provenance — 2026-09-11

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[workflow API checkpoint](project-docstrings-workflow-api-2026-09-11.md).
Branch remains `codex/project-docstrings` at `edf6bf05`, without a commit or push.
Existing unrelated changes and data-submodule work are retained.

Source-reviewed and documented in full:

- `storage/backup/__init__.py`, `store_backup_planner.py`, and
  `squashfs_backup_workflow.py`.
- Both `storage/workflows` modules, including the complete
  `sealed_artifact_workflow.py` and its private helpers.
- `tests/storage/backup/test_store_backup_planner.py`,
  `test_squashfs_backup_workflow.py`, and
  `tests/storage/workflows/test_sealed_artifact_workflow.py`.

Production paths above are relative to `src/LiuXin_alpha`. Complete source reads
from the preceding API batch remained authoritative after verifying all eight
files still matched HEAD before editing; both initializers were read in full here.
The pass includes every helper and the planner's nested flush function. Combined
source grew from 1,870 to 3,098 lines through docstrings only.

Newly completed: **8 modules, 3 classes, 64 functions = 75 declarations**.
Cumulative: **540 modules, 775 classes, 6,395 functions = 7,710 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 540 unique
paths. All 65 storage API, 22 storage_manager implementation, and two
storage/workflows source modules are complete. Three backup implementation modules
and nine other storage source modules remain unreviewed.

## Contracts clarified

- The planner materializes inventory, computes absent digests, and carries the
  last encountered non-deleted Replica at a Location without another health filter.
  It sorts normalized member paths and groups uncompressed estimates, allowing an
  oversized singleton. Duplicate member names are checked when each declaration
  is built. The nested flush appends/reset state only after Location/declaration
  construction succeeds. Plans neither reserve destinations nor pin inventory.
- SquashFS construction can create output/staging directories; routed output
  requires persistent local staging and uses a builder-UUID-specific image beside
  that directory. Source designation captures size and any manager-refreshed Asset
  evidence without hashing local bytes. Location designation uses the first matching
  non-deleted Replica and captures its current catalogue identity.
- Checkpoints remain explicit in-memory values until saved by orchestration.
  Successful staging advances one cursor position and appends one success report;
  a failure leaves that source unadvanced and adds no failure report. Exceptions
  become FAILED, while BaseException propagates and can leave RUNNING state.
  Restored FAILED state requires an explicit run_next retry. Cancellation preserves
  staging, reports, and any physical output.
- Existing staged bytes are reused after checking only available declaration
  expectations. With neither expected size nor digest, stat alone suffices and
  the original source need not exist. Routed staging can verify a current stat
  digest when the declaration omitted one; digest_verified still reports false
  because its predicate tracks only the declaration's expected digest.
- Finalization can seal, check reader availability, publish, and request cleanup
  in one call. An existing image needs a recorded SEAL_ARTIFACT milestone; builder
  failure after publication can leave bytes without that evidence, causing retry
  to reject adoption. A routed existing output is accepted only after a matching
  SHA-256 comparison. Hash/stat/read operations are not version-pinned.
- Output assignment precedes best-effort staging removal. CLEANUP records the
  attempt even if rmtree ignores errors and files remain. The workflow does not
  register an artifact Store or record Asset provenance during finalization.
- Sealed-image cataloguing materializes source pairs, rejects duplicate
  stringified paths before lookup, refreshes Asset records, and captures recipe
  input evidence before adoption/ingest. Final recipe and derivation construction
  occur afterward: duplicate dependency names or invalid provenance can leave
  a catalogued image without the requested derivation.
- Equal same-output provenance is reused before workflow conflict checks. The
  read-then-record sequence is not locked; conflicts are scoped to that output.
  Backup overrides compare exact path order but defer Asset validation and do not
  prove source bytes match archive members. Namespaced backup workflow references
  remain distinct from catalogue workflow IDs.
- Tool pinning hashes a PATH-resolved or directly named local file without
  executing it or requiring a direct file's executable bit. URI parsing uses a
  file URI's decoded path only, ignoring its authority/query/fragment. JSON uses
  compact sorted keys and standard permissive NaN/Infinity behavior. RAR compression
  accepts values equal to members of range(0, 6), without normalizing a float/bool
  before interpolation into the command. Commands remain recorded intent.
- Tests now describe their actual verification boundaries: real filesystem and
  catalogue work around opaque image markers and synthetic tool references.
  Successful SquashFS tests substitute both building and candidate validation.
  The persistence test reconstructs a manager against the same open Database;
  it does not reopen the database or restart a process.

## Verification

- Strict structural audit and normalizer passed for all eight files and the
  **540-file reviewed set**. Independent AST census confirms 775 classes, 6,395
  functions, manifest uniqueness, and the remaining storage inventory.
- All eight executable ASTs match HEAD after stripping only leading literal
  docstrings. Signatures, annotations, exports, runtime literals, assertions,
  and guard limits are unchanged. No runtime-doc exception was added. Ruff remains
  **6 to 6**, with no new finding.
- Production/test doctests and adjacent workflow/backup/manager/archive regressions:
  **156 passed, 142 explicitly skipped integration examples, 1 failed**, 148.36s.
  The sole failure remains
  `test_storage_manager_composition.py::test_manager_module_stays_a_small_composition_root`:
  `_policy_support.py` 1,213, `_support.py` 1,139, and `_contracts.py` 902 exceed
  the unchanged 900-line ceiling. No newly edited file is an offender.
- Full quality runner passed: 159 formatted files, annotations across 456 modules,
  221 protected dependency modules, lint/complexity, both production type checkers,
  188 strict-mypy files, and 37 invalid examples rejected per checker.
- Migration/public-documentation/developer-link contracts: **38 passed**, 45.04s.
- **20 isolated observations** passed on their first execution. These combine
  explicit doubles with real staging/filesystem writes and transient manager
  ingest. Checks cover pure helper normalization limits, provenance reuse ordering,
  staged reuse/evidence flags, BaseException state, post-publication seal failure,
  cleanup-attempt reporting, existing routed-output digest checks and reader
  closure, late duplicate-dependency failure after ingest, and record refresh.
  No external archive tool or live remote service was invoked. Temporary files and
  the transient manager were cleaned up.
- Final source review refined three parameter descriptions: the RAR wrapper always
  needs an executor, and _catalogue_bytes requires supplied mode/metadata rather
  than deriving defaults. The complete batch audit, normalizer, executable-AST,
  and lint comparison passed afterward. No examples or executable statements
  changed after the broader checks, so those runs were not repeated.
- All verification processes completed with observed terminal exits. The **seven**
  known docstring-sensitive pytest guard failures remain unresolved: the six earlier
  Core/CLI/terminal failures and manager composition above. The earlier six were
  not rerun; no guard was weakened or deselected.
- Root and data-submodule whitespace checks passed. All 119 local link targets
  across the index, main ledger, and this handoff resolve; the three notes have no
  trailing whitespace. The existing data-generator edit and untracked submodule
  bytecode directory remain intact.

Durable results:
`working-memory/test-results/docstrings-backup-execution-2026-09-11-{regression,quality,contracts}.{log,done}`
and `docstrings-backup-execution-2026-09-11-observations.json` in that directory.
Static reports: `/tmp/liuxin-docstring-backup-execution-batch-2026-09-11.json`,
`/tmp/liuxin-backup-execution-ast-lint-2026-09-11.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-11.json`.

Whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, no parse
failures. Missing/blank documentation remains **1,073 modules, 1,908 classes,
20,522 functions = 23,503 declarations**, down 60 in this batch. Overlapping
findings: 3,647 delimiter layout, 9,810 examples, 2,798 parameter fields,
3,685 return fields, 3,196 empty parameter descriptions, 4,174 empty return
descriptions, and 28 missing summaries.

## Next

Complete the remaining concrete backup repository, artifact registry, and
prototype pipeline, plus their full regression helpers. Previous partial
repository reads and complete registry/test dependency reads still need their own
documentation passes; the prototype implementation has not been read in full.

Storage has **12 unreviewed source modules**: those three backup files and
backend_registry, location, migrations, single_file, storage_types, store_container,
store_factory, store_manager, and store_spec_utils. Continue through those owners
and the broader metadata, catalogue/cache, formats, source/test/script/example,
inherited code, and tracked data-submodule Python. Preserve runtime-doc consumer
cautions and the three narrow exceptions in the
[main ledger](project-docstrings-2026-09-08.md).
