# Storage reconciliation docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following [storage adapters and local ingest](project-docstrings-storage-ingest-2026-09-10.md).
The preceding goal turn made verified progress; this continuation completes a
further source-reviewed package and its regression descriptions. The full goal
remains unfinished, with no overall blocker. Branch remains
`codex/project-docstrings` at `edf6bf05`; no commit or push was made.

## Completed review

- All four [storage reconciliation](../src/LiuXin_alpha/storage/reconcile) modules:
  initializer, `models.py`, `store_db_sync.py`, and `squashfs_db_sync.py`.
- Exact legacy exports in [library/unmanaged_disk_ingest.py](../src/LiuXin_alpha/library/unmanaged_disk_ingest.py).
- Complete [local registration tests](../tests/library/test_adding_unmanaged_disk_ingest.py)
  and [rclone Library tests](../tests/library/test_rclone_http_ingest_library.py).
- Complete [rclone registration tests](../tests/storage/reconcile/test_rclone_http_store_db_sync.py),
  [real-tool SquashFS workflow tests](../tests/storage/reconcile/test_squashfs_db_sync.py),
  and [SquashFS helper/persistence contracts](../tests/storage/reconcile/test_squashfs_db_sync_contracts.py).

All private helpers, static methods, nested functions/classes, module introductions,
and report fields in those files are included. Batch: **10 modules, 9 classes,
153 functions = 172 declarations**. Cumulative: **320 modules, 341 classes,
3,561 functions = 4,222 declarations**. The
[reviewed manifest](project-docstrings-reviewed-files.txt) contains the exact file
set, including the earlier tracked data-submodule generator. Earlier native-C
documentation and three verified runtime-doc metadata exceptions remain unchanged.

## Behavior now explicit

- Registration reports are mutable and unchecked. Callbacks receive the live
  report; counters can precede later errors. Duration subtracts wall-clock epoch
  milliseconds without clamping negatives, and `to_dict` recursively copies
  dataclass fields before adding duration.
- Local/rclone registration writes Store, file, and optional primary-link rows
  incrementally. The first matching Store row supplies any reused UUID; the last
  matching file key wins in the initial lookup. None does not clear file metadata,
  and volatile timestamps alone do not trigger an update. Once another value
  changes, those timestamps, including acquired time, can be assigned.
- Local traversal sorts directory/file names but does not independently ensure
  every filename entry is a regular file. Disabling symlink following controls
  directory descent, not file symlinks. Path stat and byte reads are separate
  observations. Remote registration uses the raw Location-key suffix, with
  inventory iteration outside the ordinary per-entry failure guard.
- **Existing digest mismatch retained:** local `_build_file_payload` uses
  `get_file_hash`, which returns SHA-512 hexadecimal text followed by decimal
  byte size, and assigns it to `file_hash_sha256`. Its integrity label records
  calculation rather than comparison with trusted bytes. The existing local
  regression asserts only a nonempty hash. The standalone CLI retains its fixed
  historical SHA256 wording; no executable/help literal was changed in this pass.
  Remote registration instead accepts an inventory claim exactly named `sha256`
  when requested, without independent byte verification in this workflow.
- Ordinary progress-callback failures are swallowed, but report mutations remain.
  Manager bootstrap is attempted after registration when enabled; its returned
  report is ignored and raised Exceptions are recorded. Path wrappers own their
  Database context without adding an all-run transaction.
- SquashFS source resolution allows absolute keys and resolved paths outside the
  declared root. Member-target normalization is lexical and implements only its
  stated subset of path rules. A stored syntactically valid SHA-256 can seed a
  designation snapshot; absent legacy snapshot fields can fall back to current
  source observations, which does not reconstruct historical immutability.
- Designation writes are incremental. Replacement controls target/snapshot
  refresh. The initial file-ID map is not extended after new inserts, so repeated
  new IDs in one request are not promised deduplication. Later collection rejects
  duplicate member targets. Errors can leave earlier designation writes intact.
- Pre-build checking updates current stat/hash observations and compares size and
  digest, not mtime. Those observations do not freeze later source bytes. Setup
  and source-read failures outside the publication guard propagate even when
  strict mode is false.
- Archive construction precedes the legacy-row transaction. Verification uses
  an exact `sha256` stat claim or hashes fallback member bytes. Store/link states,
  verified duplicates, and primary links then use a `BEGIN IMMEDIATE` transaction.
  The archive and report counters survive a later row rollback. A non-strict
  failed Store can coexist with duplicate rows for its verified members.
- Optional manager bootstrap and Asset/Replica adoption occur later, with a
  separate macro transaction. Unchanged archived bytes are Replicas of the source
  Asset, not derivations. Failure can leave the archive and earlier legacy rows
  committed; manager reload after adoption rollback is best-effort. Strict mode
  changes failure reporting, not the atomicity of the complete operation.
- SQL helpers bind values but require trusted interpolated identifiers. Cleanup
  ignores ordinary rollback/close failures; BaseException bypasses the explicit
  rollback handler. Only temporary-manifest deletion is attempted by publication,
  not archive removal.
- Test docs distinguish real SQLite/bytes/tools from JSON and archive doubles.
  Historical test names suggesting hash-mismatch rollback actually exercise
  pre-build source drift; transaction recorders verify calls, and successful
  shared-connection duplicate tests do not inject rollback failures. These
  boundaries are documented without changing any assertions.

## Verification

- Strict audit and normalizer pass for all 10 files and all **320 reviewed files**.
- Executable ASTs match `HEAD` after stripping only leading literal docstrings
  in all 10 files. Signatures, annotations, imports, assertions, and control flow
  are unchanged. No additional runtime-doc exception was needed.
- Ruff code/message multiplicities match baseline per file. Existing findings
  remain: report models 9, registration 42, SquashFS publication 76, real-tool
  workflow tests 1; the other six files have none. No suppression or gate-policy
  change was introduced.
- Standalone registration CLI help and parsed default arguments match extracted
  `HEAD` grammar. Combined fingerprint:
  `eff46a52a28cde792a717b55027e0d10cc654765942c51a5fb098949d6f135e8`.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, selected lint/complexity and production typing,
  188 strict-mypy files, and 37 invalid examples rejected by each checker.
- Batch doctests and registration/SquashFS regressions, plus adjacent native/wget
  registration and SquashFS CLI cases: **160 passed, 115 explicitly skipped
  examples**. Real SquashFS tools were available; no tool-dependent test skipped.
  Two CLI cases emitted Python multiprocessing fork deprecation warnings.
- Final migration, public-documentation, and developer-link contracts:
  **38 passed**. All ten links in this handoff resolve to existing local targets;
  root and data-submodule diffs are whitespace-clean. Every verification handle
  has completed; none is pending.
- The six previously recorded ownership-check conflicts remain unchanged and were
  not rerun for this unrelated source scope. No owner limit or counting rule was
  weakened; this is not an overall blocker.

Logs/exit metadata use `working-memory/test-results/docstrings-reconcile-2026-09-10-*`.
Static reports are `/tmp/liuxin-docstring-reconcile-batch-2026-09-10.json`,
`/tmp/liuxin-reconcile-ast-lint-2026-09-10.json`,
`/tmp/liuxin-reconcile-cli-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work

The whole-project audit still covers **2,730 modules, 4,400 classes, 35,119
functions** with no parse failures. Missing/blank docs: **1,087 modules, 1,989
classes, 21,673 functions = 24,749 declarations**. Further overlapping findings:
4,046 delimiter-layout, 10,310 missing-example, 2,988 parameter-field, 3,918
return-field, 4,055 empty parameter descriptions, 5,483 empty return descriptions,
and 28 missing summaries. Full JSON:
`/tmp/liuxin-docstring-current-2026-09-10.json`.

Next review shared `storage/api/store_api` and `storage/api/store_driver_api`,
then concrete implementations and remaining tests. Dependency excerpts of rclone
and unmanaged-backend constructors and `utils/storage/local/file_properties.py`
do not count as completed documentation. Preserve the full source/test/script/
example/inherited/data-submodule scope, runtime-doc consumer cautions, and six
ownership-guard conflicts recorded in the main ledger.
