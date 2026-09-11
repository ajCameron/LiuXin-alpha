# SquashFS driver, staging, manifest builder, and regressions — 2026-09-10

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[ISO writer checkpoint](project-docstrings-iso-writer-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, without a commit
or push. Existing unrelated edits and data-submodule work are retained.

Source-reviewed and documented in full:

- [Raw SquashFS driver](../src/LiuXin_alpha/storage/drivers/squashfs.py), including
  the retained process reader, current extraction spool, nested drainers, index,
  topology checks, and all pseudo-record helpers.
- All four modules in `storage/store_backend_plugins/squashfs_readonly`: lazy
  exports, Location alias, configured reader, and standalone JSON manifest builder.
- All three modules in `storage/store_backend_plugins/squashfs_build`: exports,
  Location alias, and complete staging/session/preflight/publication implementation.
- Both complete `tests/storage/store_backend_plugins/squashfs_readonly` modules
  and the complete `squashfs_build` regression module, including nested helpers.
- [Manifest launcher](../scripts/build_squashfs_from_manifest.py), preserving its
  shebang, parser help text, imports, JSON output, and process exit behavior.

Batch: **12 modules, 10 classes, 141 functions = 163 declarations**.
Cumulative reviewed set: **425 modules, 555 classes, 5,319 functions = 6,299
declarations**. The [reviewed manifest](project-docstrings-reviewed-files.txt)
contains 425 unique paths. All seventeen raw-driver modules are documented.

## Contracts clarified

- The current raw read path finishes extracting a member before returning a
  selected range. Empty ranges still check ownership, existence, and requested
  version but skip extraction and its before/after signature checks. The final
  signature check and wrapper construction have no explicit spool-cleanup guard.
- The retained process reader limits its exposed range but does not enforce full
  member size or process success at a range boundary. Its stdout reads have no
  timeout; EOF is marked checked before process wait, so a failed check is not
  retried. Nonzero-exit diagnostics are read without a size cap after wait.
- Current extraction caps offered stdout bytes against indexed size, ignores
  destination accepted counts, and compares offered totals rather than separately
  measuring or hashing the spool. Two drainers capture failures; retained stderr
  is capped. Timeout kill/wait cleanup is unbounded, with separate join limits.
  Temporary allocation/executable lookup precede the main cleanup guard, while
  final pipe-close errors can escape after the successful return path.
- Pseudo-header capture returns only the prefix before the data marker, but its
  final chunk can temporarily hold content bytes. The existing misleading inline
  comment now describes that boundary. Marker presence does not independently
  require a zero process exit. Queue timeout does not bound cleanup or thread
  liveness; missing pipes and thread startup precede the coordinating guard.
- Root pseudo records bypass ordinary count/time/topology checks after requiring
  directory type. Other directories count and validate before omission. Only
  regular files enter the index. Unused fields are not exhaustively checked;
  aggregate-ratio stat errors have no local translation. Escaped LF and an
  unterminated final record are retained by splitting; unescaping rejects only a
  trailing unmatched backslash before later path validation.
- Reader configuration selects its UUID and ignores a separate explicit UUID;
  constructor path/options independently configure the raw driver. The build
  Store instead checks UUID agreement. Neither rehydrates runtime options from
  an already supplied configuration. Timeouts use positivity without a finite
  check; compression ratios explicitly require finiteness.
- Manifest loading preserves spaces, normalizes separators/dot components, checks
  duplicate targets, and resolves source symlinks without directory confinement.
  It does not apply the complete reader policy or detect ancestor/file collisions.
  First-present aliases win even when their value is None.
- The standalone manifest builder can unlink an existing output before a forced
  build fails. It hard-links staged sources when possible, captures commands without
  timeout/output bounds, writes directly at the destination, and reports separately
  observed hashes/sizes. It does not offer candidate validation, fsync, or rollback.
  Launcher `--no-quiet` only removes the tool flag; output remains captured.
- Staging sessions check offered length and trust accepted counts. Commit/abort/
  context exit release their lease even on failure; release is marked before its
  callback. Failed context entry does not release it. Leases protect this instance's
  wrapped mutation paths, not arbitrary raw-driver or filesystem writes.
- Implicit byte/file deduplication can return existing content before validating
  a new payload/source, other write arguments, or seal state when a supplied digest
  matches it. Streaming has no equivalent deduplication. Digest paths omit algorithm
  names; file/stream false-location handling differs from None-specific branches.
- Staging preflight materializes each directory listing before per-entry count
  checks. Directories count, but full key limits apply to files before candidate
  inventory later checks directories. Regular hard links are allowed. Before/after
  hashing signatures detect changes without freezing the tree or binding path opens.
- Seal validates candidate type/link count, names, sizes, SHA-256, and observed
  identity before a separate publication operation. Force replaces; create-only
  hard-links and rejects a raced output. Fsync, adapter creation/startup, and cleanup
  may fail after publication without restoring the old image. Startup's returned
  availability is not checked before retaining built_store. A finally unlink error
  can prevent resetting sealing state. Builder reads/status still describe staging.
- Closing cleans only internally owned staging after delegated driver closure;
  it does not wait for leases or close the returned archive Store. Explicit staging
  and published images remain independently owned.

## Verification

- Strict batch audit and normalizer pass. Both checks also pass for the entire
  **425-file reviewed set**, before the manifest expansion was written.
- All twelve executable ASTs equal HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, runtime strings, and policy
  limits are unchanged. No runtime-documentation exception was added.
- Ruff findings match baseline: raw driver 4, read-only initializer 0, manifest
  builder 15, read-only Location 1, reader adapter 2, manifest tests 1, reader tests
  8, build initializer 0, build Location 1, build implementation 2, build tests 1,
  launcher 2. No new findings, suppressions, or gate changes.
- Full quality runner passed: 159 formatted files, complete annotation coverage
  across 456 modules, 221 protected dependency modules, selected lint/complexity,
  both production type checkers, 188 strict-mypy files, and 37 invalid examples
  rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**
  in 22.51 seconds.
- Selected batch/shared-archive doctests and archive, Store/API, filesystem,
  SquashFS ingest/reconcile/CLI regressions: **426 passed, 436 explicitly skipped
  integration examples**, 162.40 seconds. Two CLI cases emitted the existing
  multithreaded-fork deprecation warning. Skips are not executed integration proof.
- Isolated observations passed: nonzero-exit pseudo-header acceptance, unchecked
  short spool destination writes, one-shot EOF failure handling, reader config/UUID
  precedence, supplied-digest deduplication of incompatible offered bytes, early
  unlink on failed forced manifest build, and visible publication after injected
  fsync failure. The last observation injected candidate creation/validation to
  isolate publication behavior; it does not claim a real valid image was produced.
  Resources and stages were closed; all named artifacts used a temporary directory.
- The six known docstring-sensitive ownership-test failures remain outstanding;
  this batch did not rerun or weaken those unrelated guards. Root and data-submodule
  diff whitespace checks passed.
- All verification process handles completed with observed exit-zero results.
- Local links resolve in the index (113), main ledger (3), and this note (4).
  Final manifest recount confirms 425 unique files, 555 classes, and 5,319
  functions; the launcher shebang is retained.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-squashfs-2026-09-10-{regression,quality,contracts}.{log,done}`.
Focused observations:
`working-memory/test-results/docstrings-squashfs-2026-09-10-observations.json`.
Static reports:
`/tmp/liuxin-docstring-squashfs-batch-2026-09-10.json`,
`/tmp/liuxin-squashfs-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**,
with no parse failures. Missing/blank docs: **1,079 modules, 1,928 classes,
20,848 functions = 23,855 declarations**. Overlapping findings: 3,851 delimiter
layout, 10,240 missing examples, 2,914 parameter fields, 3,842 return fields,
3,613 empty parameter descriptions, 4,758 empty return descriptions, and 28 missing
summaries. Structural counts alone do not establish descriptive completeness.

## Next: remaining local Store compatibility plugins

The remaining backend-plugin source set is twenty modules: the package initializer
and the `on_disk_flat`, `on_disk_calibre_like`, `on_disk_existing_managed_drive`,
and `on_disk_existing_unmanaged_drive` packages. None is newly counted complete.
Full source read-ahead covered the package initializer, flat backend, Calibre-like
backend, managed backend, unmanaged backend, and unmanaged single-file facade.
Other aliases/initializers and all adjacent tests still need review.

Carry the observed implementation boundaries into that next review:

- Flat byte/file conveniences use the supplied digest or compute SHA-256, then
  choose `<digest>.file` on false location values; stream fallback uses None only.
  They do not implicitly deduplicate existing objects.
- Managed defaults allocate below `.liuxin-managed/objects`; its reserved-path
  test uses the `.liuxin-managed/` prefix and does not match the bare directory key.
  Explicit keys still use the ordinary filesystem Store policy.
- Unmanaged construction disables root creation; startup reports a missing root
  unavailable. The single-file
  facade resolves the path, uses its parent as the Store root, and derives a stable
  UUID from the file URI, without statting that particular file during binding.
- Calibre-like placement gives a preferred key priority, otherwise uses title/
  author/id/name/extension hints or falls back to digest/inherited placement.
  Its digest fallback uses `.liuxin/managed_drive`, distinct from the managed
  superclass default. Placement returns a location without reserving it.
- Calibre-like `store_stream` updates optional database metadata only after the
  underlying file write returns. Lookup, fallback attribute assignment, or sync
  can then fail after bytes commit. Missing database/file id/getter/row skips that
  update; bool file IDs reject, while integer IDs are accepted without positivity
  checking. Its setters retain values without validation.

Adjacent tests live under `on_disk_flat`, `on_disk_calibre_like`,
`on_disk_existing_managed`, and `on_disk_unmanaged_drive` in
`tests/storage/store_backend_plugins` (the last two test directory names differ
from production). The remaining manager/API/model, metadata, catalogue/cache,
formats, tests, scripts, examples, inherited, and data-submodule backlog stays in
scope. Preserve the runtime-doc consumer cautions and three narrow metadata
exceptions in the main ledger.
