# Rclone drivers, configured adapters, and regressions — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following the [FTP driver batch](project-docstrings-ftp-driver-2026-09-10.md).
The preceding goal turn completed verified FTP work and read the entire rclone
driver. This continuation confirmed that rclone remained unchanged, then completed
its documentation and the full adapter/test source review. The whole-project goal
remains active and unfinished; there is no overall blocker. Branch remains
`codex/project-docstrings` at `edf6bf05`; no commit or push was made.

## Completed review

- Complete [rclone driver](../src/LiuXin_alpha/storage/drivers/rclone.py), including
  process protocol/reader, local write session, read-only and writable drivers,
  incremental parser, nested inventory/error helpers, metadata and lifecycle helpers.
- All five [read-only compatibility modules](../src/LiuXin_alpha/storage/store_backend_plugins/rclone_http_readonly/):
  exports, Location/FileInfo aliases, configured adapter, and subprocess utilities.
- Both [writable compatibility modules](../src/LiuXin_alpha/storage/store_backend_plugins/rclone_writable/):
  exports and configured writable/native-import adapter.
- Complete [read-only regressions](../tests/storage/store_backend_plugins/rclone_http_readonly/test_rclone_http_readonly_storage_backend.py)
  and [writable regressions](../tests/storage/store_backend_plugins/rclone_writable/test_rclone_writable_storage_backend.py),
  including every nested runner, process, timing, and malformed-response double.

Batch: **10 modules, 16 classes, 188 functions = 214 declarations**.
Cumulative: **385 modules, 484 classes, 4,614 functions = 5,483 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) records all 385 unique
paths. Nine of seventeen raw-driver modules are complete. Dependency excerpts and
the next archive-helper source read do not add files to the completed manifest.

## Contracts clarified

- Root and key parsing preserve their actual normalization and ownership boundaries.
  Existing typed addresses are not reparsed. Rclone URI rendering/parsing uses exact
  text, without URL quoting/decoding. The inline-secret detector recognizes selected
  option names; it is not an exhaustive credential parser or redactor.
- Reads issue cat without stat preflight. A supplied count becomes an exact expected
  byte total; unbounded reads check EOF and process outcome without size evidence.
  Version conditions remain unsupported even for zero-length reads. Completion is
  checked on EOF or a later read after remaining reaches zero, and its marker is set
  before wait/diagnostic failure. Closing a partial read does not validate successful
  completion. Process shape checks cover fewer attributes than later operations use.
- Inventory prefers streamed output with a restricted decoded-list fallback. Ordinary
  spawn failure permits fallback; timeout does not. Stream failure permits it only
  before any yielded value and with a known nonzero process exit, excluding timeouts.
  Counts include skipped and duplicate observations. Duplicate addresses are suppressed
  after lexical prefix filtering, and returned IsDir is not independently checked.
- The incremental parser bounds newly buffered decoded text, which may contain several
  complete elements. It accepts a trailing comma, can parse a split numeric prefix too
  early, and stops reading after the closing bracket. Later stdout and a pending UTF-8
  suffix are not necessarily inspected. Documentation no longer equates these checks
  with strict validation of an exhausted JSON document. Focused observations from
  the FTP handoff already verified those cases with memory process doubles.
- Process cleanup attempts stream closure, terminate, a one-second wait, and kill
  when needed, without a second wait after kill. Many ordinary call failures are
  suppressed, while attribute lookup and BaseException failures may escape. Raw
  completion waits impose no explicit deadline; the configured adapter can supply
  a daemon timer that kills a running process and reports expiry through wait.
- Local staged writes check accepted-byte counters and an optional running digest
  after flush/fsync/close, without rereading staging. Publication uses an instance
  lock, two existence checks, copyto, and moveto. The upload-success marker controls
  remote staging cleanup; partial effects before that marker can remain remotely.
  Final stat happens outside the lock after publication and can fail with destination
  bytes already visible. Abort handles local staging rather than remote rollback.
- Native import compares staging and final reported size/digest. Missing or differently
  named digest evidence is unsupported; size/value mismatch is an integrity failure.
  Remote hashes are not independently computed from streamed bytes. Digest itself
  normalizes nonempty text but validates neither algorithm-specific length nor hex
  syntax; the helper documentation states this explicitly.
- Runtime options are shared and mutable, while raw inventory bounds and durable
  configuration capture construction-time values. Supplied configuration is retained
  without reconciling its root/policy against separate runtime arguments. Durable
  options omit env but retain arbitrary command arguments. Subprocess utility errors
  can include raw command/output text; selective storage error translation belongs
  to their callers.
- The historical global-rate option spaces command starts on one Store instance.
  It does not coordinate other Stores or count individual remote requests. Generated
  TPS arguments apply per process. Slot reservation precedes sleep/command failure,
  and positive infinity is not rejected by rate normalization.
- Native-import compatibility is a root/options heuristic, not an access check.
  Matching remote names can be accepted despite different environments. import_from
  requires a rclone Store and owned Locations but does not call that heuristic;
  placement hints are ignored by the generic adapter.
- Test docs distinguish memory remote effects, real local staging/destination bytes,
  memory-manager records, simulated sleeps, and the real timer around an event-backed
  fake. They identify specific assertion limits instead of inheriting stronger claims
  from historical test names or success markers.

## Verification

- Strict audit and normalizer pass for all ten batch files and all **385 reviewed
  files**. The manifest was expanded only after validation succeeded.
- All ten executable ASTs match HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, numeric policies, and executable statements
  are unchanged. No runtime-documentation exception was added.
- Ruff matches baseline: raw driver 7; read-only exports/Location/FileInfo 1 each;
  read-only adapter 6; utility module 2; writable exports 0; writable adapter 3;
  read-only and writable regression modules 1 each. No new finding or suppression.
- Batch doctests plus FTP/S3/HTTP/filesystem/SQLite, encryption/backed-Store, raw API,
  configuration/errors, advanced ingest, Store-redraft/examples/placement, Store
  ingest, and adjacent rclone reconciliation/Library regressions:
  **522 passed, 309 explicitly skipped integration examples**.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, lint/complexity/production typing, 188 strict
  mypy files, and 37 invalid examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Focused memory-remote observations confirmed a visible destination after final
  stat failure with local staging removed, a commit using accepted-write expectations
  despite altered local staging bytes, remote staging remaining after copy effects
  followed by failure before the upload marker, retained arbitrary durable arguments
  despite env exclusion, same-name import acceptance with different environments,
  and the EOF-check marker preventing a repeated failed completion check.
- Observation streams and local temporary directories were cleaned up, and memory
  staging records were cleared. These observations and regressions do not claim live
  rclone backend, TLS, network, or operating-system publication atomicity.
- The six previously recorded docstring-sensitive ownership-test conflicts remain
  outstanding. This unrelated storage batch did not rerun or weaken those guards.
- Final parameter prose for nested fake runners was clarified without changing
  examples or executable code; batch audit/normalizer and all AST/Ruff comparisons
  passed afterward. All eight local links in this note, three in the main ledger,
  and 113 in the index resolve. Root and data-submodule diffs are whitespace-clean.
  Every verification handle has completed; no check remains pending.

Logs/exit metadata and focused observations:
`working-memory/test-results/docstrings-rclone-driver-2026-09-10-*`.
Earlier parser observations:
`working-memory/test-results/docstrings-rclone-source-read-2026-09-10-observations.json`.
Static reports: `/tmp/liuxin-docstring-rclone-driver-batch-2026-09-10.json`,
`/tmp/liuxin-rclone-driver-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**,
without parse failures. Missing/blank docs: **1,082 modules, 1,937 classes,
21,135 functions = 24,154 declarations**. Overlapping findings: 3,919
delimiter-layout, 10,281 missing-example, 2,953 parameter-field, 3,875 return-field,
3,838 empty parameter descriptions, 5,135 empty return descriptions, and 28 missing
summaries. Categories overlap and do not measure all repaired prose.

## Next archive work

The entire **877-line `storage/drivers/archive_common.py`** was read in this
continuation and confirmed unchanged. It remains unedited and outside the reviewed
manifest. Next document shared archive mechanics and review ZIP/TAR/RAR drivers,
`store_backend_plugins/archive_backends.py` (801 lines), compatibility exports,
and `tests/storage/store_backend_plugins/test_local_archive_storage_backends.py`
(1,143 lines). Those additional sources were located but not fully read here.
The remaining raw-driver scope is archive_common, ZIP, TAR, RAR, 7z, ISO reader,
ISO writer, and SquashFS: eight modules totaling 13,830 current lines. Follow their
actual adapter/test dependencies without reducing the wider project scope.

Carry these shared-helper findings into its documentation:

- Archive records retain facts without extra dataclass validation. Signature/version
  helpers use device, inode, size, mtime_ns, and ctime_ns rather than a content hash.
  Inspection loss reasons use truthy counts and preserve supplied metadata reasons;
  the record itself does not validate nonnegative counts or prove rewrite safety.
- Canonical keys preserve spelling, whitespace, and controls other than NUL. Unicode
  encoding is checked only when max_path_bytes is supplied. That check uses UTF-8
  surrogateescape, accepting its supported low-surrogate byte representation while
  rejecting other malformed surrogates. Depth/path bounds are caller arguments,
  without an independent lower-bound/type validation step.
- Owned member readers retain source/owner before seeking/discarding the requested
  offset. A callable seek is used directly without verifying its result or falling
  back after seek failure. Discard reads have no explicit oversized-chunk check.
  Constructor range validation and failure cleanup belong to the caller's boundary.
  The remaining count is min(available, length) when bounded; inspect driver callers
  before describing how available relates to offset.
- readinto returns zero for zero remaining or zero requested capacity, rejects
  premature EOF/non-byte/oversized output, and does not inspect bytes after the
  declared range. Non-OS source-read errors become integrity errors. Memoryview
  construction and destination assignment failures are outside that read guard.
  Close suppresses ordinary source/owner close calls, but owner attribute lookup or
  BaseException can prevent the final base close.
- Archive write sessions allocate sibling member staging. hashlib setup precedes
  creation, and fdopen follows mkstemp outside its OSError guard. The size policy
  checks the entire offered chunk before writing; accepted counts are accumulated
  without the rclone session's None fallback. Expectations use accumulators rather
  than rereading staging. Commit delegates publication to the concrete driver;
  shared code alone does not establish atomic replacement or rollback guarantees.
- Session commit flush/fsync failures propagate through abort without their own
  local-error translation. Cleanup may follow already successful publication.
  __enter__ returns the session without checking finished state, unlike rclone's
  session. Abort suppresses selected OSErrors, and other failures can prevent the
  finished marker; context exit does not suppress the escaping exception.
- copy_exact requests up to 1 MiB but rejects a response only when larger than the
  entire remaining count, not necessarily larger than that one read request. It
  requires destination.write to return the full count, then reads one trailing
  unit and accepts either empty bytes or None. Unhashable trailing values can raise
  from set membership. Underlying read/write errors are not translated here.
- Parent writability probing creates and attempts cleanup of one sibling file;
  it does not prove replacement permission, future capacity, or durability.
  Directory fsync suppresses OSErrors and does not itself validate that the supplied
  path names a directory. Naming/digest helpers retain their specific normalization
  and error boundaries rather than general safe-path/content guarantees.

Preserve all remaining manager/model/source/test/script/example/inherited and
tracked data-submodule work, the main ledger's runtime-doc consumer cautions, and
its three narrowly verified metadata exceptions.
