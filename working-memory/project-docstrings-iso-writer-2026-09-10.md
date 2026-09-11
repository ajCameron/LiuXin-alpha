# ISO writer, adapter, and regressions — 2026-09-10

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[ISO reader checkpoint](project-docstrings-iso-reader-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, with no commit
or push. Existing unrelated edits and tracked data-submodule work are retained.

Source-reviewed and documented in full:

- [Raw ISO writer](../src/LiuXin_alpha/storage/drivers/iso_writer.py): passive
  source/layout records, staged session, mutation policy, publication, complete
  metadata/layout construction, copying, naming, times, and synchronization.
- [Configured writable ISO Store](../src/LiuXin_alpha/storage/store_backend_plugins/iso_writable/iso_writable_storage_backend.py),
  [Location alias](../src/LiuXin_alpha/storage/store_backend_plugins/iso_writable/iso_writable_location.py),
  and [plugin initializer](../src/LiuXin_alpha/storage/store_backend_plugins/iso_writable/__init__.py).
- [Complete writable ISO regressions](../tests/storage/store_backend_plugins/iso_writable/test_iso_writable_storage_backend.py)
  and their package initializer: 27 tests plus three nested builder/concurrency
  callbacks. Injected free-function `self` parameters remain documented.

Batch: **6 modules, 8 classes, 103 functions = 117 declarations**.
Cumulative: **413 modules, 545 classes, 5,178 functions = 6,136 declarations**.
The [exact reviewed manifest](project-docstrings-reviewed-files.txt) contains
413 unique paths. Sixteen of seventeen raw-driver modules are complete.

## Contracts clarified

- Session expectations describe accepted writes and incremental digest state.
  Commit subsequently stats and opens the staged pathname, without independently
  verifying it against those earlier expectations. Digest construction precedes
  file allocation; fdopen follows allocation outside a compensating cleanup guard.
- Sessions stage independently; an instance lock serializes mutation publication.
  Inspection is checked before staging and again at commit. Destination collision
  checks use enum identity without general coercion/validation. Missing-ok deletion
  returns before version comparison but after metadata-loss policy.
- Native copy/move preserve lazy version-conditioned source openers, and same-key
  operations still follow mode policy. Final stat occurs after publication. Allocated
  keys are neither reserved nor checked for existence; digest depth can choose a
  flattened algorithm/value component.
- Detected loss blocks normalization unless enabled. Empty loss evidence is not
  proof every feature is modelled. The writer disables UDF selection and inventories
  unsafe direct entries as omissions so they can block rebuilding.
- Candidate preflight counts regular sources; the reader later counts all entries.
  Key/size verification is not payload hashing. The final stat and os.replace remain
  separate operations. File/parent fsync, cache reset/reindex, final stat, or staging
  unlink can fail after a new image is visible; no universal rollback is promised.
- Empty creation hard-links without replacing a raced target and does not validate
  that raced file. Parent creation can precede later errors. Candidate cleanup is
  not silently suppressed; directory fsync errors propagate when O_DIRECTORY exists.
- Metadata trees/tables/directories are held in memory; payloads are streamed. The
  builder assumes validated source keys/policy, assigns stable primary aliases,
  conditionally emits Joliet, packs continuation bytes, and assigns shared extents.
  Metadata and payload output counts are not independently checked.
- `_copy_exact` checks source exhaustion and a final truthy EOF probe, but trusts
  chunk shape/length and ignores output accepted counts. It computes no digest.
  Source factories remain responsible for suitable context-managed binary streams.
- Primary directory sizing conservatively reserves root SP/ER space for non-root
  directories too. Rendering trusts assigned sizes; bytearray slices do not independently
  prevent growth. Timestamp policy differs between self/parent, child directory, and
  child file records. Year clamping retains other calendar fields without revalidation.
- Rock Ridge components use surrogateescape and a 255-byte limit; Joliet rejects
  surrogate code points and counts file suffixes in its 128-byte limit. Name hints
  of 181–255 encoded bytes can be shortened to 180, but initially oversized hints
  fail the encoder before trimming. Volume IDs uppercase before ASCII filtering.
- Supplied Store configuration retains its fields and enforces UUID agreement.
  Runtime path/options are separate. Default absent-image creation is disabled for
  read-only configuration, while an explicit flag overrides that default. Facade
  read-only policy does not remove operations from the exposed raw driver.
- Regression docs distinguish real image behavior, synthetic descriptor/link
  evidence, injected builder races, same-sequence deterministic output, namespace
  selection through the project reader, and optional external format recognition.

## Verification

- Strict batch audit and normalizer pass. Both checks passed across all **413
  reviewed files** before the manifest expanded.
- All six executable ASTs equal HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, runtime strings, and limits are unchanged.
  No new runtime-documentation exception was introduced.
- Ruff findings equal baseline: writer 7, three adapter modules 1 each, test module
  2, test initializer 0. No new findings, suppressions, or gate changes.
- Selected writer/reader/shared-archive doctests and local archive, ISO reader,
  SquashFS, filesystem, Store/API/configuration/error/ingest regressions:
  **383 passed, 334 explicitly skipped integration examples**, 50.86 seconds.
  Skipped examples are not executed integration proof.
- Full quality runner passed: 159 formatted files, complete annotation coverage
  across 456 modules, 221 protected dependency modules, selected lint/complexity,
  both production type checkers, 188 strict-mypy files, and 37 deliberately invalid
  examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Isolated observations confirmed unchecked short destination writes in `_copy_exact`,
  the name-hint initial-size boundary, and enum-identity collision behavior. A real
  staged write accepting/hash-checking `book` with expected size 4 published externally
  changed stage bytes `altered` with size 7. Injected fsync failure after replacement
  left the new member readable while the session was finished but not committed.
  These are recorded boundaries, not implementation changes.
- Named observation artifacts were scoped to a temporary directory; streams and
  stages were explicitly closed/aborted. No external archive creator was required
  by those isolated observations.
- The six recorded docstring-sensitive ownership-test failures remain outstanding.
  This batch did not rerun or weaken those unrelated guards.
- All verification handles completed with observed results. Root and data-submodule
  diffs are whitespace-clean. Local links resolve in the index (113), main ledger
  (3), and this note (7). The raw SquashFS source is unchanged from HEAD.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-iso-writer-2026-09-10-{regression,quality,contracts}.{log,done}`.
Focused observations:
`working-memory/test-results/docstrings-iso-writer-2026-09-10-observations.json`.
Static reports:
`/tmp/liuxin-docstring-iso-writer-batch-2026-09-10.json`,
`/tmp/liuxin-iso-writer-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with
no parse failures. Missing/blank docs: **1,082 modules, 1,928 classes, 20,950
functions = 23,960 declarations**. Overlapping findings: 3,875 delimiter layout,
10,256 missing examples, 2,922 parameter fields, 3,852 return fields, 3,625 empty
parameter descriptions, 4,785 empty return descriptions, and 28 missing summaries.
Structural counts do not establish descriptive completeness.

## Next: SquashFS

The entire **1,393-line raw SquashFS driver** has now been read. It is unchanged
and outside the completed manifest; its full adjacent tests and remaining adapters
still need review. Complete those next, including the build-once backend. The wider
source/test/script/example/inherited/data-submodule backlog remains in scope.

Carry these source findings forward:

- Current open_read stages full members, not the retained `_SquashfsProcessReader`
  path. The latter still needs complete docs: it skips leading bytes, limits exposed
  bytes, waits only at EOF, marks EOF checked before waiting, and reads unbounded
  diagnostics only after a nonzero exit. Its stdout reads lack a timeout; early-range
  completion need not validate process status. Cleanup can skip later steps on error.
- Constructor timeout uses a positivity check without finiteness checking. Probe
  catches StorageUnavailable/StorageTimeout into a failed cached status; other typed
  policy/integrity errors propagate. This differs from previous raw archive probes.
- Current member extraction uses two drain threads. Offered output length is capped
  against indexed size but destination accepted counts are ignored; final equality
  checks offered bytes rather than independent spool length or a checksum. Initial
  TemporaryFile and executable lookup precede the main cleanup guard. A BaseException
  guard cleans extraction failures, but pipe-finally errors can escape after success.
- Wait timeout is followed by unbounded kill/wait cleanup; pipe joins have separate
  two-second limits. Nonempty reads compare path signatures before/after staging,
  with no explicit cleanup guard around the final signature check or reader wrapper.
- Inventory captures 64 KiB stdout chunks through the pseudo-data marker. It can
  temporarily buffer content bytes from the chunk containing that marker while
  returning only the header prefix. Header caps and partial-marker timing are branch
  specific. Success does not independently require a zero process return code.
- Inventory queue timeout does not bound cleanup: terminate/wait can escalate to an
  unbounded kill/wait, then pipes close and threads join. Header thread exceptions
  enter the one-slot queue; stderr thread exceptions do not. Final joins do not test
  liveness. Missing pipes are rejected before the normal cleanup guard.
- Root pseudo records bypass count/time/topology checks after requiring type D.
  Other directories count and validate names/times before omission. Only type R
  files are accepted; size is field 5, with extra fields otherwise not fully validated.
  Aggregate ratio stats the archive without a local OSError translation guard.
- Pseudo record splitting preserves escaped LF and also yields an unterminated final
  record; it ignores empty lines. Field splitting uses the first unescaped ASCII
  space, then ordinary byte whitespace splitting. Unescaping removes any backslash
  before the next byte and rejects a trailing escape. The standalone canonical-key
  helper has no configured depth/byte cap; current driver parsing uses archive-common.

Preserve runtime-doc consumer cautions, the three narrowly checked metadata
exceptions, and the unresolved ownership-test accounting issue.
