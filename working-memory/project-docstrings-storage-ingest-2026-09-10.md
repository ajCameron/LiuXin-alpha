# Storage adapters and local ingest docstrings — 2026-09-10

Continuation of the active [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following [remote-HTML discovery and registration](project-docstrings-remote-html-2026-09-10.md).
The project goal remains unfinished. Branch is `codex/project-docstrings` at
`edf6bf05`; this batch adds documentation without changing executable code,
signatures, annotations, assertions, or gate policy. No commit or push was made.

## Completed source review

- All four [native HTML adapter](../src/LiuXin_alpha/storage/store_backend_plugins/native_html_readonly)
  modules: initializer, Location, single-file object, and backend.
- All five [wget HTML adapter](../src/LiuXin_alpha/storage/store_backend_plugins/wget_html_readonly)
  modules: the equivalent four plus compatibility utility exports.
- All three [storage ingest](../src/LiuXin_alpha/storage/ingest) modules:
  initializer, `squashfs_drive.py`, and `mixed_format.py`.
- Complete [SquashFS ingest tests](../tests/storage/ingest/test_squashfs_drive_ingest.py)
  and [mixed-format ingest tests](../tests/storage/ingest/test_mixed_format_ingest.py),
  including archive/database helpers and the nested metadata callback.

Batch: **14 modules, 19 classes, 119 functions = 152 declarations**.
Cumulative: **310 modules, 332 classes, 3,408 functions = 4,050 declarations**.
The [exact reviewed manifest](project-docstrings-reviewed-files.txt) includes
the previously documented tracked data-submodule generator. The separate native
C documentation and three verified runtime documentation exceptions remain as
recorded in the main ledger; this batch needed no additional exception.

## Contracts documented

- Native/wget adapters compose HTTP byte access with separate discovery objects.
  Construction creates their state without fetching; inherited Store lifecycle
  and MRO choose startup/probe behavior. HTTP configuration captures selected
  option values, while discovery retains the mutable options object. The adapter
  records dataclass options rather than blindly retaining caller backend pairs;
  omitted wget environment state is not a general secret-redaction guarantee.
- Compatibility Location, file, and wget utility names preserve the exact
  imported implementations. Wrapper docs distinguish facade behavior from
  discovery implementation and describe separate rate scheduling.
- Both local workflows borrow their manager and perform incremental operations.
  Persistence comes from that manager. Successful Store/Asset/Replica writes and
  accounting can survive later errors or callback failures; neither workflow
  supplies an all-run transaction. Location-presence caches persist between calls
  and adjust creation flags without skipping adoption itself.
- SquashFS discovery uses suffix/magic heuristics, sorted directory snapshots,
  and a LIFO traversal. Completed results receive a final sort, while early
  truncation retains traversal order. Count limits observe an extra candidate.
  Symlink checks and later opens are separate observations.
- SquashFS source Store creation precedes discovery. Existing source kinds are
  checked without the mixed coordinator's additional UNMANAGED-mode requirement.
  Newly failed source setup has best-effort cleanup; prior successful writes
  remain. Image adoption/backing failures can coexist with later member work
  when continuation is enabled. Member hint attributes can replace provenance.
- Mixed ingestion classifies without writes in discovery-only mode. Real runs
  adopt top-level sources before FIFO container processing. Named formats and
  limited magic bytes establish candidates, with structural validation delegated
  to the backend. EPUB/CBZ/CBR expansion is explicit, and synthetic magic tests
  do not establish extractor availability or archive validity.
- Mixed limits cover selected counts, logical member bytes, materialization
  reservations, and cooperative elapsed-time checkpoints. They do not interrupt
  arbitrary calls or bound every directory allocation. Accepted member sizes are
  charged before adoption, including later failures, and nested levels can count
  underlying content again. A scheduling ceiling can refuse new containers while
  remaining top-level source adoption continues.
- Container identities are marked expanded before an attempt, so a later
  duplicate can be skipped after the first attempt failed. Ancestor digests
  detect cycles. Asset/digest resolution and some callbacks sit outside ordinary
  container-error isolation. Reentry detection is a marker, not a lock.
- Nested containers require a local writable CACHE Store. Existing cache checks
  use nondeleted Replica stat size, not a fresh content hash. Reservation release
  does not delete cache files. Equivalent backed-Store lookup uses Asset identity,
  canonical kind, and the complete option mapping, independent of pair order.
  Stable backend timeouts use the total wall budget, not remaining run time.
- Progress callbacks are synchronous; their failure classification depends on
  the enclosing boundary. Logged context/error text is not generically scrubbed.
  Report predicates inspect different fields and do not certify complete ingest,
  bytes verified, or durable publication. Frozen report values remain unchecked.
- Regression docs identify real bytes/extractors, in-memory metadata, SQLite
  manager reload versus database close/reopen, POSIX-only bad-byte filenames,
  specific traversal-path assertions, and event presence versus ordering.

## Verification

- Strict audit and normalizer pass on all 14 batch files and all **310 reviewed
  files**, with no structural findings or parse failures.
- All 14 executable ASTs match `HEAD` after removing only leading literal
  docstrings. Ruff code/message multiplicities match the baseline per file;
  existing outside-scope findings were not suppressed or repaired.
- Adapter/SquashFS doctests and adjacent HTML/local-ingest regressions:
  **108 passed, 81 skipped examples**. Both local SquashFS tools were available
  and the real-extractor tests passed.
- After the mixed-coordinator edits, its doctests/regressions plus ingest-source
  API, initialization/ingest CLI, and operator-hardening selection:
  **93 passed, 92 skipped examples**. These selections overlap; do not report
  their sum as a distinct-test count. Explicit skipped examples need application
  objects, pytest fixtures, or live integration setup.
- Full `bash scripts/run_type_checks.sh` passed after the final source edits:
  159 formatted files, 456 annotation-covered modules, 221 protected dependency
  modules, clean selected lint/complexity and production typing, 188 strict-mypy
  files, and all 37 negative examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts passed again
  after the handoff edits: **38 passed**. All eight relative links in this new
  handoff resolve to existing local targets. Root and data-submodule diffs pass
  whitespace checks. Every verification process has finished; none is pending.
- The six previously observed ownership-test failures remain documented in the
  preceding handoff. Those guards count docstrings toward physical lines/body
  statements. This batch did not change their source scope or policy, and the
  unrelated ownership selection was not rerun. This is not an overall blocker.

Completed command logs and exit metadata live in `working-memory/test-results/`
under `docstrings-storage-adapters-squashfs-2026-09-10-*` and
`docstrings-storage-ingest-2026-09-10-*`.
Static reports are `/tmp/liuxin-docstring-storage-ingest-batch-2026-09-10.json`,
`/tmp/liuxin-storage-ingest-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work

The refreshed whole-project audit still covers **2,730 modules, 4,400 classes,
35,119 functions**. Missing/blank docs: **1,091 modules, 1,994 classes,
21,814 functions = 24,899 declarations**, with no parse failures. Additional
overlapping findings are 4,051 delimiter-layout, 10,326 missing-example,
2,999 parameter-field, 3,929 return-field, 4,056 empty parameter descriptions,
5,484 empty return descriptions, and 28 missing summaries. Full JSON is
`/tmp/liuxin-docstring-current-2026-09-10.json`.

Next continue through storage reconciliation and shared Store/driver APIs,
then concrete implementations and remaining tests. Dependency reads of HTTP
Store/driver code, driver-backed lifecycle, path naming, and ingest-source API
tests do not count as completed documentation. All source, scripts, examples,
tests, inherited code, and tracked data Python remain in scope. Preserve the
runtime-doc consumer cautions and six ownership-guard conflicts in the main ledger.
