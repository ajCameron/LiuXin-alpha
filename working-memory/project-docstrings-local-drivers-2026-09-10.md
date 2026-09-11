# Local drivers and shared failure helpers — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following [concrete Store implementations](project-docstrings-concrete-stores-2026-09-10.md).
The preceding completed goal turn made verified progress. This continuation
documents eleven more complete files, with the full project goal still active
and unfinished. There is no overall blocker. Branch remains `codex/project-docstrings`
at `edf6bf05`; no commit or push was made.

## Completed review

- Driver exports and complete shared `_errors.py`/`_validation.py` helpers.
- Complete [filesystem driver](../src/LiuXin_alpha/storage/drivers/filesystem.py),
  including its address class, range reader, write session, and private helpers.
- Complete [SQLite driver](../src/LiuXin_alpha/storage/drivers/sqlite.py), including
  its address class, spooled write session, publication and connection helpers.
- All four [single-file SQLite compatibility modules](../src/LiuXin_alpha/storage/store_backend_plugins/single_file_sqlite).
- Complete [filesystem regression module](../tests/storage/test_filesystem_storage.py),
  including its digest helper.
- Complete [SQLite compatibility regression module](../tests/storage/store_backend_plugins/single_file_sqlite/test_single_file_sqlite_storage_backend.py).

Batch: **11 modules, 8 classes, 101 functions = 120 declarations**. Cumulative:
**366 modules, 432 classes, 4,122 functions = 4,920 declarations**. Exact paths
are in the [reviewed manifest](project-docstrings-reviewed-files.txt).
Five of the seventeen raw-driver modules are now complete. The six configured
Stores, both eight-module Store/driver API packages, and five storage utilities
remain completed; other driver/model/manager families remain unfinished.

## Contracts clarified

- Error helpers classify selected OS subclasses/errno values and SQLite names/text
  into contextual storage errors, returning exceptions for callers to raise/chain.
  Filtering is selective: backend/operation labels are only stripped, recognized
  URL paths are neither length-limited nor assignment-redacted, and SQLite fallback
  text can retain SQL or values outside the redaction pattern. Invalid URL ports
  are omitted; reconstructed targets are diagnostic text, not round-trip contracts.
- Detail text drops NULs, collapses whitespace, and redacts specific labels with
  assignment delimiters. Truncation appends dots that its final rstrip removes, so
  the resulting message has no truncation marker and small limits can produce empty
  text. Documentation no longer claims exhaustive secret or SQL filtering.
- best_effort_close suppresses Exception from calling close, but attribute lookup
  occurs outside that catch and BaseException subclasses propagate. Unicode checks
  validate strict UTF-8 encoding only. Percent checks require two hexadecimal
  characters without decoding or establishing Unicode/address validity.
- Filesystem address construction and the runtime checker do not perform canonical
  text parsing. Existing typed addresses bypass the text parser's syntax checks.
  Path routing checks current resolved containment but returns the original path;
  subsequent I/O can race with symlink/path changes. Staging names are hidden from
  inventory, not protected by a direct-address access-control boundary.
- The filesystem started flag is not an operation gate. Startup controls optional
  root creation, but direct staging can also create the root. Close does not drain
  streams/sessions. Status is fresh and counts full inventory even without the
  separate first-entry probe; writability uses policy/os.access, not a write test.
- Filesystem writes count/hash accepted input, check expectations at commit, then
  link or replace. CREATE_ONLY uses link creation for collision detection; REPLACE's
  earlier existence check can race with replacement. Staging unlink, directory fsync,
  and final stat happen after publication and can fail without rollback. Abort
  suppresses selected cleanup OSErrors and never removes a published destination.
- A filesystem read checks an explicit version once using the opened descriptor's
  stat fields, without preventing later in-place mutation. Seek/fstat failures lack
  a separate cleanup guard. The limited reader owns its source; a zero-length
  readinto buffer can make read(0) mark the remaining range exhausted.
- Filesystem inventory sorts traversal components, prunes staging directories,
  skips symlinks, and uses lexical startswith prefixes. Missing/non-directory roots
  yield no entries. Inventory metadata is not a snapshot. Direct stat/read can
  follow in-root symlinks despite inventory omission.
- File URI parsing uses filesystem-byte decoding for POSIX surrogateescape support,
  ignores query/fragment components, and resolves the candidate before deriving its
  relative key. Text parsing retains whitespace, controls other than NUL, Unicode
  spelling, and surrogateescaped names. Allocation hints create no reservation or
  capacity guarantee; expected size is ignored.
- Filesystem native copy uses separate stat and unpinned read, checking accepted
  size at destination commit and not retrying partial per-chunk writes itself.
  Native move's source-version and destination-existence checks precede mutation;
  link/unlink can leave both names on failure, final stat can fail after mutation,
  and no directory fsync is performed. Raw write/move modes are not string-coerced.
- SQLite sessions spool above an eight-MiB memory threshold, but commit reads the
  entire spool into memory. Stored SHA-256 and size come from accepted-write
  accounting; an explicit expected digest rereads the spool. Publication then stat
  are separate operations, so stat failure can follow a committed row. Successful
  commit/context exit leaves the spool open; explicit abort closes it without
  deleting the published BLOB.
- SQLite opens connections with a 30-second timeout and foreign-key/WAL/FULL-sync
  settings. The context manager commits/rolls back transactions without explicitly
  closing connections. Setup PRAGMA failures have no explicit close guard. Driver
  close clears its flag rather than closing tracked resources or preventing reuse.
- SQLite startup creates missing schema objects but is not a complete migration or
  schema-validation pass. Probe can create a missing database/change journal mode;
  it is not a read-only operation. Status queries rows and volume capacity separately
  and reports writable=True on success without a write test.
- SQLite reads fetch whole BLOBs even for short/zero ranges, with version and bytes
  selected from one row. Stat and the SHA-256 digest shortcut trust stored metadata
  without verifying current BLOB bytes. Prefix inventory scans all ordered metadata
  rows and filters in Python while its query context remains active.
- SQLite publication and conditional deletion use BEGIN IMMEDIATE, keeping row
  existence/version checks in the mutation transaction. New rows start at version
  one; delete/recreate can reuse that token. An allowed missing deletion succeeds
  before checking a supplied version. Allocation uses only the digest value or UUID
  hex, ignoring expected size/name and not including the digest algorithm in keys.
- SQLite compatibility exports remain exact Location/FileInfo aliases plus a
  SQLiteStore subclass changing only the Store kind. Their tests exercise real
  temporary BLOB databases. Filesystem tests cover selected fixed symlink and normal
  publication cases, without claiming race resistance, crash recovery, or exhaustive
  concurrency behavior. Assertions and executable code were preserved.

## Verification

- Strict audit and normalizer pass on all eleven files and all **366 reviewed files**.
- All eleven executable ASTs match HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, runtime-doc exceptions, and
  implementation statements are unchanged. A final prose-only line-wrap cleanup
  was followed by another batch audit and AST/lint check.
- Ruff findings match baseline: driver exports/errors/validation 1 each, filesystem
  and SQLite drivers 8 each, compatibility exports/backend 0 each, location/file
  aliases 1 each, and both regression modules 1 each. No scope, ceiling, suppression,
  or counting-policy change was needed.
- Batch doctests plus local-driver/SQLite compatibility, encryption/backed-Store,
  HTTP/S3, raw-driver API, configuration, driver-error, advanced-ingest, Store-redraft,
  example-marker, placement-hint, and Store-ingest regressions:
  **343 passed, 167 explicitly skipped integration examples**.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, selected lint/complexity/production typing,
  188 strict-mypy files, and 37 invalid examples rejected by each checker.
- Targeted runtime observations confirmed the limited-reader zero-buffer behavior,
  absent truncation marker, close-attribute lookup propagation, a usable SQLite
  connection after context exit, an open committed spool until explicit abort, and
  version 1 reuse after delete/recreate. The temporary database was cleaned up and
  explicitly retained connections/spools were closed. These observations establish
  documentation accuracy, not repaired behavior.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- All eight local links in this handoff, three in the main ledger, and 113 in
  the index resolve. Changed Markdown and root/data-submodule diffs are
  whitespace-clean. Every verification handle has completed; no run is pending.
- Six previously recorded ownership checks still conflict with physical docstring
  lines/statements. They were not rerun for this unrelated scope, and no guard
  policy or limit was weakened.

Logs/exit metadata and runtime observations:
`working-memory/test-results/docstrings-local-drivers-2026-09-10-*`.
Static reports: `/tmp/liuxin-docstring-local-drivers-batch-2026-09-10.json`,
`/tmp/liuxin-local-drivers-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with
no parse failures. Missing/blank docs: **1,087 modules, 1,964 classes, 21,412
functions = 24,463 declarations**. Overlapping findings: 3,951 delimiter-layout,
10,296 missing-example, 2,960 parameter-field, 3,883 return-field, 3,888 empty
parameter descriptions, 5,237 empty return descriptions, and 28 missing summaries.
This batch fills 23 previously missing function docs and completes many existing
descriptions; whole-project missing counts alone do not describe that work.

Next review HTTP and S3 driver implementations and their complete regression
modules, then FTP/rclone and archive drivers with shared archive mechanics.
There are twelve remaining raw-driver modules. Shared address/model excerpts
were dependency checks, not additional completed files. Do not repeat the
completed driver exports, failure/validation helpers, filesystem/SQLite drivers,
SQLite compatibility package, or their two fully documented regression modules.
Continue through remaining model/manager APIs, source, tests, scripts, examples,
inherited Python, and tracked data-submodule Python. Preserve the main ledger's
runtime-doc consumer cautions and three narrowly verified metadata exceptions.
