# S3 driver, publication contracts, and regression helpers — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following the [HTTP driver batch](project-docstrings-http-driver-2026-09-10.md).
The previous goal turn made verified progress. This continuation completes the S3
implementation and its regression documentation; the full project goal remains
active and unfinished. There is no overall blocker. Branch remains
`codex/project-docstrings` at `edf6bf05`; no commit or push was made.

## Completed review

- Complete [S3 driver](../src/LiuXin_alpha/storage/drivers/s3.py), including the
  client protocol, address record, body reader, staged write session, raw driver,
  and all key/range/version/checksum/error helpers. Its full source read was
  completed in the preceding batch and checked against the unchanged file here.
- Complete [S3 regression module](../tests/storage/test_s3_storage.py), including
  fake client/error records, fixture construction, every test, and nested malformed
  page/body doubles. The entire regression source was read during this batch.

Batch: **2 modules, 12 classes, 124 functions = 138 declarations**.
Cumulative: **370 modules, 453 classes, 4,331 functions = 5,154 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) records all 370 unique
paths. Seven of seventeen raw-driver modules are complete; the ten remaining
drivers and the wider project backlog still require descriptive review.

## Contracts clarified

- The client protocol describes exact request fields and response evidence used
  by the adapter. Its kwargs surface is not a runtime client capability check.
  Optional client close ownership is handled separately from that protocol.
- Construction validates local bounds and key syntax, then creates/prepares local
  staging. Bucket validation does not implement every service naming rule. Probe
  only calls head_bucket and reports writable=True after successful reachability,
  without testing writes. Close cleans an owned temporary directory before optional
  client close; it does not drain sessions, suppress all failures, or reset status.
- Address records/checkers validate ownership without reparsing typed key text.
  Key parsing preserves accepted Unicode spelling, whitespace, most controls, and
  literal percent characters. External URI parsing checks exact bucket/prefix and
  uses strict UTF-8 percent decoding. Allocation suggests a digest/UUID key without
  existence, capacity, or reservation guarantees.
- Sessions account for accepted write prefixes, including short-write return values.
  Commit flushes/fsyncs/closes before checking those accumulators; it does not reread
  staging for expectation checks. fdopen follows mkstemp outside its creation error
  guard. Local abort cannot undo a remote publication and may itself propagate
  failures outside its OSError cleanup cases.
- The instance write lock covers preflight and upload, with final stat outside it.
  CREATE_ONLY sends IfNoneMatch on single put or multipart completion. REPLACE checks
  existence before upload but has no publication-time replacement condition.
  A final stat failure can follow a successfully visible object and leave the
  session finished but not marked committed. This is not a cross-object transaction.
- Single put supplies declared size and SHA-256 without recomputing either from the
  reopened file. Multipart uses sequential chunks, upload ID, and part ETags without
  supplying the accumulated whole-object checksum. Normal publication response
  contents are ignored. Abort is attempted only with a retained upload ID, and
  ordinary abort errors are suppressed.
- Nonzero reads require ContentLength, a readable Body, matching requested range
  framing, and optional version evidence. Zero-length reads bypass remote existence
  and version checks. VersionId and tagged/legacy ETag conditions use their respective
  fields; missing and changed evidence both produce precondition failure. HTTP
  status inside ResponseMetadata is not inspected by read/stat conversion.
- Body readers detect premature EOF while consuming a known length, but do not
  read past the declared count or authenticate bytes. A zero-capacity buffer with
  a positive known remainder reports premature EOF. Existing StorageError read
  failures propagate, while other failures use S3 translation. Close-attribute and
  BaseException failures can still escape best-effort cleanup.
- Page validation enforces the configured root and per-page observation bound but
  does not locally enforce the requested relative prefix or MaxKeys against returned
  entries. Falsey Contents is empty; IsTruncated is truth-coerced. Returned cursors
  are checked even on finished pages, then discarded when not truncated. Full
  iteration separately limits pages/entries and rejects duplicate keys/cursor cycles;
  earlier entries may already have been yielded when a later failure occurs.
- Stat interprets response metadata without reading bytes. SHA-256 decoding requires
  32 decoded bytes but does not examine ChecksumType. Numeric metadata uses int
  coercion, including finite-float truncation. Error extraction can itself fail on
  hostile attributes or status conversion; diagnostic filtering remains selective.
- Test documentation distinguishes real local staging/filesystem bytes, memory
  manager records, and simulated S3 behavior. The fake shares a key namespace,
  derives versions from payload MD5, trusts supplied single-put checksums, omits
  multipart checksums, and ignores completion manifests. The historical multipart
  test name includes abort, but its assertions exercise successful completion only.

## Verification

- Strict audit and normalizer pass on both batch files and all **370 reviewed files**.
- Both executable ASTs match HEAD after removing only leading literal docstrings.
  No runtime-doc exception was added; signatures, annotations, assertions, numeric
  guards, and executable statements remain unchanged.
- Ruff findings match baseline: S3 driver 5, S3 regression module 3. No scope or
  suppression changes were needed.
- Batch doctests plus configured S3 Store examples, HTTP/filesystem/SQLite,
  encryption/backed-Store, raw-driver API, configuration/error, advanced-ingest,
  Store-redraft/example/placement, and Store-ingest regressions:
  **367 passed, 226 explicitly skipped integration examples**.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, lint/complexity/production typing, 188 strict
  mypy files, and 37 invalid examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Focused runtime observations confirmed successful-probe writable status without
  publication, zero-length request bypass, returned prefix/limit handling, ignored
  ResponseMetadata HTTP status, write-accumulator expectation checking, an object
  remaining visible after final stat failure, zero-buffer reader failure, ignored
  ChecksumType, and finite-float length coercion. These are observations of current
  behavior using the memory client, not repaired behavior or live-service proof.
  Temporary stages and body resources were cleaned up. The standalone observation
  script needed the checkout on PYTHONPATH to import the existing test fake.
- A final pagination example now checks next_cursor before requesting another page.
  Package-import source doctests passed afterward: **39 examples attempted, zero
  failures**. The batch audit and AST/lint comparison also passed. The initial
  filename-based `python -m doctest` invocation prepended the driver directory and
  made its `http.py` shadow the standard-library HTTP package. Use normal package
  import plus doctest.testmod, or the established pytest doctest command; no runtime
  import implementation was changed to accommodate that invocation.
- The six previously recorded ownership-test conflicts remain outstanding. This
  unrelated driver batch neither reran nor weakened those guards.
- All five local links in this note, three in the main ledger, and 113 in the
  index resolve. Changed Markdown and root/data-submodule diffs are whitespace-clean.
  Every verification handle has completed; no run is pending.

Logs/exit metadata and observations:
`working-memory/test-results/docstrings-s3-driver-2026-09-10-*`.
Static reports: `/tmp/liuxin-docstring-s3-driver-batch-2026-09-10.json`,
`/tmp/liuxin-s3-driver-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work and FTP handoff

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**,
without parse failures. Missing/blank docs: **1,085 modules, 1,952 classes,
21,311 functions = 24,348 declarations**. Overlapping findings: 3,949
delimiter-layout, 10,296 missing-example, 2,960 parameter-field, 3,883 return-field,
3,858 empty parameter descriptions, 5,183 empty return descriptions, and 28 missing
summaries. These categories overlap; missing-doc counts do not measure repaired prose.

Next document `storage/drivers/ftp.py` and its complete FTP compatibility package
and regression module under `store_backend_plugins/ftp_readonly`, then rclone and
its read-only/writable adapters and tests. The **entire 1,194-line FTP driver** was
read ahead in this continuation but remains unedited and outside the completed
manifest. FTP adapter/test source and the 2,359-line rclone driver have not yet
received a complete source read in this continuation.

Carry these FTP findings into its documentation:

- Options remain mutable. spool_limit_bytes is a SpooledTemporaryFile threshold,
  not a hard object-size bound. Connection settings and inventory limits are read
  from the retained options; root/credential parsing happens at driver construction.
- Roots normalize decoded POSIX paths and render without user information; credentials
  remain on the driver for login. Object URI parsing checks host/scheme/port and root,
  but does not reject user information. URI decoding uses replacement behavior,
  unlike S3 strict UTF-8 decoding. Default FTPS port is 990 and the factory uses
  ftplib.FTP_TLS; describe those actual choices without assuming a TLS mode.
- Probe directly materializes client.mlsd(".") with no inventory limits, name checks,
  or NLST fallback. It is distinct from bounded enumeration. Only unavailable/timeout
  failures become cached unavailable status. Driver close is a no-op because each
  operation normally owns its connection.
- Retrieval downloads into a local spool before returning it. The bounded callback
  stores only the requested prefix of bytes, but transfer continues to completion.
  SIZE evidence determines expected accepted length when available; missing SIZE
  means no final length comparison. Any retrbinary TypeError triggers a retry without
  REST, without resetting already collected bytes/counters. Zero-length retrieval
  still rejects a version condition before creating/returning an empty spool.
- Connected-client setup conditionally calls connect/login/passive/prot_p when those
  methods are callable, then cwd for non-root paths. The context translates exceptions
  from both setup and the yielded operation. Cleanup calls the first callable of
  quit/close and breaks even if that call raises an ordinary exception; it does not
  fall back to close after failed quit. Cleanup attribute lookup can still propagate.
- MLSD listing falls back to NLST for AttributeError, NotImplementedError, or
  ftplib.error_perm. Fallback reduces returned paths to basenames and infers directory
  type with cwd; without pwd it cannot restore a successful directory change.
  Per-directory counts include ignored dot names; recursive counts include directories
  and filtered files. Directory-depth rejection occurs after yielding the directory.
  No inode/unique-fact cycle set is maintained; depth/count bounds limit traversal.
- Stat scans the parent listing and falls back to SIZE, swallowing ordinary SIZE
  failures. The optional integer helper checks set membership before conversion, so
  unhashable values can raise there; overflow conversion is not caught. Modification
  time parsing discards fractional seconds. Permission translation uses 530 first,
  denial wording next, and optional 550-as-missing last; shared filtering is selective.

Preserve the full remaining model/manager/source/test/script/example/inherited and
tracked data-submodule scope, plus the main ledger's runtime-doc consumer cautions
and three narrowly verified metadata exceptions.
