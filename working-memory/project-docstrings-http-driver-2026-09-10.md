# HTTP driver, response contracts, and regression helpers — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following the [local-driver batch](project-docstrings-local-drivers-2026-09-10.md).
The preceding orientation turn revalidated the existing checkpoint without editing
source. This continuation makes verified documentation progress; the full project
goal remains active and unfinished. No overall blocker exists. Branch remains
`codex/project-docstrings` at `edf6bf05`; no commit or push was made.

## Completed review

- Complete [HTTP driver](../src/LiuXin_alpha/storage/drivers/http.py), including its
  response protocol, owned reader, address class, lifecycle, discovery, request,
  range/version, URL-normalization, and diagnostic helpers.
- Complete [HTTP regression module](../tests/storage/test_http_storage.py), including
  its request recorder and every nested failure/response double.
- Reread the already completed [HTTP Store facade](../src/LiuXin_alpha/storage/stores/http.py)
  and corrected its inventory callback parameter: providers yield absolute object
  URLs under the configured root, not relative keys.

Newly completed: **2 modules, 9 classes, 85 functions = 96 declarations**.
Rechecked batch including the existing facade: **3 modules, 10 classes, 92 functions**.
Cumulative: **368 modules, 441 classes, 4,207 functions = 5,016 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains the exact
368 unique paths. Six of seventeen raw-driver modules are now completed.

## Contracts clarified

- Address records and runtime checkers validate subtype/UUID ownership without
  repeating canonical text parsing. Existing typed addresses bypass the text
  parser. A manually constructed absolute value can replace the root during URL
  rendering. Normal text/URI parsing performs the documented scope checks.
- Root normalization preserves explicit ports, existing percent-escape spelling,
  and root dot segments. Relative keys reject malformed escapes, decoded path
  controls/backslashes, empty/dot components, and selected sensitive query names.
  Accepted non-UTF-8 octets stay opaque percent escapes. Query filtering classifies
  known names and prefixes, not all values or possible credential labels.
- Request pacing reserves monotonic slots under a lock and sleeps outside it;
  failed requests consume slots. Timeout is passed to the opener, not applied as
  an enumeration or rate-wait deadline. Requests are not retried.
- Final status and URL validation occurs after the opener returns. The default
  transport may already have followed redirects; validation does not prevent
  that traffic. Other objects inside the root are allowed. A final root path
  bypasses object-query checks, though fragments remain rejected.
- Probe health and enumeration are separate. A successful callback/root HEAD can
  produce available=True and object_count=None when inventory fails. Only health
  StorageUnavailable/StorageTimeout failures are converted into unavailable status.
  Status is cached; driver close neither drains readers nor resets that cache.
- Inventory callbacks supply absolute URLs and always represent partial discovery.
  Every observation consumes the bound before scope parsing, prefix filtering,
  or deduplication. Prefix matching is lexical; address equality preserves escape
  spelling. Entries provide advisory names/types without stat evidence. Provider
  iterators are not explicitly closed by the driver.
- Stat falls back from unsupported HEAD to a one-byte ranged GET. A known
  Content-Range total takes precedence; an unknown total falls back to
  Content-Length, potentially the partial body's size. Stat does not run the read
  range-consistency checks. Exact-key Last-Modified access differs from the helper's
  case-insensitive ETag lookup. Metadata is not verified against body bytes.
- Zero-length reads check ownership and nonnegative ranges, then return empty
  BytesIO without checking remote existence or the requested version. Other reads
  validate range headers and explicit ETag evidence before returning an owned
  reader. Missing ETag evidence differs from a changed version.
- Known response lengths detect premature EOF during consumption but stop at the
  declared count without inspecting extra response bytes. An empty readinto buffer
  with a positive known remainder invokes read(0) and reports premature EOF.
  Unknown-length full responses use transport EOF; neither path authenticates
  content. Ordinary close-call exceptions are suppressed, while close-attribute
  lookup and BaseException failures can still propagate.
- Test documentation states the actual evidence: deterministic response objects,
  selected pathological inputs, finite duplicate-feed bounds, and ordinary hostile
  close calls. It does not claim live server interoperability, redirect prevention,
  exhaustive redaction, or every possible cleanup failure.

## Verification

- Strict audit and normalizer pass on all three batch files and all **368 reviewed
  files**, with no missing fields, summaries, examples, or delimiter findings.
- All three executable ASTs match HEAD after removing only leading literal
  docstrings. No runtime-doc exception was added; signatures, annotations,
  assertions, executable statements, and gate policy remain intact.
- Ruff findings match baseline: HTTP driver 7, facade 0, HTTP tests 2.
- Batch doctests plus HTTP/S3, filesystem/SQLite, encryption/backed-Store, driver
  API, configuration/error, advanced-ingest, Store-redraft/example/placement, and
  Store-ingest regressions: **367 passed, 145 explicitly skipped integration examples**.
- The first run found one new fixture doctest constructing Request without the
  explicit method attribute expected by that test helper. The example now passes
  method="GET". The same complete regression selection passed afterward, followed
  by another batch audit and AST/lint comparison.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, lint/complexity/production typing, 188 strict
  mypy files, and 37 invalid examples rejected by each checker. The subsequent
  doctest-only correction is outside the default gate scope.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Focused runtime observations confirmed typed-address rendering, no requests for
  zero-length reads, available-with-unknown-count probing, zero-capacity reader
  failure, no read beyond a declared body length, unknown-range size fallback,
  accepted root-response query text, and empty Content-Type label behavior. These
  establish documentation accuracy; they do not repair the observed behavior.
- The six previously recorded ownership-test conflicts remain outstanding; this
  unrelated driver batch neither reran nor weakened those guards.
- A final prose clarification records ValueError from malformed URL splitting
  and the fixture's explicit request-method requirement. The subsequent batch
  audit and AST/lint check passed; executable examples were unchanged.
- All six local links in this note, three in the main ledger, and 113 in the
  index resolve. Root and data-submodule diffs are whitespace-clean. Every
  verification handle has completed; no run is pending.

Logs/exit metadata and observations:
`working-memory/test-results/docstrings-http-driver-2026-09-10-*`.
Static reports: `/tmp/liuxin-docstring-http-driver-batch-2026-09-10.json`,
`/tmp/liuxin-http-driver-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

## Remaining work and S3 handoff

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**,
without parse failures. Missing/blank docs: **1,086 modules, 1,959 classes,
21,370 functions = 24,415 declarations**. Overlapping findings: 3,950
delimiter-layout, 10,296 missing-example, 2,960 parameter-field, 3,883 return-field,
3,858 empty parameter descriptions, 5,195 empty return descriptions, and 28 missing
summaries. Counts of missing docs alone do not measure repaired existing prose.

The next implementation is `storage/drivers/s3.py`, followed by the complete
`tests/storage/test_s3_storage.py`. The entire 2,270-line S3 driver was read during
this continuation, but its documentation is not yet rewritten and it is **not**
in the completed manifest. Its regression module has not yet received a full
source read. Both files remain unchanged from HEAD.

Carry these source-review findings into S3 documentation:

- Construction creates or prepares local staging storage. Probe calls head_bucket
  and reports writable=True on successful reachability without a write probe.
  Close cleans an owned temporary directory before optionally closing the client;
  failures are not generally suppressed and it does not drain sessions.
- Typed-address input bypasses text canonicalization. S3 keys retain whitespace,
  most controls, Unicode spelling, and literal percent text. URI parsing decodes
  escapes with strict UTF-8, unlike the HTTP opaque-octet path. Bucket validation
  is narrower than complete service naming validation.
- Write sessions hash accepted writes, then flush/fsync/close before expectation
  checks and publication; expectations do not reread staged bytes. fdopen follows
  mkstemp outside the creation error guard. Abort removes local staging only.
  Publication can succeed before final stat fails; cleanup does not roll it back.
- The driver write lock covers preflight and upload within one instance, with final
  stat outside it. CREATE_ONLY sends IfNoneMatch on single put or multipart complete.
  REPLACE checks existence earlier but supplies no atomic replacement condition.
  Multipart cleanup attempts abort only after obtaining a nonempty upload ID and
  suppresses ordinary abort exceptions. Completion response content is ignored.
- Zero-length reads bypass remote existence/version checks. Other reads require
  ContentLength and matching optional range/version fields, but do not inspect
  ResponseMetadata HTTP status. Versions use VersionId first, then tagged ETag;
  missing/changed conditional evidence raises StoragePreconditionFailed.
- Inventory pages validate entries against the configured root, but do not locally
  enforce the requested relative prefix or MaxKeys against returned entries.
  Per-page limits count observed contents; whole iteration has separate page/entry
  limits, duplicate checks, and repeated-cursor checks. Earlier entries can already
  have been yielded when later failure occurs. Continuation is not a snapshot.
- SHA-256 metadata is decoded as a 32-byte checksum without examining ChecksumType
  or hashing the body. Numeric fields use int conversion, including normal coercion
  behavior. Error extraction itself can fail on hostile attributes/status conversion;
  shared diagnostic filtering is selective. Do not promise unconditional translation
  or exhaustive secret filtering.

Then continue through FTP/rclone, shared archive mechanics and archive drivers,
remaining storage models/managers, and the full source/test/script/example/inherited
and tracked data-submodule backlog. Preserve the main ledger's runtime-doc consumer
cautions and three narrowly verified metadata exceptions.
