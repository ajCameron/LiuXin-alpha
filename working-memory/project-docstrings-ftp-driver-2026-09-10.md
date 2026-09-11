# FTP driver, compatibility Store, and regressions — 2026-09-10

Continuation of the [whole-project descriptive reST pass](project-docstrings-2026-09-08.md),
following the [S3 driver batch](project-docstrings-s3-driver-2026-09-10.md).
The intervening orientation checked the actual branch, reviewed manifest, pending
FTP edits, and executable ASTs. This continuation finishes and validates that
FTP work. The full project goal remains active and unfinished, with no overall
blocker. Branch remains `codex/project-docstrings` at `edf6bf05`; no commit or push
was made.

## Completed review

- Complete [FTP driver](../src/LiuXin_alpha/storage/drivers/ftp.py): option/address
  records, raw driver, nested transfer callbacks, connection/listing/walk helpers,
  and key/fact/error conversion.
- All three [FTP compatibility modules](../src/LiuXin_alpha/storage/store_backend_plugins/ftp_readonly/):
  package exports, the exact Location alias, and the configured Store adapter.
- Complete [FTP regression module](../tests/storage/store_backend_plugins/ftp_readonly/test_ftp_readonly_storage_backend.py):
  node/client doubles, fixture builders, every test, and all nested pathological
  clients. The full test source was read again in this continuation before editing.

Batch: **5 modules, 15 classes, 95 functions = 115 declarations**.
Cumulative: **375 modules, 468 classes, 4,426 functions = 5,269 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) records all 375 unique
paths. Eight of seventeen raw-driver modules are complete. Source reads alone do
not add dependency files or the next driver to this manifest.

## Contracts clarified

- Driver options remain mutable and shared, while Store configuration snapshots
  non-callback fields. A zero spool threshold disables automatic size-based rollover;
  the threshold is not a hard size limit. Construction normalizes endpoint/root
  data without connecting. Public root/object URLs omit user information; alternate
  user information in an accepted object URI does not replace configured login.
- Address records/checkers establish ownership without reparsing typed key text.
  Text parsing validates FTP path components. Root/object URI decoding uses URL
  replacement behavior, and root scoping is a textual path contract rather than
  a remote filesystem containment proof. FTPS uses port 990 by default and the
  stdlib FTP_TLS factory; documentation does not infer a different TLS mode.
- Probe directly materializes MLSD and has no inventory limits, name validation,
  or NLST fallback. Only unavailable/timeout errors become unavailable status.
  Successful probe establishes read-only reachability rather than a file count.
- Reads finish retrieval into a caller-owned spool before returning. A bounded
  collector retains only the requested slice while transfer continues. SIZE
  evidence enables a final length comparison; it neither authenticates content nor
  proves REST was honored. Zero-length reads bypass connection/existence checks,
  but a version condition is still rejected first.
- Any retrbinary TypeError triggers the no-REST retry without clearing previously
  accepted bytes/counters. The retry callback discards the offset locally; entirely
  discarded chunks bypass the collector's byte-type validation. Retrieval failure
  attempts spool cleanup before propagating, with the actual exception boundaries
  described rather than claiming infallible cleanup.
- Each connected operation normally creates a client, optionally invokes supported
  setup methods, selects the root, and translates setup/body errors. Cleanup tries
  the first callable of quit/close and stops even after an ordinary quit failure;
  it does not then fall back to close. Attribute lookup and BaseException failures
  can still escape cleanup.
- Inventory bounds count observed names/entries, including ignored dots, directories,
  and filtered files at their respective stages. Recursive directory entries are
  yielded before depth rejection. MLSD failure can restart listing through NLST;
  that fallback reduces names to basenames and probes directories with cwd. Without
  pwd, a successful directory probe leaves the client's directory changed.
- Stat uses parent listing and optional SIZE evidence, preserves advisory unique
  tokens and selected facts, and does not inspect body bytes. Fact conversion and
  diagnostic helpers document their actual coercion/exception limits. The configured
  Store reports spool delivery and inspection metadata for ingest; small spools
  may remain in memory. Its root_path is a remote POSIX path, and its name helper
  is a path sanitizer rather than an arbitrary URL credential redactor.
- Test documentation separates simulated FTP/TLS behavior, real local destination
  bytes, and memory-manager records. It explicitly identifies narrower assertions:
  digest representation length, the prot_p marker, and timeout propagation rather
  than direct inspection of spool cleanup in that historical test.

## Verification

- Strict audit and normalizer pass on all five batch files and all **375 reviewed
  files**. The manifest was expanded only after validation succeeded.
- All five executable ASTs match HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, numeric limits, and executable
  statements remain unchanged; no runtime-documentation exception was added.
- Ruff matches baseline: driver 8, package exports 1, Location alias 1, configured
  backend 2, regression module 2. No new finding, suppression, or gate change.
- Batch doctests plus S3/HTTP/filesystem/SQLite, encryption/backed-Store, raw-driver
  API, configuration/errors, advanced-ingest, Store-redraft/examples/placement,
  and Store-ingest regressions: **434 passed, 203 explicitly skipped integration
  examples**. This is local/fake-service evidence, not a live FTP/TLS run.
- Full quality runner passed: 159 formatted files, 456 annotation-covered modules,
  221 protected dependency modules, lint/complexity/production typing, 188 strict
  mypy files, and 37 invalid examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Focused memory-client observations confirmed option mutation versus configuration
  snapshots, unchanged configured login after alternate URI user information,
  zero-length connection bypass with version rejection, continued transfer beyond
  retained length, zero-threshold spool behavior, probe bypass of inventory limits,
  no close fallback after failed quit, NLST directory drift without pwd, and spool
  closure after a transfer timeout.
- The retry observation delivered `O`, raised TypeError, then delivered `ONEBOOK`.
  A three-byte request returned `OON` and passed the length check. This verifies the
  documented current retry behavior; it is not a repaired content-integrity claim.
  All observation streams were closed; no live network operation was used.
- The six previously recorded docstring-sensitive ownership-test failures remain
  outstanding. This unrelated storage batch did not rerun or weaken those guards.
- All six local links in this note, three in the main ledger, and 113 in the index
  resolve. Root and data-submodule diffs are whitespace-clean. Every verification
  handle has completed; no check is pending.

Logs/exit metadata and observations:
`working-memory/test-results/docstrings-ftp-driver-2026-09-10-*`.
Static reports: `/tmp/liuxin-docstring-ftp-driver-batch-2026-09-10.json`,
`/tmp/liuxin-ftp-driver-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**,
without parse failures. Missing/blank docs: **1,084 modules, 1,941 classes,
21,249 functions = 24,274 declarations**. Overlapping findings: 3,942
delimiter-layout, 10,294 missing-example, 2,959 parameter-field, 3,882 return-field,
3,838 empty parameter descriptions, 5,153 empty return descriptions, and 28 missing
summaries. These categories overlap and do not measure all repaired prose.

## Next rclone batch

The entire **2,359-line `storage/drivers/rclone.py`** was read in this continuation.
It is still unedited and outside the completed manifest. Next document it and
review the five `rclone_http_readonly` compatibility modules, two `rclone_writable`
modules, and both complete backend regression modules. Their package/test source
has only been located, not fully reviewed here. Previously completed rclone
reconciliation/Library tests remain in the manifest and should not be repeated.

Carry these findings into that next batch. Five focused parser observations used
in-memory process doubles after source review; they are saved in
`working-memory/test-results/docstrings-rclone-source-read-2026-09-10-observations.json`.
They do not claim that the rclone documentation or regression batch is complete:

- Root construction strips outer whitespace, rejects selected control characters
  and recognized inline-secret option names, and retains injected runners without
  invoking them. The secret detector is selective. Key parsing preserves Unicode,
  whitespace, and controls other than NUL; typed addresses bypass reparsing. URI
  conversion uses an exact textual root prefix and performs no URL percent decoding.
- Read-only probe calls an optional callback directly or a depth-one JSON command;
  returned JSON shape is not validated there. Writable probe reports configured
  write support after readability succeeds, without testing publication.
- Reads use cat with offset/count arguments and no stat preflight. A zero-length
  read still rejects a version condition first. The raw reader enforces a supplied
  count as an exact expected response length, but unbounded reads have no expected
  byte count. Exit checking occurs on EOF or a later read after remaining reaches
  zero. Closing a partially consumed stream stops it without validating success.
- Reader EOF-check state is set before wait/diagnostic failures. Wait uses no explicit
  timeout; stderr is read only after a failed exit. Process validation checks
  wait/poll/stdout/read but not every later-used attribute. Cleanup suppresses many
  call failures, but stream attribute lookup can still escape; after kill there is
  no second wait. Do not retain the existing blanket claim that cleanup cannot
  replace an earlier outcome.
- Inventory prefers a spawned JSON-array stream. Ordinary spawn failure permits a
  decoded-list fallback; a timeout does not. A streamed StorageError permits fallback
  only before any item was yielded and when poll reports a nonzero exit, excluding
  timeout errors. Invalid process shape and malformed successful output fail instead.
  The legacy runner may materialize its full list. Counts include skipped/duplicate
  observations; duplicates are silently suppressed after lexical prefix filtering.
  No IsDir check is applied to returned entries despite requesting --files-only.
- The incremental parser checks the entire newly buffered text against the nominal
  token limit before decoding it. Focused observations confirmed acceptance of
  `[1,]`, an unread later chunk after `[1]`, and an unfinished UTF-8 suffix after
  the closing bracket. Splitting `[12]` into `[1` and `2]` yields 1 before raising
  a malformed-JSON error. A four-character limit rejects `[1,2]` despite its
  single-digit element tokens. Trailing-data/UTF-8 checks therefore do not prove
  exhaustion and validation of all stdout. All fake streams were closed afterward;
  the original rclone source remains unchanged.
- Write sessions check accepted-write counters/digests after flush/fsync/close,
  without rereading local staging. fdopen follows mkstemp outside its creation
  error guard. Abort is local cleanup and cannot undo remote publication.
- Publication holds an instance lock across two existence checks and staged
  copyto/moveto commands. CREATE_ONLY adds --immutable; generic moves remain
  non-atomic. An upload failing after remote effects but before runner success is
  not marked uploaded, and may leave staging. Final stat occurs outside the lock
  after publication and can fail while destination bytes remain visible.
- Native URI import checks staging and final reported size/digest; it does not
  stream bytes independently. Missing/different digest algorithm is unsupported,
  wrong value/size is integrity failure. Public staging guards match only the
  `.liuxin-staging/` prefix, and are not applied by every inherited address method.
- Metadata conversions retain their actual coercions and selected fields. Hashes
  prefer SHA-256, then SHA-1, then MD5 and rely on Digest validation rather than
  verifying backend bytes. Error translation uses ordered text markers and shared
  selective diagnostic filtering, not process exit-code classification.

Preserve the remaining manager/model/source/test/script/example/inherited and
tracked data-submodule scope, along with the main ledger's runtime-doc consumer
cautions and three narrowly verified metadata exceptions.
