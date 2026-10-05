# TAR, RAR, and build-once RAR Store — 2026-09-10

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[shared archive/ZIP checkpoint](project-docstrings-archive-zip-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, with no commit
or push. Existing unrelated edits and the tracked data-submodule work are retained.

Source-reviewed and documented in full:

- [TAR driver](../src/LiuXin_alpha/storage/drivers/tar.py): bounded decompression
  wrapper, raw read/write drivers, parser/writer contexts, publication, and times.
- [RAR driver](../src/LiuXin_alpha/storage/drivers/rar.py): parser selection,
  inventory, stored/external extraction, size/checksum spooling, nested drainers,
  signature/error helpers, and native timestamp compatibility.
- [RAR builder](../src/LiuXin_alpha/storage/store_backend_plugins/rar_build/rar_build_storage_backend.py)
  and its plugin initializer: staging convenience methods, mutation leases,
  inspection, command execution, candidate comparison, and create-only publication.
- [Complete builder regression module](../tests/storage/store_backend_plugins/rar_build/test_rar_build_storage_backend.py),
  including the fixture reader and all nested successful/failed/timed-out processes.

Batch: **5 modules, 13 classes, 165 functions = 183 declarations**.
Cumulative: **400 modules, 518 classes, 4,926 functions = 5,844 declarations**.
The [exact reviewed manifest](project-docstrings-reviewed-files.txt) contains 400
unique paths. Thirteen of seventeen raw-driver modules are complete. Reused prose
was restricted to operations whose implementations/contracts were compared; format
specific extraction, parsing, and publication retain separate descriptions.

## Contracts clarified

- TAR wrapper bounds constrain requested allocation and decompressed position,
  not cumulative work. Seek checks after moving, read-all is rejected, and source
  return type/length are not independently verified. Cleanup owns the explicit
  owners tuple rather than implicitly adding the source.
- TAR indexing counts parser-yielded members rather than hidden extension headers.
  Directories are omitted after topology checks without a separate size check;
  regular-member metadata/ownership can prevent normalizing writes. Name encoding
  is not checked by shared canonical parsing when no byte limit is supplied.
- TAR copying uses the installed stdlib `tarfile.addfile`/`copyfileobj`, whose source
  was inspected. It consumes declared bytes without a trailing EOF check. Candidate
  key/size validation is not an independent payload hash pass. Rebuilding retains
  the separate stat/replace race and post-publication final-index/stat boundaries.
- TAR writer contexts detect/read compression by magic and choose output compression
  explicitly. PAX/mode/ownership/member-time normalization is separate from gzip
  header timestamp/filename policy. Naive non-deterministic member datetimes use
  local timestamp semantics; raw epoch conversion returns UTC or a handled None.
- RAR parser selection rereads initial magic and prefers the installed module.
  ImportError permits the embedded fallback only outside RAR5; a RAR5Parser attribute
  is a compatibility check, not full parser conformance. The parser-global extractor
  override is locked only during archive construction and restored before later use.
- RAR inventory exists before the entry cap is checked. It validates a single
  reported volume, key/topology, supported regular kinds, password state, declared
  sizes, and expansion ratios. CRC/BLAKE2sp fields in inventory are declared evidence.
- Nonempty RAR reads materialize a whole member, validate size and available
  checksum fields, and compare archive metadata before/after. The final signature
  check and reader construction have no explicit spool-cleanup guard. Zero/past-EOF
  ranges still check indexed existence/version before skipping materialization.
- External extraction bounds stdout by offered chunk lengths; destination write
  counts are not checked by the drainer. Final size/checksums belong to the spool
  verifier. Diagnostic retention is a prefix cap; process timeout does not cover
  later unbounded kill/wait cleanup or all pipe joins. Cleanup can itself raise.
- RAR native time conversion retains aware datetime identity and zone, attaches UTC
  to naive values, and handles tuple fractions separately. Tuple overflow can escape
  the narrower TypeError/ValueError guard. Predicate helpers describe exactly which
  methods they invoke rather than claiming all indexer link/type checks.
- The builder retains filesystem staging after sealing; ordinary reads still use
  it, while successful seal returns a separate read-only archive Store with a new
  UUID. Existing outputs are not adopted. Runtime settings and supplied durable
  configuration are not reconciled, apart from explicit UUID conflicts.
- Implicit byte/file deduplication checks existing staged content against the chosen
  expected digest. A match can return before checking new incoming bytes, source
  existence, or expected size. Content keys omit the algorithm. Stream staging has
  no equivalent deduplication branch, and false explicit locations differ from None.
- Mutation lease counters coordinate one instance. Seal rejects active work rather
  than waiting. Release is marked before its callback, so a callback failure is not
  retried. Context-entry failure lacks wrapper release. The simple unsealed check
  does not inspect in-progress sealing; acquiring an actual mutation lease does.
- Builder status decoration does not latch observed output presence or report the
  sealing-in-progress flag. Staging's read-only configuration is applied by the
  filesystem driver; seal has no separate read-only-policy check.
- Sealing computes size/CRC manifests, requests RAR4 non-solid output, runs the
  external tester, and compares declared candidate metadata. Candidate metadata
  comparison does not read payloads or independently enforce creator format flags.
  The creator drain thread does not report its exceptions back to the parent.
- Candidate names are returned absent/unreserved. Publication hard-links without
  replacing existing output and latches sealed state before synchronization. Fsync
  or later facade construction can fail with published output and no built_store.
  Candidate unlink failures can leave another hard link; no rollback is promised.
- Regression descriptions distinguish real filesystem artifacts, real parser reads,
  memory creator/test processes, requested command flags, and synthetic timeouts.

## Verification

- Strict batch audit and normalizer pass, as do both checks across all **400 reviewed
  files**. The manifest was expanded only after those full reviewed-set checks passed.
- All five executable ASTs equal HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, limits, and executable behaviour are unchanged.
  No runtime-documentation metadata exception was introduced.
- Ruff findings equal baseline: TAR 7, RAR 8, builder initializer 1, builder 1,
  builder regression module 1. No new findings, suppressions, or gate changes.
- Batch and shared archive/ZIP doctests plus local archive, 7z, ISO, SquashFS,
  backed-Store, filesystem, raw API/configuration/errors, advanced-ingest,
  Store-redraft/examples/placement, and Store-ingest regressions:
  **364 passed, 330 explicitly skipped integration examples**, 54.86 seconds.
- Full quality runner passed: 159 formatted files, complete annotation coverage
  across 456 modules, 221 protected dependency modules, selected lint/complexity,
  both production type checkers, 188 strict-mypy files, and 37 invalid examples
  rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Isolated observations confirmed TAR trailing-stage bytes are ignored beyond the
  declared copy size; failed bounded seeks retain their movement; malformed source
  response lengths are not independently checked; aware RAR times retain identity;
  tuple overflow propagates; post-spool signature failure leaves explicit cleanup
  to the caller; builder fsync failure follows visible output/sealed state; matching
  digest deduplication can skip new source/size checks; and read-only configured
  seal reaches tool lookup after manifest validation.
- Observation archives/staging trees were scoped to a temporary directory; the
  intentionally retained memory spool was closed explicitly. Creator commands were
  not run against a live external tool. The direct publication observation used an
  opaque local candidate to test only the publication helper's failure boundary.
- Final prose and skipped-example spelling corrections were followed by batch
  audit/normalizer, five-file AST/Ruff comparisons, and the 400-file checks. They
  did not alter the executed examples or runtime code.
- The six recorded docstring-sensitive ownership-test failures remain outstanding.
  This batch did not rerun or weaken those unrelated guards.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-tar-rar-2026-09-10-{regression,quality,contracts}.{log,done}`.
Focused observations:
`working-memory/test-results/docstrings-tar-rar-2026-09-10-observations.json`.
Static reports:
`/tmp/liuxin-docstring-tar-rar-batch-2026-09-10.json`,
`/tmp/liuxin-tar-rar-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**,
without parse failures. Missing/blank docs: **1,082 modules, 1,929 classes,
21,025 functions = 24,036 declarations**. Overlapping findings: 3,897 delimiter
layout, 10,266 missing examples, 2,931 parameter fields, 3,861 return fields,
3,727 empty parameter descriptions, 4,947 empty return descriptions, and 28
missing summaries. Structural counts do not establish descriptive completeness.

## Next driver work and 7z read-ahead

The entire **1,374-line `storage/drivers/sevenzip.py`** has now been read and is
unchanged, unedited, and outside the completed manifest. Its configured adapter and
initializer were documented in the archive/ZIP batch; its full regression module
still needs source review and documentation. Continue there, then ISO reader/writer
and SquashFS with their remaining adapters/tests. Four raw-driver modules remain,
totaling 7,682 current lines. The wider manager/model/source/test/script/example/
inherited/data-submodule backlog remains in scope.

Carry these 7z-specific findings forward:

- Address/native member records add no canonical/CRC validation. The spool creates
  a TemporaryFile and retains an unvalidated expected size. Write bounds current
  position plus offered length, not cumulative writes; seek itself is unbounded.
  read(None) delegates read-all. size seeks to end and restores position without a
  finally guard. close is deliberately a no-op; cleanup marks closed before closing
  the file, so a close failure is not retried. The file property exposes the same
  owned stream, and successful materialization transfers that stream to its reader.
- The factory accepts one exact original parser name, creates a spool lazily, and
  rejects both unexpected and repeated create calls. cleanup closes a created spool
  without resetting its reference; the spool property can still return it afterward.
- Header-size and entry-count checks follow py7zr opening/list allocation. Directory
  omission follows key/topology checks but precedes regular-member kind/size checks.
  Per-member ratio checking is conditional on compressed-size evidence being present.
  Total regular-member bytes must exactly match archive_info.uncompressed before the
  aggregate ratio is checked. Declared CRC is optional and masked to 32 bits.
- Index cache also retains solid status and method names. Probe can warn about solid
  block read amplification. Multi-volume unsupported text is a limitation literal;
  inspect the actual library/path behaviour before claiming a dedicated volume check.
- Materialization requires a native _SevenZipMember, extracts its exact name through
  a factory, then checks spool size and optional CRC. Solid block decompression can
  include preceding data even though only one output writer is accepted. Exception
  cleanup covers StorageError/OSError/Exception, not every BaseException; cleanup can
  replace the original failure. Post-materialization signature/read-wrapper failures
  have the same missing explicit spool guard observed in RAR.
- `_translate_py7zr_error` directly accesses the supplied module's exception classes;
  an incompatible module shape can itself fail. Password/unsupported method becomes
  unsupported, structure/CRC/bomb/decompression/archive/ValueError becomes integrity,
  other errors become unavailable. `_require_py7zr` catches ImportError only and does
  not inspect the target path; it is diagnostic context. Datetime conversion differs
  from RAR by converting aware values to UTC rather than retaining another zone.

Preserve runtime-doc consumer cautions, the three narrowly checked historical
metadata exceptions, and the unresolved ownership-test accounting issue.
