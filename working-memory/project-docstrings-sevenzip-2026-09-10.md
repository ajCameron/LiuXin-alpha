# 7z driver and regression documentation — 2026-09-10

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[TAR/RAR checkpoint](project-docstrings-tar-rar-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`, with no commit
or push. Existing unrelated edits and data-submodule changes are retained.

Source-reviewed and documented in full:

- [Raw 7z driver](../src/LiuXin_alpha/storage/drivers/sevenzip.py): address/native
  records, spool/factory, inventory, extraction, ranges, errors, and timestamps.
- [Complete 7z regression module](../tests/storage/store_backend_plugins/sevenzip_readonly/test_sevenzip_readonly_storage_backend.py):
  real archive writer, nine tests, and the nested missing-import substitute.

Batch: **2 modules, 5 classes, 50 functions = 57 declarations**.
Cumulative: **402 modules, 523 classes, 4,976 functions = 5,901 declarations**.
The [reviewed manifest](project-docstrings-reviewed-files.txt) contains 402 unique
paths. Fourteen of seventeen raw-driver modules are complete. Configured 7z
adapters/exports were already completed in the archive/ZIP batch.

## Contracts clarified

- The spool bounds current position plus offered bytes, allows overwrites and
  unbounded seeks, and adds no read-all allocation cap. Its parser close hook is
  a no-op. Cleanup marks before closing and does not retry a failed close; size
  restores position only on successful file operations.
- The factory accepts one exact original parser name and allocates lazily. It
  rejects repeated creation and retains its product reference after cleanup.
  One output writer does not bound preceding solid-block decompression work.
- Construction checks the existing regular file before validating numeric limits;
  it does not import the parser or index the container. Typed addresses check
  ownership without reparsing; text keys receive depth and whole-key byte checks.
- Header checks follow parser opening and entry limits follow list allocation.
  Directory omission follows topology checks but precedes regular-member checks.
  Per-member ratio checks require compressed-size evidence; the regular-member
  total must exactly match archive metadata and satisfy the container ratio.
- Inventory CRC fields are declared metadata. Nonempty reads stage the full
  member, check size, and add a CRC pass only when a CRC is present. Successful
  return transfers the file to its caller. Error cleanup covers Exception paths,
  not every BaseException, and cleanup failures can replace the original error.
- The post-spool signature check and range-reader construction have no explicit
  staged-file cleanup guard. Zero/past-EOF reads still check indexed existence and
  version before bypassing extraction. Filesystem signatures are metadata evidence.
- Probe records availability after metadata validation, without reading payloads.
  Failed probes leave the previous status. Error translation assumes the parser's
  exception attributes exist. Aware timestamps convert to UTC, unlike RAR's
  retained-zone behavior.
- Test documentation distinguishes real archive/Unicode/range checks, startup
  rejections, configuration-only assertions, the injected dependency failure, and
  platform/optional-dependency skips. The shared Unicode helper was read as a
  dependency, but is not newly included in the completed manifest.

## Verification

- Strict batch audit and normalizer pass. Both checks also pass across all **402
  reviewed files**; only then was the manifest expanded.
- Executable ASTs equal HEAD after removing only leading literal docstrings.
  Signatures, annotations, assertions, runtime strings, and limits are unchanged.
- Ruff findings equal baseline: driver 4, regression module 1. No new findings,
  suppressions, gate changes, or runtime-documentation exceptions.
- Selected 7z/shared-archive doctests and adjacent local archive, ISO, SquashFS,
  filesystem, Store/API/configuration/error/ingest regressions:
  **352 passed, 226 explicitly skipped integration examples**, 65.64 seconds.
  Skipped examples are not executed integration proof.
- Full quality runner passed: 159 formatted files, complete annotation coverage
  across 456 modules, 221 protected dependency modules, selected lint/complexity,
  both production type checkers, 188 strict-mypy files, and 37 deliberately invalid
  examples rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Isolated observations confirmed overwrite/seek/close semantics, no retry after
  cleanup failure, retained factory products, no extra CRC pass when absent,
  caller-owned successful materialization, missing explicit cleanup after a
  post-spool signature failure, indexed zero-length short-circuiting, and UTC
  timestamp conversion. Mock extraction does not claim real parser conformance;
  real archive behavior is covered by the regression selection.
- Named observation files were scoped to a temporary directory; all temporary
  spools and retained streams were explicitly closed. Final return/helper prose corrections were followed by
  batch audit/normalizer and AST/Ruff rechecks; executed examples were unchanged.
- The six recorded docstring-sensitive ownership-test failures remain outstanding.
  This batch did not rerun or weaken those unrelated guards.
- All verification handles completed with observed results. Root and data-submodule
  diffs are whitespace-clean. Local links resolve in the index (113), main ledger
  (3), and this note (4). The ISO reader remains unchanged from HEAD.

Durable logs/exit metadata:
`working-memory/test-results/docstrings-sevenzip-2026-09-10-{regression,quality,contracts}.{log,done}`.
Focused observations:
`working-memory/test-results/docstrings-sevenzip-2026-09-10-observations.json`.
Static reports:
`/tmp/liuxin-docstring-sevenzip-batch-2026-09-10.json`,
`/tmp/liuxin-sevenzip-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**, with
no parse failures. Missing/blank docs: **1,082 modules, 1,929 classes, 21,014
functions = 24,025 declarations**. Overlapping findings: 3,895 delimiter layout,
10,265 missing examples, 2,929 parameter fields, 3,860 return fields, 3,707 empty
parameter descriptions, 4,909 empty return descriptions, and 28 missing summaries.
Structural counts do not establish descriptive completeness.

## Next work: ISO reader/writer and SquashFS

The entire **2,472-line raw ISO reader** has now been read, remains unchanged,
and is outside the completed manifest. Its tests and remaining adapters still
need full review. Continue there, then the ISO writer and SquashFS with their
remaining adapters/tests. Three raw-driver modules remain, totaling 6,308 lines.
The wider source/test/script/example/inherited/data-submodule backlog stays in scope.

Carry these source findings into the ISO documentation:

- Separate dependency-free ISO/Rock Ridge/Joliet extent reads from optional
  pycdlib UDF materialization. Standard selection prefers detected SUSP SP/Rock
  Ridge, then highest Joliet, then primary ISO. An enabled UDF bridge can supersede
  a non-Rock-Ridge selection; only an ImportError-caused unsupported error permits
  fallback to the direct namespace. UDF-only integrity failures become unsupported.
- Index construction captures the initial opened descriptor's five-field signature;
  it has no final restat after parsing. UDF indexing separately opens the path.
  Direct reads check the opened descriptor before exposing extents; UDF reads
  close that handle, reopen through pycdlib, then restat the path after spooling.
- UDF spool writes bound position plus offered bytes, while seek is unbounded.
  Materialization compares final position, not independently measured length or
  content hash. Post-extraction stat failures explicitly close the spool, unlike
  the 7z/RAR helper boundary. PyCdlib construction follows spool allocation but
  precedes the extraction cleanup guard; inspect this when describing failure safety.
- The raw extent reader constructs mutable physical segments without validating
  its input records/range. It requires exact bytes from each read, translates
  seek/read OSErrors, and preserves partial segment progress when a later segment
  fails. Repeated close calls still invoke source.close; base close runs in finally.
- Parser state is retained across calls. Visited-directory identity uses LBA and
  data length, omitting extended-attribute blocks; repeated identities are skipped.
  Direct all-entry counting excludes self/parent records but includes relocation
  entries before their omission. Directory payloads and record tuples are built
  before entry iteration. Multi-extent files accumulate by key until a final record;
  member/total limits are applied at completion, and timestamps come from the first
  extent. Physical overlaps are not independently rejected.
- Direct unsafe symlink/non-regular entries can be skipped under the flag; UDF
  indexing rejects them regardless. Directory handling precedes direct unsafe-kind
  checks. Relocated records are omitted before name validation; zisofs/interleaving
  reject supported file paths. Root-plus-recursive directory depth differs from
  canonical path component checks.
- SUSP continuation depth is capped at four; continuation bytes decrement a mutable
  per-record budget. Initial system-use bytes are not charged. Malformed entry
  headers terminate iteration silently, ST stops it, and CE entries recurse rather
  than being yielded. Record which signatures contribute to loss inspection.
- Joliet decoding preserves surrogate code units and maps an odd trailing byte;
  canonical ISO byte limits use surrogatepass, unlike archive-common surrogateescape.
  Non-alternate file names lose a numeric version suffix and one trailing dot;
  directories and Rock Ridge alternate names retain them. None of these helpers
  normalize Unicode or validate external URI syntax.
- Timestamp helpers differ: ISO recording values require seven bytes and a bounded
  signed quarter-hour offset; UDF fields default to UTC for tz=-2047 and combine
  three subsecond components. UDF attribute-presence checks occur outside the
  narrower conversion error guard. Signature/version helpers validate no tuple shape.

Preserve the historical runtime-doc consumer cautions, three narrowly checked
metadata exceptions, and unresolved ownership-test accounting issue.
