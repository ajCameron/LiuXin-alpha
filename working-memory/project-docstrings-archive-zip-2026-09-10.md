# Shared archive mechanics, ZIP, and configured adapters — 2026-09-10

## Scope and checkpoint

Continues the unfinished whole-project documentation goal after the
[rclone checkpoint](project-docstrings-rclone-driver-2026-09-10.md).
Branch remains `codex/project-docstrings`, based on `edf6bf05`; no commit or
push was requested or performed. Existing unrelated changes are preserved.

Source-reviewed and documented in full:

- [Shared archive mechanics](../src/LiuXin_alpha/storage/drivers/archive_common.py):
  records, metadata versions, canonical keys, owned member readers, mutation
  protocol, write sessions, copy/digest/naming helpers, and filesystem probes.
- [ZIP drivers](../src/LiuXin_alpha/storage/drivers/zip.py): complete read and
  write implementations, directory preflight, topology/header checks, candidate
  publication, empty-container creation, and timestamp helpers.
- [Configured archive adapters](../src/LiuXin_alpha/storage/store_backend_plugins/archive_backends.py):
  shared Store bridge and all ZIP/TAR/RAR/7z constructors, including durable
  option and runtime-driver boundaries. All six read-only/writable plugin
  initializer modules are documented too.
- [Local archive regression module](../tests/storage/store_backend_plugins/test_local_archive_storage_backends.py):
  complete ZIP/TAR/RAR safety, publication, registry, and ingest cases, including
  all nested process doubles, import fakes, and the replacement callback.

Batch: **10 modules, 21 classes, 147 functions = 178 declarations**.
Cumulative: **395 modules, 505 classes, 4,761 functions = 5,661 declarations**.
The [exact manifest](project-docstrings-reviewed-files.txt) contains 395 unique
paths. Eleven of seventeen raw-driver modules are complete. Dependency excerpts
and TAR read-ahead do not add files to the completed manifest.

## Contracts clarified

- Archive signatures are device/inode/size/mtime/ctime evidence, not content
  hashes. Record construction does not apply canonical key validation. Typed
  ownership checks do not reparse keys, and shared key encoding checks occur
  only when a byte limit is supplied. ZIP bounds the entire encoded key.
- Member readers receive the available count after offset, own their source
  and containing archive, and stop at the declared range. Zero-capacity reads
  do no source I/O. Seek/discard and close have explicit validation/cleanup
  boundaries; source read errors are distinct from buffer-assignment errors.
- Shared write sessions compare accepted-write counters and an optional running
  digest, without rereading staging for those expectations. Publication belongs
  to the concrete driver. Abort cleans local staging and cannot restore an
  already published archive. Flush/fsync and selected cleanup failures can escape.
- `copy_exact` bounds each request to 1 MiB but checks a returned chunk against
  the whole remaining count. It requires complete reported writes and a final
  EOF probe. Parent-file creation and directory fsync are limited observations
  or best-effort operations, not future capacity or durability guarantees.
- ZIP index construction preflights directory size/count before full inventory
  allocation, validates canonical topology and selected local headers, and
  rejects unsupported regular members or declared expansion excesses. Member
  body/CRC integrity is not established by opening and immediately closing a
  member during indexing. Explicit directories are omitted from the projection.
- Index caching uses an instance lock and before/after stat comparison. Open
  readers check descriptor metadata after opening the ZIP; metadata identity
  does not pin contents against every external race. Zero-length/past-EOF reads
  still check indexed existence and version but do not reopen the descriptor.
- ZIP rebuilds validate exact candidate keys/sizes and the original archive
  signature before `os.replace`. The signature check and replacement are separate
  operations, not cross-process compare-and-swap. Final re-indexing and stat can
  fail after publication. New-container creation can precede prefix/limit errors.
- ZIP writes normalize timestamps/attributes and require explicit permission
  for inspected metadata loss. Permission to normalize does not admit unsafe
  members. Allocation suggests keys without reserving them; missing-ok deletion
  checks rebuild policy before absence and returns before version checking.
- Supplied Store configuration is retained while explicit constructor arguments
  still configure the runtime driver. UUID conflicts are rejected; other roots
  and options are not reconciled. Location aliases are the shared class, and the
  legacy archive-path prefix is not a file-URI parser or an extraction root.
- TAR metadata options bound decompressed stream position and parser allocations,
  rather than an independent sum of metadata bytes. RAR extraction timeout is a
  process-wait argument, not an end-to-end operation deadline. The 7z reported
  header-size check follows library opening and is not a pre-allocation limit.
- Test descriptions distinguish real container bytes, in-memory manager records,
  memory extractor processes, a specifically injected publication race, and
  deterministic output within the current implementation/toolchain.

## Verification

- Strict audit and normalizer pass for all ten batch files and all **395 reviewed
  files**. The manifest was expanded only after both checks succeeded.
- All ten executable ASTs match HEAD after removing only leading literal
  docstrings. Signatures, annotations, assertions, limits, and executable
  statements are unchanged; no runtime-documentation exception was added.
- Ruff matches baseline: shared archive helper 3, ZIP driver 15, configured
  adapters 1, every plugin initializer 1, regression module 1. No new finding,
  suppression, or quality-scope change.
- Batch doctests and adjacent archive/7z/RAR-build/ISO/SquashFS, backed-Store,
  filesystem, raw API, configuration/error, advanced-ingest, Store-redraft,
  example/placement, and Store-ingest regressions: **349 passed, 190 explicitly
  skipped integration examples**. These skips are not executed integration proof.
- Full quality runner passed: 159 formatter-clean files, complete annotation
  coverage across 456 modules, 221 protected dependency modules, clean selected
  lint/complexity/production typing, 188 strict-mypy files, and 37 invalid examples
  rejected by each checker.
- Migration, public-documentation, and developer-link contracts: **38 passed**.
- Isolated local observations confirmed published ZIP bytes after final re-index
  failure, accepted-write digest expectations despite same-size staging changes,
  zero/past-EOF reads without descriptor reopening, typed-versus-text validation,
  conditional Unicode encoding checks, creation before prefix validation failure,
  retained configuration alongside different runtime settings, UUID conflict
  rejection, and copy responses larger than one request but within the remainder.
  Temporary observation archives and output streams were cleaned up.
- The six previously recorded docstring-sensitive ownership-test conflicts remain
  outstanding. No guard logic or ceiling was weakened or rerun for this batch.
- Final local links resolve: six in this note, three in the main ledger, and 113
  in the index. Root and data-submodule diffs are whitespace-clean. All verification
  processes have completed; no result or handle remains pending.

Durable logs and exit metadata:
`working-memory/test-results/docstrings-archive-zip-2026-09-10-{regression,quality,contracts}.{log,done}`.
Focused evidence:
`working-memory/test-results/docstrings-archive-zip-2026-09-10-observations.json`.
Static reports:
`/tmp/liuxin-docstring-archive-zip-batch-2026-09-10.json`,
`/tmp/liuxin-archive-zip-ast-lint-2026-09-10.json`, and
`/tmp/liuxin-docstring-reviewed-2026-09-10.json`.

Fresh whole-project audit: **2,730 modules, 4,400 classes, 35,119 functions**,
without parse failures. Missing/blank docs: **1,082 modules, 1,933 classes,
21,070 functions = 24,085 declarations**. Overlapping findings: 3,907
delimiter-layout, 10,275 missing-example, 2,945 parameter-field, 3,869 return-field,
3,791 empty parameter descriptions, 5,059 empty return descriptions, and 28
missing summaries. Structural counts do not establish descriptive completeness.

## Next driver work and TAR read-ahead

The entire **1,842-line `storage/drivers/tar.py`** has now been read and remains
unchanged, unedited, and outside the reviewed manifest. Document that raw driver
next, then RAR/7z and the remaining ISO/SquashFS drivers and backend/test owners.
Six raw modules remain, totaling 10,995 current lines: TAR, RAR, 7z, ISO reader,
ISO writer, and SquashFS. Their wider adapter/tests and the full remaining project
source/test/script/example/inherited/data-submodule scope remain active.

Carry these TAR-specific findings forward:

- `_BoundedTarStream` retains source/owners and unvalidated bounds. Readability
  and seekability are unconditional; fileno/tell delegate. Seek checks returned
  position after moving. Read rejects negative/over-limit requested sizes and
  prospective stream positions, then checks resulting position, without separately
  validating returned bytes or their size. Bounds are position/request checks,
  not a cumulative work budget. Close suppresses owner OSErrors but a different
  exception can prevent closing later owners; base close still runs in finally.
- TAR key parsing applies depth but no byte/encoding limit. The shared helper
  therefore does not reject every malformed surrogate at this boundary. Typed
  addresses still bypass canonical reparsing. Ranges, inventory prefixes, cache
  state, probe caching, and member-resource ownership resemble ZIP, with different
  parser error handling and no ZIP CRC guarantee.
- Index construction counts parser-yielded members, not every hidden extension
  header. It omits directories after topology checks without an explicit directory
  size check, rejects symbolic/hard links and other non-regular kinds, and records
  sparse layout, extra PAX fields, permissions, and ownership as loss reasons.
  Aggregate compression ratio uses total regular-member bytes over container size.
- `_open_tar` detects gzip/bzip2/xz by initial magic, rewinds, wraps decompression
  in `_BoundedTarStream`, and opens `r:` with UTF-8 surrogateescape. It changes
  `_extfileobj` so TarFile owns the wrapper. The raw open precedes the setup guard;
  setup failure closes recorded owners, suppressing only OSErrors. The existing
  example omits required max_stream_bytes/max_read_bytes and needs repair.
- Descriptor identity is checked after opening/parsing. Missing fileno and later
  fstat failures have no explicit successfully-opened-archive cleanup guard in
  `_open_verified_archive`. A None extractfile result and member-reader positioning
  also require precise exception/cleanup prose rather than blanket safety claims.
- TAR rebuilding uses `archive.addfile(info, input_stream)`, not `copy_exact`.
  Inspect stdlib copying semantics before claiming trailing-byte rejection. The
  candidate index/key-size comparison does not independently reread all payloads.
  Publication, final-index/stat failure, metadata expectations, and constructor
  side effects retain the shared ZIP-style boundaries without cross-process CAS.
- Writers sort keys, use PAX, normalize mode 0600 and empty/zero ownership, and
  select mtime zero when deterministic. Otherwise datetime.timestamp is converted
  to int; naive input follows local-time timestamp semantics. The gzip helper
  suppresses filename metadata and chooses mtime zero only when deterministic.
  Non-gzip branches ignore the helper's deterministic argument; member timestamps
  were selected by the caller. `_tar_datetime` accepts float-convertible epoch
  values and returns None for overflow, OS, type, or value errors.

Preserve the main ledger's runtime-doc consumer cautions, its three narrowly
verified metadata exceptions, and the unresolved ownership-test accounting issue.
