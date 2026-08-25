# Storage production-hardening checkpoint - 2026-08-21

The storage follow-up checklist recorded in
`dev-docs/storage/storage_component_status.md` has been worked through.

Implemented and covered:

- database-generated concurrent-safe metadata IDs and UUID revisions;
- versioned schema and scratch-envelope migration, including future-version
  rejection;
- transactional compound metadata mutations and durable Store reconfiguration;
- correct SquashFS semantics: archived identical bytes are Replicas, not a
  derivation;
- aggregate operational health and suggested recovery actions;
- live PostgreSQL concurrent ingest/interruption/restart coverage;
- protected, read-only HTTP/FTP/rclone/S3 CI contracts;
- concrete database Asset/Replica/Composite/derivation repositories and an
  explicit-commit Unit of Work; and
- ID-scoped shared-cache invalidation plus a reproducible large-catalogue
  benchmark.

The cache result closes the question raised by removing
`InMemoryStorageManager`: the durable manager does not need another catalogue
copy. It either shares Core's cache or reads its database repositories
directly. Store facade/lock registries remain process-local because they are
live resources rather than persisted domain records.

The 50,000-book/250,000-link benchmark measured a 1.164 ms median one-row
refresh, one database row read, and no table scan. Construction reached about
745 MiB RSS, so cache enablement remains an explicit application-level
performance/memory decision.

## Verification

- `tests/storage`: 778 passed, five skipped (four credential-gated remote
  contracts and the opt-in PostgreSQL case).
- `tests/ingest tests/core`: 107 passed.
- `tests/databases/caches`: 142 passed, twelve documented legacy/live-plugin
  skips.
- PostgreSQL adapter/schema unit suite: 30 passed.
- Live PostgreSQL concurrent ingest, interrupted publication, restart recovery,
  and idempotency: one passed.
- Storage/cache source compilation and `git diff --check`: clean. The latter
  reports pre-existing repository line-ending conversion warnings only.

The non-overlapping verification selections total 1,058 passing tests. The
protected credential-backed HTTP/FTP/rclone/S3 workflow is configured but was
not invoked locally because its read-only environment secrets are intentionally
available only in protected CI.

## Local archive follow-up - 2026-08-22

The ISO container design has now been extended to registered ZIP and TAR
read-only/writable Stores and a read-only RAR Store. ZIP/TAR mutation is an
atomic, validated whole-archive rebuild and is deliberately advertised for
archival snapshots rather than general ingest. Rebuilds fail closed when the
regular-file projection would lose archive metadata or special entries unless
the durable `allow_lossy_rebuild` conversion flag is explicit.

General RAR access remains read-only by design. The embedded RAR 3/4 parser
inventories the archive and reads stored members; compressed members need
optional `unrar` or `rar`, are spooled privately, and are size/CRC-verified.
RAR 5, encrypted, and multi-volume archives are explicit limitations.

A separate `rar_build` backend now handles the honest write-once case. It
collects normal committed Store writes in durable filesystem staging, then an
explicit `seal()` invokes an operator-supplied licensed `rar` creator with RAR
4/non-solid settings. The candidate is command-tested, independently matched
to the staged key/size/CRC manifest, and published create-only; it is never used
to adopt, replace, or modify an existing archive. A successful seal permanently
locks the builder and returns the normal read-only RAR Store. Its complete
configuration participates in the backend registry and database reload.

The deferred sealed-artifact question is now resolved. The physical RAR and
SquashFS Store lifecycles remain provenance-neutral, while
`SealedArtifactWorkflow` adopts or ingests the completed image and records it
as an atomic `PACKAGE` derivative. Inputs are pinned by Asset identity, digest,
size, and archive path; tools and dependencies are digest-pinned; settings,
environment, command, expected output identity, and optional backup workflow
reference are durable. Backup linkage uses a namespaced `backup:<id>` value
because the older integer derivation workflow field is a foreign key into the
separate transform-run identity space. SquashFS backup results have a direct
adapter, RAR has a command-aware convenience, and the generic path covers
7z/ISO/UDF/ZIP/TAR. Managed backup designation and Store planning now preserve
existing Asset/Replica IDs.

The sealed-artifact checkpoint passed 89 focused workflow, backup, derivation,
and persistence tests. The complete storage suite passed 926 tests with 17
expected optional-dependency/live-contract skips. Direct strict basedpyright
checking of the new API and implementation reports no warnings or errors.

Verification for the archive follow-up reached 80 passing focused
archive/registry/Unicode tests and 900 passing tests across the complete
storage suite. The five skips remain the four credential-gated remote contracts
and the separately configured live PostgreSQL scenario.

The subsequent build-once RAR checkpoint reached 95 passing focused
archive/registry/Unicode tests and 915 passing tests across the complete
storage suite, with the same five expected skips.

## Mess-ingestion start - 2026-08-24

The first operational mess-ingestion layer is
`storage.ingest.SquashfsDriveIngestWorkflow`. It scans a local directory without
following symlinks, recognizes both named and suffixless SquashFS images,
registers the source drive and each image as durable read-only Stores, and
adopts both image bytes and regular members in place. It continues after
isolated failures, has progress callbacks and explicit bounded-scan reporting,
and reruns idempotently after Store/Asset/Replica database reload.

The deliberately deferred layer is a generic coordinator/handler registry for
RAR, ISO/UDF, ZIP/TAR/7z, loose ebooks, nested containers, and bibliographic
metadata/Item creation. The narrow workflow's report and member-metadata
factory are the intended seam. Existing packs do not acquire invented
derivation edges: provenance is recorded only when a producing process is
actually known.

Hostile POSIX byte names are now durable as well as readable. Local file URI
reconstruction preserves original bytes, UUID inputs accept surrogate-escaped
paths, scratch JSON round-trips exact strings, and query-friendly scalar
columns fall back to visible escapes rather than failing a database bind.

Verification: five focused workflow/example tests passed; 82 surrounding
database reload, manager contract, registry, and SquashFS tests passed with one
optional dependency skip. The complete `tests/storage` suite passed 930 tests
with 17 expected optional-dependency/live-contract skips. Direct basedpyright
checking of the new workflow and example was clean.

## Recursive Store foundation - 2026-08-24

Container nesting now has an explicit domain link. `StoreBackingReference`
persists the backing Digital Asset, a replaceable preferred Replica, and an
optional materialization Store. `StorageManager.add_backed_store()` constructs
stable read-only Store views; writable container changes remain sealed-artifact
derivations so the backing Asset digest cannot silently change.

`materialize_digital_asset()` can select an exact Replica or ordered source
modes. This closes the earlier ACTIVE-only gap for archive members and
unmanaged ingest sources. If a nested member is not already a local file, the
manager copies it through checked Store publication into a durable CACHE
Replica. Extended Replica modes are preserved in the existing Store policy
JSON without a destructive schema migration.

Database Store loading now follows declared dependencies: physical and cache
Stores precede Asset-backed views, inner views follow the Store containing
their preferred Replica, and encrypted wrappers retain their inner-Store
ordering. A ZIP-in-ZIP integration test covers creation, materialization,
innermost reading, policy persistence, and restart. The SquashFS drive workflow
now records the image Asset as each archive Store's backing identity.

Still deliberately deferred: automatic container recognition/queuing,
ancestry-based cycle detection, recursion depth, member/expanded-byte and
compression-ratio budgets, execution/time/temp-space budgets, and mixed-format
metadata/Item enrichment. Cache eviction must also gain a live Store pin/lease
before it is enabled for container materializations. Those belong above this
now-tested Store foundation.

Verification: the complete `tests/storage` checkpoint passes 932 tests with 17
expected optional-dependency/live-contract skips. A final
writable-backed-Store guard passed in the focused selection; current collection
is 950 tests (933 runnable and 17 expected skips). The focused manager,
database reload, SquashFS, ISO, local-archive, and recursive-Store matrix passes
195 tests with eight optional parser skips.

## Additional archive-reader follow-up - 2026-08-23

The local container catalogue now also includes `sevenzip_readonly`, backed by
optional `py7zr`. It provides bounded complete inventory, opaque Unicode keys,
conditional ranges, CRC/size verification, private member spooling, and
explicit solid/encryption/multi-volume characteristics. Its safety policy and
Store identity persist through the normal backend registry and database reload.

The ISO reader can prefer the UDF namespace of an ISO/UDF bridge through
optional `pycdlib`, retaining direct Rock Ridge priority and ISO/Joliet fallback
when that dependency is unavailable. UDF member reads are privately staged and
bounded; UDF-only images remain an explicit parser limitation. `enable_udf` and
the UDF spool limit are durable Store policy.

The RAR reader now selects maintained optional `rarfile` for RAR 5 while
retaining the embedded RAR 3/4 fallback. Stored RAR 5 members need no external
extractor, Unicode names remain exact, redirections are omitted, and available
CRC-32 or BLAKE2sp integrity evidence is verified. Compressed members retain
the bounded external-extractor boundary.

The `archives` optional dependency group contains `py7zr`, `pycdlib`, and
maintained `rarfile`; storage imports remain usable without the group.

Verification: 132 focused archive/registry/Unicode tests passed, followed by
36 executable affected-module doctests (132 integration examples skipped).
The complete current storage suite passed 884 tests with five expected
opt-in/live-backend skips. The suite's localhost HTTP example required a rerun
outside the managed socket-restricted sandbox; it passed there.

## Hostile ZIP hardening - 2026-08-24

ZIP is now a fail-closed container boundary rather than a permissive regular-
file projection. Before `zipfile` allocates its member inventory, the driver
reads the end records and scans central-directory headers with constant memory.
Declared and actual entry counts must agree, central-directory bytes and entry
count are bounded, and multi-disk sets are rejected. Inventory then rejects
invalid UTF-8 metadata, NUL truncation, unsafe or non-canonical paths, duplicate
file or directory records, file/directory and parent-file collisions, shared or
overlapping local headers, central/local name mismatches, encryption, links,
special files, unsupported methods, oversized members, aggregate expansion,
and excessive per-member compression ratio. No archive member is extracted to
the host filesystem.

The new durable ZIP options are `max_total_uncompressed_bytes`,
`max_compression_ratio`, and `max_central_directory_bytes`, alongside the
existing member/count/depth limits. Defaults are 4 GiB per member, 64 GiB total
declared expansion, 200:1 per member, and 128 MiB of central-directory data.
Both Store facades persist them, runtime and registry characteristics advertise
them, and database reconstruction restores them.

Writable ZIP staging now enforces the member ceiling even when the caller did
not declare an expected size. Rebuild plans are topology- and aggregate-checked
before candidate I/O, candidate archives pass the complete hostile-read policy,
create-only archive construction cannot clobber a race, explicit member-create
collisions remain errors, and an externally replaced source archive wins over
the pending rebuild rather than being overwritten.

These are per-ZIP limits. Recursive ingest must still own a cumulative budget
for depth, expanded bytes, members, time, temporary space, and ancestry/cycles;
nested archives cannot safely multiply the per-container allowance without
that coordinator-level accounting.

Verification: the complete `tests/storage` suite passes 950 tests with 17
expected optional-dependency/live skips (967 collected). The focused archive,
Unicode, registry, recursive-Store, and database-reload matrix passes 135 tests
with three optional parser skips. The directly affected doctest selection
passes 19 examples with 74 explicitly skipped examples, and direct basedpyright
checking of the ZIP and shared archive mechanics reports zero errors or
warnings.

## All compression-backend hardening - 2026-08-24

The hostile-ZIP posture now applies across TAR, SquashFS, RAR, 7z, and the
logical-expansion portions of ISO/UDF. Archive inventory is fail-closed around
unsafe names, links, special entries, duplicates, file/directory overwrite
topology, all-entry counts, member bytes, total logical bytes, and available
compression-ratio evidence. Default member/total/ratio ceilings are 4 GiB,
64 GiB, and 200:1 for compressed formats; UDF keeps its existing 8 GiB member
default. Parser-specific metadata/header input is bounded before high-level
libraries can allocate it.

TAR now bounds gzip/bzip2/xz decompression streams plus PAX/GNU metadata and
preflights complete write plans before candidate I/O. RAR extraction has timed,
size-exact stdout spooling and bounded diagnostics; 7z bounds headers and solid-
archive amplification evidence. SquashFS extraction is timed and size-spooled
with archive-identity checks, while SquashFS and RAR builders reject unsafe
staging trees, bound their external tools, and independently inventory and hash
candidate contents before publication. Failed or invalid candidates cannot
replace an existing artifact; RAR publication remains create-only.

Read-only ISO rejects unsafe selected namespaces. UDF extraction cannot exceed
the indexed member size, and image logical expansion, paths, parser metadata,
and all-entry counts are bounded. Writable ISO retains the explicit lossy-
normalization audit for legacy special entries, but never exposes them as files;
its member stage and complete rebuild plan now enforce the durable reader limits
before building and validate candidates under the same policy.

All safety settings round-trip through `StoreConfiguration` and database policy.
Runtime and registry characteristics advertise actual temporary-space needs,
default object limits, unsafe-member rejection, bounded expansion, and the fact
that recursive ingest still requires a cumulative cross-container budget for
depth, members, expanded bytes, time, temporary space, and ancestry/cycles.

Verification: the focused compression/registry matrix passes 202 tests and the
affected backup/sealed-artifact workflow selection passes 12. The complete
storage run produced 949 passes and five expected opt-in/live skips inside the
managed sandbox; its only failure was the sandbox's refusal to create a
localhost test socket, and that exact test passed outside the socket restriction
(effective result: 950 passed, five skipped). Direct basedpyright runs report no
errors in every hardened driver, builder, and facade. Direct doctest execution
across the shared/archive drivers and facade passes 94 examples, with 271
integration-only examples explicitly skipped.

## Mixed-format coordinator - 2026-08-25

The formerly deferred generic layer now exists as
`storage.ingest.MixedFormatIngestCoordinator`. It composes the hardened
SquashFS, ISO/UDF, ZIP, TAR, RAR, and 7z Stores; adopts loose source files;
recurses through Asset-backed Stores; and materializes nested container Assets
into an explicitly managed CACHE Store. It does not extract into the source
tree, follow symlinks, invent derivations, or expand ebook containers unless
asked.

The coordinator owns cumulative source/container/member, depth, path, expanded
byte, expansion-ratio, materialization, temporary-space, wall-time,
cancellation, and issue ceilings. SHA-256 ancestry detects cycles and equal
container content is expanded once per run. Stable operation IDs, durable
backed Store configuration, and CACHE Replica reuse make reruns and database
restart idempotent. `ingest_mixed_tree_example.py` supplies a non-mutating
top-level discovery mode and a database-backed real-run mode.

Remaining adjacent work is intentionally separate:

- bibliographic recognition, Item matching, and metadata enrichment above the
  technical Asset/Replica catalogue;
- a live Store pin/lease before any automatic cache eviction can overlap nested
  materialization; and
- process-level resource supervision for untrusted runs, because an in-process
  optional parser cannot be preempted safely in the middle of one bounded call.

Verification: all 16 focused coordinator tests pass. The preceding complete
storage checkpoint is 993 passed and 24 expected skips; skips are limited to
missing optional `py7zr`/`rarfile`/`pycdlib` support and disabled live
backend/PostgreSQL contracts. Direct basedpyright checking of the coordinator
and its public exports reports zero errors and zero warnings.

## Unattended mixed-ingest observability - 2026-08-25

The first remote run now has a durable LiuXin logging session rather than only
terminal progress and a final JSON report. `EventLogHandler` maps Python and
`CompatLogger` records into `EventLogAPI`; `RunLoggingSession` writes an
authoritative append-only JSONL audit plus a bounded rotating UTC text log.
Every run gets one explicit UUID, printed with both paths before database
startup and retained through Store bootstrap, every DEBUG-level source/member
event, INFO checkpoints/lifecycle events, warning/error issues, exception
tracebacks, final counters, and CLI completion/failure.

The event-log implementation is no longer treated as an untested prototype.
Its ring, filters, resizing, followers, close semantics, runtime validation,
level names, and JSONL persistence are active tests. Persistence keeps one
line-buffered append handle and flushes each event, avoids the old recursive
level-name deadlock, and ASCII-escapes JSON so surrogateescaped legacy POSIX
paths round-trip without breaking an unattended write. Legacy database stdout
is captured as structured `captured_output`; process environments, bytes, rows,
and credentials are not dumped.

The complete focused logging/coordinator/executable-example selection passes 48
tests. The complete `tests/storage` checkpoint passes 997 tests with 24 expected
skips for missing optional `py7zr`/`rarfile`/`pycdlib` support and disabled live
backend/PostgreSQL contracts. Direct basedpyright checking of the event
API/implementation, handler, run session, and coordinator reports zero errors
and zero warnings.
