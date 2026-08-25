# Storage component status and runtime composition

Updated: 2026-08-22

## Current decision

The application `StorageManager` is database-authoritative. It does not
inherit from the transient manager and does not retain private dictionaries of
Assets, Replicas, policies, Composites, derivations, Item links, or ingest
operations.

```text
StorageManager
    |
    v
DatabaseStorageMetadataRepository ---- writes ----> LiuXin database
    |
    +---- reads through Core Cache when attached
    |         (explicit invalidation/generation semantics)
    |
    +---- direct database reads for uncached helper/workflow tables
```

`CoreServices` shares its configured `Cache` with the database's storage
manager and unbinds it before closing an owned cache. A standalone database
manager works without a cache and reads directly from its repository. The
cache is therefore an optional accelerator, never a persistence requirement or
an alternative source of truth.

The manager still keeps a process-local registry of constructed Store facades,
their configurations, and runtime locks. That state represents open resources
and active routing, not a catalogue cache; it is rebuilt from durable Store
rows during startup/reload.

The current schema-backed cache exposes the main Asset/Replica hot path and
some related storage tables. Helper/workflow tables such as derivations may be
excluded by the cache schema and remain direct repository reads. This mixed
path is intentional until cache schema policy says those tables should be
cached. Single-record repository writes invalidate durable row IDs rather than
whole tables. The schema-backed cache repairs those rows and its scalar and
relation projections with bounded database reads; plugins without a targeted
refresh retain a correct whole-table fallback.

`TransientStorageManager` is the explicitly disposable implementation for
focused contract tests and one-shot work. `InMemoryStorageManager` remains as a
compatibility alias only. Neither is part of the production manager's class
hierarchy, and neither should be described as a cache.

## Persistence and recovery

The database repository persists complete versioned domain envelopes while
also filling useful scalar schema columns. It owns Assets, Replicas,
Composites and membership, derivations, policies, Item links, and completed
ingest identities. The ingest journal bridges Store publication and database
commit; restart recovery verifies published bytes before completing Replica
metadata.

An incomplete storage catalogue is rejected with a migration-oriented error.
It never silently degrades to process-local manager metadata.
Store rows referenced by live Replica claims use a restrictive foreign key:
operators retire/offline a Store first and resolve its claims rather than
deleting the row and cascading away storage evidence.

## Current physical backend coverage

The canonical backend registry includes local filesystem variants, SQLite,
HTTP/native-HTML/wget discovery, FTP, rclone, S3, SquashFS build/read, encrypted
wrapping, dependency-free read-only and writable ISO images, read-only and
writable ZIP/TAR archives, read-only RAR archives, and a build-once RAR staging
backend, plus an optional-dependency read-only 7z Store. The ISO reader
supports primary ISO 9660, Rock Ridge, Joliet, and ISO/UDF bridge namespaces and advertises
complete inventory plus conditional range reads. The writer implements staged
create/replace and conditional deletion by streaming, validating, and atomically
publishing a complete hybrid ISO 9660/Rock Ridge/Joliet rebuild. UDF members are
privately spooled through optional `pycdlib`; UDF-only and zisofs-compressed
images fail explicitly as unsupported. Read-only inventory rejects unsafe or
ambiguous entries and bounds members, total logical bytes, logical/image ratio,
paths, parser metadata, and every entry before reads. Writable ISO applies the
same durable limits to streaming stages and complete rebuild plans.

ZIP and TAR readers provide bounded, duplicate-safe regular-file inventories,
opaque Unicode paths, archive-wide conditional versions, and exact ranged
reads. Both reject links, special entries, duplicates, and file/directory
topology conflicts. ZIP additionally preflights its central directory without allocating
member names, rejects count mismatches, unsafe paths, non-regular members,
local-header aliases, and file/directory overwrite conflicts, and enforces
durable member, aggregate expansion, compression-ratio, and central-directory
budgets before reads. TAR bounds decompressed parser input and PAX/GNU metadata,
plus the same member, total, ratio, depth, and all-entry dimensions. Their writers implement staged create/replace and
conditional deletion through a validated atomic whole-archive rebuild, without
an extracted mirror or an in-memory payload cache. ZIP supports stored, Deflate, BZIP2, and LZMA;
TAR supports uncompressed, gzip, bzip2, and xz PAX output. Rebuild inspection
fails closed around normalizable container metadata unless the durable lossy-
conversion option is explicit. Special or ambiguous entries always reject the
archive; lossy conversion applies only to safe regular-file metadata that the
Store model cannot preserve.

The general RAR Store is deliberately read-only. Its in-process RAR 3/4/5
indexer reads stored members directly; RAR 5 uses optional maintained `rarfile`
while RAR 3/4 retains the embedded fallback. Compressed members use an optional
bounded `unrar`/`rar` adapter and are size/CRC-or-BLAKE2sp-verified in private
temporary storage before reads are exposed. Encrypted and multi-volume archives
fail with explicit limitation or unsupported-operation reporting. Inventory,
member/total expansion, ratios, paths, extractor output, diagnostics, and time
are all bounded by durable policy.

`sevenzip_readonly` provides bounded, Unicode-exact regular-file inventory and
verified full/ranged reads through optional `py7zr`. Member reads use private
temporary storage, and solid-archive amplification is advertised. Encrypted
and multi-volume 7z archives are explicit limitations; no general mutable 7z
Store is claimed. Header/parser bytes, all entries, paths, members, total
logical bytes, and available ratio evidence are bounded before reads.

SquashFS read/build now uses the same hostile-container posture. Listings and
all entries are bounded and topology-checked; member extraction is timed and
size-spooled with bounded diagnostics and archive-identity checks. Builders
preflight the durable tree, bound `mksquashfs`, then independently inventory and
hash every candidate member before publication. Links and special files reject
the operation, and a successful builder remains sealed.

`rar_build` adds the narrower write-once case. It uses durable filesystem
staging and an explicit, irreversible `seal()` transition rather than pretending
RAR is a mutable Store. An operator-supplied, appropriately licensed `rar`
executable creates RAR 4 non-solid output; LiuXin tests and independently
validates the candidate before a create-only publication. It never replaces or
adopts an output archive, and any successful builder is permanently locked.
Tool path, staging path, timeout, compression level, and safety bounds survive
normal database configuration reload. Registry characteristics advertise the
external creator, licensing responsibility, staging space, create-only
publication, and read-only sealed result.

Structured `StorageCharacteristics` now carries backend constraints through the
optional driver contract, configured Store, manager lookup, and backend
registry. The manager preflights declared object sizes and keeps
archival-snapshot writers out of automatic non-archive placement. Store status
warnings become attributable operational issues. Writable ISO inspection
detects skipped entries, unpreserved Rock Ridge fields, boot/partition data,
unrecognised supplementary descriptors, and hybrid-UDF markers; mutation fails
closed unless the durable `allow_lossy_rebuild` conversion option is explicit.

Characteristics coverage is complete across the built-in registry rather than
being ISO-specific. Local and remote object Stores, read-only transports,
SQLite, both SquashFS lifecycles, both ISO modes, and encrypted wrapping expose
their publication, staging-space, workload, and applicable limitation profile.
Configured read-only policy masks a writable driver's profile to read-only, and
wrapper profiles retain rather than erase their inner Store's limitations.

All registered backend families now use one shared Unicode-path contract for
exact locate/inventory/stat/read/range behavior. Filesystem, SquashFS, and Rock
Ridge tests additionally cover surrogateescaped legacy byte names; HTTP-style
backends cover opaque percent-encoded non-UTF-8 octets.

## Completed production-hardening checklist

1. A live PostgreSQL test covers concurrent manager allocation, ingest,
   interrupted metadata publication, restart recovery, and idempotent replay.
2. Storage schema migrations are versioned and older scratch envelopes upgrade
   transactionally; future envelope versions are rejected rather than guessed.
3. Database-generated record identities and UUID revisions replace
   process-local `max(id) + 1` state. Concurrent SQLite/APSW and PostgreSQL
   managers are covered.
4. `get_operational_status()` aggregates Store availability, journal state,
   Replica health, policy violations, deferred recovery, and concrete recovery
   actions.
5. Compound metadata mutations use a metadata transaction. Store replacement
   prepares the new facade before the durable swap and Store removal forgets
   its configuration only after persistence succeeds.
6. The protected `Live Storage Read Contracts` CI lane exercises HTTP, FTP,
   rclone, and S3 without PR triggers or credential-missing green skips.
7. The concrete database Unit of Work exposes Asset, Replica, Composite, and
   derivation repository ports with explicit commit and rollback. The manager's
   mapping views are compatibility adapters over those ports, not the
   persistence boundary.
8. The shared schema cache now supports ID-scoped invalidation. A 50,000-book,
   50,000-cover, 10,000-tag, 250,000-link synthetic run refreshed one row in
   1.164 ms median using one row read and no table scan. Peak process RSS was
   about 745 MiB during construction, reinforcing that storage must reuse an
   already-configured Core cache and must not create a private one. See
   `storage_cache_benchmark.md`.

## Current verification checkpoint

The pre-refactor storage/ingest baseline was 796 passed and four credential-
gated live tests skipped. The earlier broad storage/ingest/Core run reached 868
passes and four skips while exposing two Store-delete cascade failures; both
were repaired by the restrictive foreign key. The live PostgreSQL scenario
passes, and the cache suite currently passes 142 tests with twelve documented
legacy/live-backend skips. The completed verification selection totals 1,058
passes: 778 storage, 107 ingest/Core, 142 cache, 30 PostgreSQL adapter/schema,
and one live PostgreSQL recovery scenario. Storage has five expected skips
(four credential-gated remote reads plus the separately executed PostgreSQL
case); the cache suite has twelve documented legacy/live-plugin skips.

The 2026-08-22 characteristics, complete backend-profile, and ISO-safety
checkpoint ran the complete `tests/storage` suite: 863 tests passed with five
expected live-backend skips. The focused registry and representative configured
backend matrix passed 365 tests. Targeted doctest verification across the
characteristics API, driver bridge, every affected driver, encrypted wrapper,
and SquashFS lifecycle passed 133 examples with 397 intentional integration
examples skipped; the preceding complete driver docstring checkpoint passed
158 examples with 409 intentional skips.

The subsequent local-archive checkpoint added ZIP/TAR/RAR driver, plugin,
Unicode, lifecycle, deterministic-output, rebuild-safety, RAR adapter,
registry, database-loading, and ingest coverage. Its focused
archive/registry/Unicode selection passed 80 tests. The complete storage suite
then passed 900 tests with the same five expected opt-in live-backend skips.

The build-once RAR follow-up adds durable staging and validated create-only
publication. Its focused archive/registry/Unicode selection passed 95 tests;
the complete storage suite passed 915 tests with the same five expected
opt-in/live-backend skips.

The 2026-08-23 7z/UDF/RAR 5 reader follow-up passed 132 focused
archive/registry/Unicode tests. The affected driver and plugin docstrings passed
36 executable examples with 132 integration examples intentionally skipped.
The complete current storage suite passed 884 tests with the same five expected
opt-in/live-backend skips; its localhost HTTP example was run with local-socket
permission because the normal managed sandbox prohibits socket creation.

The 2026-08-24 sealed-artifact follow-up adds the catalogue workflow above
physical container builders. Completed local images are ingested and managed
Location outputs are adopted in place, then recorded as `PACKAGE` derivations
with path-pinned input identity, digest-pinned tools/dependencies, canonical
settings/environment/commands, output identity, and workflow grouping.
SquashFS backup intent is adapted directly; deterministic builds may claim
exact replay, while RAR uses a best-effort command-aware convenience. The same
generic surface covers 7z, ISO/UDF, ZIP, TAR, and other sealed formats. Backup
source designation and Store planning retain catalogue IDs where available,
and database restart coverage proves the complete recipe envelope survives.
Backup relationships use namespaced `backup:<id>` workflow references rather
than misusing the legacy transform-run foreign key.
The complete storage suite passed 926 tests; 17 expected skips covered missing
optional `py7zr`/`rarfile`/`pycdlib` parsers, disabled credential-backed live
contracts, and the separately configured live PostgreSQL scenario. The focused
sealed-artifact, backup, and derivation selection passed 89 tests, and the new
API modules passed strict direct basedpyright checking without warnings.

The next operational slice is now implemented as
`SquashfsDriveIngestWorkflow`. It adopts rather than copies a messy local drive:
the drive is a read-only unmanaged Store, each valid SquashFS image is a
read-only archive Store, the image is a Digital Asset at its drive Location,
and every regular member is a Digital Asset with an archive Replica. Discovery
uses both suffixes and magic, never follows symlinks, continues with contextual
issues after corrupt images or members, exposes progress and explicit scan
limits, and is idempotent across database-backed manager restart. Technical
metadata is filename/media/container-derived; bibliographic Item creation is
deliberately deferred to the future mixed-format enrichment layer.

That workflow also forced the last hostile-local-name gap closed. File-URI
backend reconstruction now uses filesystem-codec byte decoding, operation UUID
inputs encode lone surrogates safely, authoritative JSON envelopes ASCII-escape
them losslessly, and fallback scalar columns use visible escapes. End-to-end
coverage builds an archive whose image and member names contain undecodable
bytes, ingests it into SQLite, reloads all Stores and Replica records, and reads
the member successfully after restart.

The complete storage checkpoint now passes 930 tests with 17 expected skips
for unavailable optional archive libraries, disabled credential-backed live
contracts, and the separately configured live PostgreSQL scenario. The focused
workflow/example selection passes five tests; the surrounding database reload,
manager contract, backend registry, and SquashFS matrix passes 82 tests with one
optional-`py7zr` skip. Direct basedpyright checking of the new workflow and
executable example reports no errors or warnings.

The recursive-Store foundation now records an explicit durable
`StoreBackingReference` from a read-only container Store to its backing Digital
Asset. Archive and unmanaged Replicas can be selected exactly during
materialization; nested container bytes are copied through normal checked
publication into a managed CACHE Store rather than an ephemeral in-memory or
temporary-path side channel. All Replica modes now round-trip through the
existing Store policy envelope, and database bootstrap topologically orders
backing, materialization, nested, and encrypted Store dependencies. End-to-end
ZIP-in-ZIP coverage reads the innermost member and repeats the read after a
database restart. SquashFS drive ingestion now upgrades each archive Store to
the same explicit backing relationship. Every compressed-container backend now
supplies durable per-container expansion limits; automatic recursive discovery
and cumulative resource/cycle budgets remain a coordinator concern, not Store
API behaviour.
The complete storage-suite checkpoint passes 932 tests with 17 expected
optional-dependency/live-contract skips. A final writable-backed-Store guard
then passed in the focused regression selection; current collection is 950
tests (933 runnable and 17 expected skips). The affected
manager/database/archive matrix passes 195 tests with eight optional-dependency
skips.

The hostile-ZIP checkpoint adds allocation-free central-directory preflight,
strict entry-count agreement, path-topology and local-header validation,
non-regular-member rejection, per-member and aggregate expansion limits, a
compression-ratio ceiling, bounded streaming writes, and overwrite-safe rebuild
tests. All limits persist as Store policy and appear in backend
characteristics. The complete current storage suite passes 950 tests with 17
expected optional-dependency/live skips; current collection is 967 tests. The
ZIP/shared archive implementation also passes direct basedpyright checking
without errors or warnings. Cumulative budgets across recursively discovered
containers remain work for the mixed-format ingest coordinator.

The 2026-08-24 compression-backend hardening checkpoint extends the same
fail-closed policy to TAR, SquashFS, RAR, 7z, and ISO/UDF logical expansion.
Every reader now rejects unsafe topology and enforces durable parser/header,
all-entry, member, total, ratio, path, and format-specific limits before
exposing bytes. External extraction/build tools have bounded output and
runtime. SquashFS and RAR builders preflight staging and independently validate
candidate inventory and content before overwrite-safe or create-only
publication. Writable TAR and ISO enforce streaming and whole-plan budgets
before candidate construction. Registry profiles and database reload preserve
and advertise those constraints, including real object-spooling space.

The focused compression/registry matrix passes 202 tests; affected backup and
sealed-artifact workflows pass 12. The complete storage run yielded 949 passes
and five expected opt-in/live skips in the managed socket-restricted sandbox.
The sole sandbox failure was the localhost HTTP example, which passed on its
required socket-enabled rerun, for an effective 950 passes and five skips.
Direct basedpyright checking reports no errors in the hardened drivers,
builders, or facades; direct affected-module doctests pass 94 examples with 271
integration-only examples explicitly skipped. Per-container protection is
complete; cumulative nested-ingest budgets and ancestry/cycle accounting remain
the coordinator's next security boundary.
