# Storage component status and runtime composition

Updated: 2026-08-21

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
