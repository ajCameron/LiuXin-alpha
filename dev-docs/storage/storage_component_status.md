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
cached.

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

## Remaining production work

These are follow-ups, not reasons to redesign the public storage API:

1. Run real PostgreSQL end-to-end storage and recovery tests in addition to
   the portable schema contracts.
2. Add explicit migrations and upgrade fixtures for pre-journal and older
   storage catalogues, including versioned scratch-envelope migration.
3. Replace process-local `max(id) + 1` allocation with database-safe identity
   allocation and test concurrent managers/processes.
4. Expose operational status for pending/failed ingest journal entries,
   unavailable or corrupt Replicas, policy violations, and recovery actions.
5. Review compound Store reconfiguration, Composite, derivation, policy, and
   Item-link mutations for transaction/recovery guarantees equivalent to
   ingest where necessary.
6. Run the read-only live backend contracts in protected CI for the remote
   backends LiuXin officially supports.
7. Converge the manager's internal mapping-shaped orchestration seam onto the
   existing storage persistence SPI and unit-of-work protocols. The present
   repository views are non-caching and database-authoritative, but the SPI is
   the clearer long-term internal contract.
8. Benchmark cache invalidation and refresh costs with realistically large
   storage catalogues. Prefer table-scoped/bounded cache work over introducing
   another storage-specific object cache.

## Current verification checkpoint

The pre-refactor storage/ingest baseline was 796 passed and four credential-
gated live tests skipped. The broad storage/ingest/Core run reached 868 passes
and four skips while exposing two Store-delete cascade failures; both were
repaired by the restrictive foreign key and passed on targeted rerun. The
final focused database restart, cache-sharing, Core lifecycle, manager
contract, and PostgreSQL schema suite passes all 80 tests. A fresh broad run is
required after any further cache backend or identity-allocation work.
