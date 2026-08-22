# Storage/cache composition checkpoint - 2026-08-21

## Decision

Production storage must not use `InMemoryStorageManager` as its implementation
base or maintain a second private catalogue cache. LiuXin already has a cache
facade with lifecycle, consistency, invalidation, generation, and Core
ownership semantics.

`StorageManager` now derives from a repository-neutral orchestration core. Its
database collections are mapping-shaped repository views with no retained
record dictionary. `TransientStorageManager` is the disposable implementation;
the old `InMemoryStorageManager` name is a compatibility alias.

The remaining process-local Store registry is intentional: constructed backend
facades and locks are live resources, not catalogue records or a cache. Startup
and reload reconstruct that registry from durable Store rows.

## Cache integration

`DatabaseStorageMetadataRepository` can bind Core's existing `Cache`:

- cached main storage tables serve record reads;
- repository writes commit to the database and invalidate affected cache row
  IDs or relations;
- storage helper/workflow tables omitted by cache schema continue to read
  directly from the database;
- unbinding returns to direct repository reads; and
- Core unbinds storage before closing a cache it owns.

No cache is created merely to make storage work. The database is always
authoritative, and standalone `Database.storage` remains usable without Core.

## Evidence

- Production `StorageManager` is not a subclass of
  `TransientStorageManager`.
- Durable manager `_assets` and `_replicas` are repository mappings, not
  dictionaries.
- Cache-backed ingest becomes visible through `Cache.get(...)`, and direct
  reads continue after cache unbinding/close.
- The complete restart scenario passes with the cache attached: rich metadata,
  zero-copy policies, Composite membership, derivation provenance, custom Item
  roles, interrupted publication recovery, and idempotent retry.
- Core cache composition passes for SQLite and SQLite APSW.
- Removing the private snapshot exposed and repaired a hidden schema defect:
  deleting a Store can no longer cascade-delete its durable Replica claims.
  Claimed Stores must be retired/offlined until those claims are resolved.
- ID-scoped invalidation is now real rather than advisory: the schema-backed
  plugin reads one changed row and repairs its indexes and relation projections
  without a whole-table reload.

## Follow-ups

The production-hardening list in
`dev-docs/storage/storage_component_status.md` is complete. The 50,000-book
cache run confirms bounded mutation refresh, while its roughly 745 MiB peak RSS
also confirms the architecture decision: storage may share Core's configured
cache but must remain fully usable through direct repository reads.

## Verification checkpoint

The broad storage/ingest/Core pass reached 868 passed and four skipped, with
two failures revealing the Store-delete cascade defect described above. Both
driver cases passed after the schema correction. The final focused database,
cache/Core lifecycle, manager contract, and PostgreSQL schema pass is 80/80.
