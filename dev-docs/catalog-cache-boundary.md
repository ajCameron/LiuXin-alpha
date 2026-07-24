# Modern Catalog and cache boundary

Status: adopted for the modern stack, 2026-07-24.

## Decision

When a composed cache is attached, application and library code should prefer
its read and write APIs. `CacheAPI` is the performance-facing facade; `Catalog`
remains the authoritative metadata-aware persistence service, and `databases`
remains the raw storage layer.

The dependency and call direction is:

```text
application / library
        |
        v
composed Cache facade
        |
        +-- query --> cached rows, relationships, and indexes
        |
        `-- write --> Catalog API --> database
                           |
                           `-- authoritative result
                                      |
                                      v
                              cache reconciliation
```

The direction is deliberately one-way:

```text
cache -> Catalog -> database
```

Cache code may call `Catalog`. `Catalog` must not import a cache
implementation, receive a cache object, mutate cache maps, or invoke
cache-update callbacks.

## Read contract

The cache provides first-class immutable typed query APIs; otherwise it cannot
deliver its intended performance benefit. Depending on the backend, these APIs
may serve:

- cached scalar rows;
- cached relationship and link metadata;
- field-oriented values;
- normalized lookup indexes;
- query or search indexes owned by the cache layer; and
- cache-backed projections when their invalidation dependencies are declared.

A snapshot-backed cache answers from its loaded state and explicitly reloads
or invalidates after relevant changes. A database-backed cache may use live
reads while retaining the same public cache API. Backend capability flags
describe this difference; callers should not infer it from a class name.

A dirty dependency may cause the cache layer to reload after the surrounding
transaction closes. Core queries and known misses never silently delegate a
read downward. An explicit read-source adapter may fall back for an unavailable
or incomplete operation. The cache must not move metadata identity, matching,
validation, or mutation policy
out of `Catalog` merely to make a lookup faster. A cached index is an execution
mechanism, not a second definition of bibliographic identity.

Direct Catalog reads remain supported for maintenance, migrations, batch jobs,
tests, and deployments without an attached cache. They do not implicitly
populate or reconcile a separate snapshot cache.

## Write contract

With an attached cache, the preferred application write entry points are:

```python
cache.create_writer(...)
cache.write(...)
cache.write_one(...)
```

These methods do not create a second persistence implementation. The
cache-aware facade:

1. validates that it is still attached to the database for which its writer
   was created;
2. asks the normal Catalog writer to build and apply the semantic mutation;
3. lets `Catalog` and portable database macros validate and commit the
   authoritative database change;
4. receives the persisted result; and
5. reconciles only the affected cache objects.

Catalog may also be called directly:

```python
catalog.write(...)
catalog.write_one(...)
catalog.identifiers.replace_for_wemi(...)
```

That is the correct path when no cache is attached or when a controlled
maintenance operation owns cache invalidation separately. Ordinary
application updates should flow through the cache-aware facade in preference
so an attached snapshot cannot silently become stale.

Raw database writes are lower-level escape hatches for migrations,
maintenance, schema operations, and measured internal fast paths. They are not
the normal metadata mutation API.

## Meaning of a Catalog result

A Catalog write result is an authoritative persistence receipt, not a cached
result and not a command sent upward into a cache.

Current result shapes include:

```python
Mapping[SourceId, StoredValue]
Mapping[SourceId, tuple[LinkRow, ...]]
Mapping[NormalisedIdentifierScheme, EntityId]
```

The result tells the caller which canonical values, IDs, or relationship rows
exist after the write. The cache uses that information to decide whether to:

- do nothing because its reads are live;
- reload an affected main or link table;
- invalidate an affected object while an outer transaction is still open; or
- update a cache-owned index whose dependencies are known.

Catalog does not decide among those policies because it does not know whether
a cache exists or which backend is attached.

## Consistency and failure rules

The modern path must preserve these invariants:

1. Validation or database failure leaves cache state unchanged.
2. Cache state must never advance before the authoritative Catalog write
   succeeds.
3. Successful reconciliation touches only objects affected by the returned
   result and normalized update.
4. A cache-bound writer cannot be used after its cache is detached or attached
   to another database.
5. Direct Catalog or raw-database changes are external changes from the point
   of view of a snapshot cache and require explicit reload or invalidation.
6. If the database commit succeeds but cache reconciliation fails, affected
   dependencies are marked dirty and the raised error carries the authoritative
   Catalog receipt.
7. Readers see one complete cache generation; reload and reconciliation state
   changes are atomic under the cache lock.

The modern composed facade implements database-first reconciliation, targeted
reload/invalidation, live-backend no-ops, detached-writer rejection, defensive
dirtying after reconciliation failure, and recovery on the next safe read.

## Ownership boundaries

| Layer | Owns | Does not own |
| --- | --- | --- |
| Composed Cache | Query/index service, immutable schema/results, lifecycle/generation, invalidation, Catalog-delegating writes, and reconciliation | Canonical metadata persistence, WEMI identity policy, or raw SQL |
| StorageCache plugin | Cached storage-shaped rows, fields, links, and optional backend accelerators | Application query policy, Catalog writes, or UI state |
| Catalog | Metadata semantics, matching, validation, normalized mutations, WEMI-aware persistence, and authoritative write results | Cache maps, cache lifecycle, cache backend selection, or reconciliation policy |
| Database | Connections, transactions, SQL, schemas, constraints, migrations, and portable storage operations | Cache state or bibliographic policy |
| Application/library | Choosing the attached cache facade as the preferred entry point and coordinating workflows | Reimplementing Catalog or cache consistency rules |

Small operation-local memoization inside Catalog is not storage-cache state.
For example, an immutable update object may memoize a resolved value during one
write. It must not outlive that operation as an application cache or introduce
a dependency on the cache layer.

## Legacy scope

This contract applies to modern `CacheAPI`, its internal `StorageCacheAPI`
plugin seam, normalized Catalog writers, repositories, mutations, and the
portable database path.

The following compatibility areas are not evidence for the modern ownership
model:

- the older dual-purpose writers under `caches.write`, which still combine
  database and Calibre-style map updates;
- Calibre search-result caching currently located under `catalog.search`;
- the older cache implementations under `library.caches` and
  `customize.cache`; and
- frozen `catalog_macros` reference helpers which still display historical
  cache callback shapes but have no production callers.

No new modern feature should depend on those shapes. The intended cleanup is
to move Calibre compatibility under `utils`, while modern cache behavior
remains behind `CacheAPI`. Compatibility relocation must not reverse
the modern dependency direction or move cache reconciliation into `Catalog`.

## Enforcement

Modern boundary tests should prove that:

- the Catalog facade, repositories, matching, mutations, retrieval, and
  normalized writers do not import cache implementations;
- direct Catalog writers never reconcile cache state;
- cache-aware writes delegate to the same Catalog persistence seam;
- failed writes leave cache state unchanged;
- successful writes reconcile snapshot backends and require no refresh for
  declared live backends;
- detached cache writers reject before persistence;
- reconciliation failure dirties affected cache state and preserves the
  authoritative receipt;
- complete misses do not trigger hidden database reads; and
- all built-in plugins satisfy the same query-result contract.

Cache-specific behavior tests belong with the cache suites even when the
operation delegates to a Catalog repository.
