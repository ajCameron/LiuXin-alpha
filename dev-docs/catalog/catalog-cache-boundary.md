# Modern Catalog and cache boundary

Updated **2026-11-21**. This decision supersedes the earlier writable-core
facade split. The generic cache and database layers remain below Catalog semantics
and Catalog-cache orchestration. Existing transaction and receipt guarantees remain.

## Decision

Use `CatalogCache` for metadata operations against an attached cache. It lives
in `caches.metadata`, alongside the existing metadata service, avoiding another
top-level package. `Catalog` remains the authoritative metadata persistence
service. Generic `Cache` / `ReadCache` and `databases` supply mechanisms below
these services.

```text
application / library
        |
        v
caches.metadata.CatalogCache
        |                  |
        | metadata policy  | physical reads / refresh
        v                  v
     Catalog          generic Cache / ReadCache
        |                  |
        v                  v
     databases        storage-cache backends
                           |
                           v
                        databases
```

Catalog retrieval can also consume the lower read-cache capability explicitly;
Catalog never imports the upper `caches.metadata` composition package. Core cache
modules cannot import either Catalog or `caches.metadata`. These rules cover
runtime, deferred and type-only imports in the architecture checker.

Metadata awareness is useful for efficient operations such as tag identity lookup,
Work hydration and semantic search, but it does not require metadata policy inside
the generic cache. The upper layer selects physical queries/indexes using Catalog's
semantics. There is one set of cached state, indexes and locks. `CatalogCache`
composes the core and reuses the `MetadataAPI` service implementation; it does not
subclass the core cache or mirror its maps. Explicit operations such as
`ensure_tag(text)` now live on the metadata contract and this upper implementation.
Catalog owns exact identity and serialized reuse; the upper layer reports the
write outcome and coordinates cache freshness. See the
[core metadata coverage](../caches/metadata-api.md#core-metadata-coverage).

Vocabulary trees follow the same boundary. Catalog's Genre, Subject and Series
repositories own exact path reuse, cycle prevention, sibling ordering and explicit
leaf/subtree deletion. Their CRUD and batch update paths share these rules.
`caches.metadata` uses the shared Catalog traversal semantics with one captured
read source and coordinates reconciliation after mutations. Bounded ordered child
SQL lives in databases. See [vocabulary trees](../caches/metadata-api.md#vocabulary-trees)
for the tighter deletion contract and unchanged globally unique Series identity.

The **2026-11-22** ergonomics extension keeps literal shared-record selection
and role/membership policy in Catalog. `catalog.record_queries` owns alias/type
validation and literal comparison; `catalog.api` owns passive result values and
capability protocols. The metadata adapter coordinates lookup, suggestions,
hydrated search pages, memberships and shared edit preconditions through the
same read/write boundaries. No metadata methods move into the generic cache.
Agent credit priority allocation belongs to Catalog and uses the database's
existing extrema and writer-reservation mechanisms; the facade does not issue SQL.

## Composition and migration

```python
from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.caches.metadata import CatalogCache, MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog

core = create_cache(database)
catalog = Catalog(database)
cached = CatalogCache(catalog, cache=core)

receipt = cached.works.create({"title": "Dune"})
work = cached.works.require(receipt.result)
writer = cached.create_writer("works", "work_title")
writer.write_one(receipt.result, "Dune: revised")

# No cache required for a standalone metadata service.
uncached = MetadataAPI(catalog)
```

`CatalogCache` requires an already loaded core bound to the same database object.
The composing caller owns and closes the core and database. `cached.cache is core`;
physical operations use that core directly. Retained semantic services and writers
reject a closed or reattached core. Database fallback is explicitly configured at
construction and disabled by default. CoreServices retains its `metadata` property,
constructing `CatalogCache` when configured with a cache and `MetadataAPI` otherwise.

| Previous entry point | Current entry point |
| --- | --- |
| `cache.metadata.works` | `cached.works` |
| `cache.metadata.search` | `cached.search` |
| `cache.create_writer(...)` | `cached.create_writer(...)` |
| `cache.write(...)` / `cache.write_one(...)` | `cached.write(...)` / `cached.write_one(...)` |
| `cache.get(...)` / `cache.query(...)` | `core.get(...)` / `core.query(...)` |
| `cache.close()` | `core.close()` |

`Cache.metadata` and the core's writer entry points are removed. There are no
forwarding methods on the core. The unused `CacheWriter` protocol is removed;
schema writers retain the `catalog.write.SchemaCatalogWriter` return type. `MetadataAPI(catalog, cache=core)` remains a valid
explicit service binding; `CatalogCache` additionally requires a cache and exposes
the schema writer bridge.

Generic reconciliation accepts an authoritative receipt and physical dependencies:
`core.reconcile(receipt, tables=..., links=..., fields=...)`. The writer bridge in
`caches.metadata.writers` interprets Catalog updates and supplies these dependencies.
The core owns refresh, dirty-state recovery and generation changes, without knowing
whether a table describes Works, tags or unrelated data. Metadata service writes
still conservatively refresh complete snapshots; this separation does not claim a
new targeted-refresh optimization.

See [generic cache](../../src/LiuXin_alpha/caches/cache.py),
[core lifecycle and reconciliation](../../src/LiuXin_alpha/caches/read_cache.py),
[Catalog-cache facade](../../src/LiuXin_alpha/caches/metadata/cache.py) and
[metadata retrieval](../../src/LiuXin_alpha/catalog/retrieval/metadata.py).

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

Direct database-backed Catalog reads remain supported for maintenance,
migrations, batch jobs, tests, and deployments without an attached cache. They
do not implicitly populate or reconcile a separate snapshot cache.

Explicit cache-backed retrieval can initialize a reader: `_loaded_cache()`
creates a read cache when none is supplied, or loads a supplied empty cache.
`CacheMetadataReadSource.refresh()` explicitly requests `cache.reload()`.
These are cache lifecycle operations requested by the read adapter; they do
not make Catalog writes automatically update that cache. Cache contents and
refresh policy remain owned by the cache implementation. See
[reader initialization](../../src/LiuXin_alpha/catalog/retrieval/metadata.py) and
[the read-source adapter](../../src/LiuXin_alpha/catalog/retrieval/hydration/read_sources.py).

## Write contract

With an attached cache, metadata-shaped writes use `CatalogCache`, for example
`cached.works.update(work_id, {"title": "Revised title"})` or
`cached.mutations.replace_metadata(...)`. These operations return explicit
persistence/freshness receipts and coordinate complete metadata dependencies.

Schema-directed field writes use the same upper facade:

```python
cached.create_writer(...)
cached.write(...)
cached.write_one(...)
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

They are reconciliation-aware conveniences over the public Catalog writer
boundary, not an independent semantic-validity authority. Their internal
`_apply_*` methods are private host seams used by configured writers only.

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

Catalog mutations do not select a reconciliation policy. That decision belongs
to the cache facade holding the backend and its affected dependencies; a read
cache supplied to metadata retrieval does not transfer write reconciliation into
Catalog.

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
| Generic Cache / ReadCache | Query/index service, immutable schema/results, lifecycle/generation, invalidation and physical reconciliation | Metadata operations, Catalog imports, identity policy or persistence |
| CatalogCache | Metadata reads, coordinated Catalog writes, dependency translation and cache publication | Independent metadata policy or a second copy of cache state |
| StorageCache plugin | Cached storage-shaped rows, fields, links, and optional backend accelerators | Application query policy, Catalog writes, or UI state |
| Catalog | Metadata semantics, matching, validation, WEMI-aware persistence, authoritative write results and hydration through an explicit read source | Cache map mutation or write reconciliation policy |
| Database | Connections, transactions, SQL, schemas, constraints, migrations, and portable storage operations | Cache state or bibliographic policy |
| Application/library | Choosing the attached cache facade as the preferred entry point and coordinating workflows | Reimplementing Catalog or cache consistency rules |

Small operation-local memoization inside Catalog is not storage-cache state.
For example, an immutable update object may memoize a resolved value during one
write. It must not outlive that operation as an application cache or introduce
a dependency on the cache layer.

## Metadata access interface direction — 2026-11-18

**Interface, explicit CatalogCache facade and Core metadata binding implemented.**
[MetadataAPIInterface](../../src/LiuXin_alpha/catalog/api/metadata_access.py) defines a
native metadata-facing contract in `LiuXin_alpha.catalog.api.metadata_access`,
alongside Catalog's existing public protocols. The [bound adapter](../caches/metadata-api.md) lives
in `caches`, alongside its existing Catalog write bridge. This uses existing
top-level packages. Importing the contract does not construct Catalog, cache or
metadata services. Catalog and the generic Cache retain their own APIs;
`CatalogCache` implements the metadata service above them.

The interface takes its vocabulary from Catalog: `works`, `expressions`,
`manifestations`, `items`, `search` and `mutations`. Entity services offer
`get`, `require`, `list`, `create`, `update` and `delete`. Reads return the
existing native Work/Expression/Manifestation/Item metadata contracts. Search
returns explicit `(level, entity_id)` references; a later metadata read is a
separate operation, not an implied shared snapshot. Compound mutations retain
Catalog's `create_wemi_stack`, `attach_metadata`, `replace_metadata`, `link_wemi`
and `unlink_wemi` vocabulary and semantic rules.

The implementation borrows Catalog and an optional cache for the same
database once; their composing caller retains ownership and shutdown duties.
Core can construct and expose it, but the service must also be
usable in process without Core's command dispatcher. Cache selection and database
fallback policy belong to that binding. Known misses, unavailable operations and
failed reads remain distinct. Returned metadata is detached from cache storage;
editing a container does not implicitly persist it.

The interface deliberately does not inherit all of `CatalogAPI`: that would expose
database handles, physical row mappings, schema writers and compatibility helpers,
and raw repository writes would bypass reconciliation. Declaring the contract in
`catalog.api` does not make Catalog's mutation implementation responsible for
cache reconciliation. The interface also does not sit under
`metadata`, whose package owns pure values and rules rather than persistence
composition. Catalog owns validation, identity decisions and persistence; caches
own refresh/invalidation mechanics; the bound access service owns coordinating
the operation across those owners.

`MetadataWriteReceipt` records the operation result, whether it changed anything,
whether persistence is committed or pending an outer transaction, cache freshness
and the cache generation when configured. Pending writes must not be presented as
durable success. Receipts describe return-time state; a pending receipt does not
later become committed in place. Deferred snapshots must recover after either
commit or rollback.
Reconciliation errors retain the receipt in `MetadataReconciliationError` and
leave the cache dirty; callers must not repeat a committed write automatically.
These are implementation requirements, not guarantees supplied by a Protocol or
the passive receipt dataclass. Unsupported transaction capabilities must not
silently weaken an atomic operation.

The current contract covers native WEMI CRUD, reads, search, compound operations,
Agent/vocabulary and custom-field administration, exact vocabulary ensures,
identifiers, text attachments and Item annotations. General matching workflows
and merges retain their separate Catalog contracts. Calibre projection
and write policy remain in their compatibility layer. This is not whole-Catalog
feature parity.

The adapter in `caches` should reuse the existing Core semantic-write composition
and cache writer mechanisms. Core currently uses a broad data reload for many
semantic writes. A conservative refresh remains a valid first implementation when affected
dependencies cannot yet be proven; later targeted refresh must preserve the same
contract. Every exposed mutation must pass through that coordination, including
creation, deletion and relationship replacement. A forwarding `__getattr__` is not
an adequate implementation of this boundary.

The implementation acceptance gate is one typed consumer exercising the same
semantic operations without a cache and with each native backend, including
read-after-write, detached results, wrong-database binding, known misses,
unsupported reads, outer commit/rollback and receipt-bearing refresh failures.
See [CACHE05](../caches/next_tasks.md#cache05-implement-the-bound-metadata-access-service).

## Legacy scope

This contract applies to modern `CacheAPI`, its internal `StorageCacheAPI`
plugin seam, normalized Catalog writers, repositories, mutations, and the
portable database path.

The following compatibility areas are not evidence for the modern ownership
model:

- dual-purpose writers under `utils.calibre_compat.caches.write`, which combine
  database and Calibre-style map updates;
- legacy search-result caching and typed operators under
  `utils.calibre_compat.caches.search`;
- retained caches, base contracts, categories and lazy metadata under
  `utils.calibre_compat.caches`;
- legacy field descriptor containers under `utils.calibre_compat.metadata`.

Native `database_search.CatalogSearch` and `physical_plan` remain Catalog semantic
search responsibilities. Native descriptor snapshots and custom-field publication
belong to caches. The old paths have been removed without forwarding facades;
see the [implemented structure](../caches/structure.md).

The unused `catalog_macros` facade was removed on 2026-10-30.
`databases.metadata_sql` retains physical-table helpers without book/title
projections. Cover-flag writes and legacy format removal refuse effects
until explicit asset/replica policies are available.

No new modern feature should depend on compatibility shapes. Preserve the
read/write cache split and keep reconciliation outside Catalog mutations.
`library.library_metadata`, wider book metadata and Catalog conversion policy
remain separate review subjects.

## Enforcement

Modern boundary tests should prove that:

- Catalog metadata retrieval composes only the Catalog-independent read facade,
  while mutations and writers do not import writable cache implementations;
- protected import checks and the whole-package baseline retain no Catalog/cache
  dependency cycle;
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
