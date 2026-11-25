# Caches

Reviewed **2026-10-20**. This folder owns derived catalogue-cache guidance.
Persistent bibliographic truth remains in the database, through Catalog writes.
See [next tasks](next_tasks.md).

The [2026-11-16 publication and lifecycle review](../architectural_reviews/caches-review-2026-11-16.md)
retains these ownership boundaries. CR01 is repaired by native snapshot publication;
CR02/CR03 are repaired by the metadata-search lifecycle work, with regression
coverage for partial failure, recovery and invalidation validation. It is an initial focused review, not module sign-off.

## Current composition

[ReadCache](../../src/LiuXin_alpha/caches/read_cache.py) owns lifecycle, structured
queries and freshness without importing Catalog. Catalog retrieval can compose
this read capability. [Cache](../../src/LiuXin_alpha/caches/cache.py) exposes the
generic core. [CatalogCache](../../src/LiuXin_alpha/caches/metadata/cache.py)
composes that core with Catalog in the upper `caches.metadata` package. Its
configured writers persist through Catalog, then reconcile affected derived state.

The bridge intercepts its prepared update routes; arbitrary delegated Catalog
methods do not automatically gain cache reconciliation. A direct Catalog or
external database write must therefore be paired with explicit invalidation or
a live backend. See the [Catalog/cache contract](../catalog/catalog-cache-boundary.md).

Catalog's cache-backed read adapters can load an empty cache and explicitly
request a reload. Those lifecycle calls do not give Catalog mutations ownership
of cache contents or write reconciliation; the cache implementation performs
the refresh.

The bound [native metadata service](metadata-api.md) is available as
`CatalogCache(catalog, cache=cache)` or uncached `MetadataAPI(catalog)`. It implements the declared
[MetadataAPIInterface](../catalog/catalog-cache-boundary.md#metadata-access-interface-direction--2026-11-18)
with detached metadata reads, semantic mutations and persistence/freshness receipts.
Core command migration remains [CACHE05](next_tasks.md#cache05-implement-the-bound-metadata-access-service).

## Backend choice

| Backend | Read state | Refresh contract |
| --- | --- | --- |
| `schema_backed` | Loaded schema-aware snapshot. | Explicit dirty dependencies; supports targeted row repair. |
| `database_backed` | Live database reads with low retained state. | Query/index lifecycle still matters; do not infer snapshot semantics. |
| `numpy_vectorized` | Array-oriented loaded state. | Correct whole-table fallback where targeted refresh is unavailable. |

Read capability flags rather than selecting behaviour from a backend's name.
The [backend guide](<../08 - Storage Cache Backends.md>) owns the detailed query,
lookup and mutation contract. The [registry](../../src/LiuXin_alpha/caches/cache_plugins/registry.py)
stores import targets and loads them on demand. Names/aliases have immutable
process-local bindings; identical registration is idempotent and collisions fail
atomically. Lookup/publication is synchronized; imports run outside the lock.

A cache miss, unsupported operation and broken query are distinct. A capability
adapter may explicitly select a fallback; arbitrary exceptions must not become
empty results or silent database retries. The
[surface failure contract](../surfaces/read-model-failures.md) covers that presentation.

## What this folder does not conflate

Storage can share the application catalogue cache and invalidate changed row IDs.
That avoids a second metadata snapshot owned by the storage manager. A **CACHE
Replica**, however, is retained physical data with storage lifecycle requirements;
it is not a row in this in-memory catalogue cache. Mixed ingest still needs a
live pin/lease contract before such bytes can be evicted during active work.

The retained Calibre cache, its base family, writers, categories, lazy metadata
and search engine now live under `utils.calibre_compat.caches`. Native field
metadata is separate from legacy descriptor containers. Old imports have been
removed without forwarding facades.

See the [implemented structure](structure.md) for package contents, native field
keys, publication behavior and remaining boundaries. The
[compatibility review](calibre-compatibility-review-2026-11-16.md) records the caller
evidence and original migration outline.

See [metadata search](metadata-search.md) for native predicates, the explicit
Calibre adapter, template permission and the staged parity ledger.
The [2026-11-17 stress-test report](metadata-search-testing-2026-11-17.md) records
the correctness regressions and repeatable baseline for subsequent optimization.
The [subsequent architecture assessment](../architectural_reviews/caches-review-2026-11-17.md)
finds the metadata-search core ready for measured optimization and records the
priorities and remaining whole-cache sign-off work.
The [candidate and definition optimization](metadata-search-scope-optimization-2026-11-17.md)
records the next adapter changes, matched measurements and regression evidence.
The [native sorting and pagination pass](pagination-optimization-2026-11-18.md)
records bounded page selection, empty-page shortcuts and their measured trade-offs.
The [invalidation and lifecycle follow-up](invalidation-and-lifecycle-2026-11-18.md)
records bounded membership repair, write-route ownership and backend registry policy.

## Validation

The [long-running cache lane](long-running-tests.md) adds opt-in mutation/query
soaks and repeated real database captures, with mandatory pass auditing, timings
and optional call profiles. It has a separate manual CI workflow.

Check lookup completeness, known misses, dirty/read failures, targeted refresh
and mutation receipts with small fixtures. The
[addition/removal matrix](invalidation-and-lifecycle-2026-11-18.md#addition-and-removal-follow-up)
also covers empty/refilled tables, reused identities, shared relationships,
warm indexes and real insert/delete transaction rollback across the three backends. The
[storage benchmark](../storage_and_backup/storage_cache_benchmark.md) provides
dated performance evidence and a deliberate large-run recipe, not a routine
documentation test or universal latency target.

Return to the [developer index](../README.md).

The [2026-11-18 typing pass](typing-2026-11-18.md) extends both static checkers to
all native cache modules and documents the database, query, projection and writer
contracts, including the remaining dynamic payload boundaries.
Its [2026-11-19 follow-up](typing-2026-11-18.md#documentation-and-typing-follow-up--2026-11-19)
records the package-wide comments/docstring pass and annotation import repairs.
