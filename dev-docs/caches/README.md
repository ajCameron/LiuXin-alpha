# Caches

Reviewed **2026-10-20**. This folder owns derived catalogue-cache guidance.
Persistent bibliographic truth remains in the database, through Catalog writes.
See [next tasks](next_tasks.md).

## Current composition

[ReadCache](../../src/LiuXin_alpha/caches/read_cache.py) owns lifecycle, structured
queries and freshness without importing Catalog. Catalog retrieval can compose
this read capability. [Cache](../../src/LiuXin_alpha/caches/cache.py) adds the
Catalog mutation bridge: configured writers persist through Catalog, then
reconcile affected derived state.

The bridge intercepts its prepared update routes; arbitrary delegated Catalog
methods do not automatically gain cache reconciliation. A direct Catalog or
external database write must therefore be paired with explicit invalidation or
a live backend. See the [Catalog/cache contract](../catalog/catalog-cache-boundary.md).

## Backend choice

| Backend | Read state | Refresh contract |
| --- | --- | --- |
| `schema_backed` | Loaded schema-aware snapshot. | Explicit dirty dependencies; supports targeted row repair. |
| `database_backed` | Live database reads with low retained state. | Query/index lifecycle still matters; do not infer snapshot semantics. |
| `numpy_vectorized` | Array-oriented loaded state. | Correct whole-table fallback where targeted refresh is unavailable. |

Read capability flags rather than selecting behaviour from a backend's name.
The [backend guide](<../08 - Storage Cache Backends.md>) owns the detailed query,
lookup and mutation contract. The [registry](../../src/LiuXin_alpha/caches/cache_plugins/registry.py)
stores import targets and loads them on demand; its process-local registration
currently overwrites matching names/aliases and is not synchronized.

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

Older Calibre-oriented caches under `library.caches` and `plugins.cache` remain
compatibility areas. Their names alone are not evidence that they can be removed
or substituted for this facade.

## Validation

Check lookup completeness, known misses, dirty/read failures, targeted refresh
and mutation receipts with small fixtures. The
[storage benchmark](../storage_and_backup/storage_cache_benchmark.md) provides
dated performance evidence and a deliberate large-run recipe, not a routine
documentation test or universal latency target.

Return to the [developer index](../README.md).
