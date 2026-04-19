# Storage Cache Backends

The storage cache layer sits between the live database and higher-level views.

Its job is to expose:

- cached main-table objects
- cached link-table objects
- storage-facing field objects built on top of those tables

This is lower-level than any browse/search/interface cache.
It is concerned with storage-shaped values and relations, not presentation.

## Current plugin model

The storage cache is now plugin-backed.

Code that wants a specific backend should go through:

```python
from LiuXin_alpha.caches import create_storage_cache

cache = create_storage_cache(db, "schema_backed")
cache.read()
```

Current builtin plugins are:

- `schema_backed`
- `database_backed`
- `numpy_vectorized`

`StorageCache` still points at the default `schema_backed` implementation.

## `schema_backed`

This is the canonical baseline backend.

Semantics:

- reads schema and rows into in-memory table objects
- builds field objects over that in-memory state
- behaves like a normal explicit cache
- external database changes are not assumed to be visible until reload/invalidation

Tradeoffs:

- simplest mental model
- best baseline for correctness and mutation behavior
- most obvious backend to target when adding new cache features
- not "live" against external writers unless told to refresh

Use it when:

- ordinary app code wants a predictable cache
- tests want stable explicit cache semantics
- a new cache feature is being designed and should land on the canonical backend first

## `database_backed`

This is the correctness-first live backend.

Semantics:

- keeps a database reference on the cache and on returned child objects
- refreshes the underlying schema-backed state on access
- returned table and field objects are live proxies, not one-shot snapshots
- external database changes become visible without manual invalidation

Tradeoffs:

- much stronger live-read semantics
- safer when other code may mutate the database behind the cache's back
- intentionally more expensive than a normal explicit cache
- favors "what is true right now" over throughput

Use it when:

- external writers or direct database edits are expected
- stale child objects would be dangerous
- debugging or integration work wants the cache API without snapshot semantics

Avoid using it as the default everywhere just because it is convenient.
It is a policy choice: better freshness, worse performance.

## `numpy_vectorized`

This backend layers vectorizable arrays over the schema-backed cache state.

Semantics:

- reads the same schema-backed cache state first
- builds numpy arrays for owner ids and field values where useful
- keeps the ordinary storage cache API
- falls back to schema-backed behavior when a fast vectorized path is not appropriate

Tradeoffs:

- useful fast path for read-heavy scalar access
- good base for future high-performance cache work
- still fundamentally snapshot-oriented like `schema_backed`
- requires reload after external database changes
- relation-heavy paths may still use ordinary backend logic

Use it when:

- read-heavy code benefits from vectorized/scalar hot paths
- benchmarking and performance work wants a plugin seam instead of a rewrite
- you want the normal storage-cache API but with better scalar read characteristics

## Recommended selection rule

The intended rule of thumb is:

- `schema_backed` for ordinary explicit cache behavior
- `database_backed` when freshness against external mutation matters more than speed
- `numpy_vectorized` when read-heavy paths need a faster backend and snapshot semantics are acceptable

## Design rule for future backends

New cache backends should be explicit about what they promise.

At minimum, a backend should make clear:

- whether reads are snapshot-based or live
- whether child objects stay live after they are handed out
- whether external database changes require reload/invalidation
- whether it adds fast paths or only changes semantics

Backends should implement the storage cache API rather than teaching callers to depend on backend-specific internals.

## Testing rule for future backends

If a new backend is added, it should join the generic cache-plugin contract tests.

That contract layer is where we should pin down backend-independent behavior such as:

- Unicode handling
- canonical vs alias field lookup
- row helper/default semantics
- lifecycle behavior

Backend-specific tests should still exist where a plugin has unique behavior, but the common contract should stay shared.
