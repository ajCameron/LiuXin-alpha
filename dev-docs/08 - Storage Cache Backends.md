# Storage Cache Backends

Storage cache plugins sit underneath the modern composed `Cache` facade.

Its job is to expose:

- cached main-table objects
- cached link-table objects
- storage-facing field objects built on top of those tables

This is lower-level than structured application queries. It is concerned with
storage-shaped values and relations, not presentation, sorting, paging, or
search policy.

## Application entry point

Application code should construct the composed facade:

```python
from LiuXin_alpha.caches import CacheQuery, CacheSort, create_cache

cache = create_cache(db, "schema_backed")
page = cache.query(
    CacheQuery(
        table="works",
        sort=(CacheSort("work_canonical_title"),),
        limit=25,
    )
)
```

`create_cache()` loads its storage plugin by default. It exposes lifecycle
state, generation, immutable table/column introspection, exact lookup,
structured queries, relationship traversal, explicit invalidation, and
Catalog-delegating writes.

`create_storage_cache()` remains the low-level plugin-development seam. Its
result intentionally has no Catalog writer API.

## Current plugin model

The storage cache is now plugin-backed.

Plugin tests or backend development that wants a raw backend can use:

```python
from LiuXin_alpha.caches import create_storage_cache

cache = create_storage_cache(db, "schema_backed")
cache.read()
```

Current builtin plugins are:

- `schema_backed`
- `database_backed`
- `numpy_vectorized`

If code needs to branch on backend semantics, it should prefer declared
capabilities over class-name checks:

```python
from LiuXin_alpha.caches import get_cache_plugin_capabilities

caps = get_cache_plugin_capabilities("database_backed")
assert caps.live_reads is True
```

Instance code can also inspect `cache.capabilities`.
That is the runtime truth.
This matters for optional backends such as `numpy_vectorized`, where the plugin
declares vectorized helpers but one concrete cache instance may narrow that if
numpy is unavailable.

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

## Query and miss semantics

All built-in backends implement the same typed query contract:

- exact equality preserves original stored values and Unicode forms;
- free-text indexes use NFKC plus casefold without rewriting stored values;
- relationship constraints use cached link topology;
- sorting is deterministic and places nulls last;
- paging reports total count before slicing; and
- records and results are immutable snapshots.

The core cache never silently delegates a miss to the database. Exact lookups
report hit or miss plus completeness; queries report completeness. An
application adapter may explicitly fall back only for an unavailable or
incomplete operation. A known miss in a complete snapshot is final.

The old mutable `CacheView` contract has been removed. Query specifications and
results are immutable; UI selection and navigation state belong above the
cache.

## Read-only surface startup

The read-only web, Calibre-style web, JSON API, and OPDS surfaces can serve
metadata reads through either the live database or a storage-cache snapshot.
The live database remains the default.

For the full appliance startup checklist, including explicit ports, smoke
checks, and background-process commands, see
[`read-only-surface-appliance-startup.md`](read-only-surface-appliance-startup.md).

Use cache-backed metadata reads when startup can afford to load the cache and
route handlers should avoid repeated direct database lookups:

```bash
scripts/run_web_readonly.sh --database /path/to/library.sqlite --metadata-read-source cache
scripts/run_web_calibre_readonly.sh --database /path/to/library.sqlite --metadata-read-source cache --port 8081
scripts/run_api_readonly.sh --database /path/to/library.sqlite --metadata-read-source cache
scripts/run_opds_readonly.sh --database /path/to/library.sqlite --metadata-read-source cache --port 8082
```

The cache backend defaults to `schema_backed` and can be selected explicitly:

```bash
scripts/run_api_readonly.sh --database /path/to/library.sqlite --metadata-read-source cache --cache-type schema_backed
```

By default, unavailable cache operations may fall back to the live database.
Known misses in a complete cache do not. Disable all fallback when testing
strict snapshot behavior:

```bash
scripts/run_opds_readonly.sh --database /path/to/library.sqlite --metadata-read-source cache --no-cache-db-fallback
```

## Design rule for future backends

New cache backends should be explicit about what they promise.

At minimum, a backend should make clear:

- whether reads are snapshot-based or live
- whether child objects stay live after they are handed out
- whether external database changes require reload/invalidation
- whether it adds fast paths or only changes semantics

Backends should implement the storage cache API rather than teaching callers to depend on backend-specific internals.

Every backend must provide complete common query behavior. Capabilities
describe consistency and optimized operators, not whether a caller receives
correct results.

## Testing rule for future backends

If a new backend is added, it should join the generic cache-plugin contract tests.

That contract layer is where we should pin down backend-independent behavior such as:

- Unicode handling
- canonical vs alias field lookup
- row helper/default semantics
- lifecycle behavior

Backend-specific tests should still exist where a plugin has unique behavior, but the common contract should stay shared.
