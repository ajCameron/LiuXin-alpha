# Native metadata access

Implemented **2026-11-18**. The bound
[MetadataAPI](../../src/LiuXin_alpha/caches/metadata.py) implements
[MetadataAPIInterface](../../src/LiuXin_alpha/catalog/api/metadata_access.py)
under the existing `caches` package. Catalog owns validation and persistence;
the adapter coordinates metadata reads, mutations and cache freshness.

## Composition

```python
from LiuXin_alpha.caches.cache import create_cache
from LiuXin_alpha.caches.metadata import MetadataAPI
from LiuXin_alpha.catalog.catalog import Catalog

cache = create_cache(database, "schema_backed")
metadata = cache.metadata

created = metadata.works.create({"title": "The Metadata Garden"})
work = metadata.works.require(created.result)
matches = metadata.search.find("work.title:Garden")

# Explicit composition can reuse an existing Catalog and select fallback once.
metadata = MetadataAPI(Catalog(database), cache=cache,
                       allow_database_fallback=True)

# The same interface is available without a cache.
uncached = MetadataAPI(Catalog(database))
```

`cache.metadata` is retained lazily and requires a loaded cache. It disables
database fallback. The explicit constructor accepts a loaded `ReadCache` as well
as a writable `Cache`; the adapter itself supplies the Catalog mutation bridge.
Both objects must refer to the same database instance. Every operation checks
attachment and readiness again, including calls through retained entity services.
The composing caller closes the cache and database. The metadata adapter owns
neither resource and does not introduce a top-level package.

## Reads and writes

`works`, `expressions`, `manifestations` and `items` provide `get`, `require`,
ID-ordered `list`, `create`, `update` and `delete`. Reads return the existing native
metadata containers. Identity, relation targets and nested link payloads are copied
through the containers' serialization contract; returned values do not retain live
database Rows. Editing a value is an in-memory operation, not a write.

`search.find` compiles Catalog's native search semantics and returns
`MetadataReference` values. A subsequent hydration is a separate read. Cache
operations hold the owning cache lock for the whole page or metadata projection.
Live backends supporting `query_view` capture their read state once per operation,
avoiding repeated catalogue rebuilds for every relationship. That capture does
not add database transaction isolation against external writers. A live capture
still reads the complete catalogue, and hydration traverses related metadata;
these operations do not have the cost of an individual cached-row lookup.

Known root misses return `None`; `require` raises `CatalogNotFoundError`. Partial
or unavailable cache reads raise unless fallback was explicitly enabled. A known
miss never triggers fallback. Corrupt reads and missing referenced records raise.
The existing Catalog hydrators now offer opt-in strict error propagation; their
older best-effort callers keep their previous behavior.

Write payloads use Catalog's public aliases. `mutations` implements
`create_wemi_stack`, `attach_metadata`, `replace_metadata`, `link_wemi` and
`unlink_wemi`, including the existing distinction between omitted and cleared
attachment groups. It delegates those semantics to Catalog inside one supported
macro transaction. There is no forwarding escape hatch to raw repository writes.

The initial adapter conservatively refreshes the complete snapshot after a
committed semantic write. Live backends advance their query generation. Targeted
refresh is a later optimization requiring evidence that every affected identity,
relationship and projection is covered.

## Search requests and pages

`search.find()` remains the ID-ordered reference convenience. The richer
`search.query()` accepts immutable
[request values](../../src/LiuXin_alpha/catalog/api/metadata_search.py) and returns
a page with references, an exact total before pagination, offset/limit,
`has_more`, result level, execution source, consistency and generation:

```python
from LiuXin_alpha.catalog.api.metadata_search import (
    MetadataSearchFilter, MetadataSearchRequest, MetadataSearchSort,
)

request = MetadataSearchRequest(
    query="author:Herbert",
    level="work",
    filters=(MetadataSearchFilter("year", "gte", 1960),),
    sort=(MetadataSearchSort("title"),),
    limit=25,
)
metadata.search.validate(request)
page = metadata.search.query(request)
```

Filters are ANDed with the existing Boolean text query. Text filters use Catalog's
locations, aliases and normalized `contains`/`eq` semantics. Their operands are
literal values, including quotes, parentheses, colons and leading `=` characters.
Numeric filters add `id` and `year` on the result entity, or an explicitly scoped
`work.year`, `expression.id`, etc. Items have no own year. Supported comparisons
are `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, integer-tuple `in`, and boolean `is_null`.
Numeric fields belong to the typed filter vocabulary; this does not add numeric
operators to the existing text parser.

`search.fields(level=...)` discovers filters and aliases; `search.sort_fields()`
discovers scalar ordering keys. Sorting uses the returned entity's own value,
with no inherited title, preferred edition or author aggregation. Missing values
sort last in either direction; ascending entity ID breaks complete ties. An
explicit ID sort may choose descending IDs. An empty sort is ID ascending.
Text ordering compares normalized text and then original text before the ID tie.
Independent filters may match different reachable entities, just as existing
Catalog Boolean terms do. WEMI traversal never turns through a shared child to
borrow a sibling Work's metadata.

Validation checks syntax, levels, operands, sorting and paging without reading
schema or rows. `MetadataSearchValidationError.parameter` identifies the failing
input, such as `filters[0].value`. Execution still checks backend availability and
reference integrity. A zero-sized page still computes the total; an offset past
the end returns an empty page with that same total. Hydration remains explicit.

Cached pages carry `source="cache"` and the owning cache's generation. Uncached
execution and explicit fallback carry `source="database"`, `consistency="live"`
and `generation=None`. Fallback reruns the whole request; it never combines cached
references with a database count. A known empty result does not trigger fallback.
Partial or unavailable execution can use the bound fallback policy; corruption,
dirty transactions and failed recovery remain errors.

On databases with normalized SQL search, the existing macro selects all matching
root rows, then the shared query engine orders/counts/pages that population. This
avoids duplicating collation and missing-value rules but retains memory
proportional to the matches. Other drivers use one ephemeral database capture;
live cache backends retain their existing whole-catalogue capture cost. These are
initial correctness choices, not bounded-memory SQL pagination. Separate page
requests do not pin a snapshot. A generation is an invalidation counter, not a
database revision or a guarantee of isolation against external writers.

## Receipts and recovery

Every write returns `MetadataWriteReceipt`, separating persistence from cache
visibility:

| Situation | Persistence | Cache |
| --- | --- | --- |
| Owned transaction completes without a cache | `committed` | `not_configured` |
| Owned transaction completes and cache reconciles | `committed` | `current` |
| Caller owns an enclosing managed transaction | `pending` | `deferred`, or `not_configured` |
| Persistence completes but refresh fails | `committed` | `dirty`, carried by `MetadataReconciliationError` |

`changed` records an applied write request, not a comparison of whole metadata
graphs. Supplied updates and replacements can report true even when their values
already match. Empty entity updates and absent relationship removals report false.
The result and cache generation are return-time evidence; pending receipts never
become committed in place.

Deferred writes mark even a live cache dirty. Reads and explicit refreshes refuse
to publish while the outer managed transaction is open, including from other
threads. Completion releases this guard after either commit or rollback; the next
read recovers the cache. Rolling back an inner savepoint retains the guard until
the outer transaction finishes. Completion never reopens a cleared or closed cache.
Borrowed raw transactions are refused before mutation because their completion
cannot be observed through the macro boundary. Transaction capability errors do
not silently reduce atomic operations to partial writes.

`MetadataReconciliationError.receipt` retains the successful Catalog result and
its persistence status. A failed refresh after commit reports `committed`; a
failed reconciliation inside the caller's transaction still reports `pending`.
Do not repeat a committed write. The next successful cache refresh recovers the
dirty state; repeated refresh failures remain visible.
Catalog or SQL writes made outside this adapter still need explicit invalidation.

## Scope and validation

[Behavior tests](../../tests/databases/caches/test_metadata_api.py) cover the same
typed consumer with no cache and each native backend, detached values, CRUD,
attachment replacement, hierarchy links, transaction commit/rollback, failed
refresh, unavailable reads and retained bindings. The
[static contracts](../../tests/typing/internal_contracts.py) check both the
standalone adapter and `Cache.metadata` against the public protocol.

The [search tests](../../tests/databases/caches/test_metadata_search.py) add
SQLite/APSW and native backend parity, exact totals and stable sorting, numeric
and literal text filters, WEMI path isolation, discovery without data access,
structured validation errors, complete-page fallback and transaction guards.
All 44 search cases are mandatory; the manifest now requires **2,918 cases**,
preserving all earlier requirements.

The shared metadata fixture skips APSW cases when that optional driver is absent.
The mandatory database CI lane installs APSW and requires those cases to pass;
the ordinary full-suite lane can still run without it.

Search validation on **2026-11-18**: **84 tests passed** across the new search
cases, retained metadata read/fallback contracts and existing Catalog search
regressions. All **47 selected mandatory cases** passed setup, call and teardown;
459 saved source/test/manifest hashes match the validated tree. Formatting,
lint, dependency checks and both type checkers passed (mypy: 533 files), as did
the corrected negative typing fixtures (113 basedpyright / 109 mypy cases).
All **49 tooling/documentation tests** passed. Developer navigation covers 224
guides and 1,918 local links in the proposed snapshot. Run artifacts are retained
locally at `/tmp/liuxin-metadata-search-final`; those temporary files are not part
of the portable documentation or repository.

Adapter validation on **2026-11-18**: the combined metadata, cache lifecycle, hydration and
transaction-publication run passed **249 tests**, with six expected existing
backend-capability skips. All **61 selected mandatory cases** passed setup, call
and teardown, including the **47 new metadata cases**. That run covered the
previous 2,874-case manifest. Transaction coverage uses
SQLite and APSW, with cross-thread deferral and nested rollback exercised against
each native cache backend.

The adapter's configured quality runner passed formatting, lint, annotation coverage,
dependency/import checks, basedpyright and mypy (530 files), plus both negative
type-contract lanes. All **49 tooling/documentation tests** passed. The final run
retains matching hashes for 464 source, test and manifest files. These are
correctness results, not performance measurements or live PostgreSQL validation.

Core command migration remains separate: existing Core semantic handlers retain
their transport and receipt conventions until explicitly routed through this
adapter. Matching, merges, custom-field administration and dedicated Agent or
vocabulary services remain outside this first interface. The new transaction guard
protects this adapter; older schema-directed writer deferral needs its own
cross-thread follow-up. This is not whole-cache or whole-Catalog sign-off.
See [next tasks](next_tasks.md).

Return to [caches](README.md) or the [ownership contract](../catalog/catalog-cache-boundary.md).
