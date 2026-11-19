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
`get_many`, `read_for_edit`, ID-ordered `list`, `create`, `update` and `delete`. Reads return the existing native
metadata containers. Identity, relation targets and nested link payloads are copied
through the containers' serialization contract; returned values do not retain live
database Rows. Editing a value is an in-memory operation, not a write.

`search.find` compiles Catalog's native search semantics and returns
`MetadataReference` values. A subsequent hydration is a separate read. Cache
operations hold the owning cache lock for the whole page or metadata projection.
Live backends supporting `query_view` capture their read state once per operation,
avoiding repeated catalogue rebuilds for every relationship. Composed metadata
reads now borrow the caller's managed transaction or own a read-only snapshot,
closing its connection on completion. This also prevents a standalone PostgreSQL
search from retaining table locks that block later custom-field DDL. Composed
reads reuse the driver's schema-discovery scope, validating its fingerprint at
entry and exit instead of probing for every hydrated row. A live capture
still reads the complete catalogue, and hydration traverses related metadata;
these operations do not have the cost of an individual cached-row lookup.

Known root misses return `None`; `require` raises `CatalogNotFoundError`. Partial
or unavailable cache reads raise unless fallback was explicitly enabled. A known
miss never triggers fallback. Corrupt reads and missing referenced records raise.
The existing Catalog hydrators now offer opt-in strict error propagation; their
older best-effort callers keep their previous behavior.

Write payloads use Catalog's public aliases. The public entity protocols now take
the corresponding `WorkCreate`/`WorkUpdate`, `ExpressionCreate`/`ExpressionUpdate`,
`ManifestationCreate`/`ManifestationUpdate` or `ItemCreate`/`ItemUpdate`
[TypedDicts](../../src/LiuXin_alpha/catalog/api/metadata_payloads.py). Optional keys
preserve omitted-versus-cleared intent: omission retains an existing value, while
`None` requests NULL subject to schema constraints. Types describe portable scalar
aliases; Catalog still validates dynamic callers and physical constraints at runtime.

`mutations` implements
`create_wemi_stack`, `attach_metadata`, `replace_metadata`, `link_wemi` and
`unlink_wemi`, including the existing distinction between omitted and cleared
attachment groups. It delegates those semantics to Catalog inside one supported
macro transaction. There is no forwarding escape hatch to raw repository writes.

The initial adapter conservatively refreshes the complete snapshot after a
committed semantic write. Live backends advance their query generation. Targeted
refresh is a later optimization requiring evidence that every affected identity,
relationship and projection is covered.

## Batch reads and writes

`works.get_many(ids)` hydrates one captured read source and returns values in the
requested order. Duplicate IDs remain duplicate positions, with independently
detached values. Missing roots occupy `None` positions; `missing="raise"` instead
rejects the entire call with `CatalogNotFoundError`. Unavailable or corrupt data
never becomes a missing result. An empty request avoids capturing the catalogue.
The same method is available on all four WEMI families. Live capture cost is paid
once per call, although each distinct root still requires its own hydration.

`metadata.batch(callback)` runs dependent edits in one owned transaction and
performs one final full schema/data reconciliation. The callback receives the
same public interface and can use creation results in later writes:

```python
def create_and_describe(api):
    work_id = api.works.create({"title": "A new work"}).result
    api.mutations.attach_metadata(
        level="work", entity_id=work_id, data={"notes": ["Initial description"]}
    )
    return work_id

receipt = metadata.batch(create_and_describe)
```

Child receipts remain `pending`; the aggregate receipt reports commit and cache
reconciliation. An escaping callback exception rolls back the whole batch. A
caught failed child operation has its own savepoint, so the callback can choose
to continue with other edits. Nested batches compose; an external managed outer
transaction retains commit/rollback ownership. Cache reads after edits remain
deferred until that outer unit completes. Use returned IDs rather than reading
an uncommitted cache. Invalidations may advance the generation more than once;
the guarantee is one final refresh, not one generation increment.

## Optimistic editing

`read_for_edit(id)` opens a consistent database snapshot and returns a
[MetadataSnapshot](../../src/LiuXin_alpha/catalog/api/metadata_revisions.py):
detached native metadata, public scalar `fields`, and a matching `revision` token.
The explicit scalar mapping includes writable aliases not represented by older
identity containers. This operation deliberately reads authoritative storage,
even when ordinary metadata reads use a cache snapshot.
Inside an existing managed transaction, it instead borrows that transaction and
inherits its isolation and visibility. It does not upgrade the caller's isolation.

```python
observed = metadata.works.read_for_edit(work_id)
metadata.works.update(work_id, {"title": "Edited title"},
                      expected_revision=observed.revision)
```

`update` and `delete` accept optional `expected_revision`. For compound changes,
use `metadata.batch(callback, expected=(observed.revision, ...))`. Every token is
checked before invoking the callback. A changed or deleted root raises
`MetadataConflictError` with expected and actual tokens; a mismatched entity token
is invalid input. Checks always use the database, including when a direct Catalog
write has left the cache stale. Tokens cover the complete root row and hydrated
relationships. They are content fingerprints, not durable edit counters: changing
and restoring the same complete content can produce the same token. They are
scoped to the originating catalogue and current API encoding, and are not cache
generations. Custom values retain their separate field-wide native revisions.

The optional conditional-write path reserves the database writer before comparing
content. SQLite already reserves a writer for owned macro writes. PostgreSQL
reserves catalogue tables in a deterministic order until the transaction ends;
this also blocks uncooperating row writes and relationship inserts. PostgreSQL
preconditions require READ COMMITTED isolation; a pre-existing repeatable-read
snapshot is rejected. This coarse initial reservation prioritizes correctness
and can reduce write concurrency. It does not reserve external file bytes.

## Shared identities and custom fields

`metadata.agents` provides detached public-field reads, CRUD, person/organisation
creation and explicit name/sort-policy renaming. `metadata.vocabulary(kind)`
selects `tags`, `labels`, `genres`, `subjects`, `series`, `languages` or `ratings`.
These services retain Catalog's normalization and deletion rules; Languages stay
read-only seeded constants. Tags and Series additionally expose `link_to_work`
and `unlink_from_work`, preserving retained membership metadata. Shared edits
reconcile affected WEMI projections and search. No implicit matching or merging
is introduced. Detailed Agent subtype editing remains with Catalog's existing
aggregate services.

`metadata.custom_fields` exposes native field definitions, stored values, atomic
patches, queries, inventories, shared-value rename/delete, migration planning and
application, retirement, purge planning/application and audit. It preserves native
`FieldDefinition`, `OwnerRef`, `FieldPatch`, `ChangePlan` and field-wide revision
semantics. Those native reads use authoritative native-service snapshots. Writes
return a `MetadataWriteReceipt` containing the native result; schema changes
perform a full cache reload. A plan is revalidated by the native service when it
is applied. Retirement retains values; applying an explicit purge permanently
removes them. Composite template evaluation remains Catalog-owned.

Native service listeners can fail after SQL commits. The transaction owner now
reports that condition as `CommittedTransactionError`; the metadata adapter
retains its completed result in `MetadataReconciliationError` and marks any bound
cache dirty. This is a committed write, including without a cache, and must not
be repeated merely because publication failed.

## Core consumer workflow

`CoreServices.metadata` lazily binds the interface to Core's existing Catalog,
cache and fallback policy. Four named routes exercise native search/display/edit:

| Route | Result |
| --- | --- |
| `metadata.search` | Typed-filter search page, exact total and provenance |
| `metadata.read_many` | Ordered native metadata mappings, including missing slots |
| `metadata.read_for_edit` | Authoritative metadata, scalar fields and revision |
| `metadata.update` | Partial scalar edit with optional revision precondition |

The read routes require the existing `read` transport capability and updates
require `write`. Core reports `metadata_conflict` with both tokens, and preserves
the receipt and actual durability flag for `metadata_reconciliation_failed`.
These writes do not pass through Core's older reconciler for a second refresh.
The existing item-centred `metadata.get`/OPF payload remains a separate contract;
other Catalog endpoints have not yet migrated to this interface.

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
initial correctness choices, not bounded-memory SQL pagination. Database execution
uses the metadata owner's read scope; separate page
requests do not pin a shared snapshot. A generation is an invalidation counter,
not a database revision.

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
All 44 search cases are mandatory. That slice raised the manifest to 2,918 cases;
the extensions below preserve those requirements and raise it to **2,956 cases**.

The shared metadata fixture skips APSW cases when that optional driver is absent.
The mandatory database CI lane installs APSW and requires those cases to pass;
the ordinary full-suite lane can still run without it.

The **2026-11-19** extension adds 38 mandatory cases covering ordered batch
hydration, atomic edits, shared metadata, native custom-field lifecycles,
authoritative edit tokens, independent writers and the Core consumer workflow.
Six additional PostgreSQL cases are gated by `LIUXIN_TEST_POSTGRES_URL`; they use
disposable schemas and cover the four cache modes, blocking an independent raw
SQL writer during a checked edit, and rejecting an older repeatable-read snapshot.
Final validation on **2026-11-19**:

- The combined metadata, search, custom-field, transaction and Core regression
  suite passed **192 tests**; all **149 selected mandatory cases** passed setup,
  call and teardown.
- After the schema-read scope improvement, a current-source follow-up passed
  **39 tests**, with all 39 mandatory phase audits passing. Its 539 saved source,
  test and manifest hashes match the final tree. This run overlaps the broad run;
  its count is not additional unique coverage.
- All **six live PostgreSQL 16 tests** passed, including raw-writer blocking and
  isolation rejection. The disposable test server was shut down afterward.
- Formatting, lint, architecture/import checks and both type checkers passed
  (mypy: 554 files), including 115 basedpyright and 111 mypy rejected invalid
  examples. The tooling run passed 279 checks with one initial untracked-link
  failure; after tracking the new paths, all 16 documentation checks passed.

Local evidence is retained under `/tmp/liuxin-metadata-extensions-validation` and
`/tmp/liuxin-metadata-extensions-current`, with PostgreSQL output in
`/tmp/liuxin-metadata-postgres-final.log`. These temporary artifacts are not part
of the portable documentation. These are correctness checks, not performance
benchmarks or whole-module sign-off. Older results below describe their original
slices, not these additions.

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

The native Core workflow now consumes this interface; other Core semantic handlers
retain their existing transport and receipt conventions. Matching and merges remain
separate Catalog services. The new transaction guard
protects this adapter; older schema-directed writer deferral needs its own
cross-thread follow-up. This is not whole-cache or whole-Catalog sign-off.
See [next tasks](next_tasks.md).

Return to [caches](README.md) or the [ownership contract](../catalog/catalog-cache-boundary.md).
