# Next cache tasks

Updated: **2026-11-22**. Completed slices and remaining cross-module work.

Preserve the ReadCache/Cache split and shared Catalog write ownership. Optimize measured workloads without inventing another catalogue authority.

## CACHE01. Make invalidation responsibility explicit for every write route

**Implemented and reviewed, 2026-11-18.** The
[write-route/lifecycle review](invalidation-and-lifecycle-2026-11-18.md) records
column, shared-link and owned-row bridges, direct Catalog writes, storage record
and relationship publication, migration failure and transaction ownership.
Immediate and deferred reconciliation retain their distinct error contracts.
Extension-table transactions, replaced Item-target links and migration completion
were repaired; Item-role deletion is atomic. The real SQLite/APSW matrix and
writer read/close concurrency checks join the mandatory regression manifest.

Direct writes outside these cache-aware owners still require explicit dependency
invalidation. This does not add global observation of raw SQL or arbitrary
Catalog methods. The review names those boundaries and the existing destructive
lifecycle/descriptor recovery coverage.

**Outer-transaction follow-up implemented, 2026-11-22.** The metadata adapter and
schema-directed writers now share completion guards across threads and all three
cache backends. Reads, reloads and competing writes reject pending publication;
the owner can continue writing until commit or rollback. Guards cover destination
resolution before SQL work, and borrowed raw units reject before mutation.
Database-before-cache lock ordering also covers reads and reloads before the
owner's first edit. The expanded lifecycle run passed 138 cases with six expected skips;
see the [follow-up review](metadata-followups-2026-11-22.md).

The [2026-11-16 review](../architectural_reviews/caches-review-2026-11-16.md)
provides the first concrete repair slice: CR01 orders native custom-field
publication across reentrant receipt listeners; CR02 makes partial refresh failure
recoverable; CR03 prevents partial invalidation from leaving stale query indexes
behind a READY facade. CR01 is repaired by the native registry; CR02/CR03 were repaired on 2026-11-17
with whole-request validation and dirty full-recovery semantics.

## CACHE02. Define leases for physical cache replicas

**Next; STOR05 and ING02.** Work with storage to pin materialized inputs used by active workflows. Keep this byte-retention contract separate from catalogue snapshot invalidation.

Done when capacity cleanup respects active leases, crash recovery has a bounded expiry policy, and catalogue refresh never deletes physical inputs as an incidental effect.

## CACHE03. Make registry lifetime and collisions deliberate

**Implemented and reviewed, 2026-11-18.** Process-local bindings are immutable;
identical normalized registration is idempotent, and conflicting canonical names
or aliases reject the entire registration. Publication/lookup are locked, while
imports and constructors run outside the lock. Sixteen mandatory tests cover
collisions, concurrent registration/lookup and failures without backend substitution.
See the [registry contract](invalidation-and-lifecycle-2026-11-18.md).

## CACHE04. Reduce legacy overlap using workload evidence

**Following.** Choose one remaining Calibre cache/search adapter and map live consumers, missing capabilities and performance. Keep known-miss versus unsupported behaviour intact.

Done when the selected caller uses one read contract, old imports can be removed, and small correctness checks plus a measured benchmark support the change.

**2026-11-16 implementation:** the [compatibility caller map](calibre-compatibility-review-2026-11-16.md)
was implemented across writers, base/concrete caches, categories, lazy metadata,
legacy search and field adapters. Native field metadata now has its own registry.
See the [current structure](structure.md). This removes old import paths and
repairs CR01. The subsequent [metadata search milestone](metadata-search.md)
repairs CR02/CR03 and adds native predicate execution plus an explicit Calibre
projection adapter. Content FTS, notes, annotations and application providers
remain the named SEARCH02–SEARCH05 follow-ups.

## CACHE05. Implement the bound metadata access service

**Bound adapter implemented, 2026-11-18; extensions validated, 2026-11-19.**
[MetadataAPIInterface](../../src/LiuXin_alpha/catalog/api/metadata_access.py) is
implemented under `caches.metadata` by [MetadataAPI and CatalogCache](metadata-api.md).
`CatalogCache(catalog, cache=core)` supplies the explicit cached composition. It supports native WEMI reads, search, CRUD, compound relationships
and metadata replacement, with detached values and transaction/freshness receipts.
The contract stays in `catalog.api`; no new top-level package is needed.

Its search service now adds typed requests, exact-count reference pages, scalar
sorting, filter/sort discovery and validation. Catalog owns the field/path plan;
the cache engine owns ordering, execution and result provenance. See the
[search contract](metadata-api.md#search-requests-and-pages). Facets, saved searches
and content/note/annotation search remain later slices.

The current extension adds ordered batch hydration, typed WEMI scalar inputs,
atomic edit callbacks, authoritative content preconditions, shared Agent/vocabulary
administration and native custom-field lifecycle/value operations. A native Core
search/display/edit workflow consumes the interface with explicit conflict and
post-commit error receipts. See the [API guide](metadata-api.md) for the completed
192-case regression run, overlapping 39-case current-source follow-up and six
live PostgreSQL passes. All 38 new non-gated cases are mandatory; the manifest now
requires 2,956 cases. This validates the extension, not the whole cache module.

**Core-record coverage expanded, 2026-11-21.** Named vocabulary services, six
exact ensure conveniences, curated and Item identifiers, Notes/Comments/Synopses
and Item annotations now share the same coordinator. Catalog owns serialized
exact reuse and complete Item-owned record validation. Titles remain embedded
WEMI metadata; language constants remain read-only. See the [coverage table](metadata-api.md#core-metadata-coverage).

**Vocabulary trees added, 2026-11-21.** Genre, Subject and Series services now
provide atomic path/nested-tree creation, bounded traversal, branch movement,
sibling ordering and explicit leaf/subtree deletion. Catalog owns hierarchy and
attachment rules; the metadata facade coordinates cached reads and publication.
See [vocabulary trees](metadata-api.md#vocabulary-trees) for the stricter deletion
contract and [gated stress coverage](long-running-tests.md) for deep/wide workloads.

**Metadata ergonomics added, 2026-11-22.** Shared bulk reads, literal picker
queries, hydrated WEMI search pages, Work Tag/Series helpers, role-specific Agent
credits and shared-row edit preconditions now use the same coordinator. Runtime
and static capabilities agree. The [API guide](metadata-api.md) documents the
complete application workflow, source/fallback rules and revision scope. The
long lane includes picker/hydration and membership churn; live PostgreSQL checks
cover the same workflows and independent shared-token writers. Future tuning
should measure shared-table scans, live captures and coarse writer reservations
before adding indexes or changing transaction scope. Validation passed 393 distinct
cases, including all 39 new mandatory cases and five live PostgreSQL cases; the
mandatory manifest now contains 3,069 cases. Full-size stress remains gated.

**Follow-up implementation, 2026-11-22.** Existing PostgreSQL catalogues have an
explicit, transactional [upgrade](../databases/postgres-catalog-upgrade.md), with
nine live regression cases. The metadata long lane is available through the
standard telemetry runner and is currently running at full size. A realistic
workflow benchmark measures scans, live captures, refreshes and independent-writer
reservations. The [follow-up review](metadata-followups-2026-11-22.md) records
validation status and the performance limits without treating unfinished runs as
passes. Native live captures now load requested identities/populations/routes on
demand. Sixteen capture regressions join the manifest, which preserves 3,139
cases. The unchanged full soak restarted after the user selected capture
optimization; the four-mode smoke run passes.

Core's generic field-write routing repair is isolated in
[PR #215](https://github.com/ajCameron/LiuXin-alpha/pull/215). It selects the bound
metadata writer and preserves explicitly injected writers, with 214 targeted
passing cases. Land that separate repair alongside the cache work; it is not an
implicit fallback to a different writer after failure.

Next, route more of Core's existing semantic metadata operations through this service,
preserving its transport authorization and public receipt conventions. Do not
silently bypass existing injected service capabilities. Matching and merges retain
their dedicated Catalog services. Measure compound-write full-refresh costs before selecting
narrower dependencies; complete relationship freshness is the correctness baseline.

## Validation and routing

Use [cache tests](../../tests/databases/caches), Catalog writer/retrieval tests and surface read-failure tests. Run large cache benchmarks only for a specific performance question with resource telemetry.

Return to [cache](README.md).
