# Next cache tasks

Updated: **2026-11-18**. Completed slices and remaining cross-module work.

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

**Next, 2026-11-18. Interface-first contract declared.**
[MetadataAPIInterface](../../src/LiuXin_alpha/catalog/api/metadata_access.py) adopts
Catalog's native WEMI vocabulary with metadata-shaped reads and coordinated write
receipts. The [ownership decision](../catalog/catalog-cache-boundary.md#metadata-access-interface-direction--2026-11-18)
records placement, transaction/freshness obligations and initial scope.

Implement the adapter under `caches`, binding Catalog and an optional cache for
one database, then route Core's existing metadata operations through it. The
contract stays in `catalog.api`; no new top-level package is needed. Reuse Catalog
validation and the cache's reconciliation mechanisms. Cover CRUD, compound relationships and
metadata replacement before claiming the interface is implemented; do not expose
raw repositories through otherwise cache-aware properties.

Done when typed consumer tests exercise cached and uncached reads/writes, detached
metadata, all three native backends, binding validation, missing/unsupported
distinctions, commit/rollback and failed refresh with retained write receipts.
The declared interface alone does not close this gap or sign off the module.

## Validation and routing

Use [cache tests](../../tests/databases/caches), Catalog writer/retrieval tests and surface read-failure tests. Run large cache benchmarks only for a specific performance question with resource telemetry.

Return to [cache](README.md).
