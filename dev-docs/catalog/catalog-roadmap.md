# Catalog roadmap

Reviewed **2026-10-20**. Current repositories, relationship ownership and retrieval boundaries were checked. See [next tasks](next_tasks.md) for remaining work; older run counts retain their original dates.

Reviewed: **2026-10-20**, against `fc5de7af` plus the current documentation changes.

Status: **Current work list; completed migrations are recorded separately.**

Catalog has implemented repositories, matching, retrieval and semantic mutations.
The remaining work hardens those contracts and resolves compatibility ownership.
This list does not treat a completed caller migration as removal of every adapter.
Use the [API guide](catalog-api-usage.md) for entry points and the
[write contract](catalog-write-validation-contract.md) for guarantees and exclusions.

## Implemented ownership changes

| Area | Current implementation | Limits |
| --- | --- | --- |
| Matching | Evidence-based policy distinguishes unique matches, no match, ambiguity and conflicting evidence; scoped WEMI identity and opt-in approximate rules are explicit. | Matching policy is not a replacement for write validation or stored identity derivation. |
| Shared writes | Repository preparation and `CatalogRelationships` serve semantic mutations, schema-driven writers, container writes and Row compatibility adapters for Catalog-owned tables. | Several public APIs remain; uncovered field validators and raw low-level writes are not made safe merely by the shared route. |
| Rating values | A08 adds pure value/scale rules, partial-pair validation, checked value reuse, fraction-preserving compatibility helpers and atomic retained metadata imports. See the [route matrix](catalog-rating-validation.md). | Modern container write-back does not support ratings. The retained Library importer still depends on SQLite compatibility projections, which PostgreSQL does not provide. Other field families remain open. |
| Text identity | Declared normalized keys use the pure metadata policy across repositories, row/driver writes and portable macros. | Sort keys and approximate matching remain distinct; there is no automatic old-schema upgrade or silent repair on reads. |
| Legacy caller migration | `catalog_macros` has no production imports. Only `catalog/catalog.py` may import `catalog.metadata_tools`; database/library helper injection is gone. | `catalog.add`, `ensure`, `apply` and `intralink` are live compatibility adapters, deprecated for new code. |
| Cache composition | `ReadCache` is Catalog-independent; Catalog retrieval may compose it. Writable `Cache` delegates mutations to Catalog and reconciles derived state. | External writes still need snapshot invalidation; older Calibre cache/search code remains. |
| WEMI retrieval | Coherent path bundles, immediate hierarchy traversal and bounded descendant graphs are implemented. | Caller limits and truncation remain part of the result contract. |
| Application categories | Core browse queries own source selection, totals and category entries across CLI/API/web/OPDS consumers. | Generic Work/search/acquisition projections retain adapters; transport paging does not bound Core's server-side scans or provide an atomic snapshot. |
| Dependency checks | Whole-package layer and dynamic-import baselines, protected import-cycle checks and fresh-process budgets are active. | Reviewed legacy edges remain. Strict typing covers declared scopes, not every source file. |

The [legacy migration record](catalog-legacy-mutation-migration.md) distinguishes
retained adapters from frozen reference code. The
[Catalog/cache contract](catalog-cache-boundary.md) defines reads and reconciliation;
[matching policy](catalog-matching-policy.md) defines identity evidence.

## Remaining work

1. **Complete semantic rule coverage.** Inventory fields and operations without
   domain validators, put pure value rules in metadata, and invoke them through
   the shared preparation boundary. Keep raw database/storage operations explicitly
   outside the guarantee. Changes need invalid-input and rollback coverage through
   each public route, including partial updates and the supported database drivers.
   The first A08 slice covers Rating values and scales; use its explicit route
   matrix as the model for the next family, not as evidence of universal coverage.
2. **Make historical-data remediation explicit.** Preserve tolerant reads and
   collision-reporting audit/repair. Define any additional schema/profile migration
   before applying it to existing libraries; normalization changes must not silently
   merge rows or rewrite data during retrieval.
3. **Resolve compatibility ownership.** Calibre search remains under `catalog.search`,
   field-metadata implementations still overlap, and older caches remain under
   `library.caches` and `plugins.cache`. Select their long-term owners with caller
   and behaviour evidence. The earlier proposal to relocate compatibility under
   `utils` is not an implemented destination or permission to move modern cache
   policy there. Preserve the read/write cache split and the unified plugin host.
4. **Finish read-model policy deliberately.** Core category semantics are shared;
   broader Work/search/file/acquisition adapters and server-side listing costs
   remain. A07 now aligns SQLite primary rays and Catalog bundles under the
   [preferred-path contract](catalog-wemi-selection.md), with real-backend parity
   and incomplete/shared-path cases. Metadata relation-local selection is explicitly
   separate; graph membership and existing list order are preserved.
5. **Measure startup and retrieval costs.** Profile FRBR generation, aggregate
   views, schema introspection and category listing on representative catalogues.
   Preserve correctness and explicit completeness/truncation contracts when adding
   a faster implementation.
6. **Reduce reviewed dependency debt.** Tighten the exact layer baseline as old
   edges disappear and expand meaningful typing coverage. The repository already
   has executable gates; this task is to reduce their remaining debt, not introduce
   the first static boundary.

Jobs, ingest coordination and registry lifecycles are wider application work,
tracked by the [ownership guide](<../04 - Seperation of Concerns.md>) and
[architecture review](../architectural_reviews/architecture-review-2026-09-26.md).
They should not become Catalog responsibilities simply because they persist data.

## Working rule

Take one focused change at a time. State its contract and exclusions, verify the
relevant behaviour and database drivers, and update this work list with the actual
result. Keep historical test counts in dated records; they are not fresh evidence
for a later checkout.
