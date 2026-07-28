# Catalog legacy mutation migration

Date: 2026-07-23

## Conclusion

The database/library migration is complete. Database/library objects no longer
expose their `Add`, `Ensure`, `Apply`, or `Intralinker` facades.
As of 2026-07-25, `Catalog` is the one authorized production composition root
for the retained row-oriented metadata tools and exposes them directly as
`catalog.add`, `catalog.ensure`, `catalog.apply`, and `catalog.intralink`.
The guarded indirect-facade allowlist remains empty.

The helper sources were deliberately retained. Their behavior remains a frozen
compatibility/reference surface; callers enter through `Catalog`, and new
catalog semantics still belong in current repository, mutation, or writer
contracts.

## Resulting ownership

- repositories own ordinary entity persistence and exact-value aggregates;
- coordinated mutations own atomic WEMI and multi-table operations;
- normalized writers own schema-declared relationship changes;
- cache adapters reconcile Catalog writes without moving cache conventions
  into Catalog; and
- database compatibility helpers use portable macros without importing the
  Catalog facade.

## Evidence

- full Catalog suite: `396 passed`;
- full storage-cache suite: `115 passed`, `6 skipped` by explicit legacy-suite
  policy;
- legacy boundary and characterization slice: `21 passed`;
- terminal `new_*` command slice: `94 passed`;
- strict Catalog mypy surface: `74` source files, no issues; and
- both preserved reference entry points remain present and importable.

The durable contract and detailed caller map are in
`docs/development/catalog-legacy-mutation-migration.md`.

## Catalog metadata-tool exposure follow-up

On 2026-07-25, the public `CatalogAPI` was extended with the existing
`CatalogMetadataToolsAPI` contract, and `Catalog` began composing shared
`Add`, `Ensure`, `Apply`, and `Intralinker` instances. Their internal
collaboration links point to those same Catalog-owned instances, so calls such
as `catalog.add.work(...)` and `catalog.apply.tag(...)` use one coherent tool
graph over `catalog.db`.

Verification:

- focused API, boundary, and live `catalog.add.work(...)` slice: `10 passed`;
- complete four-worker Catalog regression: `502 passed in 567.65s`; and
- isolated strict Catalog mypy scope: no issues in `74` source files.
