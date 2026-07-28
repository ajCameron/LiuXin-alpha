# Catalog legacy mutation migration

Date: 2026-07-23

## Conclusion

The migration is complete. Production code no longer imports
`catalog.catalog_macros` or `catalog.metadata_tools`, and database/library
objects no longer expose their `Add`, `Ensure`, `Apply`, or `Intralinker`
facades. The guarded production allowlists are empty.

The legacy sources were deliberately retained. They are frozen reference and
characterization code for future, measured pure-SQL implementations behind the
current Catalog contracts; they are not an alternative extension surface.

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
`dev-docs/catalog-legacy-mutation-migration.md`.
