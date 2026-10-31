# Next database tasks

Date: **2026-10-20**. DB01–DB03 are implemented for PR review; the remaining entries are
proposed work grounded in the driver/schema review. Cross-project order belongs
to the [architecture tasks](../architectural_reviews/next_tasks.md).

## DB01. Make creation safe for an existing destination

**Implemented for PR review, 2026-10-20; architecture A03.** Ordinary SQLite/APSW
creation builds privately and publishes create-only, including the registry,
Database, Core and terminal paths. Explicit offline replacement requires a new
verified recovery backup; preparation failures preserve the original. PostgreSQL
initialization accepts absent/empty schemas, serializes creators and rejects
populated namespaces. See the [contract and regressions](creation-and-replacement.md).

## DB02. Specify transactions independently of connection lifetime

**Architecture A04; implemented for PR review, 2026-10-20.** The
[transaction contract](transactions.md) now defines owned units, nested/borrowed
savepoints, resource rollback and the reentrant facade mutex. It inventories
remaining direct commit points and explains the compatibility transition.
SQLite/APSW-named, live PostgreSQL, Catalog/Row/Agent, storage and lifecycle
checks pass. The [handoff](../../working-memory/transaction-ownership-2026-10-20.md)
records counts, optional legacy limitations and separate full-CI status.

## DB03. Align default WEMI selection

**Implemented for PR review, 2026-10-20; architecture A07.** Catalog's
[selection policy](../catalog/catalog-wemi-selection.md) now matches the existing
SQL complete-ray ranking. Partial bundles remain available when SQL has no
complete row. Bilingual, novel/introduction and omnibus fixtures retain every
reachable Work. Unlinking removes the edge without promoting flags; the next
read ranks remaining candidates. Existing SQL views need no migration.

## DB04. Keep schema declarations and actual write enforcement distinct

**Next; coordinate with Catalog A08 and storage A10.** Column metadata now
declares normalization/validation and the field guide matches all 67 explicit
tables. Add enforcement only through the domain owner. Add bounded coverage for
new declarations, uniqueness scopes and migration rollback on collisions.
Do not treat nullable placeholder rows or declared profiles as validated records.

**2026-10-30 implementation note:** [table SQL batches](table-sql-batches.md)
now provide declaration-driven key audit/repair with selected-row collision
checks, NULL scope handling and rollback. Repair is data-only and does not add
constraints or validate bibliographic meaning. DB04's broader enforcement work
remains with the relevant domain owners.

## DB05. Retire legacy connection/custom-column assumptions through callers

**Following.** Inventory `books`-based category/custom-column consumers before
changing them. Recreate connection-local cleanup after connection replacement
and metadata changes, and verify unrelated permanent integrity triggers survive.
Done when supported callers have an owner and tests cover two independent
connections; remove only compatibility code whose callers have migrated.

## DB06. Make generated PostgreSQL identifiers discoverable

**Implemented for PR review, 2026-10-20.** Full-schema cache discovery expected
`composite_digital_asset_digital_asset_link_composite_digital_asset_id`, but
PostgreSQL stores its truncated identifier. A05's live testing exposed this
before deletion; the same discovery error reproduces on unchanged A04 parent
`e9efedc4`. A08 subsequently exposed the same defect in schema-based writers.
The [discovery fix](postgres-identifier-discovery.md) resolves generated names
through PostgreSQL's own identifier representation, then requires membership in
the discovered headings. Existing physical names stay unchanged; there is no
schema migration. Live regressions cover full schema/cache discovery and public
schema writes, missing columns, multibyte clipping and DDL collisions.
Both snapshot cache backends also use native driver index metadata, replacing
SQLite-only probes that silently aborted PostgreSQL's shared transaction.

This does not provide SQLite's compatibility views or full legacy Library parity.

## Review evidence and checks

The 2026-10-20 review built a fresh SQLite schema and compared explicit table
names, columns and types with the field guide. Historical SQL models remain
labelled as proposals. Useful checks are
[WEMI schema smoke tests](../../tests/databases/test_frbr_wemi_smoke.py),
the database test stream and the
[PostgreSQL runbook](postgresql-backend.md). Use disposable databases for creation,
migration and failure injection; do not rerun live provisioning merely to check docs.

Return to [Databases](README.md).

## Table SQL ownership follow-up — 2026-10-30

**Implemented for review.** All 36 physical main tables now have a nonempty SQL
mixin; feeds and shared normalization bring the total to 38 mixins and 65 public
operations. [The ownership guide](table-sql-ownership.md) records helper families,
callers, batch/paging contracts and test evidence. SQL targets stored rows and
references; Catalog, Core and storage retain validation, selection and policy.
DB04's broader semantic enforcement and the other open tasks above remain open.

## Measured SQL follow-up — 2026-10-31

The [SQL efficiency measurements](sql-efficiency-2026-10-31.md) establish the next
priorities after the current PR passes its full-suite gates:

1. Resolve normalized identity declarations and column metadata once per bulk
   operation. The 1,000-tag benchmark reduces SQL events but does not improve
   latency; preserve persisted policy changes and atomic failure behavior.
2. Profile shared WEMI search and its integrity preflight. The 50,000-Work
   Work-to-Item query still takes about 2.5 seconds despite exact lookup indexes.
3. Provide an explicit structural repair for older PostgreSQL schemas missing
   backup-workflow tables or using the old Catalog priority direction. New-schema
   creation is corrected; index migration deliberately does not perform this DDL.
4. Consider native indexed normalized search on PostgreSQL, preserving the exact
   shared text/identifier semantics before advertising the capability.
