# Next database tasks

Date: **2026-10-20**. DB01 is implemented for PR review; the remaining entries are
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

**Next; Catalog owns the semantic choice, architecture A07.** SQL primary rays
rank primary markers and ascending priorities; Catalog bundles currently traverse
descending priorities without the same primary preference. Choose and test one
default policy, retaining all reachable paths as a separate query.
Use the bilingual, novel/introduction and omnibus fixtures in
[worked examples](wemi-link-examples.md). Done when both read paths select the
same default and unlinking has an explicit replacement policy.

## DB04. Keep schema declarations and actual write enforcement distinct

**Next; coordinate with Catalog A08 and storage A10.** Column metadata now
declares normalization/validation and the field guide matches all 67 explicit
tables. Add enforcement only through the domain owner. Add bounded coverage for
new declarations, uniqueness scopes and migration rollback on collisions.
Do not treat nullable placeholder rows or declared profiles as validated records.

## DB05. Retire legacy connection/custom-column assumptions through callers

**Following.** Inventory `books`-based category/custom-column consumers before
changing them. Recreate connection-local cleanup after connection replacement
and metadata changes, and verify unrelated permanent integrity triggers survive.
Done when supported callers have an owner and tests cover two independent
connections; remove only compatibility code whose callers have migrated.

## Review evidence and checks

The 2026-10-20 review built a fresh SQLite schema and compared explicit table
names, columns and types with the field guide. Historical SQL models remain
labelled as proposals. Useful checks are
[WEMI schema smoke tests](../../tests/databases/test_frbr_wemi_smoke.py),
the database test stream and the
[PostgreSQL runbook](postgresql-backend.md). Use disposable databases for creation,
migration and failure injection; do not rerun live provisioning merely to check docs.

Return to [Databases](README.md).
