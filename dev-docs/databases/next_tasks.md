# Next database tasks

Date: **2026-10-20**. Proposed work, grounded in the current driver/schema review.
This list does not claim the changes are implemented. Cross-project order belongs
to the [architecture tasks](../architectural_reviews/next_tasks.md).

## DB01. Make creation safe for an existing destination

**First; architecture A03.** Trace both SQLite/APSW creation and the Core bootstrap.
An ordinary create request must reject an existing database before changing bytes.
Keep replacement a separately named, explicit operation with a recoverable plan.
Done when an existing populated database survives rejected creation unchanged,
including failures between destination validation and schema initialization.

## DB02. Specify transactions independently of connection lifetime

**First; architecture A04.** Document and implement one contract for outer
transactions, nested savepoints, rollback and connection closure across drivers.
Audit helper commits and the separate `driver_wrapper.lock` connection.
Done when nested failures, exceptions on context exit and caller-owned connections
behave predictably in SQLite/APSW and PostgreSQL contract tests. A mocked server
test alone does not establish live PostgreSQL parity.

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
