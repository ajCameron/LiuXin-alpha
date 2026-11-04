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

## DB08. Correct driver-wrapper boundaries and keep its API declarative

**Implemented and locally verified, 2026-11-04.** Native relation
inspection and SQLite schema refresh preserve the primary transaction and TEMP
objects. Single-column updates submit a targeted normalized write, preserving
other writers' unrelated fields. Custom-column definition insertion and DDL now
share an owned transaction; public creation refuses nested macro units so facade
bookkeeping follows a real commit. The generated legacy DDL remains SQLite-only.

Parent lookup delegates to the driver's current `*_parent`/`*_parent_id` rule, and
table-spec iteration filters views by actual relation kind. The wrapper API and
its schema-introspection base contain abstract declarations only: concrete
mixins own initialization and case-sensitivity aliases. New regressions guard
transaction ownership, interleaved writes, early/late DDL failures, real folder
traversal, view exclusion and API bodies. See
[transaction ownership](transactions.md) and
[connection lifetime](<02 - Connection ownership and stale connections.md>).

Validation: the final SQLite/APSW boundary selection passed **73 tests**; cache,
custom-column and maintenance integration passed **94**, with **nine PostgreSQL
cases skipped in that SQLite lane**. The separate live PostgreSQL wrapper
selection passed **nine tests**, including atomic rollback when its unsupported
SQLite custom-column helper is requested. PostgreSQL's existing direct-writer
and transaction cases also passed. These selections overlap; their counts are
not a unique-test total. Logs are under `/tmp/liuxin-wrapper-fixes-*.log`.

The final API/declaration selection passed **nine checks**. The full scoped
quality runner passed formatting, lint, complexity, import/layer/footprint checks,
basedpyright, mypy and both 38-invalid-call contract probes. All 94 local links in
the four edited guides resolve to versioned content. The private PostgreSQL
server was stopped and removed, and protected preference hashes are unchanged.

The regression owners are `tests/databases/test_driver_wrapper_review.py` and
`tests/databases/test_driver_wrapper_review_postgres_live.py`. They also verify
schema-query failures preserve the caller's transaction, native column discovery
notices schema changes, and the wrapper API has no operational method bodies.
Custom-column DDL remains SQLite-specific; adding PostgreSQL compatibility DDL
is separate work. No new full-repository suite run is claimed.

## DB09. Correct remaining wrapper metadata boundaries and type the wrapper

**Implemented and locally verified, 2026-11-04.** The
[follow-up review](driver-wrapper-review-2026-11-04.md) records five reproduced
faults and their repairs: preserving WEMI custom-column attachments, atomic
metadata/deletion operations, primary-connection view inspection, working
schema-version cache invalidation and restored declared types in table specs.

The whole wrapper package and its abstract API are included in both static
checkers and the formatting/lint gates. Portable query protocols and actual
missing-row sentinels are checked through valid and rejected-call examples.
The review records regression evidence. DB10 below implements physical
constraint introspection, view-row resource handling and cycle detection;
custom-column placeholders remain unresolved. The monitored real import remains
the next operational exercise.

Validation: **119** final boundary/API/tooling passes, **137** integration passes
(three skips/four existing xfails) and **25** live PostgreSQL passes. Selections
overlap. All scoped quality gates and both 44-invalid-call contract probes pass;
105 edited-guide links resolve. The private server is removed and protected
preferences are unchanged. See the review for logs and evidence limits.

## DB10. Add physical schema facts, bounded reads and optional diagnostics

**Implemented for review, 2026-11-04.** The
[feature guide](driver-wrapper-features-2026-11-04.md) records native constraint
facts, connection-aware fetch helpers, projected/filtered scans with resumable
IDs, live backend capability reports, and opt-in query observations. View-row
reads now preserve borrowed transactions and close resources on errors; hierarchy
traversal detects cycles. API declarations, concrete signatures and typed caller
examples cover the new surface.

Scans share the portable macros' existing pagination primitive. Callers own
snapshot lifetime and checkpoint durability; observations have no default timer
or SQLite progress handler. Remaining custom-column placeholders, compatibility
DDL portability and historical direct commit points remain separately scoped.
The guide records final validation evidence and limits.

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

The [SQL efficiency measurements](sql-efficiency-2026-10-31.md) established these
priorities. PR #152 subsequently passed its full-suite gates; the 2026-11-01
follow-up addresses the first two measured sources of overhead:

1. **Implemented for review, 2026-11-01:** resolve normalized identity declarations
   and column metadata once per bulk operation. Snapshots belong to one operation;
   the next operation reloads persisted policy. Portable inserts/updates and direct
   SQLite/PostgreSQL batches preserve normalization and atomic failure behavior.
2. **Profiled and addressed for review, 2026-11-01:** millions of default progress
   callbacks dominated shared WEMI search and its integrity preflight.
   [Explicit hooks](sqlite-progress.md) retain observation/cancellation while leaving
   the handler unset by default. Further query-plan work can follow the remaining
   measured cost; the integrity checks remain enabled.
3. Provide an explicit structural repair for older PostgreSQL schemas missing
   backup-workflow tables or using the old Catalog priority direction. New-schema
   creation is corrected; index migration deliberately does not perform this DDL.
4. Consider native indexed normalized search on PostgreSQL, preserving the exact
   shared text/identifier semantics before advertising the capability.
