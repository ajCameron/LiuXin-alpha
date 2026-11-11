# Databases

Reviewed **2026-10-20**. Current schema, connections, backend status and historical notes have distinct authority. See [next tasks](next_tasks.md) for the proposed implementation order.

Databases owns persistence mechanics and structural integrity. Bibliographic
meaning belongs to metadata/Catalog; physical storage policy belongs to storage.

## Working guides and operational contracts

- [Eighth driver-plugin review, 2026-11-11](database-driver-plugins-eighth-review-2026-11-11.md):
  PostgreSQL connection string-literal mode and qmark expression boundaries
  repaired; 147 new mandatory cases, with native and committed-value comparisons.

- [Seventh driver-plugin review, 2026-11-10](database-driver-plugins-seventh-review-2026-11-10.md):
  FTS row-ID preservation, PostgreSQL continued-string rewriting and
  keyword-identifier binding repaired; 221 new mandatory cases, including
  portable value/rollback parity across all three driver names.

- [Sixth driver-plugin review, 2026-11-10](database-driver-plugins-sixth-review-2026-11-10.md):
  lossy embedded-NUL SQL export/rebuild and PostgreSQL Unicode lexical gaps;
  implemented repairs and 151 added mandatory value-preservation regressions.

- [Fifth driver-plugin review, 2026-11-06](database-driver-plugins-fifth-review-2026-11-06.md):
  completion keyword parsing, WAL surviving SQLite rebuild publication and
  qualified PostgreSQL JSON operators; implemented mitigations and 156 added
  mandatory cases.

- [Fourth driver-plugin review, 2026-11-06](database-driver-plugins-fourth-review-2026-11-06.md):
  Unicode CTE transactions, temporal parameter classification and the stale
  PostgreSQL fixture; implemented repairs and 104 added mandatory cases.

- [Third driver-plugin review, 2026-11-06](database-driver-plugins-third-review-2026-11-06.md):
  six reproduced findings, including CTE batch atomicity and PostgreSQL binary
  round-trip corruption; implemented repairs and 148 added mandatory cases.

- [Driver plugins follow-up review, 2026-11-05](database-driver-plugins-follow-up-review-2026-11-05.md):
  partial batch success, remaining SQL-adaptation ambiguity, INCLUDE-key discovery
  and the obsolete scratch surface; all four repaired/retired with 50 added
  mandatory regression cases and an independent-working-copy migration guide.

- [Database driver plugins review, 2026-11-05](database-driver-plugins-review-2026-11-05.md):
  reproduced backup, connection-lifetime, PostgreSQL recovery/adaptation and
  dump/restore findings, their implemented repairs and remaining structural work.
- [Database regression ratchet, 2026-11-05](regression-ratchet-2026-11-05.md):
  typing findings, lifecycle/transaction mitigations and mandatory named regressions.

- [Package-wide typing, 2026-11-04](package-typing-2026-11-04.md):
  completed contracts across all 225 modules, checker enforcement and runtime evidence.
- [Maintenance and driver-wrapper delivery, 2026-11-04](review-upgrade-delivery-2026-11-04.md):
  combined PR scope, review order, verification and remaining deployment limits.
- [Catalog/database ownership](../catalog/catalog-database-ownership.md):
  semantic operations above portable SQL and the `metadata_sql` migration.
- [Metadata SQL WEMI audit](metadata-sql-wemi-audit-2026-10-29.md):
  the 73-operation audit, Expression-policy migration and 2026-10-30
  physical-table boundary: 62 table SQL helpers live under `db.metadata_sql`;
  compatibility projections and unresolved policy gaps have explicit owners.
- [Physical-table SQL batches](table-sql-batches.md): vocabulary lookup/ensure,
  atomic row updates/deletes and scoped, data-only normalization repair.
- [Table-owned SQL helpers](table-sql-ownership.md): main-table inventory,
  all 36 physical main-table owners, batched relationship/owner reads and bounded pages.
- [SQL efficiency implementation, 2026-10-31](sql-efficiency-2026-10-31.md):
  pagination, many-owner graphs, normalized SQL search/matching, aggregates, bulk
  writes and explicit query-index migrations, with backend limits, validation and
  2026-11-01 progress/batch overhead measurements.
- [Schema overview](<../03 - Database Schema.md>): current WEMI graph, assets,
  replicas, generated relations, compatibility views and schema source map.
- [Worked link examples](wemi-link-examples.md): WEMI rows and paths, novels with
  introductions, omnibuses, shared editions, copy counts, primary selection,
  credits and payload attachments.
- [Fields](what-are-fields.md) and [column metadata](column-metadata.md).
- [Link capabilities](link-capabilities.md).
- [Creation and replacement](creation-and-replacement.md): create-only publication,
  explicit offline replacement, recovery backups and server-schema refusal.
- [Transaction ownership](transactions.md): resource cleanup, owned transactions,
  nested savepoints, borrowed connections and the remaining direct commit points.
- [Driver-wrapper boundary fixes](next_tasks.md#db08-correct-driver-wrapper-boundaries-and-keep-its-api-declarative):
  transaction-preserving inspection, targeted updates, atomic custom-column creation,
  current folder discovery, view filtering and a declarative wrapper API.
- [Driver-wrapper follow-up and typing](driver-wrapper-review-2026-11-04.md):
  attachment-preserving edits, transactional deletion, schema-cache hooks,
  declared types and permanently checked wrapper contracts.
- [Driver-wrapper reads and diagnostics](driver-wrapper-features-2026-11-04.md):
  native schema facts, connection ownership, resumable scans, backend capability
  reports, optional observations and hierarchy cycle detection.
- [Maintenance modes](maintenance-modes.md): transactional prompt effects,
  durable lazy delivery, Catalog consumers and explicit operator controls.
- [Malformed-plugin and failure validation](maintenance-adversarial-validation-2026-11-03.md):
  current adversarial coverage, reproduced faults and regression evidence.
- [Latest maintenance review fixes](maintenance-review-fixes-2026-11-03.md):
  retry/pause coordination, effective lazy priority and bounded jobs history retirement.
- [Maintenance operational follow-ups](next_tasks.md#maintenance-operational-follow-ups):
  monitored SquashFS import, explanatory status, opt-in retention, plugin examples,
  worker supervision and measured batching improvements; proposed after signoff.
- [Maintenance completion work](maintenance-completion-plan-2026-11-02.md):
  rebuilds, plugin upgrades, measured scale and domain repair policies.
- [Maintenance scale evidence](maintenance-scale-2026-11-02.md): reproducible
  Catalog import/rebuild/drain trials and storage/memory limits of the evidence.
- [Catalog repair policies](../catalog/maintenance-repair-policies.md): explicit
  semantic merge, attributed WEMI tags, selected disposal and durable receipts.
- [Legacy maintenance retirement](legacy-maintenance-retirement-2026-11-02.md):
  removed capabilities, retained duplicate inspection, restored Agent name/sort
  maintenance, current Catalog replacements and remaining compatibility policies.
- [Backend strategy](liuxin_database_backend_strategy.md) and
  [PostgreSQL runbook](postgresql-backend.md).
- [Connection ownership](<02 - Connection ownership and stale connections.md>) and
  [TEMP trigger cleanup](<03 - TEMP triggers for custom-column cleanup.md>).
- [Optional SQLite progress callbacks](sqlite-progress.md): explicit observers and
  cancellation hooks on existing/future connections, with no default VM handler.

## Schema design and historical notes

- [Database field guide](LiuXin_database_field_guide.md): current inventory of all 67
  explicit tables, checked against generated SQLite columns and linked to their DDL.
- [Schema draft](<00 - Schema Draft.md>): earlier design material.
- [Asset/replica policy](<04 - Digital assets, replicas and storage policy.md>):
  design history with a current implementation checkpoint.
- [Category terminology](<01 - Categories and the Tag Browser Misnomer.md>) and
  [WEMI graph](WEMI_as_a_Graph_ASCII.md).
- [Schema notes](liuXin_schema_notes.md), [metadata tables](liuXin_metadata_tables_notes.md),
  [triggers](liuXin_triggers_notes.md) and [tightened triggers](liuXin_triggers_tightened_notes.md).

The installed schema is owned by the packaged SQL tree described in
[packaging](../hacking/packaging.md); design sketches do not replace it.
For fixtures and validation, use [testing](../hacking/testing/README.md).
Return to the [developer index](../README.md).
