# Catalogue change log

The change log, `catalog_changes`, records every changed catalogue row in the same transaction as the change. It is phase 1 of the [change log and retirement design](../caches/change-log-and-retirement-design-2026-11-26.md). Its purpose is to let a snapshot cache refresh only the rows that changed, rather than reloading every table after each write.

The code is in [`databases/change_log.py`](../../src/LiuXin_alpha/databases/change_log.py).

## What it records

Every catalogue table has AFTER INSERT, UPDATE and DELETE triggers; PostgreSQL also has an AFTER TRUNCATE trigger. Each trigger appends one row:

| Column | Meaning |
| --- | --- |
| `catalog_change_id` | The log's order: SQLite `AUTOINCREMENT`, PostgreSQL identity. |
| `catalog_change_table` | Physical table changed. |
| `catalog_change_row_id` | Row changed. It is `NULL` for a table without a single integer key, such as the `*_links__types` vocabularies, and for a PostgreSQL `TRUNCATE`. Either way it means "treat the whole table as changed". |
| `catalog_change_operation` | `insert`, `update`, `delete` or `truncate`. |
| `catalog_change_old_ids` | For updates and deletes: JSON of the old row's ID column, its `*_id` columns and its foreign-key columns. |
| `catalog_change_txid` | PostgreSQL only: the writing transaction, from `pg_current_xact_id()`. |
| `catalog_change_created_ms` | Time of the change. |

Column names carry the table prefix, as every catalogue column does: column-to-table lookups rely on names being unique. The install also records a `column_metadata` policy for each of the log's columns, as the generator does for every other column.

Entries commit or roll back with their change, so the log cannot disagree with the data. It includes:

- trigger side effects, such as timestamp updates;
- foreign-key cascades: deleting a Work logs each cascaded link deletion;
- writers in other processes, and writers outside LiuXin's database layer. The triggers use only standard SQL, so they ask nothing of a writer beyond the catalogue's own index functions.

The log holds no bibliographic values; a reader re-reads current rows by ID. `old_ids` keeps a removed link's two ends (book and tag, say), both as history and so a reader knows which records to refresh.

Covered tables are every table except helpers (`HELPER_TABLES`), engine internals, `maintenance_*` and the log itself.

## Installing it

The log is an optional helper, so it needs no schema revision and older catalogues keep opening:

- **New catalogues** get it at creation, on SQLite (the generator's last step) and on PostgreSQL (in the creation transaction).
- **New native custom-field tables** are covered in the transaction that creates them.
- **Existing catalogues** add it with `change_log.install(connection, schema=...)` inside a write transaction. Running `change_log.plan_install(...)` first lists the statements without writing anything.
- **Opening** a catalogue never installs it.

Reinstalling is safe: triggers are replaced and existing entries are kept. `catalog_changes_metadata` holds two identities, and a reader whose cursor disagrees with either must reload in full:

- `log_uuid` identifies one installation of the log;
- `trigger_generation` counts trigger installations. Writes made while a trigger was missing are not in the log, so a cursor taken before triggers were (re)installed cannot be trusted. Creating a custom field's tables counts as one too.

`covered_tables` reads coverage from the triggers actually present. A table that was renamed or rebuilt after its triggers were installed, or that was created without them, is not covered. A reader should not rely on the log for such a table.

## Reading it

```python
cursor = change_log.current_cursor(connection, schema=schema)
# ... load current data ...
entries, cursor = change_log.read_changes(connection, cursor, schema=schema, limit=5000)
```

Take the cursor before reading the data it accompanies. A change committed in between is then in both the data and the log; applying it again is harmless because applying an entry only re-reads the row by ID.

**SQLite** has one writer at a time, so entries become visible in `change_id` order. The cursor is the last `change_id` read.

**PostgreSQL** assigns `change_id` before commit, so a later entry can commit first. The cursor is a transaction-ID horizon: `pg_snapshot_xmin`, read before the entries, below which every transaction has finished. Each read returns the visible entries at or above the stored horizon, skipping those already returned, which the cursor remembers in `seen` until the horizon passes them. An entry that commits late is therefore returned when it becomes visible. A long-running writer elsewhere delays nothing.

`read_changes` raises `ChangeLogUnavailable` when the log is absent, when it or its triggers were reinstalled, when its entries end before the cursor, or when more than `limit` entries are pending. In each case the reader reloads in full.

## Not logged

These must be detected separately:

- **DDL**, such as creating, migrating or purging a custom field's tables. A cache already reloads in full after a schema change it makes itself.
- **SQLite `REPLACE` conflict resolution.** It deletes conflicting rows without firing delete triggers while recursive triggers are off. LiuXin does not write catalogue tables that way, and maintenance handlers are already refused it.

## Retention

The log is kept as the catalogue's history; nothing prunes it. Its growth will be measured during ingest, and limits such as archiving are phase 4 of the design.
