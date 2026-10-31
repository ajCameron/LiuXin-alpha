# Physical-table SQL batches

Implemented **2026-10-30**, extended **2026-10-31**. `db.metadata_sql` exposes 65 operations across 36
main-table mixins, feeds and shared normalization. They address the installed physical
schema and compose with portable macro transactions. Catalog retains WEMI
selection, compatibility projections and membership policy; storage retains
physical file effects. See the [ownership decision](../catalog/catalog-database-ownership.md).

The [table ownership and WEMI read guide](table-sql-ownership.md) inventories
all physical main tables and documents batched relationship/owner reads and
bounded pagination, with a novel/introduction example. The sections below cover vocabulary and write
batches plus data-only normalization maintenance.

## Vocabulary lookup, ensure and deletion

```python
sql = db.metadata_sql
tags = sql.ensure_tags(["Science Fiction", " SCIENCE   FICTION ", "History"])
assert tags["Science Fiction"] == tags[" SCIENCE   FICTION "]

matches = sql.get_tag_ids_from_values(["science fiction", "Missing tag"])
assert matches["science fiction"] == tags["Science Fiction"]
assert matches["Missing tag"] is None

series = sql.ensure_series(["Collected Essays", " COLLECTED ESSAYS "])
assert series["Collected Essays"] == series[" COLLECTED ESSAYS "]
assert sql.get_series_ids_from_values(["collected essays"]) == {
    "collected essays": series["Collected Essays"],
}

# These delete vocabulary rows; schema constraints and link cascades apply.
sql.delete_tags_by_id([tags["History"]])
sql.delete_series_by_id([series["Collected Essays"]])
```

Lookup and ensure return dictionaries keyed by the original input values.
Repeated inputs collapse; different spellings may share an ID. Ensure keeps an
existing display spelling and uses the first supplied spelling for a new key.
Lookup never creates rows. Neither operation links a Work or chooses a Series
position. Use Catalog's membership APIs for those operations.

The generic implementations are `db.macros.find_table_values()` and
`ensure_table_values()`. Stored text comparison keys use bounded `IN` lookups;
missing keys are inserted with conflict handling, then looked up again. A
conflict that leaves an input unresolved raises `DatabaseIntegrityError` and
rolls back the batch. Existing scalar matching remains the fallback for policies
without stored text keys. These helpers reuse column/identity declarations and
`metadata.text_identity`; they introduce no new normalization policy.

Scoped identities require every scope field, including an explicit `None` for
root Genres or Subjects:

```python
roots = db.macros.ensure_table_values(
    "genres", "genre", ["History", " HISTORY "],
    additional_values={"genre_parent_id": None},
)
assert roots["History"] == roots[" HISTORY "]
```

Other `additional_values` fields affect new rows only; they do not restrict
lookup or overwrite existing rows.

## File metadata and generic row batches

```python
file_id = db.macros.insert_row("files", {"file_name": "original.epub"})
with db.macros.transaction():
    db.metadata_sql.update_files_bulk({
        file_id: {"file_name": "recorded-name.epub", "file_size_bytes": 12345},
    })
    # Other database operations can share this commit/rollback boundary.

db.metadata_sql.delete_files_by_id([file_id])
```

`update_files_bulk()` accepts only `file_name` and `file_size_bytes`. These are
recorded metadata: no source is renamed, read, resized or removed. The method
returns `None`; empty payloads and missing IDs do nothing. File deletion retains
its integer-ID and one-cell binding input shapes.

`db.macros.update_rows(table, updates)` provides the shared implementation for
other physical tables. It validates all payloads before writes, forbids changing
the ID column and prepares normalized keys through the same declarations as
`update_row()`. Consecutive payloads with matching column sets share an
`executemany()` batch, preserving input order. File name/size updates need no
per-row reads. A key-only update can read the existing display value to derive
the correct key instead of trusting a supplied key.

`db.macros.delete_rows_by_id(table, ids)` deletes by explicit integer IDs in
bounded `IN` statements. Duplicates collapse and absent IDs do nothing. Both
generic helpers reject views, missing tables, booleans and projection rays.
Constraints and cascades still apply; the caller chooses which rows to delete.
Deletion returns `None`, rather than a potentially backend-dependent row count.

## Inserts, predicate deletes and relationship replacements

`macros.insert_rows(table, rows)` prepares normalized keys before writes and
groups consecutive payloads with the same columns into multi-value INSERTs.
Each statement binds at most 500 values. Omitted columns use schema defaults;
the operation returns no generated IDs. Use explicit IDs or a declared natural
identity when later work needs to identify inserted records. A conflict in a
later chunk rolls back earlier chunks.

`macros.delete_rows_where(table, where=...)` deletes an equality/NULL scope in
one statement. A nonempty predicate is required. Composite-member replacement
uses these helpers inside its existing transaction; typed member envelopes and
cache invalidation remain owned by storage. Backup source replacement has an
atomic `replace_backup_workflow_sources(workflow_id, rows)` table operation.
Saving a workflow declaration still does not form one transaction with every
checkpoint/output record.

Physical integer-ID `replace_links()` and `replace_links_bulk()` now stage
desired identities in indexed temporary tables. They delete omitted links,
move surviving priorities to a temporary range, update supplied fields and
insert missing links with `INSERT ... SELECT`. Final readback verifies all
desired identities survived conflict handling. Surviving link IDs and omitted
extra fields remain intact; scoped NULL types and other types remain distinct.
Temporary tables and all writes share the caller's rollback boundary. Older
adapters and non-integer endpoints retain the sequential implementation.

`upsert_links()` batches its initial and final relationship reads and stages
consecutive new rows with matching columns. Existing updates retain input order
and immediate priority-constraint behavior. Returned results follow input order,
while replacements follow stored priority order. Both retain the existing live
allowed-type validation and explicit NULL-type identity rules.

## Counts and totals

`aggregate_rows(table, where=..., sum_columns=..., counts=...)` returns a total
row count, SQL sums and named conditional counts in one query. Conditions use
exact equality/NULL predicates inside the outer filter; empty sums remain NULL.
`has_rows()` uses `SELECT 1 ... LIMIT 1` for existence checks.
`get_grouped_counts_for_ids()` returns stored grouping columns and `row_count`
for bounded owner batches, retaining empty owner groups.

`get_link_counts_bulk()` returns `links`, `distinct` existing destinations and
`missing` endpoints per source. Callers select the appropriate meaning; the
primitive does not silently redefine category popularity. Core's replica
assessment uses `count_asset_replicas_for_assets()` raw label groups and applies
the same Python health/default rules as before. These counts do not replace the
storage manager's effective typed-record or cache semantics.

## Audit and rebuild declared comparison keys

```python
report = db.metadata_sql.audit_normalized_keys(
    "tags", value_columns=["tag"], row_ids=[tags["Science Fiction"]],
)
if report.collisions:
    # Resolve conflicting display values or choose a semantic merge via Catalog.
    raise ValueError(report.collisions)
if report.rows_needing_update:
    repaired = db.metadata_sql.rebuild_normalized_keys(
        "tags", value_columns=["tag"], row_ids=[tags["Science Fiction"]],
    )
    assert repaired.columns_added == repaired.indexes_created == ()
```

Select identities by their **display-column names**, such as `tag` or `series`.
Omitting `value_columns` selects all declared identities for that table;
omitting `row_ids` selects all rows. Empty selections do nothing and absent IDs
select nothing. Missing declarations or undeclared column names raise
`InputIntegrityError`; a `*_norm` suffix alone is insufficient. An existing empty
declaration registry is authoritative.

The audit is read-only and uses the current connection. To inspect a consistent
transactional snapshot across calls, put them in the caller's transaction.
Collision checking scans the table for each selected declaration, even when
only a few rows are selected. It compares proposed selected keys with **stored,
unchanged keys on unselected rows**, using exact scopes including NULL parents.
Collisions involving only unselected rows are omitted.

Rebuild audits again inside its write transaction and refuses collisions before
effects. It clears changing keys to NULL before assigning final values, allowing
key swaps under unique indexes. A schema constraint or trigger can still reject
the repair, including a NOT NULL constraint on a changing key; the entire call
then rolls back. Successful repair preserves display values, scope fields,
identity declarations, columns and indexes. Existing update triggers still run.
It does not merge rows or install missing schema. Use
`db.macros.migrate_normalized_identities()` for explicit schema/declaration
migration instead.

Both methods return `NormalizedIdentityMigrationReport`. `rows_examined` counts
scanned rows per declaration; `rows_needing_update` and `rows_updated` count key
changes, so a row with two declared identities can count twice. `clean` means no
collisions, and does not imply that every key is up to date.

## Batching and validation limits

Inputs are materialized and validated before mutations. SQL key lookups use at
most 500 bindings, reduced for non-null scope bindings; write batches contain
at most 500 rows. This bounds statement sizes, not whole-call memory or
transaction duration. Split very large inputs into application batches when
independent commits are acceptable. Each call is atomic, including earlier
chunks when a later chunk fails; an outer transaction retains commit/rollback
ownership. No helper promises a commit per chunk.

The [batch regressions](../../tests/databases/test_metadata_sql_bulk.py) cover
SQLite and APSW, Unicode identity matching, NULL scopes, rollback, source-byte
preservation, physical-table validation and data-only repair. A measured query
count verifies that ensuring 1,001 new Tags requires six key SELECTs: three
before insertion and three afterward. Inserts still process each missing row;
this is a lookup reduction, not a wall-clock throughput claim. Live PostgreSQL
and the full suite were not run for this addition.
