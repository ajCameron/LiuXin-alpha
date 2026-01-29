# TEMP triggers for custom-column cleanup

When a normalized custom column exists, its values are typically stored in a link table, for example:

- `<in_table>_custom_column_<num>` (items)
- `<in_table>_custom_column_<num>_link` (links)

On deletion of a row in the base table (`in_table`, usually `books`), the link-table rows must be deleted as well.

Calibre solves this by creating a **TEMP trigger** per base table that deletes from *all* known custom-column link
tables for that base table.

LiuXin follows the same approach.

## Why TEMP (not permanent)

Treat these triggers as **derived, runtime state**.

A permanent trigger in `sqlite_master` is a long-lived schema object with a migration/upgrade burden:

- You must remember to update it when custom columns change.
- Older databases can retain stale trigger bodies.
- External tools that open the database can be surprised by side effects.
- Debugging is harder because schema objects are “invisible” in the normal application code paths.

By making the trigger TEMP:

- It is scoped to the application connection (and disappears when the process exits).
- The trigger body can be regenerated freely based on the current metadata.
- It avoids schema drift.

## Correct idempotence: drop + recreate

Because the trigger body depends on *the current set* of custom columns for a table, idempotence must be implemented as:

- `DROP TRIGGER IF EXISTS temp.custom_<table>_delete_trg;`
- `CREATE TEMP TRIGGER custom_<table>_delete_trg ...;`

Do **not** use `CREATE TRIGGER IF NOT EXISTS ...`.

`IF NOT EXISTS` would keep the old trigger body and fail to incorporate newly created link tables, causing orphan rows
in those link tables.

## Naming and scoping

Use a per-base-table trigger name to avoid collisions and to support “custom columns can attach to any table”:

- Trigger name: `custom_<in_table>_delete_trg`
- Event: `AFTER DELETE ON <in_table>`

When generating the SQL, use:

- the correct primary key column for `<in_table>` (do not hardcode `OLD.id`)
- the correct foreign key column name in the link table (calibre-style singularization is often used)

## Connection ownership matters

TEMP triggers are per-connection. If code creates the TEMP trigger on one connection but later executes deletes using a
different connection, the trigger will not fire.

Therefore:

- Create TEMP triggers on `db.driver.conn`.
- Avoid creating “one-off” connections for direct SQL execution.
- Helper objects should not hold stale `self.conn` references.

## Tests

Tests should ensure:

- the trigger is TEMP (exists in `sqlite_temp_master` and not `sqlite_master`)
- creating multiple custom columns does not raise “trigger already exists”
- the trigger body is updated when new columns are added (drop/recreate semantics)
