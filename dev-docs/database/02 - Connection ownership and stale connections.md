# Connection ownership and stale connections

SQLite connections are *stateful* objects. In this codebase they can be rotated or replaced during:
- driver refresh / metadata refresh,
- fixture provisioning,
- wrapper-level “lock” connection creation/teardown,
- test harnesses that patch or alias `db.conn`.

If helper objects (e.g. `CustomColumns`) store a connection reference (`self.conn = ...`) and that connection later gets closed,
you will eventually hit:

- `sqlite3.ProgrammingError: Cannot operate on a closed database`
- missing TEMP objects (TEMP triggers/tables are per-connection)

## Rule: the driver owns the live connection

Treat the database driver (`db.driver`) as the single source of truth for “the live connection”.
Helper objects should *not* cache a connection long-term unless they have a very specific reason to do so.

Prefer:

- `db.driver.conn` for normal execution
- `db.driver_wrapper.lock` (or `db.conn`) only when you explicitly want “run under the wrapper lock connection”

## Implementation pattern: `@property conn`

For helper classes that historically accepted/stored `conn`, use a property that resolves a fresh connection from the owner:

- `conn` getter:
  1. optional override (backwards-compat), validated lazily and discarded if unusable
  2. `db.driver.conn` if available
  3. (DriverWrapper fallback) `self.driver.conn`

- `conn` setter:
  sets the optional override for backwards-compat, but the preferred usage is leaving it unset.

This pattern avoids stale connection references while preserving call sites that still pass `conn=`.

## Notes on TEMP triggers/tables

SQLite TEMP triggers and TEMP tables are scoped to the connection that created them.
If code creates TEMP triggers during cache/custom-column initialization, it must do so on the same live connection that
will later be used for the operations relying on those triggers.

If you *must* create TEMP objects, ensure you create them on `db.driver.conn` (or whatever connection you will later use
to execute the dependent statements), and avoid “one-off” connections in the execution path.

### TEMP triggers are *derived state* 

When we use TEMP triggers (for example, the custom-column delete trigger used to clean up link tables), treat them as
**derived state** that is regenerated from metadata.

That has two important consequences:

1. **Never make these triggers permanent** (do not create them in `sqlite_master`). Permanent triggers are hard to
   reason about, are easy to forget to update during migrations, and can surprise external tools that open the DB.
2. **Make TEMP trigger creation idempotent by drop/recreate**, not `IF NOT EXISTS`.

Why not `CREATE TRIGGER IF NOT EXISTS`?

Because the trigger body depends on the current set of custom columns (and their link-table names). When you add/remove
custom columns you must regenerate the trigger definition; `IF NOT EXISTS` would silently keep the stale body and leak
rows in newly created link tables.

Recommended pattern:

- `DROP TRIGGER IF EXISTS temp.custom_<table>_delete_trg;`
- `CREATE TEMP TRIGGER custom_<table>_delete_trg AFTER DELETE ON <table> ...;`

Also note that TEMP triggers will disappear if the connection is replaced. This is one more reason to treat
`db.driver.conn` as the stable, owned connection for database helpers.
