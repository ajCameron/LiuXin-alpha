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
