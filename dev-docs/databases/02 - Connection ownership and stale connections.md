# Connection ownership and stale connections

Reviewed **2026-11-12**. Source: [custom-column connection resolution](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/adapter.py).

SQLite connections are *stateful* objects. In this codebase they can be rotated or replaced during:

- explicit driver reconnection or recovery of a closed connection,
- fixture provisioning,
- explicit auxiliary connection creation/teardown,
- test harnesses that patch or alias `db.conn`.

Ordinary schema refresh, including `get_tables(force_refresh=True)`, now
invalidates caches without replacing the primary handle. It preserves pending
transactions and TEMP objects. Relation-type lookup also preserves transaction
ownership; it no longer uses the committing direct-SQL executor. An explicit
reconnect remains a separate lifecycle operation.

SQLite view-heading and declared-type inspection use the same primary connection
as table discovery, so pending DDL remains visible. They quote relation names,
close their cursors on success or failure, and close only a temporary connection
they opened themselves. See the
[wrapper follow-up review](driver-wrapper-review-2026-11-04.md) for regression
coverage and the remaining compatibility read limitations.

If helper objects (e.g. `CustomColumns`) store a connection reference (`self.conn = ...`) and that connection later gets closed,
you will eventually hit:

- `sqlite3.ProgrammingError: Cannot operate on a closed database`
- missing TEMP objects (TEMP triggers/tables are per-connection)

## Rule: the driver owns the live connection

Treat the database driver (`db.driver`) as the single source of truth for “the live connection”.
Helper objects should *not* cache a connection long-term unless they have a very specific reason to do so.

Prefer:

- `db.driver.conn` for normal execution
- `db.get_connection()` for explicitly caller-owned SQL, with explicit transaction
  and cleanup responsibility.
- The active macro connection for composed semantic writes. `db.lock` and
  `db.driver_wrapper.lock` are the same reentrant mutex and never execute SQL.

See [transaction ownership](transactions.md) for resource cleanup, borrowed
transactions and nested savepoints. A raw connection context can commit; a
Database resource context discards pending work when it closes.

## Implementation pattern: `@property conn`

For helper classes that historically accepted/stored `conn`, use a property that resolves a fresh connection from the owner:

- `conn` getter:
  1. optional override (backwards-compat), validated lazily and discarded if unusable
  2. for the custom-column service, the active macro transaction connection
  3. `db.driver.conn` if available
  4. (DriverWrapper fallback) `self.driver.conn`

- `conn` setter:
  sets the optional override for backwards-compat, but the preferred usage is leaving it unset.

This pattern avoids stale connection references while preserving call sites that still pass `conn=`.

## TEMP objects and persistent custom cleanup

SQLite TEMP tables and triggers belong to the connection that created them.
The custom-column bulk writer keeps its TEMP scratch tables and dependent SQL
inside one owned write transaction. Replacing that connection loses its TEMP
objects, so a helper must never silently substitute another handle mid-operation.

Custom-column owner cleanup now uses persistent per-column triggers. This
protects macro and independent-connection deletes as well as the primary handle.
Construction/refresh repairs existing storage using an owned transaction or a
borrowed savepoint; neither commits the caller's pending work. See the
[cleanup lifecycle](<03 - TEMP triggers for custom-column cleanup.md>) for the
migration from connection-local aggregate triggers.
