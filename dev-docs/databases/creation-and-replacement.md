# Database creation and replacement

Implemented for PR review **2026-10-20**, architecture A03 / database DB01.

## Ordinary creation

`Database(..., create=True)`, Core library creation and the terminal creation
wizard now reject an existing SQLite/APSW file with `FileExistsError`. The legacy
`blank` and `backup` arguments do not authorize deletion or replacement.
`create=False` still opens an existing library, or initializes a missing file.
Normal opening can perform the separately configured bootstrap repairs.

Creation constructs and checks the complete FRBR schema in a private sibling
directory, closes its connections and publishes with an exclusive hard link.
A target appearing during preparation wins the collision: it is never opened,
overwritten or deleted by the creator. Failed preparation releases only private
scratch. Dangling symlinks and orphaned SQLite journal/WAL files also prevent
creation. A filesystem without hard-link support fails without an overwriting
fallback. This is an atomic publication contract, not a cross-filesystem operation
or a guarantee against loss of the final directory entry after sudden power failure.

The file-driver entry point requires `set_conn=False` before
`direct_create_new_database()`: constructing an ordinary connected SQLite driver
can itself create an empty file. The registry's path-level `create_new_database`
now uses the same FRBR driver contract. Connection-level FRBR and Calibre schema
builders remain lower-level mechanisms for caller-owned empty connections.

## Explicit offline replacement

Stop every user of the database, including other processes, then select an
absent recovery destination:

```python
from LiuXin_alpha.core.library_services import LibraryServices

backup = LibraryServices.replace_database(
    database_path="library.sqlite",
    backup_path="recovery/library-before-reset.sqlite",
    db_type="SQLite",  # SQLite_apsw is also supported
)
```

The operation builds and validates a new schema privately, obtains a verified
[SQLite backup snapshot](https://www.sqlite.org/backup.html), publishes the backup
create-only, then atomically replaces the original file. It preserves the original
file's mode on both the replacement and the backup. The backup remains available after successful
replacement or a failed final rename. The operation does not open a library graph;
open the resulting database normally afterward.

An existing backup is never overwritten. Preparation or backup failure leaves the
original database in place. Missing/nonregular/symlink targets, SQLite sidecars,
and changes observed during preparation cause refusal. Close all users and
checkpoint or recover SQLite normally before retrying a sidecar refusal; do not
delete journal or WAL files as a shortcut.

Closed WAL-mode databases with no remaining sidecars are supported. The snapshot
connection opens the existing file with `mode=rw` so SQLite can release its own
temporary WAL coordination files on close; its SQL is read-only. It obtains a
read lock with a bounded busy timeout before starting the backup.

Offline exclusion is the caller's responsibility. Identity checks are not a lease
against a process that reopens the target during replacement. This operation does
not replace a live database behind existing connections. Restore a retained backup
only with database users stopped, preserving any newer target for inspection first.
Parent directories created during preparation can remain after failure.

The terminal wizard records replacement intent and a required backup path
separately. Its historical `backup_existing=True` field alone cannot authorize
replacement. Core and database APIs expose the same explicit operation.

## Driver dump and rebuild

`driver.dump_and_restore()` prepares and validates a complete replacement before
publishing it. Pending writes on tracked connections are refused. At publication,
the driver finalizes its owned cursors and closes its connections; retained
SELECT cursors therefore cannot leave the old SQLite WAL attached to the new file.
Cursor bookkeeping uses weak references and does not keep abandoned readers alive.

Failed closes or surviving WAL, shared-memory or journal files prevent replacement
on both driver routes. The original remains in place and the driver attempts to
reopen it. Close other database users and checkpoint/recover normally before
retrying; the rebuild never removes a live WAL to force publication. The caller
must still coordinate exclusive access throughout the operation: the final
sidecar check is not a lease against another process reopening the file.

The method does not make an automatic recovery backup or offer rollback after
successful replacement. Create a verified backup first when one is required.
[Completion/rebuild regressions](../../tests/databases/test_driver_completion_boundaries.py)
check retained and custom readers, failed closes, external WAL refusal, retry,
and the reopened candidate's actual schema/data.

## Independent SQLite working copies

**2026-11-05:** `Database.lock_writing()` and driver `make_scratch()` have been
removed from the implementations and abstract APIs. No in-tree database callers
remain. Despite its name, the old facade never acquired a write lock: it attempted
a raw file copy and changed a path attribute without reopening existing handles.
That could omit WAL data and split subsequent operations between two files. Its
temporary-folder dependency also failed before the copy could run.

For an already open SQLite or SQLite_apsw `source_driver`, create a verified
committed snapshot at an absent destination and open a separate owner:

```python
from contextlib import closing
from pathlib import Path

copy_path = source_driver.direct_backup(Path("working-copy.sqlite"))
with closing(type(source_driver)({"database_path": str(copy_path)})) as working:
    with closing(working.direct_execute("SELECT COUNT(*) FROM works")) as cursor:
        print(cursor.fetchone()[0])
    # Apply experimental changes through working, then close it normally.
```

Backup includes committed WAL content and excludes uncommitted source work.
It refuses an existing destination. The source path, connection and transaction
remain with their original owner; writes to the copy do not affect the source.
The source remains writable. This workflow provides neither source write locking
nor a read-only guarantee, and does not switch an existing Database/Core graph.
For a complete library graph, open a new graph configured with the backup path.

[Regression coverage](../../tests/databases/test_driver_plugin_sqlite_safety.py)
checks the removed surface, WAL visibility, pending source work and independent
writes after opening the backup. The separate filesystem scratch helpers in
`utils.ptempfiles` are outside this database API retirement.

## PostgreSQL

Creation initializes an absent or empty schema in an existing server database.
It accepts an empty namespace provisioned by `postgres setup-sql`, but rejects
one containing relations, routines or types before seeding any data.

A transaction-scoped advisory lock serializes LiuXin initializers for the same
database/schema. Strict object DDL rejects collisions with other creators, and
schema/object creation rolls back together on failure. This applies to the
builder, driver, `Database(create=True)` and `postgres init` paths.
Use `postgres check` to inspect an existing library; initialization is not an
idempotent migration or repair command. File-style replacement is unsupported
for PostgreSQL; server backups and migrations require their own operator contract.

## Evidence and remaining work

[File regressions](../../tests/databases/test_database_creation_safety.py) cover
real SQLite/APSW creation, populated-library preservation, a destination arriving
during preparation, competing creators, partial build failure, replacement/backup
failures, recovery readback and terminal caller behaviour.
[Live PostgreSQL regressions](../../tests/databases/test_database_creation_postgres_live.py)
cover absent/empty/populated schemas, concurrent creators and transactional rollback.
They require `LIUXIN_TEST_POSTGRES_URL` and use uniquely named disposable schemas.

Resource contexts and transaction contexts remain distinct. A03 does not change
`Database.__exit__`, implicit commits during close, or general driver transaction
semantics; those remain [A04/DB02](next_tasks.md#db02-specify-transactions-independently-of-connection-lifetime).
