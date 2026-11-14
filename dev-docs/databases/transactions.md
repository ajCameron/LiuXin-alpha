# Transaction ownership

Reviewed **2026-11-06**, including the driver-plugin fixes, for architecture A04.
This contract separates a database facade's lifetime, a connection's lifetime and
a SQL transaction's lifetime.

## Resource contexts and compatibility

`with Database(...) as db:` owns resources. Both normal and exceptional exits
call `close()`, discard uncommitted primary work and close owned connections.
Completed commits remain durable. Replacing the driver also discards the old
driver's pending work; reattaching the same driver keeps its primary connection.
Close stops owned maintenance and storage before releasing database dependencies.
A service or connection shutdown failure propagates, retains ownership and leaves the facade
`closing` for retry; it never requests a commit. Injected storage stays caller-owned.
The SQL driver attempts every handle and reports an exception group containing
failed closes. Some handles may already have closed, so `closing` is a cleanup
state, not a usable database. If replacing a driver fails while retiring old
handles, the new binding has already been published: both drivers remain owned,
and `close()` retries retired cleanup before releasing the active driver. Do not
repeat the replacement or treat its candidate as caller-owned after publication.
Finish active operations before closing or replacing their facade. If a context
body already raised, cleanup errors become notes on that original exception.
See the [lifecycle contract](module-boundary-and-lifecycle-review-2026-11-13.md#implemented-lifecycle-contract).

This deliberately changes the old primary-connection commit-on-close behaviour.
Raw SQL callers that intend to persist writes must explicitly commit or use an
owned unit before closing. Wrapping independently committed operations in a
Database resource context does **not** make them atomic.

`db.lock` and `db.driver_wrapper.lock` share a reentrant thread mutex. It
serializes cooperating callers on that facade; it has no SQL methods and does
not commit, roll back, lock another facade or coordinate another process.
Former lock-as-connection callers must obtain a connection using
`db.get_connection()` and own its transaction and cleanup.

## The shared semantic write boundary

[`SQLPortableMacrosMixin.transaction()`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/portable_macros_mixin.py)
delegates transaction ownership to
[`transaction_scope`](../../src/LiuXin_alpha/databases/transactions.py).
Catalog aggregate writes, catalog-backed Row/Agent operations and storage's
database unit of work use this macro boundary. Generic `Row.sync()` retains the
direct update boundary listed below.

| Entry | SQL ownership | Connection lifetime |
| --- | --- | --- |
| Outermost macro with a connection factory | SQLite write units use `BEGIN IMMEDIATE`; PostgreSQL uses `BEGIN`. Commit on success, rollback on failure. | Close the newly acquired connection afterward. |
| Persistent fallback, initially idle | Own a new explicit transaction. | Leave the caller's connection open. |
| Persistent fallback with pending work | Savepoint only; success remains pending and failure rolls back only this unit. | Leave connection and outer transaction with their caller. |
| Nested macro call | Savepoint on the active thread's connection. | Reuse the outer connection. |
| Temporary value/ID table | On the reader's connection, an owned transaction or borrowed savepoint covers creation, body and removal. | Leave the connection open. |

Every nested unit has a unique savepoint. Catching an inner failure does not
retain its partial writes; the outer caller may continue and commit its other
work. A successful inner unit cannot publish before the outer owner. PostgreSQL
constraint errors recover through rollback to that savepoint before more SQL.

The state probe recognizes SQLite's `in_transaction`, APSW's `getautocommit()`
and the PostgreSQL adapter's underlying transaction status. Transaction adapters
must also expose `execute`, `commit` and `rollback`. The supported `SQLite_apsw`
driver uses stdlib SQLite connections for ordinary runtime work; these tests do
not claim that native APSW connection contexts have the same API. A persistent adapter
without observable state is refused rather than assuming its work can be
committed. A borrowed scope must already have an active transaction. Do not
manually commit or roll back a connection while a managed unit owns it.

For temporary tables, a failed population, body or drop rolls back that scope,
including the table's creation. It never retries cleanup by rolling back the
caller's entire transaction. Body writes made on that same connection belong to
the temporary-table scope. A standalone temporary-table context does not enlist
other connections; compose macro writes in an outer `macros.transaction()`.

```python
# db is an open facade; use Catalog operations for bibliographic semantics.
with db.macros.transaction():
    db.macros.insert_row("tags", {"tag": "Travel"})
    db.macros.insert_row("tags", {"tag": "History"})
# Both inserts commit together; either failure rolls back this outer unit.
```

## Serialized storage decisions

A05 extends the same owner with `transaction(serialized=True)`: owned SQLite
write units already reserve their writer with `BEGIN IMMEDIATE`; PostgreSQL takes an advisory transaction lock
shared by participating units in that database/schema. A nested serialized unit
requires an already serialized outer owner; ordinary/borrowed units cannot be
upgraded. Raw SQL and external Store byte operations do not participate in the
PostgreSQL advisory contract.

Native custom-field operations preserve ordinary managed composition by acquiring
that PostgreSQL advisory key themselves before their protected revision/plan reads.
This does not mark or upgrade the entire outer macro unit as serialized; decisions
made before entering the native operation are not retroactively protected. Each
operation reacquires after a possible savepoint rollback. Caller-owned deferred SQLite
transactions retain their native snapshot-conflict behavior: a stale unit
must be rolled back and retried. See [native custom fields](native-custom-fields.md).

Owned transaction scopes reject an already active connection before BEGIN. Writer
waiting is bounded by SQLite's busy timeout; journal mode remains a deployment
choice. Arbitrary transaction bodies are never replayed implicitly. See the
[November review contracts](review-repairs-2026-11-14.md) for migration, pruning,
preference publication and index-version rules.

`require_outermost=True` refuses nesting or borrowing before effects. Successful
exit must represent a real commit, so deletion reservations cannot remain hidden
inside a caller's uncommitted transaction. Storage database units serialize by
default, and their repository reads bypass per-facade caches while serialized.
Explicit `serialized=False` is a lower-level escape, not a safe policy-decision
boundary. See [managed deletion](../storage_and_backup/deletion_policy.md) for
independent reservation, external byte and final metadata phases.

## Commit-point inventory and lower-level escapes

The [wrapper read helpers](driver-wrapper-features-2026-11-04.md) select an
explicit connection, active macro transaction, primary handle, then a temporary
owned handle. Fetches, table scans and view-row reads close cursors but never
complete borrowed transactions. Use the same `macros.read_transaction()` handle
across pages for a stable snapshot; a resumable ID alone does not pin one. Raw
fetch SQL is trusted input, so backend read-only enforcement belongs to that
transaction boundary. PostgreSQL statement errors may require caller-owned
rollback/savepoint recovery even though the helper itself never rolls back.

The common write boundary does not turn every historical SQL helper into an
aggregate operation. The following inventory records the live exceptions.

For SQLite `direct_execute` and `direct_executemany`, row writes explicitly start
a transaction if the primary is idle, including leading `WITH` statements. Their
existing whole-primary-context policy remains: success commits earlier pending
work; a failure rolls it back. Use the macro boundary for nested/savepoint ownership.
`direct_execute` buffers result-producing write rows and closes the native statement
before committing, then returns a readable query cursor. Early result closure
discards unread rows without affecting the completed write. SELECT cursors remain
streamed. Large RETURNING results therefore cost memory proportional to their rows;
use a configured connection with explicit transaction/cursor lifetime when that
cost is inappropriate. `executemany` retains SQLite's no-returned-rows contract.

| Owner / callers | Boundary and remaining limitation |
| --- | --- |
| [Catalog aggregates](../../src/LiuXin_alpha/catalog/catalog.py), [row mutations](../../src/LiuXin_alpha/catalog/row_mutations.py), [relationships](../../src/LiuXin_alpha/catalog/relationships.py) | Compose portable macro writes. Their outer unit owns rollback of the semantic operation; metadata validation remains the domain owner's responsibility. |
| [Storage database unit of work](../../src/LiuXin_alpha/storage/storage_manager/database_unit_of_work.py) | Delegates to the metadata repository transaction. Requires an explicit `unit.commit()` request; otherwise exit rolls back. It cannot restore external Store bytes. Use a fresh unit per operation. |
| [DBPrefs](../../src/LiuXin_alpha/databases/dbprefs.py), [wrapper update helpers](../../src/LiuXin_alpha/databases/driver_wrapper/driver_wrapper_update_mixin.py) | The mutex serializes local callers; selected row writers own their writes. Single-column updates submit only that field and its derived keys, preserving concurrent changes to other fields. Their existence check and write are separate operations; they do not promise compare-and-swap semantics. Memory/cache changes and separate calls are not one database transaction. |
| [Wrapper schema inspection](../../src/LiuXin_alpha/databases/driver_wrapper/driver_wrapper_metadata_mixin.py) | Relation lookup uses native read-only backend introspection. SQLite table/column refresh preserves the primary connection, pending writes and TEMP objects. Query errors propagate; inspection never completes the caller's transaction. |
| [Direct SQL execution](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sql_execution_mixin.py) | `direct_execute`/`direct_executemany` enter the primary connection's backend context and can commit earlier primary work. `direct_execute_sql` uses and commits a separate connection. Script helpers retain backend script semantics, including SQLite's implicit commits. Do not compose these inside a macro unit. |
| [Direct inserts](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/add_mixin.py), [updates](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/update_mixin.py), [row/value deletes](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/delete_mixin.py) | Each call owns a transaction on a fresh SQLite connection, commits only on success, rolls back the whole batch on failure and closes the connection. Separate calls do not form an aggregate transaction or join an enclosing macro unit. Whole-table clearing and link DDL retain their documented separate boundaries. |
| [Value casting](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/value_casting_mixin.py), [trigger helpers](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/triggers_mixin.py) | Separate connections, explicit commits or per-operation contexts remain. These privileged maintenance helpers are not atomic Catalog writers. |
| [Custom-column creation](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/adapter.py) | The explicit compatibility adapter and facade create native storage in one owned transaction on SQLite, APSW and PostgreSQL. Definition and DDL roll back together. Projection preparation runs before commit and publication follows commit. Active primary or macro transactions are refused by this creation facade. |
| [Custom-column management](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns/macros.py) | Compatibility macro names delegate to native validation, revisions and managed transactions. Explicit connections must be the active managed connection. Raw storage creation and implicit purge are unsupported. Field retirement retains data; purge and repair require explicit native plans. |
| [Link DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/utility_mixins.py) and remaining legacy scripts/value helpers | These retain their documented local commit points. Do not assume an enclosing resource or macro context groups arbitrary scripts or compatibility value writes. |
| [PostgreSQL direct driver](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/databasedriver/__init__.py) | Direct row and batch writes use one primary-connection transaction per call, rolling back the batch on failure. These contexts can include earlier pending primary work; they do not join a separate macro connection. Fresh connection setup commits its search path before lending the handle to the macro owner. |
| [Creation/replacement](creation-and-replacement.md) and schema generators | Separate initialization/publication contract, with explicit schema-building commits. Database construction is not part of a later resource or macro transaction. |
| Caller-owned raw connections | The caller explicitly owns `BEGIN`, commit/rollback and close. Backend connection contexts may commit; they are distinct from facade resource contexts. |

The direct methods are privileged persistence/maintenance mechanisms. New domain
write paths must use the existing semantic owner. A08 and A10 track additional
writer consolidation; A05 tracks metadata/byte-deletion policy and recovery.
Transaction rollback does not promise undo of an already completed external
effect or a commit whose outcome is unknown after connection loss.

Storage callers composing manager units must reserve serialization at their
outer boundary; an ordinary transaction cannot be upgraded after reads.
[SquashFS publication](../../src/LiuXin_alpha/storage/reconcile/squashfs_db_sync.py)
uses a serialized macro unit around source/archive registration so its nested
manager writes retain the same transaction and writer reservation.

## Evidence

[Ownership regressions](../../tests/databases/test_transaction_ownership.py)
cover borrowed success/failure, caught nested failure, commit failure, temporary
tables, driver replacement, resource exit and mutex exclusion. The
[live PostgreSQL cases](../../tests/databases/test_transaction_ownership_postgres_live.py)
add real constraint-error recovery, borrowed state and resource cleanup.
Existing Catalog, Row/Agent, driver, storage and connection-cleanup contracts
remain the parity checks; a SQLite-backed PostgreSQL-shaped stub alone is not
live-backend evidence.

[Direct-write regressions](../../tests/databases/test_row_write_safety.py) cover
mapping-order-independent inserts, complete batch rollback, prompt deletion
connection cleanup, preserved integrity-error causes, literal `"None"` versus
SQL NULL, and derived-key-only updates. They also check Row equality, draft
hash rejection and Unicode string conversion. The
[PostgreSQL direct-write cases](../../tests/databases/test_row_write_safety_postgres_live.py)
exercise durability and normalization on the live backend in disposable schemas.

These fixes deliberately remove partial-batch persistence and string-to-NULL
coercion. Callers must pass Python `None` to clear a nullable value. Updating a
derived identity key without its display column recomputes the key from stored
text; PostgreSQL locks that row until the update completes. Draft Rows cannot be
used as dictionary keys or set members until they have a table and row ID.


## Custom values and definitions (2026-11-12)

Custom fields use one native storage engine. The compatibility facade owns its
complete public write transactions and rejects active primary or macro work,
including `set_custom(..., commit=False)`. Native operations and compatibility
macros support managed composition. There is no v1 reading, creation or conversion.

Facade cache rows are journaled before mutation. Dirty hooks and projection
preparation run before commit; metadata publication and notifications follow it.
Pre-commit failures restore SQL and cache. Post-commit publication failures state
that the write committed. Enumeration rename changes allowed choices and values
in the same native transaction.

Construction and refresh read definitions without schema repair or deletion.
Borrowed reads do not commit the caller's transaction. Refresh projections again
if a caller rolls back definition changes they previously read. Native retirement
keeps storage and integrity constraints; purge and repair are explicit planned
operations. See the [custom-column contract](custom-columns.md) and
[regression ratchet](regression-ratchet-2026-11-05.md).
