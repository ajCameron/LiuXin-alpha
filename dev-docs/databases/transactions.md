# Transaction ownership

Reviewed **2026-10-20** for architecture A04. This contract separates a database
facade's lifetime, a connection's lifetime and a SQL transaction's lifetime.

## Resource contexts and compatibility

`with Database(...) as db:` owns resources. Both normal and exceptional exits
call `close()`, discard uncommitted primary work and close owned connections.
Completed commits remain durable. Replacing the driver also discards the old
driver's pending work; reattaching the same driver keeps its primary connection.
Cleanup is best effort and suppresses ordinary close errors, but never requests
a commit. Finish active operations before closing or replacing their facade.

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
Catalog aggregate writes, supported Row/Agent writes and storage's database
unit of work use this macro boundary.

| Entry | SQL ownership | Connection lifetime |
| --- | --- | --- |
| Outermost macro with a connection factory | Explicit `BEGIN`, commit on success, rollback on failure. | Close the newly acquired connection afterward. |
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

## Commit-point inventory and lower-level escapes

The common write boundary does not turn every historical SQL helper into an
aggregate operation. The following inventory records the live exceptions.

| Owner / callers | Boundary and remaining limitation |
| --- | --- |
| [Catalog aggregates](../../src/LiuXin_alpha/catalog/catalog.py), [row mutations](../../src/LiuXin_alpha/catalog/row_mutations.py), [relationships](../../src/LiuXin_alpha/catalog/relationships.py) | Compose portable macro writes. Their outer unit owns rollback of the semantic operation; metadata validation remains the domain owner's responsibility. |
| [Storage database unit of work](../../src/LiuXin_alpha/storage/storage_manager/database_unit_of_work.py) | Delegates to the metadata repository transaction. Requires an explicit `unit.commit()` request; otherwise exit rolls back. It cannot restore external Store bytes. Use a fresh unit per operation. |
| [DBPrefs](../../src/LiuXin_alpha/databases/dbprefs.py), [wrapper update helpers](../../src/LiuXin_alpha/databases/driver_wrapper/driver_wrapper_update_mixin.py) | The mutex serializes local callers; selected row writers own their writes. Memory/cache changes and a sequence of separate calls are not one database transaction. |
| [Direct SQL execution](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sql_execution_mixin.py) | `direct_execute`/`direct_executemany` enter the primary connection's backend context and can commit earlier primary work. `direct_execute_sql` uses and commits a separate connection. Script helpers retain backend script semantics, including SQLite's implicit commits. Do not compose these inside a macro unit. |
| [Legacy updates](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/update_mixin.py), [value casting](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/value_casting_mixin.py), [trigger helpers](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/triggers_mixin.py) | Separate connections, explicit commits or per-operation contexts remain. Some legacy error paths commit in cleanup; these privileged maintenance helpers are not atomic Catalog writers. |
| [Link DDL](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/utility_mixins.py), [custom-column macros](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/cc_macros_mixin/__init__.py) and [management](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/cc_macros_mixin/cc_management_macros.py) | DDL/script and metadata updates retain local commit points. Schema maintenance must not assume an enclosing resource or macro context groups these operations. |
| [PostgreSQL direct driver](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/databasedriver/__init__.py) | Connection-local operation contexts remain for direct legacy helpers. Fresh connection setup commits its search path before lending the handle to the macro owner. |
| [Creation/replacement](creation-and-replacement.md) and schema generators | Separate initialization/publication contract, with explicit schema-building commits. Database construction is not part of a later resource or macro transaction. |
| Caller-owned raw connections | The caller explicitly owns `BEGIN`, commit/rollback and close. Backend connection contexts may commit; they are distinct from facade resource contexts. |

The direct methods are privileged persistence/maintenance mechanisms. New domain
write paths must use the existing semantic owner. A08 and A10 track additional
writer consolidation; A05 tracks metadata/byte-deletion policy and recovery.
Transaction rollback does not promise undo of an already completed external
effect or a commit whose outcome is unknown after connection loss.

## Evidence

[Ownership regressions](../../tests/databases/test_transaction_ownership.py)
cover borrowed success/failure, caught nested failure, commit failure, temporary
tables, driver replacement, resource exit and mutex exclusion. The
[live PostgreSQL cases](../../tests/databases/test_transaction_ownership_postgres_live.py)
add real constraint-error recovery, borrowed state and resource cleanup.
Existing Catalog, Row/Agent, driver, storage and connection-cleanup contracts
remain the parity checks; a SQLite-backed PostgreSQL-shaped stub alone is not
live-backend evidence.
