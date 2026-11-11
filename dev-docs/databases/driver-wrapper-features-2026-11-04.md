# Driver-wrapper schema, reads and diagnostics

Implemented **2026-11-04** on `maintenance-plugin-modes`, following the
[boundary review](driver-wrapper-review-2026-11-04.md). This guide describes the
physical database interface. Catalog continues to own bibliographic selection,
validation, normalization policy and WEMI projections.

## Physical schema facts

`db.driver_wrapper.get_table_spec(table, *, connection=None)` now obtains native
column declarations, nullability, SQL defaults, generated/identity flags,
ordered primary keys, indexes and foreign keys. `id_column` identifies a sole
physical primary key; a name ending in `_id` is insufficient. Composite keys
remain in `primary_key`. `schema` records the resolved backend namespace,
including temporary-table shadowing. Parent/date/scratch roles and LiuXin table
classification remain naming-based conventions.

Column `is_unique` describes a full single-column uniqueness guarantee. A
composite unique index does not make either individual column unique; partial
and expression indexes remain separate `StorageIndexSpec` facts. Included index
columns do not participate in uniqueness. Foreign keys retain ordered local and
referenced columns, update/delete actions, and backend-exposed deferral facts.

SQLite inspection uses `table_xinfo`, `table_list`, `index_list`, `index_xinfo`
and `foreign_key_list`. It preserves SQLite's nullable primary-key exceptions,
rowid aliases, STRICT/WITHOUT ROWID behavior and native type affinity. It does
not parse CREATE SQL to invent foreign-key names or deferral flags; those are
`None`. Partial indexes retain full SQL, with an unavailable standalone
predicate left `None`. See the [SQLite PRAGMA reference](https://www.sqlite.org/pragma.html)
and [CREATE TABLE rules](https://www.sqlite.org/lang_createtable.html).

PostgreSQL uses native catalogs, preserving native types, identity/generated
columns, ordered composite constraints, expression/partial indexes and INCLUDE
columns. Nullability and defaults follow nested domain inheritance. The source
facts are documented in [pg_attribute](https://www.postgresql.org/docs/16/catalog-pg-attribute.html),
[pg_type](https://www.postgresql.org/docs/16/catalog-pg-type.html),
[pg_index](https://www.postgresql.org/docs/16/catalog-pg-index.html) and
[pg_constraint](https://www.postgresql.org/docs/16/catalog-pg-constraint.html).

View nullability is unknown (`None`). SQL defaults, including `DEFAULT NULL`,
are text in `default_sql`; they are never evaluated as Python values. Identity
columns advertise server generation. The row-dataclass factory does not turn
SQL defaults or identities into Python defaults. Check constraints and arbitrary
expressions are not interpreted, so these specs are not a complete validator or
an automatic migration engine.

Explicit connections and active macro transactions bypass the table-spec cache.
Other calls may reuse a cached spec: request `force_refresh=True` after DDL.
The native driver hook `direct_get_physical_table_spec()` is uncached. These
rules do not change the separate whole-schema/link-spec cache contracts.

## Reads and connection ownership

`fetch_one`, `fetch_all` and `fetch_scalar` accept SQL, optional bindings, and a
keyword-only `connection`. Selection order is explicit connection, active macro
transaction, primary driver handle, then a temporary connection. They always
close their cursor and close only a temporary connection they acquired. They
never commit or roll back a borrowed handle, including after query/conversion
errors. A PostgreSQL SQL error may still leave its transaction aborted; recovery
belongs to the caller's transaction owner.

`fetch_one` returns a complete row or `None`. `fetch_all` materializes all result
rows. `fetch_scalar` requires a single result column and returns its first value;
no row and SQL NULL both produce `None`. Values retain their backend types and
row containers. SQL is trusted caller input; these helpers do not prohibit DML.
Use [read transactions](transactions.md) for backend-enforced read-only access
and a stable snapshot.

```python
wrapper = db.driver_wrapper
with db.macros.read_transaction() as snapshot:
    count = wrapper.fetch_scalar("SELECT COUNT(*) FROM works", connection=snapshot)
    spec = wrapper.get_table_spec("works", connection=snapshot)
```

Legacy `get_view_row_from_id` now uses the same ownership rules, quotes names,
sees pending schema/data on its selected connection, and closes resources on
conversion errors. The compatibility contract still requires a view with a
column literally named `id`. No row returns `None`; duplicate matching IDs raise
`DatabaseIntegrityError`, using a two-row limit to detect ambiguity.

## Bounded, resumable scans

`scan_page` returns `RowPage(rows, next_after_id, complete)`; `iter_row_batches`
yields pages and resolves schema once per iterator. Both support `columns`,
equality/NULL `where` filters, positive `batch_size`, exclusive `after_id`, and
an optional borrowed connection. They require one non-null integer primary key
on a physical table; composite keys and views are rejected. SQLite rows with
non-integer values in an integer-affinity key are rejected before yielding.

```python
for page in wrapper.iter_row_batches(
    "works", columns=["work_id", "work_title"], batch_size=500,
):
    consume(page.rows)  # application-owned processing
    save_checkpoint(page.next_after_id)  # only after successful processing

# Reopen later with the same table, projection and filters.
page = wrapper.scan_page("works", batch_size=500, after_id=saved_id)
```

SQL fetches at most `batch_size + 1` rows, using the final row only as lookahead.
The last returned ID is retained even if it was omitted from the public
projection. Empty pages preserve the supplied checkpoint; an empty iterator
yields nothing. No cursor or owned connection remains open while a batch is
being processed, including when iteration stops early. Callers own any borrowed
connection's lifetime. The portable macros' existing `get_rows_page` shares the
same SQL primitive; its offset/zero-limit contract remains available there.

Pages do not create an implicit long transaction. With separate page snapshots,
earlier IDs are not revisited, deleted rows disappear, updates may be observed,
and later inserts may appear. `complete` describes the query's lookahead, not a
promise that future inserts cannot occur. Moving primary keys can change which
rows are visited. For one stable view, pass the same `read_transaction()`
connection throughout iteration. A saved ID alone cannot restore that snapshot
after process exit and does not provide exactly-once application effects.

## Backend capabilities

`get_backend_capabilities()` returns `BackendCapabilities` with backend name,
informational version, a frozen feature set and an advertised binding limit.
Use `supports(BackendFeature.SAVEPOINTS)` for branching, or `require(...)` to
reject an unsupported facility before starting work.

| Feature | SQLite runtime adapters | PostgreSQL adapter |
| --- | --- | --- |
| Savepoints, read snapshots, physical schema inspection | Yes | Yes |
| RETURNING | Runtime version dependent | Yes |
| Compatibility custom-column DDL | Yes | No |
| SQLite VM progress callbacks | Yes | No |
| Binding limit | Current connection limit when exposed | No finite limit advertised |

The PostgreSQL adapter uses [psycopg2's client-side binding](https://www.psycopg.org/docs/usage.html#passing-parameters-to-sql-queries);
`None` is not an
unlimited-memory or unlimited-statement-size promise. The `SQLite_apsw` runtime
driver currently uses stdlib SQLite connections, so its report identifies that
actual SQLite runtime. Reports describe adapter facilities, not connection
privileges or whether a particular schema permits an operation.

## Optional query observations

```python
from LiuXin_alpha.databases.db_types.query_observation import QueryObservation

def observe(event: QueryObservation) -> None:
    metrics.record(event.operation, event.duration_seconds, event.rows_read)

wrapper.set_query_observers([observe])
wrapper.fetch_all("SELECT work_id FROM works LIMIT 10")
wrapper.set_query_observers()  # disable
```

Observers are synchronous and local to the wrapper. Events contain operation
name, elapsed seconds, returned-row count where known, and exception class name
on failure. They contain no SQL, bindings, result values or exception messages.
Counts describe returned rows, not affected rows, total-table counts or backend
rows examined. Scalar fetches and writes leave the count unknown. Raw `execute`
timing ends when its cursor is returned; subsequent caller fetching is outside
the observation.

Empty observer lists perform no timing, event construction or dispatch. Nested
instrumented calls produce the outer event only; callback-triggered queries do
not recursively notify observers. Ordinary observer exceptions are logged and
isolated, while interruption exceptions retain normal Python behavior. Async
observers are rejected. These hooks cover instrumented wrapper fetches, scans,
schema/capability reads and common legacy row/SQL operations. Direct driver and
macro calls are outside that coverage. They are separate from
[SQLite progress/cancellation hooks](sqlite-progress.md).

## Hierarchy safety and remaining work

`get_linear_row_list` and `walk` now detect repeated ancestors/descendants and
raise `InputIntegrityError` instead of looping. Walks may yield earlier rows
before encountering a cycle; sibling order remains unspecified and the visited
set grows with the traversal. These checks do not repair malformed hierarchies.

Custom-column placeholders, SQLite-specific compatibility DDL, and historical
explicit commit points remain separate follow-ups. The real SquashFS import is
still the next operational exercise; synthetic backend checks do not replace it.

## Verification

The [combined PR checkpoint](review-upgrade-delivery-2026-11-04.md) records delivery
and checks on the PR branch. The evidence below predates PR preparation.

Regression owners are the [portable feature cases](../../tests/databases/test_driver_wrapper_features.py),
[native SQLite cases](../../tests/databases/test_physical_schema_sqlite.py),
[live PostgreSQL cases](../../tests/databases/test_driver_wrapper_features_postgres_live.py),
and [typed caller contracts](../../tests/typing/internal_contracts.py).

The final focused feature/native-schema/API/tooling selection passed **138**
tests. The broader SQLite/APSW integration selection passed **230**. Live
PostgreSQL wrapper, paging and transaction checks passed **49**; the final six
observer/capability/resource checks also passed after lint-driven refinements.
Selections overlap, and no unique-test total or full-repository run is claimed.

Logs are `/tmp/liuxin-wrapper-feature-signoff.log`,
`/tmp/liuxin-wrapper-feature-integrate.log`,
`/tmp/liuxin-wrapper-feature-pg-final.log` and
`/tmp/liuxin-wrapper-feature-pg-check.log`. Tests isolate configuration, cache,
scratch and telemetry under `/tmp` and discard per-test scratch. All 114 local
links in the five edited guides resolve, including files awaiting their eventual
commit. Protected preference hashes remain unchanged.

The complete scoped quality runner passed: **303** formatted targets, lint,
complexity, annotation coverage, import/layer/footprint checks, zero basedpyright
diagnostics and **254** mypy source files. Both contract probes accept valid
calls and reject all **47** invalid examples. The log is
`/tmp/liuxin-wrapper-features-quality-signoff.log`. Schema, hierarchy and view
doctests also pass. The temporary PostgreSQL cluster is stopped and removed;
its log is `/tmp/liuxin-wrapper-features-pg-server.log`.

Return to [Databases](README.md).
