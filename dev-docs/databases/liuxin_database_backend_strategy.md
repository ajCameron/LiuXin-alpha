# Database backend strategy

Reviewed **2026-10-20** against the
[backend registry](../../src/LiuXin_alpha/databases/database_driver_plugins/registry.py)
and driver tree.

Canonical catalogue state lives in a relational SQL database. A primary backend
must implement the project's transaction, schema, integrity, migration and query
contracts, including failure behaviour. CRUD support alone is insufficient.

## Current implementations

SQLite is the embedded default, with SQLite/APSW driver implementations.
PostgreSQL is already registered, with aliases `postgres`, `postgresql` and `pg`;
it is not merely a future option. Its installation and deployment checks are in
the [PostgreSQL runbook](postgresql-backend.md). Driver availability does not imply
every backend has identical semantics or that a live server was tested by this
documentation review.

Use [connection ownership](<02 - Connection ownership and stale connections.md>)
for connection-bound helpers. Transaction entry/exit, nested rollback and schema
creation over an existing destination still need the explicit contracts in
[next tasks](next_tasks.md).

## Proposed secondary roles

An analytical mirror could serve reporting and audit workloads. A search or graph
projection could support retrieval and exploration. Each must be rebuildable from
canonical data, with clear freshness and failure behaviour. DuckDB is a candidate
for analytical work, not an implemented peer of the transactional drivers.

`primary_relational`, `analytical_mirror` and `projection/search` are design
vocabulary here. The runtime registry does not require that three-value role
declaration. A future role contract should be introduced with a concrete consumer,
not documented as an existing plugin requirement.

New backends should serve a measured workload. First demonstrate the required
integrity, transaction and migration behaviour in the shared database tests.
