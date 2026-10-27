# PostgreSQL generated identifier discovery

Implementation review, **2026-10-20**, follow-up DB06.

PostgreSQL stores identifiers using its server-defined byte limit. The generated
column `composite_digital_asset_digital_asset_link_composite_digital_asset_id`
exceeds the usual limit. Creation succeeded because PostgreSQL clipped the name,
but wrapper discovery demanded the unshortened spelling. Full schema discovery
therefore failed before a cache or schema-based Catalog writer could be used.

`DriverWrapper.get_link_column` still tries the exact conventional name first.
When it is absent, PostgreSQL's driver resolves the spelling through a bound
`CAST(... AS name)` query. The wrapper accepts that result only if it is among the
actual discovered headings. Other drivers retain exact-name lookup.

This uses the same server representation that creation already uses, including
its compiled identifier limit and multibyte clipping. It does not rename columns,
alter existing schemas, infer a match from arbitrary common prefixes or suppress
missing-column errors. Name collisions still fail during transactional DDL.
Queries use the resolved physical name afterward.

Full-cache testing then exposed a second discovery defect: unknown-cardinality
links ran SQLite `PRAGMA` probes on PostgreSQL's shared connection, caught the
syntax error, and left the transaction aborted. Both snapshot cache backends now
ask the existing driver unique-index metadata owner. Missing metadata capability
falls back to observed records; backend read failures propagate. Composite,
partial and expression indexes do not imply single-column uniqueness.

The regression in
[`test_postgres_link_discovery_live.py`](../../tests/databases/test_postgres_link_discovery_live.py)
checks both endpoint directions, full schema and both snapshot cache backends, a public Catalog
schema write, unchanged physical headings, unknown-column refusal, Unicode byte
clipping and transactional rejection of colliding names. It uses a disposable
live PostgreSQL schema. It verifies the shared connection remains usable after
cache loading. Existing SQLite schema/relationship and cache tests cover the
exact-name path and cardinality behavior.

This is a narrow compatibility fix for generated link columns. It does not add
SQLite's `titles`/`books` projections to PostgreSQL, claim complete legacy Library
support, or introduce a new portable naming scheme for future migrations.

Return to the [PostgreSQL runbook](postgresql-backend.md) and
[database tasks](next_tasks.md).
