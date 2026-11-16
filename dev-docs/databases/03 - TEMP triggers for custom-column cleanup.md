# Custom-column cleanup triggers

Custom fields now have one native storage format. V1 readers, writers and upgrade
support have been removed. See the [service contract](custom-columns.md) and
[native API](native-custom-fields.md) for current ownership.

SQLite and APSW install durable triggers for owner deletion and owner/value
reference integrity when native storage is created. These triggers work on every
connection, including when SQLite foreign-key enforcement is disabled. PostgreSQL
uses real foreign keys. No TEMP trigger installation or legacy reconciliation runs
when a facade is loaded.

Native audit checks the complete set of required SQLite triggers against canonical
DDL, including missing, renamed and altered triggers. Repair requires an explicit
`plan_repair(actions=("cleanup_trigger",))` followed by `apply_repair(plan)`.
Retirement retains cleanup constraints and values; only explicit purge removes them.

The [earlier review](custom-columns-review-2026-11-12.md) records the historical
TEMP-trigger and connection defects. Its v1 repair workflow has been superseded
by removal of that format. Current regression tests cover independent connections,
owner-deletion rollback, invalid references, explicit repairs and retirement.
