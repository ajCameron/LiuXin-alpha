# Custom columns: one storage format

All custom fields use the [native field service](native-custom-fields.md).
The Calibre-shaped facade lives in
[`utils/calibre_compat/custom_columns`](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns).
It translates calling conventions, metadata descriptors and optional cache cells;
it does not implement another storage engine. `databases.custom_columns` exposes
native operations and types only. Compatibility callers must import `CustomColumns`,
`CustomColumnsManager` and `CompatibilityError` from the explicit Calibre package.
The former facade, manager, mixin and writer module paths have been removed;
imports through those paths fail rather than forwarding.

V1 format support has been removed completely. There is no v1 reader, writer,
creator, importer or upgrade tool. An unsupported catalogue raises
`UnsupportedStorage` without changing its definitions or values. The supported
format retains persisted marker `2`; that marker does not select between two
runtime implementations. New catalogues and every supported creation route use it.

## Ownership and creation

`CustomFieldService` owns definitions, identity keys, ordered entries, validation,
revisions, migrations, retirement and purge. It works with SQLite, APSW and
PostgreSQL. Database wrappers expose no custom-column compatibility methods.
Core's older custom-field commands explicitly select the Calibre facade, which
delegates to this service. See [the ownership contract](custom-column-ownership.md). The old unregistered physical-table builders on database drivers
have been removed.

```python
from LiuXin_alpha.utils.calibre_compat.custom_columns import CustomColumns

columns = CustomColumns(db, table="works")
number = columns.create_custom_column("Shelf", label="shelf", is_multiple=True)
columns.set_custom(work_id, ["Reading", "Reference"], num=number)
assert columns.get_custom(work_id, num=number, index_is_id=True) == [
    "Reading", "Reference"
]
```

The facade maps `books` to `manifestations` on current FRBR catalogues, or accepts
an explicit canonical attachment. Values belong to persistent owner IDs; cached
row positions are used only when `index_is_id=False`. A results cache is optional.
Getters read native storage, including when a cache exists.

The compatibility API supports scalar values, scalar Series values with an extra
position, and text sets without embedded `|` delimiters. Use native collections
for lists, non-text collections, delimiter-bearing values and multiple Series
positions. Compatibility creation refuses unsupported collection shapes before
creating a field. Composite evaluation belongs to Catalog.

## Transactions and publication

| Operation | Boundary |
| --- | --- |
| Construction and refresh | Read definitions on the current connection. No schema repair or deletion. A borrowed read does not commit caller work. |
| Facade creation, metadata changes, retirement and value writes | Require their own transaction. Reject active primary or macro transactions before changing storage or projections. |
| Native operations and compatibility macros | Join a native managed transaction or own a new one. Compatibility macros require a bound database; explicit connections must be the active managed connection. |
| Purge and repair | Explicit native plans, checked against the catalogue fingerprint when applied. |

Facade value writers journal optional cache rows. SQL, dirty hooks, cache writes
and projection preparation run before commit; a failure restores SQL and cache.
Metadata maps and database/wrapper custom-table registries publish after commit.
Database row projections preserve native entry order. Notifications run afterwards. A publication
or listener failure reports that SQL has committed; do not retry the write blindly.
Embedded dirty hooks must honor `commit=False` and avoid external side effects.
`set_custom(commit=False)` is unsupported; use native managed composition instead.

A facade refreshed inside a caller transaction reflects that transaction. Refresh
it again after the caller rolls back definition changes. Hosts must serialize
facade/cache access while a write is in progress.

## Retirement and cleanup

`delete_custom_column` retires a definition and hides its descriptors. It retains
stored values and cleanup constraints. Construction, refresh and compatibility cleanup entry points never physically
purge a field. Only an explicit native
`plan_purge` / `apply_purge` removes retired storage.

Native foreign keys and durable SQLite triggers clean memberships when an owner
is deleted. Audit compares every required SQLite cleanup/reference trigger with
canonical DDL. Repairs remain explicit; loading a facade cannot silently fix
schema damage. `clean_custom` removes unused normalized inventory values through
native mutations and never removes field definitions.

The SQLite-only `custom_columns_in_meta` helper can describe legacy cache SQL
projections over representable native fields. It uses entry ordinals and creates
no view. Normal facade reads do not require a `meta2` view.
