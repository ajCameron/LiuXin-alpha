# Custom-column ownership: LiuXin first

Custom fields are a native LiuXin feature. Responsibility determines where code
lives; a method's former placement on a driver or wrapper does not establish
ownership. Keep custom-field behaviour together in `databases/custom_columns`,
using separate service, repository, value, query and migration modules.

| Responsibility | Owner |
| --- | --- |
| Field definitions, typed values, identities, collection semantics, revisions, migration and purge | [Native custom fields](../../src/LiuXin_alpha/databases/custom_columns) |
| Physical custom-field tables, constraints and owner-cleanup rules | Native custom-field repository |
| Connections, transaction/savepoint mechanics, SQL execution and general schema inspection | Database, driver, wrapper and portable macros |
| Table-category registries and wrapper aliases | `Database.refresh_db_metadata()` |
| Composite evaluation | Catalog |
| Cached values and native metadata publication | Caches; [CustomFieldCache](../../src/LiuXin_alpha/caches/custom_fields.py) coordinates metadata with writes |
| Calibre calling conventions, synthetic fields, optional views and result shapes | [Explicit Calibre adapters](../../src/LiuXin_alpha/utils/calibre_compat/custom_columns) |

SQL that describes custom-field storage belongs to the feature repository even
when its spelling differs between SQLite and PostgreSQL. General SQL capabilities
belong to the driver. A driver executes field-creation statements; the native
service decides what constitutes a valid field and which statements are needed.

## Native entry points

```python
from LiuXin_alpha.databases.custom_columns import CustomFieldService, OwnerRef

fields = CustomFieldService(db)  # Also available as catalog.custom_fields.
field = fields.create_field("works", "reading_notes", datatype="comments", mode="list")
fields.set_value(OwnerRef("works", work_id), field.id, ["First note", "Second note"])
```

The [native API contract](native-custom-fields.md) describes supported types,
queries, plans and transaction rules. Core's `custom-fields.native.*` routes use
this contract and compose the cache-owned metadata projection for writes.

Native definitions require an explicit attachment table. There is no implicit
`books` attachment or Calibre attachment-name translation. Series positions are
part of `SeriesValue` and collection entries. Native metadata does not invent an
`_index` field, and `reading` and `reading_index` can be independent native fields.
The Calibre adapter retains its own synthetic-companion restrictions.

The supported persisted storage marker remains `2`. This separation does not add
a storage format, restore v1 support, or rename existing physical columns. A
historical column spelling does not define the native API's owner semantics.

## Explicit compatibility boundary

Database, DriverWrapper and the SQL macro provider no longer inherit or advertise
custom-column compatibility methods. The old API exports, wrapper mixin,
database mixin, custom macro forwarding modules and notification module have been
removed. There are no forwarding aliases at their old paths.

| Former access | Explicit replacement |
| --- | --- |
| `db.driver_wrapper.create_custom_column(...)` and related legacy definition operations | Prefer native service operations; compatibility callers can use `CustomColumnAdapter(db)` from `calibre_compat.custom_columns.adapter` |
| `db.macros.get_cc_id_from_value(...)` and other custom-column macros | Prefer native queries/writes; compatibility callers use `CustomColumnMacros(db)` from `calibre_compat.custom_columns.macros` |
| `db.get_interlinked_rows_cc(...)` | Prefer native values; legacy row consumers use `CustomColumnRows(db)` from `calibre_compat.custom_columns.database` |
| `databases.api.custom_columns_api` | Compatibility contracts live in `calibre_compat.custom_columns.api` |
| `databases.notify` custom-column hooks | `calibre_compat.custom_columns.notify` |

`CustomColumns`, `CustomColumnsManager` and the older Core custom-field command
signatures remain explicit consumers of the compatibility package. The adapter
translates into native operations; it does not own another storage implementation.
Its optional cache/update coupling remains local to that facade.

Temporary ID tables are general SQL infrastructure. Their macro names are now
`create_temp_id_tables`, `insert_values_into_temp_table` and `destroy_temp_tables`.
The old custom-column-prefixed names are removed.

## Optional Calibre SQL views

Native field creation, migration and purge do not install or maintain Calibre
tag-browser views. SQLite compatibility consumers that need those views explicitly
call `refresh_category_views(db)` from `calibre_compat.custom_columns.schema`.
It replaces its owned views in one managed transaction and removes obsolete ones.
Refresh after native schema changes; these views are optional projections, not
part of native storage. This compatibility view interface is SQLite-specific;
native custom fields continue to support SQLite, APSW and PostgreSQL.

## Regression boundary

[Ownership tests](../../tests/databases/test_custom_column_ownership.py) verify
that database imports do not load the custom-column compatibility package, old
methods and module paths are absent, Series labels remain independent, attachment
tables are explicit, and category views require explicit compatibility setup.
Existing cache tests retain preparation-before-commit and publication-after-commit
checks, including savepoint rollback and publication failure recovery.
