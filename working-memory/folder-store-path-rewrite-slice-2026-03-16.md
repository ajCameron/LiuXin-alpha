# Folder Store Path Rewrite Slice

Date: 2026-03-16

## Why This Is The Next Slice

- The first direct `test_harness` replacements are now in place:
  - tree-generator coverage
  - `TestObjectsHandler` smoke coverage
  - `replace_in_folder_store_path(...)` macro coverage
- That leaves one narrow remaining seam from the old harness/storage boundary that is still visible in alpha:
  - `folder_store_path`
  - `folder_store_marker_path`
- This is a better next rewrite slice than jumping straight into the full cache/emulation corpus because:
  - it is narrower
  - it is still directly referenced by current alpha support code
  - it cleanly bridges the gap between legacy `folder_stores` fixtures and the modern storage rewrite

## Current Live Callers

### 1. Support DB builders

- [test_db_4/__init__.py](../tests/support/test_databases/test_db_4/__init__.py)
- [test_db_11/__init__.py](../tests/support/test_databases/test_db_11/__init__.py)

These still assign:
- `folder_store_path`
- `folder_store_marker_path`

That is the real active runtime seam inside alpha support code.

### 2. Support property corpus

Examples:
- [common_db_properties.py](../tests/support/test_databases/test_db_properties/common_db_properties.py)
- [test_db_6_properties.py](../tests/support/test_databases/test_db_properties/test_db_6_properties.py)
- [test_db_18_properties.py](../tests/support/test_databases/test_db_properties/test_db_18_properties.py)
- [test_db_19_properties.py](../tests/support/test_databases/test_db_properties/test_db_19_properties.py)
- [test_db_21_properties.py](../tests/support/test_databases/test_db_properties/test_db_21_properties.py)
- [test_db_22_properties.py](../tests/support/test_databases/test_db_properties/test_db_22_properties.py)
- [test_db_23_properties.py](../tests/support/test_databases/test_db_properties/test_db_23_properties.py)
- [test_db_24_properties.py](../tests/support/test_databases/test_db_properties/test_db_24_properties.py)
- [test_db_25_properties.py](../tests/support/test_databases/test_db_properties/test_db_25_properties.py)

These are not the next rewrite target by themselves.

They belong to:
- `salvage_existing`
- DB-property promotion

Meaning:
- keep them as schema-contract signal for now
- do not mix them into the storage rewrite slice unless the live schema actually changes

### 3. Shared macro surface

- `src/LiuXin_alpha/databases/api/macros.py`
- [__init__.py](../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/__init__.py)
- direct test now exists:
  - [test_macros_folder_store_path.py](../tests/databases/api/test_macros_folder_store_path.py)

This surface is now covered enough to stop guessing about it.

## Decision

- The next rewrite slice should be:
  - remaining `folder_store_path` builder behavior in `tests/support/test_databases`
- Not yet:
  - the full `database_caches` cluster

## Recommended Work Order

1. Audit whether `folder_stores` is still a real table in the generated support DBs that matter.
2. If yes:
  - add focused builder tests for the support DBs that still materialize `folder_store_path` / marker rows
  - keep the schema/property assertions until DB-property promotion is finished
3. If no:
  - retire the remaining builder-side `folder_store_path` writes
  - delete the dead support fixture baggage with a clear migration note
4. Only after that:
  - move to the larger `database_caches` rewrite cluster

## Practical Boundary

- `folder_store_path` in support DB properties is currently a schema-contract issue.
- `folder_store_path` in support DB builders is currently a rewrite issue.
- Do not collapse those into one task. They are different migration streams.

## Update

- The builder-side `folder_stores` baggage in `test_db_4` and `test_db_11` has now been pruned.
- Replacement coverage now lives at the resource-manager profile layer:
  - `test_db_4`: `0 folders / 0 files`, no `folder_stores` table
  - `test_db_11`: `40 folders / 120 files`, no `folder_stores` table
- Remaining `folder_store_path` references in DB-property files stay in the salvage/schema stream.
