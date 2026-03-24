# Remaining Rewrite Seams

Date: 2026-03-16

## Summary

The manifest rewrite set now stands at `16` rows.

Current manifest totals:
- `covered`: `76`
- `rewrite`: `16`
- `salvage_existing`: `0`
- `retire`: `16`
- `integration_frozen`: `9`
- `vendor_frozen`: `7`

## Remaining Seams

### 1. `core_xmlrpc_compat`

Rows: `1`

- `src/LiuXin_tests/core/self_test.py`

Reason it stays `rewrite`:
- the old test is XML-RPC `system.listMethods()` compatibility, not just generic core health
- alpha now has a different core/runtime/proxy/HTTP model
- this should only move when there is an explicit compatibility claim to make

### 2. `folder_store_runtime`

Rows: `4`

- `src/LiuXin_tests/folder_stores/cover_cache/on_disk/utils_test.py`
- `src/LiuXin/folder_stores/drivers/on_disk/test_store.py`
- `src/LiuXin/folder_stores/drivers/on_disk_flat/test_store.py`
- `src/LiuXin/folder_stores/drivers/zip/test_store.py`

Reason it stays `rewrite`:
- this is the old folder-store runtime surface, not a utility-dependency problem
- cover-cache still lacks a live alpha seam

### 3. `db_property_secondary_uuid_cluster`

Rows: `3`

- `src/LiuXin_tests/test_databases/test_db_properties/test_db_18_properties.py`
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_19_properties.py`
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_21_properties.py`

Reference:
- [db-property-secondary-uuid-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-secondary-uuid-cluster-2026-03-16.md)

### 4. `db_property_identifier_cluster`

Rows: `1`

- `src/LiuXin_tests/test_databases/test_db_properties/test_db_20_properties.py`

Reference:
- [db-property-identifier-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-identifier-cluster-2026-03-16.md)

### 5. `db_property_compatibility_projection_cluster`

Rows: `5`

- `src/LiuXin_tests/test_databases/test_db_properties/test_db_1_properties.py`
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_14_properties.py`
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_15_properties.py`
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_16_properties.py`
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_17_properties.py`

Reference:
- [db-property-compatibility-projection-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-compatibility-projection-cluster-2026-03-16.md)

### 6. `db_property_rich_content_cluster`

Rows: `2`

- `src/LiuXin_tests/test_databases/test_db_properties/test_db_4_properties.py`
- `src/LiuXin_tests/test_databases/test_db_properties/test_db_10_properties.py`

Reference:
- [db-property-rich-content-cluster-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-rich-content-cluster-2026-03-16.md)

## Recommended Next Step

Do not invent another salvage stage.

The remaining legacy-test work is now explicitly one of:
1. defer `core_xmlrpc_compat` until there is a deliberate compatibility goal
2. keep `folder_store_runtime` blocked until a real replacement implementation seam exists
3. leave the DB-property rewrite families in `rewrite` until replacement builders/tests exist
