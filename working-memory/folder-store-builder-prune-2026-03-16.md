# Folder Store Builder Prune - 2026-03-16

Context:
- Support DB provisioning now comes from `tests.support.test_resources_manager` synthetic FRBR-native builders.
- Provisioned `test_db_4` and `test_db_11` do not materialize a `folder_stores` table.
- The remaining `FolderStore` / `FolderStoreManager` logic inside the legacy support builders was dead baggage.

What changed:
- Pruned legacy `folder_stores`-backed asset generation from:
  - `tests/support/test_databases/test_db_4/__init__.py`
  - `tests/support/test_databases/test_db_11/__init__.py`
- Removed direct `FolderStore` / `FolderStoreManager` imports from both modules.
- `TestDB4Builder.generate_fake_asset_data(...)` is now an explicit no-op with a note pointing to the modern synthetic builder path.
- `TestDB11Builder.detail_databases(...)` no longer tries to rebuild asset data through `folder_stores`.
- `TestDB11Builder.build_valid_asset_data(...)` is now an explicit no-op for the same reason.
- Removed the dead `generate_folder_stores(...)` method and the unreachable folder-store asset helper block from `test_db_4`.
- Removed the dead `draw_fs_resource_id_combo(...)` helper from `test_db_11`.

Regression coverage:
- Added `test_provisioned_profiles_do_not_materialize_legacy_folder_stores(...)` to `tests/databases/test_test_resources_manager.py`.
- This pins the current architectural contract at the resource-manager layer:
  - `test_db_4` provisions with `0 folders / 0 files`
  - `test_db_11` provisions with `40 folders / 120 files`
  - neither provisions a `folder_stores` table

Validation:
- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/databases/test_test_resources_manager.py -k 'folder_stores or semantic_asset_profile_partition or semantic_book_count_bands'`
  - `4 passed, 32 deselected`
- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/support/test_databases/test_tree_generators.py tests/support/test_databases/test_legacy_objects_smoke.py tests/databases/api/test_macros_folder_store_path.py`
  - `5 passed`
- `PYTHONPATH=src:. .venv/bin/python -m py_compile tests/support/test_databases/test_db_4/__init__.py tests/support/test_databases/test_db_11/__init__.py`
  - passed

Implication for the migration plan:
- Builder-side `folder_stores` baggage is now pruned for the two main legacy support builders.
- Remaining `folder_store_path` mentions in DB-property files stay in the schema/salvage stream.
- Remaining legacy `folder_stores` cleanup should focus on duplicate support artifacts and manifest cleanup, not compatibility shims.
