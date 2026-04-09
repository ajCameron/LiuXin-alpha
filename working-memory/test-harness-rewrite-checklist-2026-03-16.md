# Test Harness Rewrite Checklist

Date: 2026-03-16

## Purpose

- Turn the `test_harness` cluster in [legacy-test-migration-manifest-2026-03-16.csv](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-test-migration-manifest-2026-03-16.csv) into concrete keep/rewrite/retire decisions.
- Avoid leaving those six files in a vague “rewrite later” bucket.

## Legacy Input Mapping

### 1. Scratch FSM loader tests

Legacy source:
- [test_fsm_loader.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/test_self/folder_stores/test_fsm_loader.py)

Legacy behaviors:
- load a prepared scratch folder-store manager
- assert import-cache isolation and dedicated mode
- assert folder-store row paths point at the expected scratch layout

Current alpha seams:
- [test_resources_manager.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_resources_manager.py)
- [test_test_resources_manager.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/test_test_resources_manager.py)
- [test_resources_manager_assets_test.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_resources_manager_assets_test.py)

Decision:
- `rewrite`

Rewrite target:
- resource/database provisioning behavior under:
  - `tests/support/test_resources_manager.py`
  - `tests/databases/test_test_resources_manager.py`
  - `tests/support/test_resources_manager_assets_test.py`

Retire from the old file:
- dedicated import-cache mode assertions
- folder-store-manager scratch path assertions

Reason:
- the useful behavior is “provision reproducible scratch assets/databases”
- the old `folder_stores`/import-cache object model is gone

### 2. Scratch FSM system tests

Legacy source:
- [test_fsms.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/test_self/folder_stores/test_fsms.py)

Legacy behaviors:
- build/copy scratch folder-store managers
- ramdisk-backed setup and path copying
- SQL/path generalize-specialize behavior for `folder_store_path`

Current alpha seams:
- [test_location_contract.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/location/test_location_contract.py)
- [test_storage_manager_impl.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/api/test_storage_manager_impl.py)
- [macros.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/databases/api/macros.py)

Decision:
- `rewrite`

Rewrite target:
- storage contract behavior in `tests/storage/...`
- if still needed, a focused database macro test for `replace_in_folder_store_path(...)`

Retire from the old file:
- ramdisk/FSM build-copy lifecycle
- direct `FolderStore` and `FolderStoreManager` assertions

Reason:
- only the path-rewrite semantics still look reusable
- the old scratch-FSM lifecycle is part of the retired storage architecture

### 3. Test database tree generator

Legacy source:
- [test_make_test_databases.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/test_self/test_make_test_databases.py)

Legacy behaviors:
- `generate_test_tree(...)` creates deterministic subject-tree rows
- naming/UUID stream behavior stays stable
- generated tree size is predictable for the chosen seed

Current alpha seams:
- [test_db_4/__init__.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/test_db_4/__init__.py)

Decision:
- `rewrite`

Rewrite target:
- add a collected pytest contract around:
  - `generate_test_tree(...)`
  - `generate_test_tree_with_datestamps(...)`

Reason:
- this is still live behavior in alpha support code
- it deserves a direct collected test rather than only being exercised indirectly

### 4. Test objects handler smoke

Legacy source:
- [test_test_objects.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/test_self/test_test_objects.py)

Legacy behaviors:
- `TestObjectsHandler` initializes
- `get_rand_test_cover_path()` runs without error

Current alpha seams:
- [objects.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/_legacy/objects.py)
- [test_resources_manager_assets_test.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_resources_manager_assets_test.py)

Decision:
- `rewrite`

Rewrite target:
- keep most asset-provision coverage on the resource-manager path
- if `TestObjectsHandler` remains a live helper, add one small direct smoke test near `tests/support/test_databases/_legacy`

Reason:
- the old file is too narrow and too legacy-shaped
- but the helper still exists and is still imported in alpha support code

### 5. Dynamic unittest loader tools

Legacy source:
- [test_processing_tools.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/test_utils/test_processing_tools.py)

Legacy behaviors:
- discover Python modules in a folder
- dynamically build/load unittest suites

Decision:
- `retire`

Reason:
- this is old test-runner infrastructure, not product behavior
- alpha uses `pytest`, not the old dynamic unittest discovery path

### 6. Test utility helper module

Legacy source:
- [test_utils.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/test_utils/test_utils.py)

Legacy contents:
- `BuildScratchTree`
- `BasicMetadataFramework`
- `DatabaseValidator`
- assorted diagnostic helpers

Current alpha seams:
- [tools.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/_legacy/tools.py)
- [objects.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/_legacy/objects.py)

Decision:
- `retire`

Reason:
- this file is mostly support code, not a real collected test module
- the still-useful helper classes have already been salvaged into `_legacy`
- no value in preserving the old test-file shape

## Recommended Order

1. Add a direct collected test for `generate_test_tree(...)`.
2. Decide whether `TestObjectsHandler` gets a tiny direct smoke test or is covered only through higher-level resource-manager tests.
3. Add a focused test for `replace_in_folder_store_path(...)` if that macro still matters.
4. Retire the two old unittest-loader/helper files in the manifest.

## Immediate Review Result

- The old blanket “rewrite all six harness files” classification was too coarse.
- Updated split should be:
  - `rewrite`: `4`
  - `retire`: `2`
- This keeps only the genuinely live behaviors in the active migration stream.

## Implemented Now

- Direct collected tree-generator coverage landed in:
  - [test_tree_generators.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/test_tree_generators.py)
  - shared helper extraction:
    - [\_tree_generators.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/_tree_generators.py)
- `TestObjectsHandler` smoke coverage landed in:
  - [test_legacy_objects_smoke.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/test_legacy_objects_smoke.py)
- Focused macro coverage landed in:
  - [test_macros_folder_store_path.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/api/test_macros_folder_store_path.py)

Small supporting fixes were required:
- tree generators now accept normal Python 3 iterators instead of only Py2-style `.next()`
- tree generators now resolve live `*_parent_id` / `*_parent_position` style columns from the current schema
- legacy object copying now works for binary cover files because `file_hasher(...)` reads bytes, not text
- `TestObjectsHandler(..., scratch_file_handler=None)` now uses a local ephemeral scratch-folder fallback

Validation:
- targeted replacement slice:
  - `tests/support/test_databases/test_tree_generators.py`
  - `tests/support/test_databases/test_legacy_objects_smoke.py`
  - `tests/databases/api/test_macros_folder_store_path.py`
  - `5 passed`
- adjacent sanity slice:
  - `tests/support/test_resources_manager_assets_test.py`
  - `tests/databases/api/test_macros_api_signature_parity.py`
  - plus the three files above
  - `8 passed`


## Final Outcome

The seam is now closed.

Final row split:
- `covered`: `5`
- `retire`: `2`
- `rewrite`: `0`

Covered rows:
- `src/LiuXin_tests/test_objects.py`
- `src/LiuXin_tests/test_self/folder_stores/test_fsm_loader.py`
- `src/LiuXin_tests/test_self/folder_stores/test_fsms.py`
- `src/LiuXin_tests/test_self/test_make_test_databases.py`
- `src/LiuXin_tests/test_self/test_test_objects.py`

Retired rows:
- `src/LiuXin_tests/liuxin_base_test.py`
- `src/LiuXin_tests/test_fsms/test_fsm_properties.py`

The direct alpha evidence for closure is now:
- `tests/support/test_databases/test_tree_generators.py`
- `tests/support/test_databases/test_legacy_objects_smoke.py`
- `tests/databases/api/test_macros_folder_store_path.py`
- `tests/support/test_resources_manager_assets_test.py`
