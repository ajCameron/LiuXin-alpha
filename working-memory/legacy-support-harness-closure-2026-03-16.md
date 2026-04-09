# Legacy Support Harness Closure

Date: 2026-03-16

## Summary

The `legacy_support_harness` seam is now closed.

Manifest effect:
- `covered`: `53`
- `rewrite`: `5`
- `retire`: `24`
- `salvage_existing`: `26`
- `integration_frozen`: `9`
- `vendor_frozen`: `7`

Remaining rewrite backlog is now only:
- `core_xmlrpc_compat`: `1`
- `folder_store_runtime`: `4`

## Row Outcomes

### Covered

- `src/LiuXin_tests/test_objects.py`
  - helper behavior lives on in `tests/support/test_databases/_legacy/objects.py`
  - direct smoke now covers scratch cover copying and named metadata-file copying
- `src/LiuXin_tests/test_self/folder_stores/test_fsm_loader.py`
  - surviving reproducible scratch asset/database provisioning is covered by the resource-manager tests
  - old import-cache and folder-store path assertions are retired with the runtime
- `src/LiuXin_tests/test_self/folder_stores/test_fsms.py`
  - the only live behavior worth keeping was `folder_store_path` generalize/specialize
  - that is now covered by `tests/databases/api/test_macros_folder_store_path.py`
- `src/LiuXin_tests/test_self/test_make_test_databases.py`
  - replaced by direct collected tree-generator coverage in `tests/support/test_databases/test_tree_generators.py`
- `src/LiuXin_tests/test_self/test_test_objects.py`
  - replaced by direct `TestObjectsHandler` smoke in `tests/support/test_databases/test_legacy_objects_smoke.py`

### Retired

- `src/LiuXin_tests/liuxin_base_test.py`
  - old unittest base, banners, ramdisk cleanup, and assertion helpers
  - no live alpha code references this class shape
- `src/LiuXin_tests/test_fsms/test_fsm_properties.py`
  - hard-coded expectations for the removed folder-store-manager runtime
  - no honest alpha compatibility target remains

## Validation

- `tests/support/test_databases/test_legacy_objects_smoke.py`
- `tests/support/test_databases/test_tree_generators.py`
- `tests/databases/api/test_macros_folder_store_path.py`
- `tests/support/test_resources_manager_assets_test.py`
- result: `8 passed`

## Next Step

Take the remaining `rewrite: 5` set as two separate streams:
1. `core_xmlrpc_compat`
2. `folder_store_runtime`
