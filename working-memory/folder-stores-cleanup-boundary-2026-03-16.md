# Folder Stores Cleanup Boundary

Date: 2026-03-16

## Purpose

- Record what was actually safe to delete from the duplicate legacy `folder_stores` support footprint.
- Make the cutoff explicit so later duplicate-tree cleanup is not blocked on already-settled baggage.

## What Changed

- The resource-manager fixture locator now treats the support-tree CSV bundle as authoritative:
  - [test_resources_manager.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_resources_manager.py)
  - `_bundled_test_db_1_csv_dir()` now resolves `tests/support/test_databases/test_db_1`
- Deleted duplicate legacy-only artifacts from `src/LiuXin_tests`:
  - [folder_stores.csv](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/test_databases/test_db_0/folder_stores.csv)
  - [folder_stores.csv](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/test_databases/test_db_1/folder_stores.csv)
  - [build_test_fsms.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/test_setup/build_test_fsms.py)

## Why This Was Safe

- The duplicate `folder_stores.csv` files in `src/LiuXin_tests/...` and `tests/support/test_databases/...` were identical.
- The only live alpha code path still pointing at the legacy `src/LiuXin_tests/test_db_1` fixture bundle was the resource-manager helper.
- After repointing that helper, the deleted files were no longer on any active alpha path.
- [build_test_fsms.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/test_setup/build_test_fsms.py) had no live references in alpha and represented only the retired `folder_stores` build entrypoint.

## Validation

- `PYTHONPATH=src:. .venv/bin/python -m py_compile tests/support/test_resources_manager.py`
- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/databases/test_test_resources_manager.py -k 'lists_default_dbs or test_db_2_generates_and_is_pruned or provisioned_profiles_do_not_materialize_legacy_folder_stores'`
  - `4 passed, 32 deselected`

## Practical Meaning

- The first real duplicate `src/LiuXin_tests` support artifacts are now gone, not merely marked for future deletion.
- The remaining duplicate-tree cleanup can proceed slice-by-slice as each legacy seam is either:
  - promoted into `tests/support/...`, or
  - retired outright.
- `folder_stores` no longer needs special-case protection at the duplicate-fixture level.
