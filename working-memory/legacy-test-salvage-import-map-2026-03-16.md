# Legacy Test Salvage Import Map

Date: 2026-03-16

## Outputs

- detailed CSV: [legacy-test-salvage-import-rewrite-map-2026-03-16.csv](legacy-test-salvage-import-rewrite-map-2026-03-16.csv)

## Scope

- Imported namespace audit for:
  - [tests/support/test_databases](../tests/support/test_databases)
- Focus:
  - all remaining `LiuXin_tests...` imports that keep the support tree coupled to the duplicate legacy tree

## Totals

- initial files affected: `54`
- initial import statements mapped: `117`

## Progress

- completed:
  - all `relative_property_import` rewrites in `tests/support/test_databases/test_db_properties`
- completed:
  - all `relative_parent_import` rewrites
  - all `relative_sibling_builder_import` rewrites
  - all helper imports now point at local `_legacy` shims
  - `test_db_1/__main__.py` now imports `build_test_db` from the same package
- validation:
  - no remaining `from LiuXin_tests.test_databases.test_db_properties...` imports in that package
  - `python3 -m py_compile tests/support/test_databases/test_db_properties/*.py` passed
  - `find tests/support/test_databases -name '*.py' -print0 | xargs -0 python3 -m py_compile` passed
  - direct file-level imports of:
    - `_legacy/tools.py`
    - `_legacy/objects.py`
    - `_legacy/setup_constants.py`
    passed
- current support-tree coupling to `LiuXin_tests`:
  - `0` files
  - `0` import statements
- package-root import blocker resolved:
  - `tests.support.test_databases` now imports successfully with the new `liuxin_clint` wrapper
- current remaining blocker:
  - deeper builder imports now trip over missing/renamed project surfaces, with `LiuXin_alpha.folder_stores` currently the first one

## Rewrite Classes

- `relative_property_import`: `55`
  - property package should import itself locally
  - examples:
    - `LiuXin_tests.test_databases.test_db_properties.common_db_properties`
    - `LiuXin_tests.test_databases.test_db_properties.test_db_22_properties`
- `relative_parent_import`: `26`
  - builder packages should import from the parent support package
  - example:
    - `LiuXin_tests.test_databases -> from .. import TestDatabaseBuilder`
- `relative_sibling_builder_import`: `14`
  - builder packages should import sibling DB-builder modules locally
  - examples:
    - `...test_db_4`
    - `...test_db_19`
    - `...test_db_23`
- `extract_legacy_constants_helper`: `12`
- `extract_legacy_tools_helper`: `5`
- `extract_legacy_objects_helper`: `2`
- `extract_legacy_setup_helper`: `2`
- `local_builder_entrypoint_rewrite`: `1`

## Remaining Rewrite Classes After Property Pass

- none; this import-rewrite queue is complete

## What This Means

Most of the normalization is not conceptually hard.

The bulk of the work is:
- localizing imports that already point at duplicated code now present under `tests/support/test_databases`

The real design work is much smaller:
- choose where the still-missing legacy helper shims live

## Recommended Helper Targets

Keep the debt visible and local to the DB-support world.

Recommended extraction package:
- `tests/support/test_databases/_legacy/`

Recommended modules:
- `tests/support/test_databases/_legacy/constants.py`
  - subset only:
    - `test_uuids`
    - `rand_ints`
    - `extended_rand_ints`
    - `rand_size_ints`
    - `rand_cent_ints`
    - `rand_decade_ints`
    - `rand_quad_ints`
    - `rand_names_list`
- `tests/support/test_databases/_legacy/tools.py`
  - subset only:
    - `BasicMetadataFramework`
    - `DatabaseValidator`
- `tests/support/test_databases/_legacy/objects.py`
  - narrow `TestObjectsHandler` shim or rewrite
- `tests/support/test_databases/_legacy/setup_constants.py`
  - only:
    - `test_asset_version`

Reason:
- these helpers are only needed by the legacy DB-support builders
- putting them under `_legacy` avoids pretending they are clean modern shared test infrastructure
- it also avoids scattering the migration debt across unrelated `tests/support` locations

## Specific Decision On `test_db_1/__main__.py`

- current import:
  - `from LiuXin_tests.test_data.test_db_1 import build_test_db`
- recommended rewrite:
  - import `build_test_db` from the same package
- reason:
  - [test_db_1/__init__.py](../tests/support/test_databases/test_db_1/__init__.py) already defines `build_test_db`
  - this is a stale namespace reference, not a missing helper

## Important Constraint

- [tests/test_constants.py](../tests/test_constants.py) is unrelated
- do not reuse it for the legacy DB-support constants
- its purpose is testing the main constants module, not hosting support data

## Execution Order

1. Decide whether the legacy `clint` dependency in `tests/support/test_databases/__init__.py` should be:
   - removed
   - shimmed
   - or treated as an explicit frozen dependency
2. Review the `13` divergent files between:
   - `tests/support/test_databases`
   - `src/LiuXin_tests/test_databases`
3. Once that review is complete, archive or remove the duplicate `src/LiuXin_tests/test_databases` tree.

## Practical Deliverable

The CSV gives the file-by-file rewrite queue with:
- source file
- line number
- legacy module
- imported names
- rewrite class
- proposed replacement target
- notes
