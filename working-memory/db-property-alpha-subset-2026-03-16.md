# DB Property Alpha Subset

Date: 2026-03-16

## Scope

- Alpha-native live-schema normalization for the legacy DB-property support corpus.
- This now spans all `26` support classes under:
  - [test_db_properties](../tests/support/test_databases/test_db_properties)

## What Landed

- Shared alpha-facing live-schema declarations in [common_db_properties.py](../tests/support/test_databases/test_db_properties/common_db_properties.py):
  - `alpha_focus_tables`
  - `alpha_focus_table_columns`
  - `alpha_database_version_required_substrings`
- Per-DB row-count declarations:
  - `alpha_focus_row_counts`
- Collected live-schema contract:
  - [test_property_alpha_schema_subset.py](../tests/support/test_databases/test_db_properties/test_property_alpha_schema_subset.py)

## Contract Shape

The alpha subset currently pins:

- `database_version`
- `works`
- `series`
- `expressions`
- `manifestations`
- `items`
- `files`
- `agents`
- `labels`

It asserts for every `test_db_0 .. test_db_25`:

- those tables exist in provisioned DBs
- their column order matches the current alpha schema
- per-DB row counts match the provisioned live database
- `database_version.database_version_version` contains the expected version-string components

## Why This Shape

- The legacy `theo_*` declarations are still largely pre-FRBR/WEMI and still mention tables like `titles`, `books`, and `folder_stores`.
- Overwriting them wholesale would blur the migration state.
- The `alpha_*` declarations let collected alpha tests pin the current schema without pretending the old declarations are already migrated.

## Validation

- `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/support/test_databases/test_db_properties/test_property_alpha_schema_subset.py tests/support/test_databases/test_db_properties/test_property_support_registry.py`
  - `58 passed`

## Status

- All `26` support classes now participate in the live alpha subset.
- This does not by itself mean all `26` legacy rows are fully migrated.
- It does mean the support corpus is no longer inert: every named DB now has a collected alpha schema/count contract.
- The remaining decision is status split:
  - minimal legacy property files can move to `covered`
  - larger semantic property files remain `salvage_existing` until their unique old assertions are either replaced or retired
