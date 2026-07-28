# DB Property Support Registry

Date: 2026-03-16

## Summary

First promotion slice for the `salvage_existing` DB-property corpus is now in place.

Added:
- support registry in [tests/support/test_databases/test_db_properties/__init__.py](../tests/support/test_databases/test_db_properties/__init__.py)
- collected contract in [test_property_support_registry.py](../tests/support/test_databases/test_db_properties/test_property_support_registry.py)

Validation:
- `tests/support/test_databases/test_db_properties/test_property_support_registry.py`
- result: `32 passed`

## What This Covers

- all `26` legacy support property classes are now discoverable from one registry
- registry names match the full `test_db_0 .. test_db_25` range
- each class is structurally sane:
  - subclass of `CommonDBProperties`
  - consistent naming
  - sane `theo_db_main_tables` / `theo_main_tables` / `theo_tables_and_columns` containers when declared
- registry names are wired to the live resource manager
- representative DBs (`test_db_0`, `test_db_1`, `test_db_19`, `test_db_25`) provision successfully against live resources

## What This Does Not Claim

This does **not** mean the old per-DB property snapshots are fully normalized.

The old DB-facing declarations are still substantially stale relative to the current alpha schema:
- old `titles` / `books` / `folder_stores` naming vs current FRBR/WEMI tables such as `works`
- many old `theo_tables_and_columns` maps no longer match live alpha schemas exactly

So the `26` rows remain `salvage_existing` for now.

## Next Sensible Step

Take a selective normalization slice rather than a blanket port.

Best target:
1. choose a small subset of still-meaningful declarations from the support classes
2. normalize them onto the current alpha schema
3. move only the rows that become honestly live into `covered`
