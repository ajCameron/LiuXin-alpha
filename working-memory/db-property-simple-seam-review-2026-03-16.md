# DB Property Simple Seam Review

Date: 2026-03-16

## Scope

- Review of the smallest remaining `salvage_existing` DB-property rows after the full alpha subset landed.
- Files inspected:
  - `test_db_10_properties.py`
  - `test_db_13_properties.py`
  - `test_db_14_properties.py`
  - `test_db_15_properties.py`
  - `test_db_16_properties.py`
  - `test_db_20_properties.py`

## Result

- Only `test_db_13_properties.py` is now honestly `covered`.

Reason:

- Its only meaningful surviving signal was effectively:
  - empty title/work set
- The collected alpha schema/count contract already pins that:
  - `works=0`
  - `expressions=0`
  - `manifestations=0`
  - `items=0`

## Why The Others Stayed `salvage_existing`

- `test_db_10_properties.py`
  - legacy counts and `series_41_val` are clearly stale against the current provisioned DB
- `test_db_14_properties.py`
  - old author/UUID/title semantics do not line up cleanly with the current provisioned DB
- `test_db_15_properties.py`
  - old author/comment semantics are not replaced yet by current alpha-native tests
- `test_db_16_properties.py`
  - the old comment-count expectations conflict with the current provisioned DB
- `test_db_20_properties.py`
  - the old identifier map is stale against the current provisioned DB
  - current identifier tables are `entity_identifiers` and `item_identifiers`
  - both are empty in the current provisioned `test_db_20`

## Status

- This seam does not justify a bulk reclassification.
- It justifies one more move:
  - `test_db_13_properties.py` -> `covered`

The remaining large rows need seam-specific replacement tests, not optimism.
