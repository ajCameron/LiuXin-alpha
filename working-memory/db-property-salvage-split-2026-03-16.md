# DB Property Salvage Split

Date: 2026-03-16

## Decision

- The DB-property corpus is no longer one undifferentiated `salvage_existing` bucket.
- After the live alpha subset was extended across all `26` support classes, the minimal legacy property files were reviewed separately from the large semantic ones.

## Reclassified To `covered`

These original rows are now `covered`:

- `test_db_0_properties.py`
- `test_db_2_properties.py`
- `test_db_3_properties.py`
- `test_db_5_properties.py`
- `test_db_7_properties.py`
- `test_db_8_properties.py`
- `test_db_9_properties.py`
- `test_db_11_properties.py`
- `test_db_12_properties.py`
- `test_db_13_properties.py`

Reason:

- their remaining legacy signal was small
- the collected alpha registry plus live schema/count validation now subsume that signal more honestly than the old declarations did

## Left In `salvage_existing`

These remain `salvage_existing`:

- `test_db_1_properties.py`
- `test_db_4_properties.py`
- `test_db_6_properties.py`
- `test_db_10_properties.py`
- `test_db_14_properties.py`
- `test_db_15_properties.py`
- `test_db_16_properties.py`
- `test_db_17_properties.py`
- `test_db_18_properties.py`
- `test_db_19_properties.py`
- `test_db_20_properties.py`
- `test_db_21_properties.py`
- `test_db_22_properties.py`
- `test_db_23_properties.py`
- `test_db_24_properties.py`
- `test_db_25_properties.py`

Reason:

- they still carry unique old semantics not yet replaced by the current alpha-native tests
- examples include:
  - large value maps
  - identifier semantics
  - trigger inventories
  - old per-entity content expectations
  - large stale `theo_tables_and_columns` or `theo_*_main_tables` blocks

## Manifest Effect

- `covered`: `63`
- `salvage_existing`: `16`

The remaining salvage work is now explicit and bounded instead of hidden inside one `26`-row bucket.

## Follow-on Seam

- [db-property-blank-optional-metadata-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/db-property-blank-optional-metadata-2026-03-16.md)

That follow-on pass added a real alpha-native contract for the current blank optional-metadata fixture profile across `13` of the remaining DBs, but it did **not** justify any more row moves.

Reason:

- the live profile is now pinned
- the original legacy rows still contain stale author/UUID/identifier/trigger/custom-column semantics that have not been honestly replaced yet
