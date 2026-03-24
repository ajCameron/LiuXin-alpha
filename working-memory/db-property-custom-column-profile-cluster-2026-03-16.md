# DB Property Custom-Column Profile Cluster

Date: 2026-03-16

## Scope

- Reviewed the last custom-column-style salvage rows:
  - `test_db_6_properties.py`
  - `test_db_22_properties.py`
  - `test_db_23_properties.py`
  - `test_db_24_properties.py`
  - `test_db_25_properties.py`
- Added direct guardrail coverage in:
  - [test_property_empty_custom_column_profiles.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/test_db_properties/test_property_empty_custom_column_profiles.py)

## Finding

- These rows no longer represent live custom-column-rich support fixtures.
- Current alpha provisioned DBs for these names expose:
  - `custom_columns` as an empty table
  - no `custom_column_defs`
  - no `custom_column_links`
  - no `custom_columns_v`
  - generated `titles` / `books` compatibility views over generic profiled DBs

So the old huge trigger/table/value inventories are gone from the live provisioning path.

## Decision

- Reclassify:
  - `test_db_6_properties.py`
  - `test_db_22_properties.py`
  - `test_db_23_properties.py`
  - `test_db_24_properties.py`
  - `test_db_25_properties.py`
- from `salvage_existing`
- to `covered`

Reason:

- the current fixture profile is now pinned by the new empty-profile guardrail
- the actual current custom-column semantics are already covered on active alpha seams:
  - [test_calibre_emulation_d1_custom_columns_introspection.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/database_calibre_emultation/test_calibre_emulation_d1_custom_columns_introspection.py)
  - [test_calibre_emulation_d2_custom_values.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py)
  - [test_calibre_cache_06_custom_column_semantics.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py)
- that is a better replacement than pretending the removed old fixture inventories are still salvageable

## Validation

- targeted guardrail:
  - `tests/support/test_databases/test_db_properties/test_property_empty_custom_column_profiles.py`
  - `5 passed`
- combined DB-property support slice:
  - `87 passed`

## Manifest Effect

- repaired working manifest now records:
  - `covered`: `76`
  - `salvage_existing`: `0`
  - `rewrite`: `16`
  - `retire`: `16`
  - `integration_frozen`: `9`
  - `vendor_frozen`: `7`

## Practical Effect

- the custom-column-style DB-property rows are no longer a salvage backlog
- current alpha custom-column behavior is now represented at active seams, while the profiled support DBs are explicitly pinned as empty custom-column fixtures
