# DB Property Secondary UUID Cluster

Date: 2026-03-16

## Scope

- Reviewed the remaining legacy DB-property rows:
  - `test_db_18_properties.py`
  - `test_db_19_properties.py`
  - `test_db_21_properties.py`
- Added direct guardrail coverage in:
  - [test_property_profiled_fixtures_do_not_materialize_legacy_secondary_uuid_cluster.py](../tests/support/test_databases/test_db_properties/test_property_profiled_fixtures_do_not_materialize_legacy_secondary_uuid_cluster.py)

## Finding

- These rows are not honest `salvage_existing` candidates anymore.
- The current alpha resource-manager pipeline does not provision the old specialized builders for these names.
- Active fixtures for `test_db_18`, `19`, and `21` are generic profiled FRBR-native DBs.

Observed live absence:

- `secondary_uuids`
- `books_secondary_uuid`
- `loc_shelf_numbers`
- `content_levels`
- `secondary_uuid_title_links`
- `book_books_secondary_uuid_links`
- `loc_shelf_number_title_links`
- `content_level_title_links`

## Decision

- Reclassify:
  - `test_db_18_properties.py`
  - `test_db_19_properties.py`
  - `test_db_21_properties.py`
- from `salvage_existing`
- to `rewrite`

Reason:

- recovering the old behavior now requires explicit replacement builders/tests
- support-tree normalization alone cannot restore semantics that are no longer in the live provisioning path

## Validation

- targeted guardrail:
  - `tests/support/test_databases/test_db_properties/test_property_profiled_fixtures_do_not_materialize_legacy_secondary_uuid_cluster.py`
  - `3 passed`
- combined DB-property support slice:
  - `74 passed`

## Manifest Effect

- repaired working manifest now records:
  - `covered`: `76`
  - `salvage_existing`: `0`
  - `rewrite`: `16`
  - `retire`: `16`
  - `integration_frozen`: `9`
  - `vendor_frozen`: `7`

## Next Best Salvage Target

- `large rich-content maps`
  - `test_db_4`
  - `test_db_10`
- or
- `legacy custom-column / trigger maps`
  - `test_db_6`
  - `test_db_22`
  - `test_db_23`
  - `test_db_24`
  - `test_db_25`

Reason:

- the compatibility-projection and rich-content families have now moved out to `rewrite`
- there is no remaining DB-property salvage set
- the remaining legacy work here is explicit rewrite, not salvage
