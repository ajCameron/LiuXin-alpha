# DB Property Identifier Cluster

Date: 2026-03-16

## Scope

- Reviewed the remaining legacy identifier row:
  - `test_db_20_properties.py`
- Added direct guardrail coverage in:
  - [test_property_identifier_profiled_fixture_is_empty.py](../tests/support/test_databases/test_db_properties/test_property_identifier_profiled_fixture_is_empty.py)

## Finding

- `test_db_20` is not an honest `salvage_existing` row anymore.
- The current alpha resource-manager pipeline provisions a generic profiled FRBR-native fixture for this name.
- That live fixture has:
  - `entity_identifiers = 0`
  - `item_identifiers = 0`
  - `identifiers_v = 0`
  - `identifiers = 0`
- It does not materialize the old legacy table:
  - `identifier_title_links`

## Decision

- Reclassify:
  - `test_db_20_properties.py`
- from `salvage_existing`
- to `rewrite`

Reason:

- the old identifier maps and title-link semantics are not present in the live provisioning path
- replacement now means explicit new builders/tests around current identifier seams, not support-tree normalization

## Validation

- targeted guardrail:
  - `tests/support/test_databases/test_db_properties/test_property_identifier_profiled_fixture_is_empty.py`
  - `1 passed`
- combined DB-property support slice:
  - `75 passed`

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

- `identifier` is no longer a salvage seam
- the compatibility-projection and rich-content families have now moved out to `rewrite`
- there is no remaining DB-property salvage set
