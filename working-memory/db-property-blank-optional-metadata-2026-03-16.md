# DB Property Blank Optional Metadata

Date: 2026-03-16

## What Landed

- Added a collected alpha-native contract:
  - [test_property_blank_optional_metadata_profiles.py](../tests/support/test_databases/test_db_properties/test_property_blank_optional_metadata_profiles.py)

## Scope

The test pins the current support-fixture profile for:

- `test_db_1`
- `test_db_4`
- `test_db_6`
- `test_db_10`
- `test_db_14`
- `test_db_15`
- `test_db_16`
- `test_db_20`
- `test_db_21`
- `test_db_22`
- `test_db_23`
- `test_db_24`
- `test_db_25`

## Contract

For those provisioned DBs, alpha now explicitly checks:

- `human_agents = 0`
- `notes = 0`
- `comments = 0`
- `synopses = 0`
- `annotations = 0`
- `entity_identifiers = 0`
- `item_identifiers = 0`
- `agents` contains only the placeholder null organisation row:
  - `(0, "organisation", "DELIBERATELY SET NULL")`

## Why This Matters

- It replaces one real seam from the old property files:
  - these fixtures currently behave as deliberately blank optional-metadata profiles
- It gives the support DBs a live alpha-native contract instead of leaving that shape implicit

## What It Does Not Claim

- It did **not** by itself justify moving the final legacy DB-property salvage rows to `covered`.
- It also does **not** justify pulling the `3` `secondary_uuid / content_level / loc_shelf` rows back out of `rewrite`.
- The old rows still carry stale or unreplaced legacy semantics:
  - author maps
  - UUID expectations
  - identifier maps
  - trigger inventories
  - custom-column data maps

So this is a real replacement seam, but not a false `covered` claim for those rows.
