# DB Property Compatibility Projection Cluster

Date: 2026-03-16

## Scope

- Reviewed the remaining legacy rows:
  - `test_db_1_properties.py`
  - `test_db_14_properties.py`
  - `test_db_15_properties.py`
  - `test_db_16_properties.py`
  - `test_db_17_properties.py`
- Added direct guardrail coverage in:
  - [test_property_compatibility_projection_profiles.py](../tests/support/test_databases/test_db_properties/test_property_compatibility_projection_profiles.py)

## Finding

- These rows are not honest `salvage_existing` candidates anymore.
- The current alpha resource-manager pipeline provisions generic profiled FRBR-native fixtures for these names.
- Those fixtures do expose deliberate compatibility views:
  - `titles`
  - `books`
- But they do **not** expose the old richer compatibility shape:
  - no `creators` table/view in the legacy shape
  - no author-name projection fields like `title_authors` / `book_authors`
  - no populated comments or identifier tables

Current live compatibility contract:

- `titles` and `books` exist as views
- `title_creator_sort` is `NULL`
- `title_last_modified` is present and stable within a DB
- `book_uuid == book_id`
- `book_last_modified` is present and stable within a DB
- `comments`, `entity_identifiers`, and `item_identifiers` are all empty

## Decision

- Reclassify:
  - `test_db_1_properties.py`
  - `test_db_14_properties.py`
  - `test_db_15_properties.py`
  - `test_db_16_properties.py`
  - `test_db_17_properties.py`
- from `salvage_existing`
- to `rewrite`

Reason:

- the old author/UUID/comment maps are no longer in the live provisioning path
- there is a real current compatibility-view seam, but it is much narrower than the original legacy builder output
- replacement now means explicit new builders/tests, not support-tree normalization

## Validation

- targeted guardrail:
  - `tests/support/test_databases/test_db_properties/test_property_compatibility_projection_profiles.py`
  - `5 passed`
- combined DB-property support slice:
  - `80 passed`

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

Practical conclusion:

- the DB-property salvage backlog is now closed
