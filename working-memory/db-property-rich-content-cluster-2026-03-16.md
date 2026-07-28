# DB Property Rich-Content Cluster

Date: 2026-03-16

## Scope

- Reviewed the last rich-content salvage rows:
  - `test_db_4_properties.py`
  - `test_db_10_properties.py`
- Added direct guardrail coverage in:
  - [test_property_rich_content_profiles_are_generic.py](../tests/support/test_databases/test_db_properties/test_property_rich_content_profiles_are_generic.py)

## Finding

- These rows are not honest `salvage_existing` candidates anymore.
- The current alpha resource-manager pipeline provisions generic profiled FRBR-native fixtures for both names.
- Those fixtures expose:
  - generated `titles` and `books` compatibility views
  - blank optional metadata tables
- They do not expose the old rich synthetic-content builder output:
  - no `creators` table/view
  - no populated comments/notes/synopses/annotations
  - no populated human or org agent rows
  - no rich work-title maps in the base `works` table

Current live contract pinned here:

- `works.work_title` is entirely `NULL`
- `titles.title` is generated as `<db_name> title NNN`
- `titles` and `books` counts match `works`
- rich-content side tables stay empty

## Decision

- Reclassify:
  - `test_db_4_properties.py`
  - `test_db_10_properties.py`
- from `salvage_existing`
- to `rewrite`

Reason:

- the old large author/comment/series/value maps are no longer in the live provisioning path
- replacement now means explicit new builders/tests if that rich synthetic-content seam still matters
- support-tree normalization cannot honestly recover those old declarations

## Validation

- targeted guardrail:
  - `tests/support/test_databases/test_db_properties/test_property_rich_content_profiles_are_generic.py`
  - `2 passed`
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

- there is no remaining DB-property salvage backlog
- the only remaining legacy DB-property work is explicit `rewrite`, not normalization
