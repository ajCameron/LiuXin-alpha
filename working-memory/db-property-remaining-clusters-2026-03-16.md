# DB Property Remaining Clusters

Date: 2026-03-16

## Scope

This note splits the remaining `16` `salvage_existing` DB-property rows into real semantic clusters.

Current manifest state:

- `covered`: `63`
- `salvage_existing`: `16`

## Remaining Rows

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

## Cluster 1: Author / UUID / Compatibility Projection

Rows:

- `test_db_1`
- `test_db_14`
- `test_db_15`
- `test_db_16`
- `test_db_17`

Main old signals:

- title/creator compatibility-view metadata
- `book_uuid`
- `title_last_modified`
- creator-sort / author projection
- small note/comment expectations in the truncated DBs

Current finding:

- `test_db_14`, `15`, and `16` are now clearly blank optional-metadata profiles in alpha.
- `test_db_1` still has compatibility views, but the old `title_author` / creator expectations are stale against the current projection.
- `test_db_17` is not a small compatibility case anymore; it is a large rich-content semantic map.

Status:

- partly replaced by:
  - [test_property_blank_optional_metadata_profiles.py](../tests/support/test_databases/test_db_properties/test_property_blank_optional_metadata_profiles.py)
- no additional rows can be honestly moved yet

## Cluster 2: Large Synthetic Rich-Content Maps

Rows:

- `test_db_4`
- `test_db_10`
- `test_db_17`

Main old signals:

- huge author/comment/series maps
- many per-title count/value assertions
- broad synthetic-content expectations from the old builders

Current finding:

- current alpha fixtures keep only the coarse FRBR/WEMI row-count/profile signal
- the giant legacy maps are not replaced yet

Status:

- still `salvage_existing`

## Cluster 3: Secondary UUID / Content Level / Shelf Number Fixtures

Rows:

- `test_db_18`
- `test_db_19`
- `test_db_21`

Main old signals:

- `secondary_uuids`
- `books_secondary_uuid`
- `loc_shelf_numbers`
- `content_levels`
- per-title link/value maps around those tables

Current finding:

- these are still coherent as a separate family
- they should be migrated together, not file-by-file

Status:

- still `salvage_existing`
- likely the next best true semantic salvage target

## Cluster 4: Identifier Semantics

Rows:

- `test_db_20`

Main old signals:

- `identifiers`
- `identifier_title_links`
- title-to-identifier maps
- ISBN/UUID grouping expectations

Current finding:

- current alpha provisioned `test_db_20` leaves `entity_identifiers` and `item_identifiers` empty
- the old identifier maps are stale and not yet replaced

Status:

- still `salvage_existing`
- should be handled as a dedicated identifier seam, not mixed with the other clusters

## Cluster 5: Legacy Custom-Column / Trigger Maps

Rows:

- `test_db_6`
- `test_db_22`
- `test_db_23`
- `test_db_24`
- `test_db_25`

Main old signals:

- `existing_triggers`
- `theo_tables_and_columns`
- custom-column table/link inventories
- custom-column value maps

Current finding:

- the current alpha fixtures expose the tables, but the old trigger/table/value inventories are not matched
- for `test_db_22..25`, current `custom_columns` rows are `0`
- these rows are much closer to current custom-column builder/cache seams than to the blank-metadata cluster

Status:

- still `salvage_existing`
- should be taken as one custom-column-support cluster

## Recommendation

Next semantic salvage target should be:

1. `secondary_uuid / content_level / loc_shelf` cluster (`test_db_18`, `19`, `21`)

Reason:

- smaller and cleaner than the giant rich-content maps
- more honest replacement seam than trying to force the stale author/comment rows
- more likely to produce real row moves than the empty custom-column fixtures
