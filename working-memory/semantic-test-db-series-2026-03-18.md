# Semantic Test DB Series

Date: 2026-03-18

## What Landed

Alpha-native semantic fixture families are now live through the shared test
resource manager, including second-shape `_db_1` variants where they were
useful and the first dedicated relation-stress and weird-data fixtures.

Live names:

- `metadata_rich_db_0`
- `metadata_rich_db_1`
- `stores_assets_db_0`
- `stores_assets_db_1`
- `images_covers_db_0`
- `images_covers_db_1`
- `custom_columns_populated_db_0`
- `custom_columns_populated_db_1`
- `identifiers_db_0`
- `identifiers_db_1`
- `pathological_relations_db_0`
- `weird_data_db_0`

These are imported builder modules under:

- [metadata_rich_db_0.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/metadata_rich_db_0.py)
- [metadata_rich_db_1.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/metadata_rich_db_1.py)
- [stores_assets_db_0.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/stores_assets_db_0.py)
- [stores_assets_db_1.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/stores_assets_db_1.py)
- [images_covers_db_0.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/images_covers_db_0.py)
- [images_covers_db_1.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/images_covers_db_1.py)
- [custom_columns_populated_db_0.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/custom_columns_populated_db_0.py)
- [custom_columns_populated_db_1.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/custom_columns_populated_db_1.py)
- [identifiers_db_0.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/identifiers_db_0.py)
- [identifiers_db_1.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/identifiers_db_1.py)
- [pathological_relations_db_0.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/pathological_relations_db_0.py)
- [weird_data_db_0.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/weird_data_db_0.py)

## Resource-Manager Change

The imported-builder path in
[tests/support/test_resources_manager.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_resources_manager.py)
was fixed in two ways:

- default prefixes now prefer `tests.support.test_databases`
- imported modules only count as available/provisionable if they expose a
  supported modern entrypoint:
  - `populate_bundle(...)`
  - `build(...)`
  - `build_database(...)`
  - `build_test_database(...)`

This matters because the support tree contains helper modules and legacy
packages that import successfully but should not shadow the built-in profiled
fixtures.

## `metadata_rich_db_0`

Purpose:

- realistic optional metadata for browse/search/detail work
- reusable named DB for interface-facing metadata realism

Current shape:

- `3` books / works
- `4` `human_agents`
- `1` `org_agents`
- `7` `agent_work_links`
- `4` labels with `5` label-work links
- `2` notes
- `2` comments
- `2` synopses
- `2` annotations
- `3` work identifiers
- `3` item identifiers
- non-trivial subject / series / language link rows

This DB is intentionally small but semantically dense.

## `metadata_rich_db_1`

Purpose:

- second metadata-rich shape with denser optional metadata
- multilingual titles and wider link fanout

Current shape:

- `4` books / works
- `5` `human_agents`
- `1` `org_agents`
- `10` `agent_work_links`
- `5` labels with `7` label-work links
- `3` notes
- `3` comments
- `3` synopses
- `3` annotations
- `4` entity identifiers
- `4` item identifiers
- denser subject / series / language link rows than `_0`

## `stores_assets_db_0`

Purpose:

- current store-backed asset semantics
- real local files for download/retrieval paths

Current shape:

- `2` books / works
- `1` store
- `2` folders
- `2` ebook files
- `2` linked images
- all asset rows point at real files inside the provisioned bundle
- bundle token rewriting rewrites:
  - `stores.store_root_uri`
  - `files.file_original_path`
  - `images.image_original_path`
  - `items.item_source_path`

Store details:

- `store_kind = on_disk_existing_unmanaged_drive`
- `store_access_protocol = file`
- file retrieval works through the live storage manager, not just direct path
  reads

## `stores_assets_db_1`

Purpose:

- multi-store shape for retrieval and path-rewrite coverage
- real assets spread across more than one provisioned store root

Current shape:

- `3` books / works
- `2` stores
- `4` folders
- `4` ebook files
- `3` linked images
- all asset rows point at real files inside the provisioned bundle
- retrieval is exercised across both store roots

## `images_covers_db_0`

Purpose:

- image/cover-heavy interface realism
- reusable named DB for cover resolution and acquisition tests

Current shape:

- `3` books / works
- `1` store
- `1` cover folder
- `5` images
- `5` image-work links
- real image assets inside the provisioned bundle
- mixed link types using the current allowed image-work vocabulary:
  - `cover`
  - `illustration`
  - `diagram`

This fixture is intentionally small, but it has multi-image works and real
store-backed image retrieval.

## `images_covers_db_1`

Purpose:

- second image/cover shape with variant link types and an intentional gap case
- cover-resolution behavior where not every work has an image

Current shape:

- `4` books / works
- `1` store
- `2` folders
- `6` images
- `6` image-work links
- mixed allowed image-work link types using:
  - `cover`
  - `illustration`
  - `diagram`
  - `map`
- one work intentionally has no linked images

## `custom_columns_populated_db_0`

Purpose:

- reusable named DB for populated custom-column cases
- current alpha custom-column schema generation on a real FRBR-native table

Current shape:

- `3` books / works
- `4` custom columns on `works`
  - `facet_tags` (`text`, multiple)
  - `editor_rating` (`rating`)
  - `featured_pick` (`bool`)
  - `staff_note` (`comments`)
- generated `custom_column_N` tables
- generated `works_custom_column_N_link` tables for normalized types
- deterministic seeded values for all four columns

Important constraint recorded here:

- creation uses the live `CustomColumns` API
- value seeding currently writes the generated tables directly
- reason: the current setter path is still biased toward the historical
  `books` seam

That is a real product seam, not hidden by the fixture.

## `custom_columns_populated_db_1`

Purpose:

- second populated custom-column shape with broader datatype coverage
- explicit series-style normalized values and scalar variants

Current shape:

- `4` books / works
- `5` custom columns on `works`
  - `curator_tags` (`text`, multiple)
  - `reading_order` (`series`)
  - `priority_score` (`float`)
  - `reference_flag` (`bool`)
  - `review_blob` (`comments`)
- deterministic generated-table content for normalized and scalar types

Implementation note:

- normalized value seeding now dedupes through the generated value table before
  inserting the link row
- this keeps repeated logical values honest against the live uniqueness rules

## `identifiers_db_0`

Purpose:

- entity/item identifier realism
- reusable named DB for identifier views and browse/detail work

Current shape:

- `3` books / works
- `7` entity identifiers across:
  - `work`
  - `expression`
  - `manifestation`
  - `item`
- `3` item identifiers
- deterministic `identifiers_v` / `identifiers` view content

This fixture deliberately uses the current alpha identifier model, not the
retired legacy identifier-link tables.

## `identifiers_db_1`

Purpose:

- wider entity/item identifier matrix than `_0`
- enough density to exercise grouped identifier views across more entity types

Current shape:

- `4` books / works
- `10` entity identifiers
- `5` item identifiers
- deterministic `identifiers_v` / `identifiers` view content across `work`,
  `expression`, `manifestation`, and `item`

## `pathological_relations_db_0`

Purpose:

- valid but awkward relation density for browse/search stress
- not a timing benchmark, but a correctness fixture with dense fanout

Current shape:

- `8` books / works
- `24` `agent_work_links`
- `24` `label_work_links`
- `16` `subject_work_links`
- dense shared labels and agent reuse across the corpus

## `weird_data_db_0`

Purpose:

- unicode-heavy metadata and odd-but-valid asset naming
- robustness fixture for interface and storage-facing code

Current shape:

- `3` books / works
- multilingual and symbol-heavy titles
- unicode file names and paths
- long-form note/comment/synopsis coverage
- `1` cover image asset with a unicode filename

## Validation

Targeted contracts landed in
[test_test_resources_manager.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/test_test_resources_manager.py):

- imported provider resolution for the new semantic DBs
- availability filtering for supported entrypoint modules only
- `metadata_rich_db_0` content contract
- `metadata_rich_db_1` dense metadata contract
- `stores_assets_db_0` store-root rewrite and live file retrieval contract
- `stores_assets_db_1` multi-store retrieval contract
- `images_covers_db_0` cover-link and live image retrieval contract
- `images_covers_db_1` variant-link and gap contract
- `custom_columns_populated_db_0` generated-schema and seeded-value contract
- `custom_columns_populated_db_1` series/scalar generated-schema contract
- `identifiers_db_0` identifier-view contract
- `identifiers_db_1` wider identifier-view contract
- `pathological_relations_db_0` dense relation-graph contract
- `weird_data_db_0` unicode/odd-path contract

Validation run:

- `tests/databases/test_test_resources_manager.py`
  - `52 passed`
- `tests/support/test_databases/test_db_properties/test_property_support_registry.py`
  - `32 passed`

## Naming Direction

The intended pattern is semantic family plus numeric series member:

- `metadata_rich_db_0`
- `metadata_rich_db_1`
- `stores_assets_db_0`
- `stores_assets_db_1`
- `images_covers_db_0`
- `images_covers_db_1`
- `custom_columns_populated_db_0`
- `custom_columns_populated_db_1`
- `identifiers_db_0`
- `identifiers_db_1`
- `pathological_relations_db_0`
- `weird_data_db_0`

That keeps the standard series semantic and extensible without reviving the
legacy numbered `test_db_*` meaning.

## Next Likely Steps

1. add `_db_2` members only where a third semantic shape is justified
2. add `compat_projection_db_0` only if we actually want to support that
   contract
3. start writing focused benchmark scripts against the semantic families rather
   than inventing more correctness fixtures by default
