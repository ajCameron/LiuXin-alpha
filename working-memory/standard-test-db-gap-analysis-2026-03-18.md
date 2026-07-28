# Standard Test DB Gap Analysis

Date: 2026-03-18

## Current State

The standard `test_db_*` series is thinner than the legacy names imply.

In practice the live alpha resource-manager path currently provides:

- `test_db_0`
  - minimal title/schema smoke
- `test_db_2`
  - single-book FRBR-native smoke fixture
- `test_db_3`
  - deterministic folder/file volume fixture
- `test_db_13`
  - blank schema fixture
- most of `test_db_1..25`
  - generic profiled FRBR-native fixtures that differ mainly by row counts

This was partly an intentional simplification, and partly an infrastructure gap:

- `ImportedModuleDatabaseProvider` is in the provider chain
  - [test_resources_manager.py](../tests/support/test_resources_manager.py#L294)
- and the default import prefixes now include `tests.support.test_databases`
  - with `tests._support.test_databases` left as fallback-only compatibility
- imported modules are now only advertised/resolved if they expose a supported
  modern builder entrypoint

This means the resource manager can now host alpha-native semantic fixture
families without helper modules or legacy packages shadowing the built-in
profiles.

## What Is Missing

### 1. Historical generic `test_db_*` profiles still dominate

The biggest remaining gap is now naming and default usage, not raw fixture
coverage.

Many `test_db_*` names still differ only by:

- total book count
- total folder count
- total file count

The new semantic families fix that coverage problem, but the old numeric family
still dominates the standard-series surface by count.

### 2. Secondary UUID / content-level / shelf-number fixtures

Those older semantics are still not represented as standard provisionable DBs.

### 3. Dedicated compatibility-view fixtures

The standard series still does not provide a dedicated compat-view fixture
family for any narrow legacy projection we may eventually choose to support.

### 4. A clear third-shape policy

The `_db_0` / `_db_1` wave is now live for the main semantic families. The next
gap is process discipline: `_db_2` should only exist where a third semantic
shape clearly earns its keep.

## Recommended Replacement Plan

### Immediate infrastructure work

1. Decide which existing builder modules are still honest alpha-native fixtures
   - versus legacy holdovers that should remain parked
2. Keep benchmark DBs separate from the standard `test_db_*` family
   - already done with:
     - `benchmark_db_smoke`
     - `benchmark_db_medium`
     - `benchmark_db_large`

### New alpha-native standard fixtures to add

1. `metadata_rich_db`
   - rich agents, notes, comments, synopses, annotations
   - good for interface and browse tests
   - live members landed:
     - `metadata_rich_db_0`
     - `metadata_rich_db_1`

2. `stores_assets_db`
   - current storage/backend semantics
   - not legacy `folder_stores`
   - should include files, folders, stores, and downloadable assets
   - live members landed:
     - `stores_assets_db_0`
     - `stores_assets_db_1`

3. `images_covers_db`
   - realistic image and cover linkage
   - good for web/OPDS/acquisition surfaces
   - live members landed:
     - `images_covers_db_0`
     - `images_covers_db_1`

4. `custom_columns_populated_db`
   - real populated custom-column rows
   - good for cache/database compatibility tests
   - live members landed:
     - `custom_columns_populated_db_0`
     - `custom_columns_populated_db_1`

5. `identifiers_db`
   - populated entity/item identifiers and realistic identifier shapes
   - live members landed:
     - `identifiers_db_0`
     - `identifiers_db_1`

6. `compat_projection_db`
   - whatever narrow compatibility-view contract we actually intend to support
   - no broader than the current product goal

7. `pathological_relations_db`
   - valid but awkward relation density and tree depth
   - intended for search/browse/path stress, not timing assertions
   - first live member landed: `pathological_relations_db_0`

8. `weird_data_db`
   - unicode, long text, odd filenames, awkward-but-valid metadata
   - first live member landed: `weird_data_db_0`

## Recommended Rollout Order

1. Keep using imported semantic families instead of adding more hardcoded
   built-ins.
2. Expand the new semantic families as series where more than one shape is
   useful.
   - done for:
     - `metadata_rich_db_1`
     - `stores_assets_db_1`
     - `images_covers_db_1`
     - `custom_columns_populated_db_1`
     - `identifiers_db_1`
3. Add the more specialized `compat_projection_db` only if the product intends
   to make a real compatibility claim there.
4. Add `_db_2` members only where a third semantic shape earns its keep.

## Why This Order

- `metadata_rich_db` and `stores_assets_db` gave the highest leverage across
  interfaces, library behavior, and storage-facing work.
- `images_covers_db` closed the image/cover fixture gap for interface realism.
- `custom_columns_populated_db` closed the named custom-column fixture gap.
- `identifiers_db` closed the named identifier-view fixture gap.
- `pathological_relations_db` now covers dense relation fanout without turning
  correctness fixtures into benchmarks.
- `weird_data_db` now covers unicode-heavy metadata and odd-but-valid asset
  naming.
- benchmark work should remain separate from this series.
