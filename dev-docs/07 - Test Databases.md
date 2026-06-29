# Test Databases

The standard `test_db_*` series exists to provide reusable, named fixtures for
correctness work.

It should not try to do all jobs at once.

In particular:

- correctness fixtures should be stable and semantically distinct
- benchmark fixtures should be explicit and opt-in
- legacy names should not be kept alive if they no longer describe real alpha
  behavior

## Current Situation

Right now the standard series is thinner than the names imply.

Operationally, the resource manager mostly provides:

- a minimal schema/title smoke DB
- a single-book smoke DB
- one folder/file volume fixture
- a blank schema fixture
- a large family of generic profiled FRBR-native DBs that differ mainly by
  counts

That means a lot of historical names exist without providing distinct current
semantic value.

There was also a real infrastructure problem:

- the resource manager supports imported builder modules
- the live modules are under `tests.support.test_databases`
- imported modules now only count if they expose a supported modern builder
  entrypoint

That infrastructure is now fixed, so semantic imported fixture families can
participate by default without helper modules polluting discovery.

## What A Good Standard Series Should Cover

The standard series should provide named fixtures for distinct behavior, not
just distinct sizes.

At minimum, LiuXin_alpha wants explicit standard DBs for:

1. minimal smoke
2. blank schema
3. asset volume
4. rich metadata
5. populated custom columns
6. identifier-heavy metadata
7. image/cover behavior
8. current storage-backed asset semantics
9. compatibility-view behavior, if we explicitly choose to support it
10. pathological but valid relation shapes
11. weird-but-valid data

## Benchmark DBs Are Separate

Large performance fixtures are useful, but they should not be part of the
ordinary `test_db_*` correctness family.

Those now live as explicit benchmark resources:

- `benchmark_db_smoke`
- `benchmark_db_medium`
- `benchmark_db_large`

and can also be built with custom counts via:

- [build_benchmark_test_db.py](/home/blackjane/LiuXin-alpha-wsl/scripts/build_benchmark_test_db.py)

The first benchmark harness now exists separately from pytest:

- [benchmark_read_paths.py](/home/blackjane/LiuXin-alpha-wsl/scripts/benchmark_read_paths.py)
- [benchmark_surface_paths.py](/home/blackjane/LiuXin-alpha-wsl/scripts/benchmark_surface_paths.py)
- [benchmark_baseline_suite.py](/home/blackjane/LiuXin-alpha-wsl/scripts/benchmark_baseline_suite.py)
- [summarize_benchmark_report.py](/home/blackjane/LiuXin-alpha-wsl/scripts/summarize_benchmark_report.py)

Current baseline artifact:

- [benchmark-baseline-2026-03-18.json](/home/blackjane/LiuXin-alpha-wsl/working-memory/test-results/benchmark-baseline-2026-03-18.json)
- [benchmark-baseline-2026-03-18-summary.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/test-results/benchmark-baseline-2026-03-18-summary.md)

Benchmark profile rule:

- `interactive`
  - default
  - excludes `benchmark_db_medium`
- `nightly`
  - includes `benchmark_db_medium`
  - intended for slower scheduled baselines

## Recommended Alpha-Native Fixture Families

### `metadata_rich_db`

Purpose:

- browse/search/detail realism
- agent/note/comment/synopsis coverage

Should contain:

- non-trivial `agents`, `human_agents`, `notes`, `comments`, `synopses`,
  `annotations`
- realistic multi-link metadata across works

First live member:

- `metadata_rich_db_0`
- `metadata_rich_db_1`

### `stores_assets_db`

Purpose:

- current storage-backed file semantics
- surface and acquisition realism

Should contain:

- `stores`
- `folders`
- `files`
- linked items/files with actually usable asset rows

This should target current storage seams, not revive legacy `folder_stores`.

First live member:

- `stores_assets_db_0`
- `stores_assets_db_1`

### `images_covers_db`

Purpose:

- web/OPDS/acquisition cover behavior

Should contain:

- image rows
- cover-like linkages
- mixed direct and indirect cover resolution paths

First live member:

- `images_covers_db_0`
- `images_covers_db_1`

### `custom_columns_populated_db`

Purpose:

- reusable named DB for populated custom-column cases

Should contain:

- real custom-column rows and values
- enough variety to exercise the supported field types

First live member:

- `custom_columns_populated_db_0`
- `custom_columns_populated_db_1`

Current implementation note:

- creation uses the live `CustomColumns` API
- value seeding still writes the generated tables directly because the current
  setter path is still biased toward the historical `books` seam

That limitation is real and should be fixed in the product code, not hidden by
the fixture.

### `identifiers_db`

Purpose:

- entity/item identifier semantics

Should contain:

- realistic identifiers
- multiple identifier shapes
- enough density to test browse/search/detail behavior

First live member:

- `identifiers_db_0`
- `identifiers_db_1`

### `compat_projection_db`

Purpose:

- only if LiuXin_alpha makes a real compatibility claim here

Should be narrow:

- only the compatibility views and behavior we actually intend to support

### `pathological_relations_db`

Purpose:

- awkward-but-valid relation shapes

Should contain:

- deep series trees
- dense link sets
- wide fan-out

This is for correctness and browse/search stress, not timing assertions.

First live member:

- `pathological_relations_db_0`

### `weird_data_db`

Purpose:

- robustness against valid messy data

Should contain:

- unicode-heavy metadata
- long text payloads
- odd filenames/extensions
- messy-but-valid values

First live member:

- `weird_data_db_0`

## Suggested Rollout Order

1. Keep using semantic imported fixture families rather than hardcoding more
   built-in specs.
2. Expand the live semantic families with `_db_1` members where they need more
   than one shape.
   - done for:
     - `metadata_rich_db_1`
     - `stores_assets_db_1`
     - `images_covers_db_1`
     - `custom_columns_populated_db_1`
     - `identifiers_db_1`
3. Add `compat_projection_db` if we decide to support one.
4. Add `_db_2` members only where a third semantic shape is justified.
5. Keep benchmark reporting in scripts and JSON artifacts, not pytest timing
   assertions.
6. Treat `benchmark_db_medium` as part of the slower nightly profile, not the
   default interactive baseline.

## Design Rule

When adding a new standard DB:

- give it a semantic name
- define the behavior it is supposed to represent
- add one collected contract that proves that representation
- do not add it just to preserve a historical number

That keeps the standard series honest and keeps benchmark work separate from
correctness fixtures.

## Current Validation

The first semantic-family pass is live and covered in
[test_test_resources_manager.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/test_test_resources_manager.py):

- imported provider discovery now ignores helper modules without supported
  entrypoints
- `metadata_rich_db_0` has a collected optional-metadata contract
- `metadata_rich_db_1` has a denser multilingual optional-metadata contract
- `stores_assets_db_0` has a collected store-root rewrite and live file
  retrieval contract
- `stores_assets_db_1` has a multi-store rewrite and live retrieval contract
- `images_covers_db_0` has a collected cover-link and live image retrieval
  contract
- `images_covers_db_1` has a variant-link and no-cover-gap contract
- `custom_columns_populated_db_0` has a collected generated-schema and seeded
  value contract
- `custom_columns_populated_db_1` extends that with series/float/bool/comments
  coverage
- `identifiers_db_0` has a collected identifier-view contract
- `identifiers_db_1` widens the identifier-view matrix
- `pathological_relations_db_0` has a collected dense relation-fanout contract
- `weird_data_db_0` has a collected unicode and odd-path contract

Current validation:

- [test_test_resources_manager.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/test_test_resources_manager.py)
  - `52 passed`
- [test_property_support_registry.py](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/test_db_properties/test_property_support_registry.py)
  - `32 passed`
