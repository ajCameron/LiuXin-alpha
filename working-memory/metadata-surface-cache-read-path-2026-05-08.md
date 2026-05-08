# Metadata Surface Cache Read Path - 2026-05-08

Branch: `metadata-surface-cache-read-path`

Base: current `main` after PRs #26, #27, #28, and #29 were merged.

## Scope

This slice starts item 5 from the metadata interaction surface plan: let the
read-model path consume either the live database or an explicit metadata read
source/cache snapshot.

## Completed

- Fast-forwarded local `main` to `origin/main` after the interaction-surface
  review stack landed.
- Added `get_all_rows`, `get_record_count`, and `get_interlinked_rows` to
  `DatabaseMetadataReadSource` and `CacheMetadataReadSource`.
- Extended `CacheMetadataReadSource` so table-wide reads can come from loaded
  cache table row ids, including the existing fake cache shape used by tests.
- Updated `ReadModelBackend` to use an injectable read source for work rows,
  category rows/counts, work credits, related-row lookup, file discovery, and
  search result payloads.
- Added `read_source=` constructor injection to `ReadOnlyWebApplication` and
  `CalibreReadOnlyWebApplication`; direct database access remains the default.
- Added a read-model regression test that loads a schema-backed cache, disables
  database fallback, mutates the database after the cache read, and verifies the
  read model still serves the cached snapshot.
- Fixed a metadata API import cycle exposed by importing metadata read sources
  from the surface test path: Calibre metadata API leaf modules now import
  write contracts and Calibre type contracts from their leaf modules rather
  than from `LiuXin_alpha.metadata.api`.

## Validation

```bash
python3 -m pytest tests/surfaces/test_read_model_api.py::test_read_model_can_use_cache_read_source_without_database_fallback
python3 -m pytest tests/surfaces/test_read_model_api.py tests/surfaces/test_metadata_facets.py tests/surfaces/test_read_model_metadata_parity.py tests/metadata/api/test_calibre_metadata_api.py tests/metadata/api/test_metadata_package_surface.py
python3 -m compileall -q src/LiuXin_alpha/metadata src/LiuXin_alpha/surfaces
git diff --check
```

Results:
- cache read-source regression: `2 passed`
- focused surface/metadata API slice: `49 passed`
- compileall: passed
- diff check: passed

## Follow-Up

- Consider exposing CLI flags for read-only web and Calibre-style web apps to
  build and use a cache read source without custom Python wiring.
- OPDS search now inherits read-model search payload behavior when routed
  through the read model, but Calibre web still has some direct helper paths
  for category-specific page rendering. Those can be migrated gradually.
