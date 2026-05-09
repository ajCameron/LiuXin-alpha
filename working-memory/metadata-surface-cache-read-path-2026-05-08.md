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
- Added `--metadata-read-source`, `--cache-type`, and
  `--no-cache-db-fallback` CLI flags to the read-only web, Calibre-style web,
  JSON API, and OPDS surfaces so they can serve through a loaded storage cache
  without custom Python wiring.
- Added `metadata_read_source`, `metadata_cache_type`, and
  `metadata_cache_allow_database_fallback` fields to the read-only web config
  so programmatic app construction can also request the cache path.
- Extracted shared surface helpers for metadata read-source CLI arguments and
  config kwargs to keep the four read-only startup paths aligned.
- Added `build_metadata_read_source(...)` in the standard read-only web app;
  Calibre-style app construction inherits the same config-driven setup.
- Added a read-model regression test that loads a schema-backed cache, disables
  database fallback, mutates the database after the cache read, and verifies the
  read model still serves the cached snapshot.
- Added parser/route coverage for cache read-source startup wiring across the
  read-only surface CLIs.
- Fixed a metadata API import cycle exposed by importing metadata read sources
  from the surface test path: Calibre metadata API leaf modules now import
  write contracts and Calibre type contracts from their leaf modules rather
  than from `LiuXin_alpha.metadata.api`.

## Validation

```bash
python3 -m pytest tests/surfaces/test_read_model_api.py::test_read_model_can_use_cache_read_source_without_database_fallback
python3 -m pytest tests/surfaces/test_read_model_api.py tests/surfaces/test_metadata_facets.py tests/surfaces/test_read_model_metadata_parity.py tests/metadata/api/test_calibre_metadata_api.py tests/metadata/api/test_metadata_package_surface.py
python3 -m pytest tests/surfaces/test_web_readonly.py::test_web_readonly_cache_read_source_cli_options_serve_snapshot tests/surfaces/test_web_calibre_readonly.py::test_web_calibre_readonly_parser_accepts_cache_read_source_options
python3 -m pytest tests/surfaces/test_api_readonly.py::test_api_readonly_parser_accepts_cache_read_source_options tests/surfaces/test_opds_readonly.py::test_opds_readonly_parser_accepts_cache_read_source_options tests/surfaces/test_web_readonly.py::test_web_readonly_cache_read_source_cli_options_serve_snapshot tests/surfaces/test_web_calibre_readonly.py::test_web_calibre_readonly_parser_accepts_cache_read_source_options
python3 -m pytest tests/surfaces/test_read_model_api.py tests/surfaces/test_web_readonly.py tests/surfaces/test_web_calibre_readonly.py
python3 -m pytest tests/surfaces/test_read_model_api.py tests/surfaces/test_api_readonly.py tests/surfaces/test_opds_readonly.py tests/surfaces/test_web_readonly.py tests/surfaces/test_web_calibre_readonly.py
python3 -m compileall -q src/LiuXin_alpha/metadata src/LiuXin_alpha/surfaces
python3 -m compileall -q src/LiuXin_alpha/surfaces/web_readonly src/LiuXin_alpha/surfaces/web_calibre_readonly
python3 -m compileall -q src/LiuXin_alpha/surfaces
git diff --check
```

Results:
- cache read-source regression: `2 passed`
- focused surface/metadata API slice: `49 passed`
- web cache CLI focused slice: `3 passed`
- read-model plus web surface slice: `46 passed`
- all read-only surface cache CLI focused slice: `5 passed`
- read-model plus API/OPDS/web surface slice: `57 passed`
- compileall: passed
- diff check: passed

## Follow-Up

- OPDS search now inherits read-model search payload behavior when routed
  through the read model, but Calibre web still has some direct helper paths
  for category-specific page rendering. Those can be migrated gradually.
