# Metadata Interface Review Plan - 2026-05-08

Branch: `metadata-interface-review-plan`

## Scope

Reviewed the current metadata public facade, abstract API/protocol layer,
metadata container implementations, metadata read-source adapters, and storage
cache public API. This is a planning branch only; no interface behavior has
been changed yet.

Primary files inspected:
- `src/LiuXin_alpha/metadata/__init__.py`
- `src/LiuXin_alpha/metadata/api/**`
- `src/LiuXin_alpha/metadata/read_sources.py`
- `src/LiuXin_alpha/metadata/containers/metadata_containers/**`
- `src/LiuXin_alpha/caches/__init__.py`
- `src/LiuXin_alpha/caches/api/**`
- `tests/metadata/api/**`
- `tests/metadata/test_metadata_top_level_facade.py`
- `tests/databases/caches/test_cache_imports.py`
- `tests/databases/caches/test_cache_plugin_contract.py`

## Current Signal

Commands run:

```bash
.venv/bin/python -m pytest -q tests/metadata/api tests/metadata/test_metadata_top_level_facade.py
.venv/bin/python -m pytest -q tests/databases/caches/test_cache_imports.py tests/databases/caches/test_cache_plugin_contract.py
```

Results:
- metadata API/facade slice: `56 passed`
- cache import/plugin contract slice: `53 passed`

These tests confirm basic imports and current behavior, but they do not fully
protect the interface contracts.

## Findings

1. API source-hygiene tests are currently ineffective.

   `tests/conftest.py` changes cwd to `tmp_path` for every test. Several API
   tests use repo-relative `Path("src/LiuXin_alpha/metadata/api")`, so they scan
   an empty path under `tmp_path`. Directly calling the test functions from the
   repo root exposes real offenders: `Any` annotations and
   `raise NotImplementedError` placeholders remain in `metadata/api`.

2. `LiuXin_alpha.metadata.api` does not export the database/read-source API.

   The root API exports container contracts, but not `MetadataReadSourceAPI`,
   `MetadataHydratorAPI`, `DBMetadataSourceAPI`, or the WEMI getter APIs from
   `metadata.api.from_database_api`. Those are now operational contracts because
   metadata hydrators accept database and cache read sources.

3. The cache API roots are incomplete.

   `LiuXin_alpha.caches.api` is effectively empty, and `LiuXin_alpha.caches`
   exports concrete/cache helper names but not several formal API types such as
   `StorageCacheAPI`, `StorageCacheBaseTableAPI`, `StorageCacheSingleTableAPI`,
   `FieldBasicInterfaceAPI`, or `TableTypes`. Metadata now depends on the cache
   read surface, so the intended stable import roots should be explicit.

4. There is a stale duplicate database metadata namespace.

   `src/LiuXin_alpha/metadata/from_database/__init__.py` still contains an old
   skeletal `DBMetadataSourceAPI` unrelated to
   `metadata.api.from_database_api`. This is a trap for imports and should be
   redirected or removed with a compatibility plan.

5. `LiuXinWEMIMetadataAPI` lags the concrete object.

   The concrete object now exposes:
   - `sync_legacy_genres_from_wemi`
   - `sync_legacy_subjects_from_wemi`
   - `sync_legacy_series_from_wemi`
   - `sync_legacy_identifiers_from_wemi`
   - `from_database`
   - `from_opf`

   The API contract only lists title/tags/labels sync and misses the newer
   operational entry points.

6. Lazy metadata has no formal public contract.

   `LazyLiuXinWEMIMetadata` adds `force_hydrate`, `hydrate_field`,
   `lazy_fields`, `is_lazy_field_loaded`, and lazy relation-loader behavior.
   The top-level facade exposes lazy hydration, but `metadata.api` does not
   describe the lazy-specific surface.

7. Write-back reports are concrete-only.

   `LiuXinWEMIMetadataWriteReport` is exported from the public metadata facade,
   but there is no protocol/API contract for its shape. The write methods mostly
   return `Any`, which obscures the useful operational fields:
   `changed`, `fields_checked`, `rows_added`, `rows_updated`, `rows_removed`,
   `links_added`, `links_removed`, `skipped`, and `errors`.

8. Read-source typing is too loose for the new cache path.

   `MetadataReadSourceAPI` uses `Any` for `driver_wrapper`, rows, search terms,
   and interlink rows. The concrete adapters are useful, but the contract does
   not yet explain the row-like/mapping-like shape hydrators need from databases
   and caches.

9. Cache staleness after metadata write-back is not an interface contract yet.

   Current cache capability flags say whether a backend requires reload for
   external DB changes. Metadata write-back mutates the database directly, but
   the metadata facade/API does not state whether supplied caches are invalidated,
   reloaded, or intentionally stale until the caller refreshes them.

## Action Plan

### 1. Fix the interface tests first

- Resolve repo paths in source-scanning tests via `Path(__file__).resolve()`
  instead of cwd-relative paths.
- Add a regression test proving the API source scan sees at least one known API
  file before asserting hygiene rules.
- Decide whether `Any` is forbidden everywhere in `metadata/api` or only in
  leaf/container protocols. If it remains forbidden, replace current `Any`
  annotations with explicit protocol/type aliases.

### 2. Define public import roots

- Make `LiuXin_alpha.metadata` the workflow facade:
  database/cache hydration, OPF conversion, concrete high-level metadata
  classes, read-source adapters, and write reports.
- Make `LiuXin_alpha.metadata.api` the abstract contract root:
  container APIs, read-source APIs, hydrator APIs, lazy metadata APIs, and write
  report APIs.
- Make `LiuXin_alpha.metadata.containers` the concrete container root.
- Make `LiuXin_alpha.caches` the cache workflow/concrete root.
- Make `LiuXin_alpha.caches.api` the cache contract root.
- Add package-surface tests for those roots so new API additions are deliberate.

### 3. Bring metadata contracts up to current behavior

- Add missing WEMI sync methods to `LiuXinWEMIMetadataAPI`.
- Add `from_database` and `from_opf` classmethod expectations where callers use
  them as public constructors.
- Add a `LazyLiuXinWEMIMetadataAPI` protocol for lazy-only methods and export it
  from `metadata.api`.
- Add `MetadataWriteReportAPI` and use it as the return type for supported
  `write_to_database` methods.
- Make the high-level metadata API acknowledge actual operational objects:
  eager WEMI, lazy WEMI, LiuXin projection, Calibre projection, OPF projection,
  and write-back report.

### 4. Tighten database/cache read-source contracts

- Replace broad `Any` in `MetadataReadSourceAPI` with explicit row-like and
  database-like protocols where possible.
- Export `DatabaseMetadataReadSource`, `CacheMetadataReadSource`, and
  `metadata_read_source_from` from a stable API or facade location by design,
  not just as implementation details.
- Add tests that the same hydrator accepts:
  direct database, explicit read-source adapter, explicit loaded cache, and a
  cache-backed read source with/without database fallback.
- Document cache freshness after write-back: either explicit reload/invalidate
  is required, or the writer should optionally invalidate touched cache IDs.

### 5. Clean stale namespaces

- Replace `metadata/from_database/__init__.py` with a compatibility re-export
  to `metadata.api.from_database_api`, or delete it if no public import needs to
  survive.
- Keep `metadata/metadata.py` as the legacy `MetaData` compatibility alias for
  now, because file-source code still imports it.
- Add import tests so stale skeletons cannot reappear as parallel APIs.

### 6. Add operational parity tests around the interfaces

- Extend API/implementation parity tests so concrete WEMI containers satisfy
  the contract names used in `metadata.api`.
- Add top-level facade tests for cache-only and read-source-first hydration.
- Add lazy API tests that assert lazy state before/after `force_hydrate`.
- Add write-report shape tests independent of full database mutation behavior.
- Add a cache-staleness contract test after metadata write-back.

## Suggested PR Order

1. Test harness/root-path fix plus current API drift assertions.
2. Public import-root cleanup for `metadata.api` and `caches.api`.
3. WEMI/lazy/write-report protocol updates.
4. Read-source/cache contract tightening.
5. Stale namespace cleanup.
6. Additional operational parity tests.

This order keeps the first PR diagnostic-only, then moves from import surfaces
to contracts, then into behavior-sensitive cache/read-source semantics.
