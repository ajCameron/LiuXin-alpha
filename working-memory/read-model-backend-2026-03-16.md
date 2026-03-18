# Read Model Backend

Date: 2026-03-16

## Summary

Extracted a neutral shared read/query backend at `interfaces/read_model` and wired the main read-only hosts to it.

## What Changed

- Added `interfaces/read_model`:
  - `ReadModelBackend`
  - `ReadModelHostApi`
- `ReadOnlyWebApplication` now owns `self.read_model`.
- `CalibreCatalogBackend` now composes `ReadModelBackend` for neutral concerns instead of owning those implementations directly.
- `api_readonly` now builds work, file, search, and category payloads from `self.read_model`, keeping the public JSON shape stable while removing the Calibre-shaped dependency from its internal model.
- `web_calibre_readonly` and `opds_readonly` now pass the shared `self.read_model` into `CalibreCatalogBackend`.
- `web_readonly` now also consumes the neutral backend directly for:
  - global search result payloads
  - specialized `works` detail pages
  - specialized `files` detail pages
- Generic category/query shaping has moved further into `read_model`:
  - category display names
  - category index payload
  - sorted/paginated category collection payload
- Neutral work-list/query shaping has moved further into `read_model`:
  - work sorting/windowing payloads used by Calibre-style search/list views
  - id-window extraction for paged work lists
  - neutral books-metadata mapping keyed by work id
- `catalog` now uses those neutral category payloads and adds only Calibre-specific translation such as encoded AJAX URLs, icons, and route targets.
- `catalog` now also uses the neutral work-list and books-metadata helpers, adding only the Calibre-specific envelope fields and per-book extras like `category_urls`.
- Exported `read_model` from top-level `interfaces`.

## Neutral Concerns Moved Into `read_model`

- work listing and sort behavior
- linked-entity resolution
- category row discovery and browse counts
- category index and collection payloads
- work list/window payloads
- books metadata mapping
- work metadata/detail payloads
- file metadata/detail payloads
- search result payloads
- work file discovery

## Why

This gives the repo a clearer framework seam:

- `read_model` for neutral browse/query/data shaping
- `catalog` for Calibre-compatible/category-specific translation
- `opds` for protocol rendering
- `acquisition` for `/get/...` compatibility

That is a better base for future HTML, OPDS, JSON, and client-facing interfaces than letting each host own its own read model.

## Validation

- `PYTHONPATH=src .venv/bin/python -m py_compile src/LiuXin_alpha/interfaces/read_model/api.py src/LiuXin_alpha/interfaces/catalog/api.py src/LiuXin_alpha/interfaces/api_readonly/app.py src/LiuXin_alpha/interfaces/web_readonly/app.py src/LiuXin_alpha/interfaces/web_calibre_readonly/app.py src/LiuXin_alpha/interfaces/opds_readonly/app.py tests/interfaces/test_read_model_api.py`
  - passed
- `PYTHONPATH=src .venv/bin/python -m pytest -q tests/interfaces/test_read_model_api.py tests/interfaces/test_catalog_api.py tests/interfaces/test_api_readonly.py tests/interfaces/test_acquisition_api.py tests/interfaces/test_opds_api.py tests/interfaces/test_opds_readonly.py tests/interfaces/test_web_calibre_readonly.py tests/interfaces/test_web_readonly.py`
  - `37 passed`
- `PYTHONPATH=src .venv/bin/python -m pytest -q tests/interfaces/test_read_model_api.py tests/interfaces/test_catalog_api.py tests/interfaces/test_api_readonly.py tests/interfaces/test_web_readonly.py tests/interfaces/test_web_calibre_readonly.py tests/interfaces/test_opds_readonly.py`
  - `32 passed`

## Next Step

- extract an even more presentation-neutral catalog/query service only if the remaining `interfaces/catalog` responsibilities stop being clearly Calibre-shaped
