# Shared Calibre Catalog Backend

Date: 2026-03-16

## Summary

Extracted the remaining Calibre-shaped catalog/data logic into a neutral shared package:

- `surfaces/catalog`
- shared backend/service: `CalibreCatalogBackend`
- explicit host protocol: `CalibreCatalogHostApi`

This backend now owns the Calibre-shaped category/work/file discovery and payload shaping that had previously been duplicated inside `web_calibre_readonly`.

## What It Covers

- Calibre-style category display names and icon names
- work sorting and category row generation
- category summary and category item payloads
- work metadata payloads and books metadata payloads
- interface-data setup/basic payloads
- tag-browser payloads
- linked work lookup for authors/tags/series
- work file discovery across `works -> expressions -> manifestations -> items -> files`
- image handling delegated to the shared `surfaces/images` backend

## Current Hosts

- `surfaces/web_calibre_readonly`
- `surfaces/opds_readonly`

`surfaces/opds_readonly` now subclasses `ReadOnlyWebApplication` directly and composes:

- `CalibreCatalogBackend`
- `ImageBackend`
- `OpdsApi`
- `AcquisitionCompatApi`

It no longer subclasses `web_calibre_readonly`.

## Why

This is the next architectural step after extracting:

- `surfaces/opds`
- `surfaces/acquisition`

The Calibre-style web UI is no longer the owner of the data-shaping layer. It is now one host surface over shared backend infrastructure.

## Validation

- direct backend contract:
  - `PYTHONPATH=src .venv/bin/python -m pytest -q tests/surfaces/test_catalog_api.py`
  - `3 passed`
- `PYTHONPATH=src .venv/bin/python -m pytest -q tests/surfaces/test_acquisition_api.py tests/surfaces/test_opds_api.py tests/surfaces/test_opds_readonly.py tests/surfaces/test_web_calibre_readonly.py tests/surfaces/test_web_readonly.py`
  - `29 passed`
- combined shared interface slice:
  - `PYTHONPATH=src .venv/bin/python -m pytest -q tests/surfaces/test_catalog_api.py tests/surfaces/test_acquisition_api.py tests/surfaces/test_opds_api.py tests/surfaces/test_opds_readonly.py tests/surfaces/test_web_calibre_readonly.py tests/surfaces/test_web_readonly.py`
  - `32 passed`
- `PYTHONPATH=src .venv/bin/python -m py_compile src/LiuXin_alpha/surfaces/catalog/api.py src/LiuXin_alpha/surfaces/opds_readonly/app.py src/LiuXin_alpha/surfaces/web_calibre_readonly/app.py tests/surfaces/test_opds_readonly.py tests/surfaces/test_opds_api.py tests/surfaces/test_acquisition_api.py tests/surfaces/test_web_calibre_readonly.py tests/surfaces/test_web_readonly.py`
  - passed

## TODO

- decide whether the next extraction should be a generic catalogue/query service rather than more web-specific helpers
- add direct tests for `CalibreCatalogBackend` itself, not just host-level surface tests
- later reduce the remaining inheritance in `web_calibre_readonly` if more shared browse/catalog surfaces appear
