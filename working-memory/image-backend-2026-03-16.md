# Shared Image Backend

Date: 2026-03-16

## Summary

Extracted the neutral cover/image path into `interfaces/images`.

## What Changed

- Added `interfaces/images`:
  - `ImageBackend`
  - `ImageHostApi`
- `ReadOnlyWebApplication` now owns `self.images`.
- `ReadModelBackend` now composes `self.images` instead of owning cover/image discovery and resolution logic directly.
- `CalibreCatalogBackend` now composes the same shared `ImageBackend` instance instead of treating image handling as part of its own compatibility layer.
- `web_calibre_readonly` and `opds_readonly` now satisfy acquisition-cover routes by delegating directly to `self.images`.

## What `images` Owns

- `works -> expressions -> manifestations -> items -> images` cover discovery
- image download-name and content-type resolution
- storage-backed image retrieval metadata
- local/redirect image target resolution
- primary cover selection for a work
- placeholder cover SVG generation

## Why

This is a cleaner framework seam than keeping cover/image logic mixed into:

- `read_model`
- `catalog`
- specific host apps

It gives later hosts a direct shared backend for cover and image behavior without having to depend on Calibre-shaped infrastructure.

## Validation

- `PYTHONPATH=src .venv/bin/python -m py_compile src/LiuXin_alpha/interfaces/images/api.py src/LiuXin_alpha/interfaces/read_model/api.py src/LiuXin_alpha/interfaces/catalog/api.py src/LiuXin_alpha/interfaces/web_readonly/app.py src/LiuXin_alpha/interfaces/web_calibre_readonly/app.py src/LiuXin_alpha/interfaces/opds_readonly/app.py tests/interfaces/test_images_api.py`
  - passed
- `PYTHONPATH=src .venv/bin/python -m pytest -q tests/interfaces/test_images_api.py tests/interfaces/test_read_model_api.py tests/interfaces/test_catalog_api.py tests/interfaces/test_acquisition_api.py tests/interfaces/test_api_readonly.py tests/interfaces/test_opds_api.py tests/interfaces/test_opds_readonly.py tests/interfaces/test_web_calibre_readonly.py tests/interfaces/test_web_readonly.py`
  - `39 passed`

## Next Step

- only extract another shared interface service if a second concrete consumer appears for it; `images` is now a good example of a justified seam rather than speculative abstraction
