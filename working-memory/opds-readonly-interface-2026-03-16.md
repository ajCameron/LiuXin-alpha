# OPDS Read-Only Interface

Date: 2026-03-16

## Summary

Added a new top-level interface package, `interfaces/opds_readonly`, as a narrow standalone OPDS/acquisition surface.

## What It Exposes

- `/` -> redirects to `/opds`
- `/opds`
- `/opds/navcatalog/...`
- `/opds/category/...`
- `/opds/categorygroup/...`
- `/opds/search/...`
- `/get/...`
- `/legacy/get/...`
- `/stanza` -> redirects to `/opds`
- `/robots.txt`
- `/favicon.png`
- `/apple-touch-icon.png`
- `/icon/...`

## Design

- Standalone OPDS routing now reuses the neutral shared `interfaces/opds` API, with `interfaces/web_calibre_readonly` acting as one host implementation.
- `interfaces/opds_readonly` now subclasses `ReadOnlyWebApplication` directly.
- It composes `CalibreCatalogBackend`, `OpdsApi`, and `AcquisitionCompatApi` instead of inheriting the Calibre HTML app.
- This keeps OPDS route and payload compatibility aligned while making the extraction seam explicit for later interfaces.

## Launch

- `PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.opds_readonly --database /path/to/library.sqlite`
- `./scripts/run_opds_readonly.sh --database /path/to/library.sqlite --port 8082`
- `python3 scripts/run_opds_readonly.py --database /path/to/library.sqlite --port 8082`

## Validation

- `pytest -q tests/interfaces/test_opds_readonly.py`
- `pytest -q tests/interfaces/test_opds_readonly.py tests/interfaces/test_web_calibre_readonly.py`
- `bash -n scripts/run_opds_readonly.sh`
- `python3 -m py_compile scripts/run_opds_readonly.py src/LiuXin_alpha/interfaces/opds_readonly/app.py tests/interfaces/test_opds_readonly.py`


Update:
- Calibre-compatible acquisition/download behavior now also lives in a neutral shared package, `interfaces/acquisition`, instead of being owned by `web_calibre_readonly`.
- Calibre-shaped category/work/file/image discovery and payload shaping now also live in a neutral shared package, `interfaces/catalog`.

## TODO

- add OPDS client compatibility fixtures once real clients are in the loop
- decide whether the standalone OPDS surface should expose any minimal human-readable landing page beyond the `/ -> /opds` redirect
