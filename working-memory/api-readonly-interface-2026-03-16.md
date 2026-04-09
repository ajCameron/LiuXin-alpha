# API Read-Only Interface

Date: 2026-03-16

## Summary

Added a new top-level machine-facing interface package, `interfaces/api_readonly`, as the first standalone read-only JSON API surface.

## What It Exposes

- `/` -> same JSON index as `/api`
- `/api`
- `/api/categories`
- `/api/works`
- `/api/works/<id>`
- `/api/authors`
- `/api/authors/<table>/<id>`
- `/api/authors/<table>/<id>/works`
- `/api/tags`
- `/api/tags/<id>`
- `/api/tags/<id>/works`
- `/api/series`
- `/api/series/<id>`
- `/api/series/<id>/works`
- `/api/search?q=...`
- `/api/files/<id>`
- plus the existing safe file delivery routes:
  - `/files/<id>/download`
  - `/files/<id>/preview`

## Design

- Built on `ReadOnlyWebApplication` for DB/file safety and row/link helpers.
- Now builds its route payloads on the neutral `self.read_model` backend for work/category/file/search shaping.
- Keeps `CalibreCatalogBackend` only for the Calibre-shaped category summary compatibility layer.
- Returns stable JSON payloads instead of raw table dumps.
- Keeps HTML and OPDS routes separate; this is intended as the machine boundary for later interfaces and clients.

Update:
- `/api/categories` now comes from the neutral shared category index payload rather than the Calibre compatibility layer.
- category collection endpoints (`/api/authors`, `/api/tags`, `/api/series`) now use the neutral shared category-collection payload before adding API-specific links.

## Why

This is the next framework-oriented interface step after extracting:

- `interfaces/catalog`
- `interfaces/opds`
- `interfaces/acquisition`

The intent is for future HTML, OPDS, terminal, or client-facing surfaces to reuse a common read model instead of each one inventing its own route-local data shape.

## Launch

- `PYTHONPATH=src python3 -m LiuXin_alpha.interfaces.api_readonly --database /path/to/library.sqlite`
- `./scripts/run_api_readonly.sh --database /path/to/library.sqlite --port 8083`
- `python3 scripts/run_api_readonly.py --database /path/to/library.sqlite --port 8083`

## Validation

- `PYTHONPATH=src .venv/bin/python -m pytest -q tests/interfaces/test_api_readonly.py`
  - `2 passed`
- `PYTHONPATH=src .venv/bin/python -m pytest -q tests/interfaces/test_api_readonly.py tests/interfaces/test_catalog_api.py tests/interfaces/test_acquisition_api.py tests/interfaces/test_opds_api.py tests/interfaces/test_opds_readonly.py tests/interfaces/test_web_calibre_readonly.py tests/interfaces/test_web_readonly.py`
  - `34 passed`
- `PYTHONPATH=src .venv/bin/python -m py_compile src/LiuXin_alpha/interfaces/api_readonly/app.py src/LiuXin_alpha/interfaces/api_readonly/__init__.py src/LiuXin_alpha/interfaces/api_readonly/__main__.py tests/interfaces/test_api_readonly.py scripts/run_api_readonly.py`
  - passed
- `bash -n scripts/run_api_readonly.sh`
  - passed
- `python3 scripts/run_api_readonly.py --help`
  - passed

## TODO

- add richer file/image/media payloads once the read model settles
- decide whether pagination/ranking/search semantics should be standardized across API and HTML hosts
