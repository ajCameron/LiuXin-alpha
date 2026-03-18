# Shared OPDS API

Date: 2026-03-16

## Summary

Extracted the OPDS route/feed/token implementation out of `interfaces/web_calibre_readonly` into a neutral shared package:

- `interfaces/opds`
- shared router: `OpdsApi`
- explicit host protocol: `OpdsHostApi`

## Design

The shared OPDS layer now owns:

- token encoding/decoding
- category normalization
- OPDS feed and entry rendering
- paging helpers
- grouped category feed logic
- `/opds`, `/opds/navcatalog/...`, `/opds/category/...`, `/opds/categorygroup/...`, `/opds/search/...` route handling

Host interfaces now provide explicit methods such as:

- `opds_search_work_rows(...)`
- `opds_work_rows(...)`
- `opds_category_rows(...)`
- `opds_rows_for_category_item(...)`
- `opds_work_metadata_payload(...)`
- `opds_xml_response(...)`
- `opds_text_response(...)`

## Current Hosts

- `interfaces/web_calibre_readonly`
- `interfaces/opds_readonly`

`web_calibre_readonly` is now an OPDS host instead of an OPDS implementation.

## Why

This gives a real reuse seam for later interfaces:

- OPDS can be rolled into other read-only or mixed interfaces without subclassing the Calibre web app
- compatibility logic now lives in one place instead of drifting between interfaces
- the host contract is explicit and testable

## Validation

- `pytest -q tests/interfaces/test_opds_api.py tests/interfaces/test_opds_readonly.py tests/interfaces/test_web_calibre_readonly.py tests/interfaces/test_web_readonly.py`
  - `25 passed`
- `python -m py_compile src/LiuXin_alpha/interfaces/opds/api.py src/LiuXin_alpha/interfaces/opds_readonly/app.py src/LiuXin_alpha/interfaces/web_calibre_readonly/app.py tests/interfaces/test_opds_api.py tests/interfaces/test_opds_readonly.py tests/interfaces/test_web_calibre_readonly.py`
  - passed


Update:
- Calibre-compatible acquisition/download behavior now also lives in a neutral shared package, `interfaces/acquisition`, instead of being owned by `web_calibre_readonly`.

## TODO

- if another non-Calibre interface wants OPDS, implement `OpdsHostApi` directly instead of subclassing `web_calibre_readonly`
- decide whether downloads/acquisition routes should get a similarly neutral shared layer next
