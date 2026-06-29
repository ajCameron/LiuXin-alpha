# Shared Acquisition API

Date: 2026-03-16

## Summary

Extracted the Calibre-compatible acquisition/download path into a neutral shared package:

- `surfaces/acquisition`
- shared router/service: `AcquisitionCompatApi`
- explicit host protocol: `AcquisitionHostApi`

## What It Covers

The shared acquisition layer now owns the `/get/...` and `/legacy/get/...` compatibility behavior for:

- format downloads like `/get/epub/<book>/main`
- cover/thumb routes like `/get/cover/<book>/main` and `/get/thumb/<book>/main?sz=...`
- book-token parsing driven acquisition lookups
- cover size coercion and placeholder fallback behavior

## Host Contract

Hosts now provide explicit methods such as:

- `acquisition_split_book_token(...)`
- `acquisition_work_row(...)`
- `acquisition_work_image_row(...)`
- `acquisition_resolve_storage_image(...)`
- `acquisition_resolve_image_target(...)`
- `acquisition_work_file_rows(...)`
- `acquisition_download_name_for_file_row(...)`
- `acquisition_serve_file_download(...)`
- response helpers for text / bytes / redirect / file streaming

## Current Hosts

- `surfaces/web_calibre_readonly`
- `surfaces/opds_readonly` via a direct `ReadOnlyWebApplication` host implementation

## Why

This reduces the remaining protocol behavior owned by `web_calibre_readonly`:

- OPDS route/feed behavior is already shared under `surfaces/opds`
- acquisition/download compatibility is now also shared
- remaining inheritance is increasingly about the HTML/compatibility surface rather than protocol implementation

## Validation

- `pytest -q tests/surfaces/test_acquisition_api.py tests/surfaces/test_opds_api.py tests/surfaces/test_opds_readonly.py tests/surfaces/test_web_calibre_readonly.py tests/surfaces/test_web_readonly.py`
  - `29 passed`
- `python -m py_compile src/LiuXin_alpha/surfaces/acquisition/api.py src/LiuXin_alpha/surfaces/opds/api.py src/LiuXin_alpha/surfaces/opds_readonly/app.py src/LiuXin_alpha/surfaces/web_calibre_readonly/app.py tests/surfaces/test_acquisition_api.py tests/surfaces/test_opds_api.py tests/surfaces/test_opds_readonly.py`
  - passed

## TODO

- if another host wants Calibre-compatible acquisition routes without inheriting the Calibre web UI, implement `AcquisitionHostApi` directly
- later reconsider whether the remaining shared cover/image helpers should move into a smaller neutral helper layer too
