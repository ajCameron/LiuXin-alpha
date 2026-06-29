# Discovery Sources Refactor - 2026-03-12

## Decision

Remote HTML crawling now lives under `src/LiuXin_alpha/ingest/sources/`, and the shared remote-HTML DB ingest loop lives under `src/LiuXin_alpha/ingest/pipelines/remote_html.py`.

Reason:

- `file_formats` is for parsing file payloads after acquisition
- `metadata.file_sources` is for metadata extraction from concrete files
- HTML crawling is earlier: remote discovery feeding storage ingest

## What Changed

Added a dedicated discovery layer:

- `DiscoverySourceAPI`
- `WgetHtmlDiscoverySource`
- `NativeHtmlDiscoverySource`
- shared HTML/wget utility modules

Kept the existing HTML store backends, but made them thin wrappers over the new discovery sources:

- `wget_html_readonly`
- `native_html_readonly`

Reconcile now uses one shared HTML-discovery ingest helper instead of separate duplicated `wget` and native processing loops.

## Why This Matters

This is the first clean separation between:

1. store identity / storage registration
2. remote URL discovery
3. DB reconciliation of discovered candidates

It reduces duplicated logic and makes it easier to add future discovery engines or site adapters.

## Compatibility Notes

- terminal commands and store rows stayed stable
- existing HTML backend tests still pass through the wrapper classes
- existing monkeypatch points on backend classes were preserved

## Validation

Passed:

- `pytest -q tests/storage/store_backend_plugins/wget_html_readonly/test_wget_html_readonly_storage_backend.py tests/storage/store_backend_plugins/native_html_readonly/test_native_html_readonly_storage_backend.py tests/storage/reconcile/test_native_html_store_db_sync.py -k 'wget_html or native_html'`
- `pytest -q tests/surfaces/test_text_browser.py -k 'native_html'`

## Next Step

Possible next slice:

1. move site-specific URL classification into adapters under the ingest layer
2. stop naming native crawler options `--wget-*` in the terminal
3. add direct tests for `ingest/sources/*` and `ingest/pipelines/*` instead of relying only on wrapper coverage
