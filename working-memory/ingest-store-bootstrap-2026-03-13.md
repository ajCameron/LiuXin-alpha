# Ingest Store Bootstrap Move - 2026-03-13

## Decision

The HTML `ensure_*_readonly_store(...)` helpers now live in `src/LiuXin_alpha/ingest/remote_html.py`, alongside the public remote-HTML registration wrappers.

Moved:

- `ensure_wget_html_readonly_store(...)`
- `ensure_native_html_readonly_store(...)`

## What Changed

`src/LiuXin_alpha/ingest/remote_html.py` now owns:

- remote URL normalization
- minimal schema checks for `stores`
- remote HTML store-row upsert logic
- backend option construction
- HTML store policy JSON generation
- the public `register_*` wrappers

`src/LiuXin_alpha/storage/reconcile/store_db_sync.py` no longer imports or defines:

- `WgetBackendOptions`
- `WgetHtmlReadOnlyStorageBackend`
- `NativeHtmlBackendOptions`
- `NativeHtmlReadOnlyStorageBackend`
- HTML `ensure_*`

That file is now back to local disk + rclone reconciliation responsibilities.

## Why This Matters

The previous state still had HTML store bootstrap logically owned by storage reconciliation, even after the public ingest API moved.

This change makes the boundary more coherent:

1. `ingest.remote_html` owns remote HTML store setup and remote HTML ingest entrypoints
2. `ingest.pipelines.remote_html` owns the shared candidate-to-DB ingest loop
3. `storage.reconcile.store_db_sync` no longer needs to know about HTML crawler backends

## Validation

Passed:

- `pytest -q tests/storage/reconcile/test_wget_html_store_db_sync.py tests/storage/reconcile/test_native_html_store_db_sync.py tests/surfaces/test_text_browser.py tests/library/test_native_html_ingest_library.py tests/storage/api/test_storage_manager_database_wiring.py -k 'native_html or wget_html'`

Result:

- `24 passed, 349 deselected`

## Next Step

Likely next slices:

1. rename terminal crawler flags so native mode stops using `--wget-*`
2. move remote HTML report dataclasses out of `storage.reconcile.models` if ingest keeps growing
3. add site adapters/classifiers under `ingest`
