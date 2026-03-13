# Ingest Public API Move - 2026-03-12

## Decision

The public remote-HTML registration API now lives under `src/LiuXin_alpha/ingest/`, not `src/LiuXin_alpha/storage/reconcile/`.

Current public entrypoints:

- `LiuXin_alpha.ingest.register_wget_html_readonly_store_files`
- `LiuXin_alpha.ingest.register_wget_html_readonly_with_database_path`
- `LiuXin_alpha.ingest.register_native_html_readonly_store_files`
- `LiuXin_alpha.ingest.register_native_html_readonly_with_database_path`

## What Changed

Added:

- `src/LiuXin_alpha/ingest/remote_html.py`

Rewired callers:

- `src/LiuXin_alpha/library/library.py`
- `src/LiuXin_alpha/interfaces/terminal/commands/sync.py`
- remote-HTML tests now import from `LiuXin_alpha.ingest`

Removed old public exports from:

- `src/LiuXin_alpha/storage/reconcile/__init__.py`

Removed the old HTML `register_*` wrapper definitions from:

- `src/LiuXin_alpha/storage/reconcile/store_db_sync.py`

`store_db_sync.py` still owns the internal HTML `ensure_*_readonly_store(...)` helpers for store-row creation/bootstrap. Public registration now wraps those from the ingest side.

## Import Graph Fixes

Moving the public API exposed two package-init cycles:

1. `ingest.__init__` eagerly importing `remote_html`
2. `storage.__init__` eagerly importing `reconcile`

Both packages now expose those modules lazily instead:

- `src/LiuXin_alpha/ingest/__init__.py`
- `src/LiuXin_alpha/storage/__init__.py`

The shared ingest pipeline also now imports `UnmanagedDiskRegistrationReport` lazily inside the function body so importing the pipeline does not force `storage.reconcile.__init__`.

## Why This Matters

This makes the architectural boundary real:

1. `storage` handles actual storage backends and reconciliation helpers
2. `ingest` owns acquisition/discovery-facing public APIs
3. remote HTML crawling no longer looks like a storage concern from the public surface

It also reduces the risk of import-order bugs while the ingest area keeps growing.

## Validation

Passed:

- `pytest -q tests/storage/reconcile/test_wget_html_store_db_sync.py tests/storage/reconcile/test_native_html_store_db_sync.py`
- `pytest -q tests/interfaces/test_text_browser.py tests/library/test_native_html_ingest_library.py tests/storage/api/test_storage_manager_database_wiring.py -k 'native_html or wget_html'`

## Next Step

Likely next slices:

1. move `ensure_*_html_readonly_store(...)` ownership out of `store_db_sync.py`
2. rename terminal crawler flags so native mode stops using `--wget-*`
3. add site adapters/classifiers under `ingest`
