# Crawler Default Preference Rename - 2026-03-13

## Decision

Remote HTML crawl rate defaults are now owned by a backend-neutral shared preference key:

- `crawler_http_max_requests_per_hour_default`

Legacy backend-specific keys remain as fallback-only reads:

- `wget_http_max_requests_per_hour_default`
- `native_html_max_requests_per_hour_default`

## What Changed

Added:

- `src/LiuXin_alpha/ingest/sources/crawler_defaults.py`

That module now owns:

- the canonical shared default value
- the canonical shared preference key
- legacy fallback key names
- `get_default_crawler_http_requests_per_hour(...)`

Updated active callers to use the shared helper:

- `src/LiuXin_alpha/ingest/remote_html.py`
- `src/LiuXin_alpha/surfaces/terminal/commands/sync.py`
- `src/LiuXin_alpha/storage/store_backend_plugins/wget_html_readonly/*`
- `src/LiuXin_alpha/storage/store_backend_plugins/native_html_readonly/*`
- `src/LiuXin_alpha/ingest/sources/wget_html.py`
- `src/LiuXin_alpha/ingest/sources/native_html.py`

Updated defaults:

- `src/LiuXin_alpha/preferences.py`

The canonical default now ships under `crawler_http_max_requests_per_hour_default`.

## Why This Matters

The terminal surface, ingest API, and preference/config layer now use the same backend-neutral language.

That removes the last obvious `wget`-named default from code paths shared by:

1. `wget_html_readonly`
2. `native_html_readonly`

It also keeps existing local configuration from breaking, because old backend-specific preference keys are still honored if the new shared key is absent.

## Validation

Passed:

- `pytest -q tests/preferences/test_preferences_regression.py tests/storage/store_backend_plugins/wget_html_readonly/test_wget_html_readonly_storage_backend.py tests/storage/store_backend_plugins/native_html_readonly/test_native_html_readonly_storage_backend.py tests/storage/reconcile/test_wget_html_store_db_sync.py tests/storage/reconcile/test_native_html_store_db_sync.py tests/library/test_native_html_ingest_library.py tests/storage/api/test_storage_manager_database_wiring.py`
  - `54 passed`
- `pytest -q tests/surfaces/test_text_browser.py tests/core/test_core_runtime_phase1.py -k 'sync_store or sync\\.store\\.start or crawler'`
  - `48 passed, 303 deselected`

## Follow-up

Likely next slices:

1. remove the legacy `get_default_wget_http_requests_per_hour()` / `get_default_native_html_http_requests_per_hour()` compatibility wrappers once callers are fully cleaned up
2. decide whether the compatibility `--wget-*` crawler flag aliases should stay or be removed after one more transition cycle
