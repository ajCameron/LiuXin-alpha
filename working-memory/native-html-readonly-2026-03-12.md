# Native HTML Readonly - 2026-03-12

## Scope

Added a second remote HTML crawler backend:

- `native_html_readonly`

This is intended to complement `wget_html_readonly`, not replace it.

## What It Is

`native_html_readonly` uses:

- stdlib HTTP requests
- stdlib HTML parsing
- breadth-first crawl over HTML pages
- per-store HTTP rate limiting
- optional robots.txt respect

It is deliberately lightweight:

- no JS rendering
- no framework dependency
- no payload mirroring

## Main Benefit

Unlike the current `wget` spider path, the native crawler can descend through non-file-like HTML pages and still discover linked book assets deeper in the site.

That is the main low-hanging-fruit improvement over the current Faded Page-style flow.

## Wiring Added

- new backend plugin under `src/LiuXin_alpha/storage/store_backend_plugins/native_html_readonly/`
- storage reconcile helpers:
  - `ensure_native_html_readonly_store`
  - `register_native_html_readonly_store_files`
  - `register_native_html_readonly_with_database_path`
- `StorageManager` bootstrap alias + policy parsing
- `Library.register_native_html_store(...)`
- terminal `sync store` routing for `native_html_readonly`
- `new store` preset for `native_html_readonly`

## Terminal Behavior

`sync store <id>` now recognizes stores with:

- `store_kind = native_html_readonly`
- or `store_access_protocol = native_html`

The native path reuses the current generic spider controls already exposed in terminal sync:

- `--max-http-requests-per-hour`
- `--crawler-max-depth`
- `--crawler-timeout-s`
- `--crawler-parent` / `--crawler-no-parent`
- `--crawler-span-hosts`
- `--crawler-ignore-robots` / `--crawler-respect-robots`
- `--crawler-user-agent`

## Validation

Passed:

- `pytest -q tests/storage/store_backend_plugins/native_html_readonly/test_native_html_readonly_storage_backend.py tests/storage/reconcile/test_native_html_store_db_sync.py tests/library/test_native_html_ingest_library.py tests/storage/api/test_storage_manager_database_wiring.py -k 'native_html'`
- `pytest -q tests/surfaces/test_text_browser.py -k 'native_html'`

## Next Step

Test the backend against one real candidate site and then decide whether:

1. generic native crawling is enough
2. site-specific adapter logic is needed
3. a heavier crawler stack is justified
