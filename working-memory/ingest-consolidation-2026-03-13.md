# Ingest Consolidation - 2026-03-13

## Decision

Remote HTML ingest now has:

1. backend-neutral terminal crawler flags
2. its own report dataclass under `ingest`

## What Changed

### Terminal Flags

`sync store` now documents and accepts backend-neutral shared crawler controls:

- `--crawler-recurse` / `--crawler-no-recurse`
- `--crawler-max-depth`
- `--crawler-timeout-s`
- `--crawler-parent` / `--crawler-no-parent`
- `--crawler-span-hosts` / `--crawler-no-span-hosts`
- `--crawler-ignore-robots` / `--crawler-respect-robots`
- `--crawler-user-agent`
- `--crawler-incremental-db-writes` / `--crawler-no-incremental-db-writes`

The old `--wget-*` forms for those shared controls are still accepted as compatibility aliases.

Kept as wget-specific:

- `--wget-arg`
- `--wget-verbose`
- `--wget-no-verbose`

### Report Model

Added:

- `src/LiuXin_alpha/ingest/models.py`
  - `RemoteHtmlRegistrationReport`

Updated:

- `src/LiuXin_alpha/ingest/pipelines/remote_html.py`
- `src/LiuXin_alpha/ingest/remote_html.py`
- `src/LiuXin_alpha/library/library.py`
- `src/LiuXin_alpha/ingest/__init__.py`

Remote HTML ingest no longer imports its report model from `storage.reconcile.models`.

## Why This Matters

This removes two lingering mismatches:

1. native HTML sync no longer looks like a wget-only interface from the terminal
2. `ingest` no longer depends on a reconciliation-owned dataclass for its own public/reporting surface

The result is a cleaner seam between:

- storage reconciliation for disk/rclone/squashfs
- ingest for remote HTML discovery and registration

## Validation

Passed:

- `pytest -q tests/storage/reconcile/test_wget_html_store_db_sync.py tests/storage/reconcile/test_native_html_store_db_sync.py tests/library/test_native_html_ingest_library.py tests/storage/api/test_storage_manager_database_wiring.py`
  - `32 passed`
- `pytest -q tests/surfaces/test_text_browser.py tests/core/test_core_runtime_phase1.py -k 'sync_store or sync\\.store\\.start or crawler'`
  - `48 passed, 303 deselected`

## Next Step

Likely next slices:

1. rename `get_default_wget_http_requests_per_hour()` / preference keys if you want terminal and preference naming fully backend-neutral
2. move remote-HTML-specific policy JSON shaping into smaller helpers or adapters
3. add site adapters/classifiers under `ingest`
