# Folder Stores Rewrite Plan

Date: 2026-03-16

## Scope

- This note splits the legacy `folder_stores` work out of the DB-property salvage stream.
- It covers the original tests classified as `rewrite` in [legacy-test-migration-manifest-2026-03-16.csv](/home/blackjane/LiuXin-alpha-wsl/working-memory/legacy-test-migration-manifest-2026-03-16.csv).

## Why This Is Separate

- `folder_stores` changed in alpha by design.
- This is not a wrapper or import-compatibility problem.
- Any attempt to shim the old `folder_stores` API would hide real storage architecture drift and produce low-signal tests.

## Legacy Inputs

- [src/LiuXin_tests/folder_stores/cover_cache/on_disk/utils_test.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/folder_stores/cover_cache/on_disk/utils_test.py)
- [src/LiuXin_tests/folder_stores/drivers/generic_driver_tests/self_test.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/folder_stores/drivers/generic_driver_tests/self_test.py)
- [src/LiuXin_tests/folder_stores/drivers/on_disk_flat/writebytes_detailed_test.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/folder_stores/drivers/on_disk_flat/writebytes_detailed_test.py)
- [src/LiuXin/folder_stores/drivers/on_disk/test_store.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin/folder_stores/drivers/on_disk/test_store.py)
- [src/LiuXin/folder_stores/drivers/on_disk_flat/test_store.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin/folder_stores/drivers/on_disk_flat/test_store.py)
- [src/LiuXin/folder_stores/drivers/zip/test_store.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin/folder_stores/drivers/zip/test_store.py)

## Rewrite Targets

- [tests/storage/store_backend_plugins](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins)
- [tests/storage/api](/home/blackjane/LiuXin-alpha-wsl/tests/storage/api)
- [tests/storage/reconcile](/home/blackjane/LiuXin-alpha-wsl/tests/storage/reconcile)
- [tests/library](/home/blackjane/LiuXin-alpha-wsl/tests/library)

## Behavior Buckets

### 1. Backend contract behavior

Legacy intent:
- generic driver correctness
- on-disk store behavior
- zip-backed store behavior
- on-disk-flat byte-write semantics

Rewrite seam:
- backend contract tests under `tests/storage/store_backend_plugins`
- storage-manager behavior under `tests/storage/api`

### 2. Reconcile and ingest behavior

Legacy intent:
- what concrete files become visible to the library
- how remote/local content is materialized or enumerated

Rewrite seam:
- `tests/storage/reconcile`
- where needed, ingest-facing tests in `tests/library`

### 3. Cover/image cache behavior

Legacy intent:
- cache-path utilities
- derived artifact placement and repeatability

Rewrite seam:
- do not recreate old cache utilities verbatim
- rewrite around current image/storage/library seams only if the behavior still matters

## Non-Goals

- Do not recreate `LiuXin_alpha.folder_stores` as a compatibility layer.
- Do not preserve old test class hierarchy or fixture builders just because they existed.
- Do not block DB-property salvage on this work.

## Recommended Order

1. Finish DB-property salvage normalization independently.
2. Create a separate storage rewrite checklist for legacy `folder_stores` behaviors.
3. Start with generic backend contract coverage, because that gives the cleanest mapping into alpha.
4. Only then tackle on-disk-flat specifics and any remaining cache behavior.

## Definition Of Done

- Every legacy `folder_stores` test is either:
  - replaced by a current storage/backend/reconcile test, or
  - retired with a storage-redesign rationale
- No legacy `folder_stores` import path remains on the critical path for test-support loading

## Concrete Checklist

- Detailed replacement mapping now lives in:
  - [folder-stores-rewrite-checklist-2026-03-16.md](/home/blackjane/LiuXin-alpha-wsl/working-memory/folder-stores-rewrite-checklist-2026-03-16.md)
