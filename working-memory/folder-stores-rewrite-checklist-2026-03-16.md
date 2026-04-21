# Folder Stores Rewrite Checklist

Date: 2026-03-16

## Purpose

- Turn the high-level [folder-stores-rewrite-plan-2026-03-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/folder-stores-rewrite-plan-2026-03-16.md) into a concrete replacement checklist.
- Each legacy input is mapped to one of:
  - already covered in alpha
  - needs targeted replacement tests
  - retire with rationale

## Legacy Input Mapping

### 1. Generic driver baseline

Legacy source:
- [self_test.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/folder_stores/drivers/generic_driver_tests/self_test.py)

Legacy behaviors:
- store startup succeeds
- driver/root location exists
- valid path checks
- existing/non-existing file and directory location generation
- scratch file loading smoke

Current alpha coverage:
- [test_on_disk_existing_managed_drive.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/on_disk_existing_managed/test_on_disk_existing_managed_drive.py)
- [test_on_disk_unmanaged_drive.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/on_disk_unmanaged_drive/test_on_disk_unmanaged_drive.py)
- [test_on_disk_unmanaged_location.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/on_disk_unmanaged_drive/test_on_disk_unmanaged_location.py)
- [test_location_contract.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/location/test_location_contract.py)
- [test_location_glob_and_iter.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/location/test_location_glob_and_iter.py)
- [test_location_security_and_bounds.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/location/test_location_security_and_bounds.py)

Decision:
- `covered` for the modern storage model

Notes:
- the legacy assertion about “two folder stores should have been created” is obsolete
- it belongs to the old folder-store manager/bootstrap shape, not the alpha storage architecture
- `load_scratch_file` is also a harness-era concern and should not be ported directly

### 2. On-disk store health/marker checks

Legacy sources:
- [test_store.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin/folder_stores/drivers/on_disk/test_store.py)
- [test_store.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin/folder_stores/drivers/on_disk_flat/test_store.py)

Legacy behaviors:
- check store type
- check store marker file exists and matches identifier
- check read/write access
- seek moved stores by plausible paths

Current alpha coverage:
- [test_on_disk_existing_managed_drive.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/on_disk_existing_managed/test_on_disk_existing_managed_drive.py)
- [test_on_disk_unmanaged_drive.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/on_disk_unmanaged_drive/test_on_disk_unmanaged_drive.py)
- [test_storage_manager_impl.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/api/test_storage_manager_impl.py)

Decision:
- split

Covered:
- root creation
- read/write vs read-only mode reporting
- file existence and retrieval semantics

Retire:
- marker-file identity checks
- moved-store path seeking

Rationale:
- alpha stores use explicit `store_kind`, `store_root_uri`, storage-manager registration, and location bounds
- the old marker/seek logic is specific to the retired `folder_stores` model

### 3. On-disk-flat `writebytes` matrix

Legacy source:
- [writebytes_detailed_test.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/folder_stores/drivers/on_disk_flat/writebytes_detailed_test.py)

Legacy behaviors:
- write bytes into root
- write bytes into existing directory
- write bytes into many valid existing directories
- reject writing bytes over directories
- reject some nested implicit-parent creation cases
- reject overwrite onto existing files
- allow creation into non-existing file targets

Current alpha partial coverage:
- [test_on_disk_existing_managed_drive.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/on_disk_existing_managed/test_on_disk_existing_managed_drive.py)
- [test_location_filesystem_ops_more.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/location/test_location_filesystem_ops_more.py)
- [test_location_filesystem_rename_replace_more.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/location/test_location_filesystem_rename_replace_more.py)

Decision:
- `needs targeted replacement tests`

Recommended replacement:
- add a dedicated managed-write contract slice under:
  - `tests/storage/store_backend_plugins/on_disk_existing_managed/`
  - or `tests/storage/location/`

Replacement cases to add:
- writing new bytes at store root succeeds
- writing new bytes inside existing nested directory succeeds
- writing to a path whose parent directory does not exist has explicit, tested behavior
- writing over an existing file has explicit, tested behavior
- writing bytes to a directory path fails

Reason:
- alpha has good path/rename/replace coverage, but not the old write-matrix as a first-class contract

### 4. Zip-backed store validity/read behavior

Legacy source:
- [test_store.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin/folder_stores/drivers/zip/test_store.py)

Legacy behaviors:
- archive exists
- marker exists inside archive
- read/write capability checks
- seek moved archive path
- read archive contents through store abstraction

Current alpha analog:
- [test_squashfs_readonly_storage_backend.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/squashfs_readonly/test_squashfs_readonly_storage_backend.py)
- [test_squashfs_db_sync.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/reconcile/test_squashfs_db_sync.py)

Decision:
- split

Covered in the current archive-backed model:
- readonly archive startup/status
- file lookup and read
- listing archived files
- mutation rejection
- hash verification
- archive publication/reconcile behavior

Retire:
- zip-specific marker checks
- archive path seeking by plausible-path expansion
- zip writeability semantics

Rationale:
- alpha’s archive story is `squashfs_readonly`, not legacy writable zip folder stores
- do not revive zip-specific lifecycle assumptions unless zip support returns as a real product surface

### 5. Cover-cache path utilities

Legacy source:
- [utils_test.py](/home/blackjane/LiuXin-master/LiuXin-master/src/LiuXin_tests/folder_stores/cover_cache/on_disk/utils_test.py)

Legacy behaviors:
- theoretical cover name/path generation
- retrieving cached cover paths
- raising on missing cache entries

Current alpha seams:
- [test_images_api.py](/home/blackjane/LiuXin-alpha-wsl/tests/surfaces/test_images_api.py)
- [test_web_calibre_readonly.py](/home/blackjane/LiuXin-alpha-wsl/tests/surfaces/test_web_calibre_readonly.py)
- [test_metadata_files_and_covers.py](/home/blackjane/LiuXin-alpha-wsl/tests/metadata/containers/calibre_like_book_metadata/test_metadata_files_and_covers.py)
- legacy runtime still references cover-cache machinery in:
  - [backend.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/databases/backend.py)

Decision:
- `defer: no live alpha implementation to test directly`

Reason:
- this is no longer naturally a storage-backend concern
- it may belong under image/backend or database-backend tests instead
- there is no live `LiuXin_alpha.folder_stores.cover_caches.on_disk` implementation in this checkout
- if current cover-cache naming/path semantics are not a public contract anymore, retire the old test shape

Recommended next decision:
- [cover-cache-triage-2026-03-16.md](/mnt/c/Users/Thane-Winterscale/LiuXin-alpha-mainline/working-memory/cover-cache-triage-2026-03-16.md)
- only add focused tests if a real current implementation or replacement seam exists

## Priority Order

1. Add replacement tests for the on-disk managed write matrix.
2. Declare the marker/seek behaviors retired in the migration manifest notes.
3. Keep archive-backed coverage on the `squashfs` path rather than reviving zip-store tests.
4. Triage cover-cache semantics separately from storage.

## Immediate Concrete Additions

### Implemented now

- [test_on_disk_existing_managed_write_contract.py](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins/on_disk_existing_managed/test_on_disk_existing_managed_write_contract.py)
  - writes new bytes at store root
  - writes text inside an existing nested directory
  - rejects writing to a directory path
  - makes overwrite behavior explicit
  - makes missing-parent behavior explicit

Validation:
- `tests/storage/store_backend_plugins/on_disk_existing_managed/test_on_disk_existing_managed_drive.py`
- `tests/storage/store_backend_plugins/on_disk_existing_managed/test_on_disk_existing_managed_write_contract.py`
  - `9 passed`
- surrounding location contract slice:
  - `tests/storage/location/test_location_filesystem_sync.py`
  - `tests/storage/location/test_location_filesystem_ops_more.py`
  - `tests/storage/location/test_location_filesystem_rename_replace_more.py`
  - `22 passed, 6 skipped`

### Add next

- `tests/storage/store_backend_plugins/on_disk_existing_managed/test_on_disk_existing_managed_write_contract.py`
  - landed; use it as the replacement anchor for the legacy `writebytes` matrix
- next likely follow-on:
  - broaden managed-store coverage only if the old matrix reveals more behavior than the new contract already captures

### No direct replacement needed

- legacy generic driver startup/path generation smoke
- on-disk and zip marker/seek logic

### Separate triage item

- cover-cache utility semantics
