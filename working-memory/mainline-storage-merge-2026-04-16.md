# Mainline Storage Merge - 2026-04-16

Context: `main` moved forward significantly after merging both
`codex/port-mainline-tests-2026-04-16` and `pre-alpha-code-review-storage`.
This note records the major storage/jobs/metadata shape that landed, plus which
test-port assumptions were later invalidated.

## What Changed

- The storage subsystem was re-cut around three explicit layers:
  - `StorageManager`
  - `StoreContainer`
  - `StorePlugin`
- The public storage API was split into narrower packages instead of one broad
  surface:
  - `backup_api`
  - `info_containers_api`
  - `policy_apis`
  - `store_api`
  - `store_container_api`
  - `storage_manager_api`
  - `store_plugin_api`
- The database/storage schema changed materially:
  - new storage policy tables
  - new backup workflow tables
  - new backup presence link tables
  - expanded digital-asset oriented tables
- A durable top-level jobs subsystem landed under `src/LiuXin_alpha/jobs`
- Metadata gained the item-container layer:
  - `ItemContainer`
  - `ItemMetadataContainerAPI`
  - `ItemMetadataHydrator`
- Store backend coverage expanded substantially, including:
  - `ftp_readonly`
  - `on_disk_flat`
  - `single_file_sqlite`
  - `squashfs_build`
  - `squashfs_readonly`
  - several on-disk and remote-readonly backends

## What This Invalidated

- `DriverWrapper` is no longer abstract on `main`
- `Row.__hash__` now exists in `src/LiuXin_alpha/databases/row.py`
- `rename_item` now exists in both:
  - `src/LiuXin_alpha/databases/maintenance/engine.py`
  - `src/LiuXin_alpha/databases/api/maintenance.py`
- Earlier test-port reasoning that assumed a partially restored `interfaces`
  tree is now stale; the live tree has since been normalized onto `surfaces`

## Follow-Ups After The Later Surfaces Pass

1. `field_metadata` import normalization

   - Resolved in the later normalization pass
   - Canonical home is now `src/LiuXin_alpha/surfaces/field_metadata.py`
   - The bridge module now imports directly from `surfaces`

2. CLI / terminal surface exposure

   - The earlier package-exposure concern has been superseded by the rename
     pass: the live tree now uses `LiuXin_alpha.surfaces.*`
   - The relevant tests now live under `tests/surfaces`

3. Storage seams found while validating moved surfaces

   - `bootstrap_storage_manager()` had drifted enough to create a
     `StorageManager` without binding the live database first; this was fixed
     during validation of the moved surfaces
   - SquashFS provenance is still wired to legacy `file_derivations`, while
     the live FRBR schema exposes `digital_asset_derivations`

4. Still-open code seam outside storage

   - `file_formats` expects `LiuXin_alpha.surfaces.gui2`
   - there is still no `src/LiuXin_alpha/surfaces/gui2/` package in this
     checkout

## Suggested Next Triage

1. Decide whether to restore/port `surfaces.gui2` or redirect those imports to
   the real current Qt helper location.
2. Port SquashFS provenance from `file_derivations` to
   `digital_asset_derivations`.
3. Revisit any remaining stale xfails only after those live source seams are
   cleared.
