# Surfaces Normalization Follow-Ups

Date: 2026-04-16

## Context

The top-level package/test/docs naming has now been normalized from
`interfaces` to `surfaces` across the live tree.

## Main Code Follow-Up

- `file_formats` now consistently imports from `LiuXin_alpha.surfaces.gui2`,
  but there is no `src/LiuXin_alpha/surfaces/gui2/` package in this checkout.
- This was already an unresolved seam before the normalization pass: the old
  tree referenced `interfaces.gui2`, but no matching live package exists in the
  current mainline source tree either.
- The rename pass did not try to invent a `gui2` implementation. That needs a
  deliberate decision:
  - restore / port the missing `gui2` surface package, or
  - redirect those imports to the real current Qt helper location if it now
    lives elsewhere.
- `bootstrap_storage_manager()` had drifted far enough that it could build a
  `StorageManager` without binding the live database first. The normalization
  pass fixed that wiring so store bootstrap works again in moved surface paths.
- SquashFS provenance is still wired to legacy `file_derivations`, while the
  live FRBR schema in this checkout exposes `digital_asset_derivations`.
  Surface CLI provenance tests are currently gated on the legacy table until
  lineage writing/reading is ported to the FRBR workflow tables.

## Validation Intent

- The normalization pass verifies the `surfaces` package boundary, moved
  surface tests, benchmark script naming, and direct `field_metadata` imports.
- Validation also flushed out two non-rename storage seams while exercising the
  moved surfaces:
  - storage bootstrap needed the live database bound before loading stores
  - SquashFS provenance still targets legacy lineage tables
- GUI-adjacent `file_formats` imports remain a separate follow-up because the
  implementation package is missing from the repo snapshot itself.
