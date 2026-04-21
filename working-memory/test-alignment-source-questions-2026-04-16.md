# Test Alignment Source Questions - 2026-04-16

Context: this note originally captured the source-level questions that blocked
the post-merge test tidy on `main` at `06c9dbf`. Most of those questions were
resolved during the later `interfaces` to `surfaces` normalization pass.

## Resolved In The Normalization Pass

1. `FieldMetadata` home and imports

   - Canonical implementation: `src/LiuXin_alpha/surfaces/field_metadata.py`
   - Bridge module: `src/LiuXin_alpha/databases/field_metadata_bridge.py`
   - Live imports were updated onto `surfaces`
   - The temporary `tests/support/_import_compat.py` helper is gone

2. `LiuXin_alpha.surfaces` package importability

   - `src/LiuXin_alpha/surfaces/__init__.py` now lazy-loads submodules instead
     of eagerly importing the whole surface tree
   - Direct imports such as `LiuXin_alpha.surfaces.field_metadata` now work
     without partial-initialization failures

3. CLI / terminal package exposure

   - The live tree is now normalized onto `LiuXin_alpha.surfaces.*`
   - Surface tests live under `tests/surfaces`
   - The old question about restoring the legacy CLI / terminal package names
     is no longer the right framing

## Remaining Follow-Ups

1. `surfaces.gui2` is still missing

   - `file_formats` now consistently points at `LiuXin_alpha.surfaces.gui2`
   - There is still no `src/LiuXin_alpha/surfaces/gui2/` package in this
     checkout

2. SquashFS provenance still uses legacy lineage tables

   - The current CLI / reconcile path still targets `file_derivations`
   - The live FRBR schema in this checkout exposes `digital_asset_derivations`
   - Provenance-oriented surface CLI tests are therefore gated on the legacy
     table until that path is ported

## Current Validation Snapshot

Targeted moved-surface slice:

- `32 passed`
- `3 skipped`

Skip reasons still in force:

- three SquashFS provenance checks are gated because `file_derivations` is not
  present in the current FRBR schema
- the slice still emits one unrelated `titlecase.py` syntax warning during
  import
