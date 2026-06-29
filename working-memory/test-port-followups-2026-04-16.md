# Test Port Follow-Ups - 2026-04-16

Historical note: this file captured the first donor-to-mainline test port before the storage review merge landed.

Status update after `main` moved to `06c9dbf`:

- the old `DriverWrapper is abstract` conclusion is no longer true on `main`
- `Row.__hash__` now exists
- `rename_item` now exists

That means the earlier xfail reasoning in several merged tests is now stale and should be treated as cleanup work, not as an active source limitation.

Still-live follow-ups from the original port:

1. Source import normalization for `field_metadata` is still incomplete.

   Current source imports still include `LiuXin_alpha.surfaces.field_metadata` in:

   - `src/LiuXin_alpha/customize/cache/base_tables.py`
   - `src/LiuXin_alpha/library/backend.py`
   - `src/LiuXin_alpha/library/caches/calibre/tables/base.py`
   - `src/LiuXin_alpha/metadata/book/base.py`
   - `src/LiuXin_alpha/metadata/book/json_codec.py`

   The test helper in `tests/support/_import_compat.py` may still be needed until those imports are normalized onto the bridge or a canonical module path.

2. Interface package exposure is still incomplete for the terminal-side tests.

   Current state on `main`:

   - `src/LiuXin_alpha/surfaces/` exists
   - `LiuXin_alpha.surfaces.cli` does not exist
   - `LiuXin_alpha.surfaces.terminal` does not exist

   This is still the main reason the CLI/text-browser/windowed-UI tests remain skip-oriented.

Superseding note:

- `working-memory/mainline-storage-merge-2026-04-16.md`
