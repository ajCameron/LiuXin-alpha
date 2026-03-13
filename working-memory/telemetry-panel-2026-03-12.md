# Telemetry Panel - 2026-03-12

## Scope

Added optional database-write telemetry to the terminal interface, primarily for the windowed UI.

This was driven by the store sync case where the job counter was moving but visible file counts were not, making it hard to see whether work was landing in the database or only being probed externally.

## What Changed

- Added lightweight DB write telemetry recording in `src/LiuXin_alpha/databases/database/dirtied_mixin.py`.
- Wrapped both dirtied-record queue writes and trigger/maintainer callbacks so we can observe:
  - queue activity
  - trigger-side dirty callbacks
  - interlink dirty callbacks
- Added `Database.get_write_telemetry_snapshot(...)` to expose a live snapshot.
- Added an optional telemetry auxiliary pane to the windowed terminal UI.
- Added a `telemetry panel` terminal command to attach/detach the pane and choose tracked tables.

## Windowed UI Behavior

The telemetry pane is separate from:

- the main status board
- the job output panel

It shows:

- observed write-event total
- in-memory dirtied queue depth
- persisted dirtied queue depth
- tracked table totals
- per-table deltas since the panel was attached
- recent observed events with source/table/row/reason

Default tracked tables are:

- `files`
- `folders`
- `items`
- `works`
- `stores`

## Commands

- `telemetry panel`
- `telemetry panel on`
- `telemetry panel on files folders items`
- `telemetry panel off`

Alias group:

- `debug panel ...`

If the browser does not support a dedicated telemetry pane, the command falls back to emitting a one-shot snapshot in the normal terminal output.

## Important Fix

The first telemetry test run exposed an init-order bug: `write_telemetry` was being installed after DB startup paths that already tried to wrap the maintainer callback.

That was fixed by moving telemetry object creation to the top of `Database.__init__`.

## Validation

Passed:

- `pytest -q tests/interfaces/test_windowed_ui.py tests/interfaces/test_text_browser.py -k 'telemetry or windowed_ui or parser_accepts_windowed_mode_options or main_windowed_mode_dispatches'`
- `pytest -q tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py -k 'telemetry or dirty_records_queue or dirty_record_sql_function_is_registered_and_enqueues or close_breaks_cycles_including_dirty_records_queue'`
- `python3 -m py_compile src/LiuXin_alpha/databases/database/__init__.py src/LiuXin_alpha/databases/database/dirtied_mixin.py src/LiuXin_alpha/interfaces/terminal/text_browser.py src/LiuXin_alpha/interfaces/terminal/windowed_ui.py src/LiuXin_alpha/interfaces/terminal/commands/telemetry.py`

## Next Read Points

- `src/LiuXin_alpha/databases/database/dirtied_mixin.py`
- `src/LiuXin_alpha/interfaces/terminal/windowed_ui.py`
- `src/LiuXin_alpha/interfaces/terminal/commands/telemetry.py`
