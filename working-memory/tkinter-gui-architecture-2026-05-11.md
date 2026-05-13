# Tkinter GUI Architecture - 2026-05-11

## Context

Started a first `tkinter-gui-surface` spike after the metadata/surface fast-start
work. On 2026-05-12, after PR #36 landed on `main`, that spike was carried onto
the `tkinter-gui-foundation` branch. After PR #37 merged, the current read-source
slice moved to `tkinter-gui-read-source-mode`. The spike proves a stdlib Tkinter
surface can reuse LiuXin database and metadata hydrator paths, but it should be
treated as a prototype until it has been manually validated with a Tk runtime.

Canonical design note:

- `docs/development/tkinter-gui-architecture.md`
- `docs/development/tkinter-gui-implementation-plan.md`

## Current Spike State

- Branch: `tkinter-gui-read-source-mode`
- Base: `origin/main` after PR #37 promoted the GUI foundation changes.
- Earlier spike branch/context: `tkinter-gui-surface`.
- New package exists under `src/LiuXin_alpha/surfaces/tkinter_gui/`.
- Phase 1 refactor has split the spike into `app.py`, `backend.py`,
  `controller.py`, `state.py`, `tasks.py`, and `views/`.
- Phase 2 added `session.py`, with GUI-owned `Database`, `Library`,
  `CoreRuntime`, local library/database/jobs proxies, core health/API helpers,
  read-source refresh hook support, and session-backed backend close behavior.
- Phase 3 added a display-independent `TkGuiTaskRunner` in `tasks.py`. The GUI
  uses one serial worker by default because `Database` is not thread-safe, and
  now schedules database open, table list loading, row page loading, and
  metadata hydration off the Tk thread.
- Phase 4 added `TableSchema`/schema display, task-aware control enabling,
  disabled metadata hydration until an item row is selected, disabled paging
  buttons when previous/next pages are unavailable, and clearer loading status.
- Phase 5 added read-source mode support. `TkGuiConfig` now carries direct/cache
  source settings, `TkGuiSession` owns direct database and cache-backed metadata
  read sources, the backend can switch/refresh the selected source, and the
  toolbar exposes source mode, cache type, and `Refresh Source`.
- The full-suite runners now have opt-in Tk smoke flags. Use
  `--only-tk-smoke` to create/use the repo venv and run just the real Tk smoke;
  use `--tk-smoke` to append it after the normal full suite.
- The follow-on `tkinter-gui-core-metadata-writes` branch adds the first
  non-visual metadata write path: core commands for `metadata.write` and
  field-specific replace operations, plus Tk session/backend adapters that run
  writes through core and refresh the selected read source.
- The write bridge expands iterable relation payloads before assigning them to
  metadata containers, because some fields such as `genre` and `series` accept
  scalar entries rather than whole lists.
- Non-display backend tests exist in `tests/surfaces/test_tkinter_gui.py`.

## Validation So Far

- `python3 -m pytest tests/surfaces/test_tkinter_gui.py` passed.
- `py_compile` passed for the new GUI package and tests.
- Real backend smoke against local ISFDB smoke DB worked:
  - latest smoke used
    `LiuXin_data/test_databases/isfdb_smoke_title_word_labels/isfdb_smoke_title_word_labels.test_db`
  - direct mode open: about 9.8s in this environment
  - direct mode table list: 206 tables in about 0.17s
  - direct mode first `items` page plus schema: 50 total rows, 5 rendered rows,
    21 columns in about 2.2s
  - schema-backed cache mode open: about 193s, then table list was effectively
    instant; first `items` page plus schema still took about 3.0s
- The local Python environment does not have `tkinter` installed, so the actual
  window was not launched here.
- Runner syntax/dry-run checks passed for the Tk smoke flags:
  `python3 scripts/run_full_test_suite.py --new-venv --python python3.12 --only-tk-smoke --dry-run`
  and the equivalent shell wrapper.
- Core metadata command and Tk backend write checks passed, including direct
  coverage for field-specific replace commands:
  `PYTHONPATH=src python3 -m pytest tests/core/test_core_runtime_metadata_commands.py tests/surfaces/test_tkinter_gui.py tests/metadata/containers/test_metadata_round_trip_examples.py::test_contract_liuxin_metadata_round_trips_editable_metadata_fields -q`.

## Architecture Decision

Before adding write/edit features, split the GUI into:

- `backend.py`: no Tk imports; database/read-source/search/metadata operations
- `state.py`: selected database/table/row/page/search/source state
- `tasks.py`: worker-thread task runner and Tk-safe result delivery
- `app.py`: root assembly and high-level controller wiring
- `views/`: toolbar, table sidebar, row grid, inspector, metadata panel, status

The first production PR should remain read-only: open DB, list/filter tables,
page/search rows, inspect raw row details, and hydrate item metadata lazily.

## Next Step

After the core-metadata-write branch, the next useful GUI work is Tk edit forms
and report rendering for the core-backed metadata commands. Manual Tk
validation remains blocked until this environment has `tkinter` installed.
Direct mode should remain the default; the current schema-backed cache path is
functional but too expensive as a fast-start path.

Use the implementation plan as the detailed checklist. It explicitly routes GUI
mutations through core command paths, reserves direct DB/read-model access for
read-only inspection, and pushes long-running sync/storage operations through
core jobs.
