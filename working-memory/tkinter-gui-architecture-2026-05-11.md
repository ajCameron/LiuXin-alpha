# Tkinter GUI Architecture - 2026-05-11

## Context

Started a first `tkinter-gui-surface` spike after the metadata/surface fast-start
work. On 2026-05-12, after PR #36 landed on `main`, that spike was carried onto
the new `tkinter-gui-foundation` branch. The spike proves a stdlib Tkinter
surface can reuse LiuXin database and metadata hydrator paths, but it should be
treated as a prototype until the GUI is split into clearer layers.

Canonical design note:

- `docs/development/tkinter-gui-architecture.md`
- `docs/development/tkinter-gui-implementation-plan.md`

## Current Spike State

- Branch: `tkinter-gui-foundation`
- Base: `origin/main` after PR #36 promoted the fast-start changes.
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
- Non-display backend tests exist in `tests/surfaces/test_tkinter_gui.py`.

## Validation So Far

- `python3 -m pytest tests/surfaces/test_tkinter_gui.py` passed.
- `py_compile` passed for the new GUI package and tests.
- Real backend smoke against local ISFDB smoke DB worked:
  - latest Phase 4 smoke used
    `LiuXin_data/test_databases/isfdb_smoke_title_word_labels/isfdb_smoke_title_word_labels.test_db`
  - open: about 9.0s in this environment
  - table list: 206 tables in about 0.13s
  - first `items` page plus schema: 50 total rows, 5 rendered rows, 21 columns
    in about 0.22s
- The local Python environment does not have `tkinter` installed, so the actual
  window was not launched here.

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

Move on to cache/read-source mode or perform manual Tk validation on a machine
with `tkinter` installed before opening the first GUI PR.

Use the implementation plan as the detailed checklist. It explicitly routes GUI
mutations through core command paths, reserves direct DB/read-model access for
read-only inspection, and pushes long-running sync/storage operations through
core jobs.
