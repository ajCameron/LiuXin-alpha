# Tkinter GUI Implementation Plan

This is the staged plan for turning the Tkinter GUI spike into an operational
LiuXin desktop surface. It complements
`docs/development/tkinter-gui-architecture.md`.

The key boundary is:

- read-only inspection may use the local database/read-model/cache adapters
- mutations must route through core command paths
- long-running operations must route through core jobs or a GUI task wrapper
  that mirrors core job semantics

## Current Starting Point

The `tkinter-gui-foundation` branch currently carries the original
`tkinter-gui-surface` spike forward on top of `origin/main` after PR #36 merged
the metadata fast-start stack. The spike provides:

- `LiuXin_alpha.surfaces.tkinter_gui`
- a combined `app.py` with backend, state-ish logic, and widgets
- a read-only table list, row grid, search, raw detail panel, and item metadata
  hydration
- non-display tests for the backend/parser

Phase 1 has split the spike into the architecture described in the GUI
architecture note. The GUI remains read-only while the next phases add session
and task-runner behavior.

## Phase 0: Branch And Base Hygiene

Status on 2026-05-12: complete enough to begin the refactor. The active branch
is `tkinter-gui-foundation`, based on `origin/main` after the fast-start
promotion landed.

1. Keep the GUI branch based on current `main`; pull/rebase before opening the
   first GUI PR if `main` moves materially.
2. Avoid carrying any generated database or smoke-test artifacts into commits.
3. Keep the GUI startup path on the read-only fast-open defaults:
   - no storage manager by default
   - no maintenance service by default
   - no bootstrap row repairs by default
4. Keep `tkinter` imports lazy so headless CI can import and test the package.

Acceptance:

- `python3 -m LiuXin_alpha.surfaces.tkinter_gui --help` works without opening a
  display.
- Backend tests run in CI without `tkinter` installed.

## Phase 1: Refactor The Spike Into Stable Modules

Status on 2026-05-12: initial split complete.

Create the package shape:

```text
src/LiuXin_alpha/surfaces/tkinter_gui/
  __init__.py
  __main__.py
  app.py
  backend.py
  session.py
  state.py
  tasks.py
  controller.py
  views/
    __init__.py
    database_toolbar.py
    table_sidebar.py
    row_grid.py
    inspector.py
    metadata_panel.py
    status_bar.py
```

Move the current spike code:

- non-visual database operations into `backend.py`
- dataclasses such as config/page/search/selection into `state.py`
- root window assembly into `app.py`
- event coordination into `controller.py`
- concrete Tk widgets into `views/`

Acceptance:

- The GUI remains read-only and functionally equivalent to the spike.
- Unit tests cover backend and state without display access.
- Widget modules are thin enough that most behavior is testable outside Tk.

## Phase 2: Core Session Adapter

Status on 2026-05-12: initial session adapter complete.

Add a GUI-owned session object that creates and owns:

- `Database`
- `Library`
- `CoreRuntime`
- local core proxies where useful
- `ReadModelBackend`
- optional metadata read source/cache snapshot

Recommended module:

```text
tkinter_gui/session.py
```

The session should mirror the terminal surface pattern:

- build `Library(database=db, close_database_on_close=False)`
- build `CoreRuntime(library=library, job_manager=...)`
- keep direct DB/read-model reads available for fast inspection
- expose explicit helpers for core command/query execution

Core-related integration points already exist:

- `CoreRuntime.execute_command(CoreCommand(...))`
- `CoreRuntime.execute_query(CoreQuery(...))`
- `CoreRuntime.invoke_command(target=..., method=..., ...)`
- `CoreRuntime.invoke_query(target=..., method=..., ...)`
- `LocalLibraryProxy`, `LocalDatabaseProxy`, `LocalJobsProxy`
- `refresh_metadata_read_source_after_write(owner)`

Acceptance:

- GUI status bar reports core availability.
- The session can execute `health` and `api.describe` queries.
- Closing the GUI closes the session and database cleanly.

## Phase 3: GUI Task Runner

Status on 2026-05-12: initial serial worker task runner complete and wired into
database open, table loading, row page loading, and metadata hydration.

Tk callbacks must not block the main event loop. Add a small task runner:

- background worker thread or `ThreadPoolExecutor`
- task request/result dataclasses
- `queue.Queue` result channel
- `root.after(...)` polling on the Tk thread
- cancellation flag for cancellable GUI tasks

Use it for:

- opening a database
- loading table schema
- loading row pages
- hydrating metadata
- refreshing cache/read sources

Core jobs should still be used for long-running core operations such as sync,
ingest, and storage work. The GUI task runner is for UI-level responsiveness;
core jobs are for LiuXin operations that need job tracking.

Acceptance:

- Opening/loading a large database does not freeze the window.
- Failed tasks surface one user-facing error path.
- Tests cover task success, failure, and result polling without real Tk widgets.

## Phase 4: Read-Only Browser Completion

Status on 2026-05-12: initial completion pass done. The GUI has a schema panel,
task-aware status/control state, paged row loading, search, row details, and
explicit metadata hydration. Remaining manual validation is launching the real
window in an environment with `tkinter` installed.

Finish the first production GUI slice:

- database open/reload toolbar
- filterable table sidebar
- selected table schema display
- paged row grid
- column search
- row detail inspector
- metadata hydrate button for rows with `item_id`
- status bar showing active table, row range, and task state

Read paths can use the local backend/read model directly, but should be shaped
so cache-backed reads can be swapped in later.

Acceptance:

- User can inspect any table without write access.
- Large tables page instead of loading all rows.
- Metadata hydration is lazy and explicit.
- The backend can be smoke-tested against an ISFDB test database.

## Phase 5: Cache And Read Source Mode

Status: implemented for read-only inspection.

Add read-source selection to the session:

- direct database
- schema-backed cache
- later optional faster immutable cache backend

Reuse existing read-source/cache helpers rather than inventing GUI-local cache
logic. The GUI should keep one refresh hook:

```python
refresh_metadata_read_source_after_write(gui_session_or_backend)
```

Acceptance:

- User can select direct DB or cache source at startup with `--read-source`.
- The toolbar can switch an open session between direct and cache reads, using
  `Refresh Source` to apply the selected source or reload the current one.
- Cache refresh/reload is visible in status.
- After writes, the GUI refreshes the attached read source best-effort.

Current note: the schema-backed cache path works but can take minutes to load
against generated ISFDB smoke databases. Keep direct mode as the default until a
lighter immutable read snapshot exists.

## Phase 6: Core-Backed Metadata Writes

Status: non-visual core/session/backend write path implemented.

Do not add GUI metadata editing by mutating rows directly.

First add explicit core command handlers for metadata operations, then wire the
GUI to those commands. Candidate command names:

- `metadata.write`
- `metadata.relation.add`
- `metadata.relation.remove`
- `metadata.tags.replace`
- `metadata.labels.replace`
- `metadata.genre.replace`
- `metadata.series.replace`
- `metadata.identifiers.replace`
- `metadata.opf.import`

Implemented so far:

- `metadata.write`
- `metadata.tags.replace`
- `metadata.labels.replace`
- `metadata.genre.replace`
- `metadata.series.replace`
- `metadata.identifiers.replace`
- Tk session/backend adapters that execute the core command, clear stale
  hydrator state, and refresh the selected read source best-effort.

Still pending:

- Tk edit forms and report display
- relation add/remove commands
- OPF import command

These commands should call the existing metadata writer/report APIs and return
structured write reports. The GUI should display:

- rows added/updated/removed
- links added/removed
- skipped fields
- errors
- whether a read source refresh ran

Short-term fallback:

- use `CoreRuntime.invoke_command(target="library" or "database", ...)` only for
  operations already safely wrapped on `Library`
- avoid GUI-only direct database writes

Acceptance:

- Editing tags/labels/genres/series goes through core.
- The user sees the metadata write report before/after refresh.
- Tests assert GUI write adapters call core commands, not database methods.

## Phase 7: Core-Backed Generic Row Mutations

For generic row editing, prefer stable library methods over low-level direct DB
mutations:

- `Library.update_row_fields`
- `Library.describe_row_delete_impact`
- any future explicit row create/delete commands

Where generic core commands are missing, add them before broad GUI editing:

- `row.update`
- `row.delete.preview`
- `row.delete`
- `row.create`

The GUI should distinguish:

- metadata-aware edits, which use metadata commands
- generic row edits, which use row commands
- read-only views/tables, which are not editable

Acceptance:

- Generic edit form refuses id-column edits.
- Delete has a preview/confirmation path.
- Write completion refreshes read models/cache.

## Phase 8: Core Job Panel

Add a job/status panel that can observe core jobs:

- list jobs
- inspect one job
- wait/poll
- cancel
- show job log path/output preview where available

Use existing core job endpoints:

- `jobs.list`
- `jobs.get`
- `jobs.wait`
- `jobs.cancel`
- `sync.store.start`
- `sync.store.cancel`

Acceptance:

- Long-running sync/storage operations are visible and cancellable.
- GUI status does not depend on terminal-only job display code.

## Phase 9: OPF Tools

Add OPF import/export around the metadata APIs:

- export selected item metadata to OPF
- import OPF into a preview metadata object
- compare current DB metadata against imported OPF
- write selected fields through core-backed metadata commands

Acceptance:

- OPF import never writes until explicitly confirmed.
- Write-back returns and displays the metadata write report.

## Phase 10: Storage And File Panels

Add storage/file panels after metadata editing is stable:

- stores list and store details
- files table filtered by selected work/item/store
- locate/open file actions where safe
- refresh storage manager through core
- sync/ingest actions as core jobs

Acceptance:

- Storage refresh and sync run through core/job paths.
- Read-only stores are not presented as writable.
- File actions report unavailable/offline states clearly.

## Phase 11: Remote Core Readiness

Do not require remote core immediately, but keep the surface ready for it:

- keep core calls behind a GUI core adapter
- make command/query envelopes JSON-friendly
- avoid passing raw `Row` objects across core boundaries for transport-stable
  commands
- use `api.describe` to discover capability where possible

Acceptance:

- The GUI can use in-process core now.
- Later remote-core support does not require rewriting widgets.

## Phase 12: Test Matrix

Keep these tests as the GUI grows:

- backend fake-DB tests
- state transition tests
- task runner tests
- core adapter tests with fake runtime
- command wiring tests proving writes go through core
- read-only real fixture smoke tests
- optional manual Tk smoke checklist for environments with `tkinter`

Manual smoke checklist:

```bash
PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.tkinter_gui --database path/to/isfdb.test_db
```

Verify:

- window opens
- tables populate
- selecting `items` loads rows
- search works
- selecting a row shows details
- metadata hydration works
- closing the window releases database handles

## Definition Of Operational

The GUI can be considered operational when:

- it opens real LiuXin databases reliably
- read-only browsing is responsive on ISFDB-scale fixtures
- metadata inspection is useful without blocking the UI
- edits/write-backs go through core and return reports
- cache/read source refresh is automatic after successful writes
- long operations show up as jobs
- the non-GUI logic is covered by CI
