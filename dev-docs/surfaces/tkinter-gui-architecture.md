# Tkinter GUI Architecture

This note describes the implemented architecture for LiuXin's desktop GUI,
built with Python's standard-library `tkinter` toolkit. The non-visual backend
uses the same transport-neutral Core boundary as the web, terminal, API, and
OPDS interfaces.

Detailed staged implementation plan:

- [Tkinter GUI implementation plan](tkinter-gui-implementation-plan.md)

## Goals

- Provide a practical desktop surface for browsing and later editing a LiuXin
  database.
- Reuse stable named Core operations instead of creating GUI-only data paths.
- Keep the non-visual logic testable in CI without a graphical display.
- Keep database and metadata work off the Tk main thread when it can block.
- Start with a small read-only browser, then layer metadata editing and
  operational tools on top.

## Tkinter Constraints

Tkinter uses a single root window and a single event loop:

```python
root = tk.Tk()
app = LiuXinTkApp(root)
root.mainloop()
```

All widget updates must happen on the Tk main thread. Button callbacks, list
selection handlers, search boxes, and menu actions all run on that same thread.
If a callback performs slow database, cache, storage, or metadata work directly,
the GUI will freeze until the callback returns.

The GUI should therefore separate:

- quick event handlers that update state and schedule work
- background tasks for slow database/cache/metadata operations
- main-thread render callbacks that update widgets with completed results

Use `ttk` widgets by default for native-looking controls:

- `ttk.Frame`, `ttk.PanedWindow`, `ttk.Notebook` for layout
- `ttk.Treeview` for row grids
- `ttk.Combobox` for table/search column selection
- `ttk.Button`, `ttk.Entry`, `ttk.Label` for toolbar/status controls

## Proposed Package Shape

The GUI should live under:

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

The current spike has most of this in `app.py`. Before adding much more
behavior, split it into the modules above.

## Layer Responsibilities

### Launcher

`__main__.py` and CLI parsing stay small:

- parse exactly one of `--database` or `--core-endpoint`, plus page and runtime
  flags
- build a `TkGuiConfig`
- create `tk.Tk()`
- run the app
- close resources during shutdown

Local and daemon-backed launch commands are:

```bash
PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.tkinter_gui --database path/to/library.test_db
PYTHONPATH=src python3 -m LiuXin_alpha.surfaces.tkinter_gui --core-endpoint http://127.0.0.1:8765
```

### Session

The session does not import `tkinter`. It owns one `SurfaceCoreSession`, one
`CoreClientAPI`, and presentation-safe Core model/database views. In local mode
the shared session composes and owns `CoreRuntime`; in daemon mode it borrows a
`RemoteCoreClient`. GUI behavior cannot reach Database, Library, Catalog,
Cache, storage, job-manager, or runtime implementation objects.

Read-source selection is a Core startup concern. Changing it requires reopening
the local database or reconfiguring the daemon rather than swapping subsystem
objects underneath an active GUI. Closing the GUI closes only the resources
owned by its `SurfaceCoreSession`.

### Backend

The backend must not import `tkinter`.

It owns read-oriented LiuXin operations:

- list tables and columns
- page rows from a table
- search table rows
- produce row labels and detail fields
- hydrate metadata for rows with `item_id`
- report the Core/read-source configuration selected at startup

This layer is the main CI test target. It is tested with fake Core clients,
locally composed fixture databases, and the RPC client without opening a
window.

### State

The state layer should be a small set of dataclasses representing:

- selected database path
- selected table
- selected row
- current page offset and page size
- active search column and text
- the read-source mode selected when Core was opened
- current task status
- dirty/write state once editing exists

State should be serializable enough that later sessions can restore window
position, last table, and preferred page size.

### Views

Views should mostly render data and forward user actions to the controller.
They should avoid direct database calls.

Recommended first set:

- Database/Core toolbar: local path or daemon endpoint, open, and reload.
- Table sidebar: filterable table list.
- Row grid: current table rows, paging, search controls.
- Inspector: selected raw row fields.
- Metadata panel: lazy metadata hydration for item rows.
- Status bar: current action, row range, errors.
  It should include core availability once a session is open.

### Controller

The controller coordinates between state, backend, views, and tasks:

- reacts to table selection
- requests row pages
- reacts to row selection
- schedules metadata hydration
- updates status
- displays errors through a single path

The controller should be the only layer that knows how the pieces are wired.

### Tasks

Potentially slow operations should run through a small task runner:

- open local or remote Core
- page large tables
- hydrate metadata
- write metadata
- OPF import/export
- storage/file operations

A simple standard-library approach is enough:

- `queue.Queue` for task results
- `threading.Thread` or `concurrent.futures.ThreadPoolExecutor` for workers
- `root.after(50, poll_queue)` to apply completed results on the Tk thread

Never update Tk widgets directly from worker threads.

GUI Core calls use a serial worker by default. This keeps the Tk event loop
responsive and matches the runtime's serialized access to its composed
database graph.

## Implemented GUI Slice

The current slice provides:

- open an existing LiuXin database
- show a filterable table list
- show a paged row grid for a selected table
- search within a selected column
- show raw row details
- hydrate and pretty-print metadata for `items` rows
- start without storage manager, maintenance, or bootstrap repairs unless
  explicitly requested

This keeps behavior aligned with the read-only web and terminal surfaces while
providing enough UI to be useful for metadata testing.

## Later Slices

After the read-only slice is stable:

- lighter cache/read snapshot backend for faster startup
- richer metadata inspector with structured W/E/M/I sections
- metadata edit forms for tags, labels, genres, series, identifiers, notes, and
  comments
- richer semantic write-back operations and conflict presentation
- explicit daemon-side cache/read-source administration
- OPF import/export tools
- storage/file panels for files, stores, and locator results
- job/status panels for long-running operations
- user preferences for default database, page size, and source mode

## Design Rules

- Keep GUI imports lazy enough that package imports work on headless CI systems.
- Keep non-GUI behavior covered by tests.
- Prefer existing LiuXin read/write APIs over direct SQL.
- Make expensive work explicit and cancellable where possible.
- Do not hide write paths behind generic row editing until metadata-specific
  write reports are integrated.
- Keep startup read-only and fast by default.
