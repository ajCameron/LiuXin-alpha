# Target architecture

Date: 2026-09-26

Status: Planning. This records target decisions and their rationale; none of it
is implemented yet.

This guide records the architecture that code changes should move towards, one
decision area at a time. [Top-level structure](<02 - Top Level Structure.md>)
describes the intended layering. The
[2026-09-26 architecture review](architecture-review-2026-09-26.md) describes
where the code currently departs from it. Where this guide and older design
notes disagree, this guide is the newer intent. It is still a proposal until an
area's migration is complete.

Each decision area records the problem, the decision, the rules that follow,
the alternatives rejected, the migration order and the open questions. Line
references are to `01674d15` and are evidence for the current state only.

## 1. Lifecycle ownership

### Problem

No single component owns starting and stopping a library's services:

- **Local startup chain.** Local startup runs `SurfaceCoreSession.open` →
  `create_core` → `Library(...)` → `Database(...)`.
- **`Database` starts services as a side effect of construction.**
  - It starts the maintenance thread (`databases/database/__init__.py:571-574`).
  - It builds the StorageManager (`:296-297`, via `databases/runtime.py:93`).
  - Both flags default to on (`:229,233`).
  - `databases/runtime.py:65` already notes that this belongs in `library`.
- **Nothing on the way back closes storage.**
  - `Database.close` does not call `StorageManager.close` (`:755`).
  - `Library.close` only closes the database (`library/library.py:889-894`).
  - `StorageManager.close` exists and closes every Store
    (`storage/storage_manager/mixins/stores.py:662`), but nothing calls it.
  - So S3, FTP, rclone, archive and SQLite store handles are never released.
- **A failed startup undoes nothing.**
  - `core/factory.py:62` and `core/runtime.py:93` both say construction failure
    has no compensating teardown.
  - A `create_core` that fails after the library is built leaves the database
    open and the maintenance thread running.
- **Shutdown can skip steps.**
  - `CoreServices.close` runs its steps in sequence without `try/finally`
    (`core/services.py:617-652`), so one failure skips the rest.
  - `CoreRuntime.shutdown` does not take the handler lock
    (`core/runtime.py:571-599`).
- **Resources are split between two owners.**
  - `CoreServices` creates the cache and binds it to storage.
  - It also creates Catalog and, sometimes, a second maintenance service.
  - `Library` and `Database` own everything else.
- **Other code paths open libraries their own way:**
  - `core/workflow_jobs.py` (six `with Library(...)` sites);
  - `ingest/mixed_application.py:351`;
  - `storage/reconcile/store_db_sync.py:1234,1308`;
  - `ingest/remote_html.py:692,783`;
  - `storage/backup/prototype_pipeline.py:801`;
  - `surfaces/cli/storage_audit.py:92`.
- **Defaults drift between entry points.** Maintenance defaults to on in
  `Library` and `create_core`, but to off in `SurfaceCoreSession.open`
  (`surfaces/core.py:203`).

### Decision

Ownership is split across two levels.

- **`Library` owns the resources of one open library.** It is the only
  component that creates, and later closes, that library's:
  - database connection;
  - maintenance service;
  - StorageManager, and through it the Stores;
  - cache;
  - Catalog.
- **`core` owns the running process.** That means:
  - the job manager;
  - event subscribers;
  - endpoint registries;
  - the transports.

  It either owns or borrows `Library` instances.
- **Surfaces create nothing.** They choose settings (system profile, local or
  remote) and hold a session handle.
- **Lower layers create only their own handles.** `databases`, `storage` and
  `caches` never start a sibling service.

| Owner | Creates and closes | Shutdown order |
|---|---|---|
| `Library` (per library) | Database connection, maintenance, StorageManager and Stores, cache, Catalog | Reverse of creation. Errors are collected so that one failure does not skip the remaining steps |
| `core` (per process) | Job manager, events, endpoint registries, transport; owned `Library` instances | Stop accepting requests (under the handler lock), cancel or drain jobs, close core services, then close owned libraries |
| `surfaces` | Nothing; they select settings and hold a session | — |
| `databases`, `storage`, `caches` | Their own handles only. `Database` opens a connection and schema; `StorageManager` receives a database handle and never closes it | — |

### Rules

1. **One way to open a library.** A single entry point, for example
   `Library.open(settings)`, builds on a `contextlib.ExitStack`. If any step
   fails, every earlier step is undone in reverse order before the error
   propagates.
2. **Every live code path uses it:** Core, job subprocesses, one-shot CLI
   commands, and the reconcile and ingest helpers. Defaults therefore cannot
   drift between entry points.
3. **Background services are a setting, not a default.** The entry point
   decides whether maintenance runs: on for a long-lived `core serve`, off for
   one-shot commands and jobs. Constructors do not start threads unless told
   to.
4. **Closing is complete and idempotent.** Closing a library releases
   everything it opened, including every Store. Closing twice is harmless.
5. **Construction is guarded by an import rule.** Only `library` (and tests)
   may construct `Database` or `StorageManager`. This belongs in the
   dependency ratchet described in
   [maintainability quality gates](maintainability-quality-gates.md).
6. **Core shutdown is ordered and locked.** Shutdown takes the handler lock,
   stops accepting requests, deals with running jobs, and only then closes the
   libraries.

### Rationale

`Library` is the right owner because it is already the unit the design and
the code use:

- **The design:** the top-level structure describes a library as "one-ish
  database" plus "many stores", and says Core "has access to one or many
  libraries".
- **The code:** job subprocesses already reopen a library by path with
  `with Library(...)`.
- **The call sites:** about two dozen already reach storage through
  `library.storage`, which today just forwards to `self._database.storage`.
  Moving storage ownership into `Library` therefore changes the internals, not
  that interface, which keeps the migration cheap.

Keeping per-library and per-process ownership separate means a process can
later host more than one library. It also means a job subprocess or a CLI
command can open a library without a full `CoreRuntime`.

### Alternatives considered

- **Leave it in `databases` (the current state).** Rejected. The lowest layer
  would keep owning threads, storage and deletion decisions, which contradicts
  its stated job of "raw persistence machinery".
- **Make `core` the only owner, with `create_core` as the single entry point.**
  This is attractive because the factory already exists. It was rejected
  because:
  - job subprocesses, `storage ingest`, the reconcile helpers and tests would
    all need a `CoreRuntime` (handler registries, events and a lock) just to
    open a database with storage;
  - it would mix hosting the API with owning resources;
  - it would make multiple libraries awkward.

  Revisit this only if the project commits to one library per process
  permanently.
- **Add a new top-level `application` or `runtime` package.** Rejected as
  unnecessary. `library` already names the right unit, and the
  [2026-09-17 review](top-level-architecture-review.md) found no need for new
  top-level packages.

### Consequences

- **`library` must mean one thing.** Of its 51 modules, 46 are unreachable
  from Core or surfaces: the legacy Calibre cache, `legacy`, `backend`,
  `restore` and so on. They should move to a quarantined legacy package, so
  that `library` becomes the resource owner rather than a mix of that and dead
  code.
- **`CoreServices` becomes a view.** It borrows the cache, Catalog and
  maintenance from `Library` instead of creating them. Its own responsibilities
  shrink to process-level concerns.
- **`db.storage` stays temporarily.** During migration, `Database` may keep a
  `storage` back-reference, set by `Library`. It should be removed once callers
  use `library.storage`.
- **Workflows are a separate decision.** Evacuation, placement and other
  workflows can stay in `core/program_services` for now. This decision covers
  who owns resources, not where workflows live.

### Migration order

Each step should leave the full test suite green.

1. **Pin the current contract with tests.**
   - A normal shutdown calls `StorageManager.close`.
   - A startup that fails after the database opens closes the database and
     stops the maintenance thread.
   - No LiuXin threads remain after shutdown.
   - Repeated `close` calls are harmless.

   Some of these tests will be expected failures until step 3.
2. **Introduce the single entry point.** Add `Library.open` / `close` built on
   an `ExitStack`. Move `bootstrap_storage_manager` out of
   `databases/runtime.py` and have `Library` call it.
3. **Point Core at it.**
   - `create_core` builds the library through the entry point.
   - `CoreServices` borrows the cache and Catalog from it.
   - `CoreRuntime.shutdown` takes the handler lock and runs the ordered
     shutdown with collected errors.
4. **Migrate the other code paths:**
   - `core/workflow_jobs.py`;
   - `ingest/mixed_application.py`;
   - `store_db_sync`;
   - `remote_html`;
   - the backup prototype;
   - `storage_audit`.

   Then add the import rule from rule 5.
5. **Retire the old defaults.** Change `Database` to
   `enable_storage_manager=False` and `enable_maintenance=False`, then remove
   the flags and the `db.storage` back-reference. Update
   [top-level structure](<02 - Top Level Structure.md>) and
   [core workflow ownership](core-program-workflows.md) to describe the result.

### Open questions

- **Naming.** Keep the class name `Library` with a `Library.open` entry point,
  or introduce a distinct name such as `LibraryServices` while the package
  still contains legacy code?
- **Maintenance.** Does it stay a per-library service owned by `Library`, or
  become a Core-scheduled job that runs against a library?
- **Settings.** Where does the resolved settings object passed to
  `Library.open` come from? This depends on the configuration decision below.
- **Multiple libraries.** When, if ever, does Core host more than one library,
  and how would requests select between them?

## Future decision areas

These areas come from the architecture review's structural priorities. They
are not yet decided and should be added here as sections when they are:

- whole-package layer rules and cycle enforcement;
- a single write path into bibliographic records;
- a single catalogue read model;
- configuration ownership and precedence;
- containment of legacy Calibre code.

## Related documentation

- [Top-level structure](<02 - Top Level Structure.md>)
- [Responsibility boundaries](<04 - Seperation of Concerns.md>)
- [Whole-repository architecture review, 2026-09-26](architecture-review-2026-09-26.md)
- [Top-level architecture review, 2026-09-17](top-level-architecture-review.md)
- [Core API](core-api.md) and [core workflow ownership](core-program-workflows.md)
- [Maintainability quality gates](maintainability-quality-gates.md)
