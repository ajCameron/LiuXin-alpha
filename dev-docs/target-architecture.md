# Target architecture

Date: 2026-09-26

Status: Planning. Lifecycle ownership was decided on 2026-09-28 and is not yet
implemented. Other areas are still to be decided.

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

**Core owns lifecycle.** Two objects divide the work that `Library` and
`Database` currently mix:

- **`LibraryServices`, in `core`: the lifecycle owner for one open library.**
  - It opens and closes that library's database connection, StorageManager
    (and through it the Stores), cache and Catalog.
  - It builds the `Library` object and hands it out.
  - It works without a `CoreRuntime`. It lives in a core module that imports
    only downward (`library`, `catalog`, `storage`, `caches`, `databases`),
    never the runtime, transports or endpoint registries. Job subprocesses and
    one-shot commands therefore open a library exactly as the daemon does.
- **`Library`, in `library`: the object callers use.**
  - It is a facade over one open library's database, storage, Catalog and
    cache.
  - Its collaborators are passed in. It neither opens nor closes them, and it
    never imports `core`.
- **Core at process level (`CoreRuntime` and the core bootstrap):**
  - resolves configuration for the whole program;
  - creates and owns `LibraryServices` instances, keyed by library identity;
  - owns the job manager, event subscribers, endpoint registries and
    transports;
  - schedules maintenance as jobs.
- **Surfaces create nothing.** They parse their own arguments into selectors,
  pass those to core, and hold a session handle.
- **Lower layers own only their own handles.** Helpers in `storage` and
  `ingest` that currently open a database by path receive handles from their
  caller instead.

| Owner | Creates and closes | Shutdown order |
|---|---|---|
| Core process (`CoreRuntime`, bootstrap) | Resolved settings, job manager, events, endpoint registries, transport, and the `LibraryServices` it owns | Stop accepting requests (under the handler lock), cancel or drain jobs (including maintenance), close core services, then close owned `LibraryServices` |
| `LibraryServices` (core, one per library) | Database connection, StorageManager and Stores, cache, Catalog; builds the `Library` | Reverse of creation. Errors are collected so that one failure does not skip the rest |
| `Library` (library) | Nothing; it is handed its collaborators | — |
| `surfaces` | Nothing; they turn arguments into selectors and hold a session | — |
| `databases`, `storage`, `caches`, `ingest` | Their own handles only. `Database` opens a connection and schema; `StorageManager` receives a database handle and never closes it | — |

Supporting decisions (2026-09-28):

- **Maintenance becomes a Core job.** Constructing a `Database` no longer
  starts a background thread. Core schedules maintenance against a library when
  it is running as a long-lived service. One-shot commands do not schedule it
  unless asked to.
- **Core resolves configuration.** Core is what bootstraps and configures the
  program. It builds one resolved settings object from selectors (flags,
  environment, manifest or profile) and defaults. It then passes that object to
  `LibraryServices.open` and down to lower layers. How precedence works, and
  what happens to the legacy preference and tweak stores, is left to the
  configuration decision area.
- **Multiple libraries stay possible.** Core keeps `LibraryServices` instances
  in a registry keyed by library identity, with a single default library today.
  New APIs should take or resolve a library rather than assume one global
  library. Choosing a library per request is not yet designed.

### Rules

1. **One way to open a library.**
   - `LibraryServices.open(settings)` in `core`, built on a
     `contextlib.ExitStack`.
   - If any step fails, every earlier step is undone in reverse order before
     the error propagates.
   - A successful open yields the `Library` together with its services.
2. **Every live code path uses it:** Core, job subprocesses and one-shot CLI
   commands. Lower-layer helpers (reconcile, ingest, backup) receive an open
   library or database from their caller instead of opening their own.
3. **`Library` does not manage lifecycle.** It never opens or closes its
   collaborators, and never imports `core`.
4. **Background work is Core-scheduled.** No constructor starts a thread.
   Maintenance and similar periodic work run as Core jobs.
5. **Closing is complete and idempotent.** `LibraryServices.close` releases
   everything it opened, including every Store. Closing twice is harmless.
6. **Import rules.** Enforce these in the dependency ratchet described in
   [maintainability quality gates](maintainability-quality-gates.md):
   - only the `LibraryServices` module (and tests) may construct `Database`,
     `StorageManager`, a cache or `Catalog` for a library;
   - that module must not import the core runtime, transport or endpoint
     modules;
   - `library`, `storage`, `ingest`, `databases` and `caches` must not import
     `core`.
7. **Core shutdown is ordered and locked.** Shutdown takes the handler lock,
   stops accepting requests, cancels or drains jobs, and only then closes
   libraries.
8. **Configuration is resolved once, in core, and passed down.** Lower layers
   do not read global configuration to decide what starts or stops.

### Rationale

**Core is the natural owner.** It is already described as the part that
"orchestrates and exposes all the relevant things", and it is what bootstraps
the program. Putting lifecycle, configuration resolution and background
scheduling in one component means one place decides what runs, with which
settings, and in what order it stops.

**Splitting `LibraryServices` from `Library` separates owning resources from
using them.**

- Callers hold a `Library` and cannot accidentally tear it down.
- Tests can build a `Library` from fakes.
- About two dozen call sites already reach storage through `library.storage`.
  That interface survives; only who fills it in changes.

**The earlier objection to core-only ownership is answered.** That objection
was that job subprocesses and commands would need a full `CoreRuntime`.
`LibraryServices` avoids it because it depends on none of the runtime
machinery.

The dependency direction stays consistent: `core` → `library` → lower layers,
with nothing below `core` importing it.

### Alternatives considered

- **Leave it in `databases` (the current state).** Rejected. The lowest layer
  would keep owning threads, storage and deletion decisions, which contradicts
  its stated job of "raw persistence machinery".
- **Let `Library` own its own lifecycle (`Library.open`).** This was the first
  draft of this decision (2026-09-26), and it has been superseded. It would have
  put configuration and background-service policy in the library layer, or
  split them from lifecycle. It would also have made the object callers hold
  responsible for teardown.
- **Put everything inside `CoreRuntime`, with `create_core` building it all.**
  Rejected. Job subprocesses and one-shot commands would need a runtime just to
  open a database with storage, and hosting the API would be mixed up with
  owning resources. `LibraryServices` is the part of core that avoids this.
- **Add a new top-level `application` or `runtime` package.** Rejected as
  unnecessary. `core` already names the right owner, and the
  [2026-09-17 review](top-level-architecture-review.md) found no need for new
  top-level packages.

### Consequences

- **`CoreServices` shrinks or retires.** Its per-library responsibilities move
  to `LibraryServices`: cache, Catalog, read source, library preferences, field
  metadata and maintenance. What remains is process-level, or `CoreServices`
  is retired entirely.
- **`library` holds only the facade.** Of its 51 modules, 46 are unreachable
  from Core or surfaces: the legacy Calibre cache, `legacy`, `backend`,
  `restore` and so on. They should move to a quarantined legacy package.
- **Settings resolution moves into core.** The manifest and profile resolver in
  `surfaces/system_profile.py` becomes core's settings resolver, and surfaces
  keep only argument parsing. This reverses the current placement, where
  deployment manifests are owned only by surfaces.
- **Lower-layer helpers that open a database by path change signature.** They
  take handles instead. This covers:
  - `storage/reconcile/store_db_sync.py`;
  - the `*_with_database_path` helpers in `ingest/remote_html.py`;
  - `ingest/mixed_application.py`;
  - `storage/backup/prototype_pipeline.py`.

  Their callers open the library through `LibraryServices`.
- **Maintenance needs a durable signal.** Today `MaintenanceEngine` is a
  `threading.Thread` fed by in-process queues
  (`databases/maintenance/engine.py:90,124-125`), which receive driver
  callbacks. A Core job may run in another process, so dirty-record
  notifications must become durable or queryable. The jobs system must also
  support recurring or long-running Core-scheduled work. This connects to the
  split between the in-memory `utils/jobs` manager and the unused durable
  `jobs/` package.
- **`db.storage` stays temporarily.** During migration, `Database` may keep a
  `storage` back-reference set by `LibraryServices`. Remove it once callers use
  `library.storage`.
- **Workflows are a separate decision.** Evacuation, placement and other
  workflows can stay in `core/program_services` for now. This decision covers
  who owns resources, not where workflows live.

### Migration order

Each step should leave the full test suite green.

1. **Pin the current contract with tests.**
   - A normal shutdown calls `StorageManager.close`.
   - A startup that fails after the database opens closes the database.
   - No LiuXin threads remain after shutdown.
   - Constructing a `Database` starts no thread.
   - Repeated `close` calls are harmless.

   Some of these tests will be expected failures until later steps.
2. **Introduce `LibraryServices` in core.**
   - Add `open` / `close` built on an `ExitStack`.
   - Have it build the `Library`, and let `Library` accept its collaborators.
   - Move `bootstrap_storage_manager` out of `databases/runtime.py` into it.
3. **Point Core at it.**
   - `create_core` and `CoreRuntime` hold a registry of `LibraryServices` with
     a default library.
   - The per-library parts of `CoreServices` move across.
   - Shutdown becomes ordered and locked, with collected errors.
4. **Move settings resolution into core.** `surfaces/system_profile.py` becomes
   core's resolver, and surfaces pass selectors to it.
5. **Migrate the other code paths.**
   - `core/workflow_jobs.py` uses `LibraryServices`.
   - Lower-layer helpers accept handles.
   - `ingest/mixed_application.py` and `surfaces/cli/storage_audit.py` go
     through core.

   Then add the import rules from rule 6.
6. **Make maintenance a Core job.**
   - Replace the in-process queues with a durable dirty-record signal.
   - Schedule maintenance from Core.
   - Stop `Database` from starting a `Maintainer`.
7. **Retire the old defaults.** Remove `enable_storage_manager`,
   `enable_maintenance` and the `db.storage` back-reference. Update
   [top-level structure](<02 - Top Level Structure.md>),
   [core API](core-api.md) and
   [core workflow ownership](core-program-workflows.md) to describe the result.

### Open questions

- **Location in core.** A single `core/library_services.py`, or a
  `core/lifecycle/` package that also holds settings resolution?
- **Maintenance jobs.** Run on a recurring schedule or triggered by events?
  Which durable signal replaces the in-process queues? Should one-shot
  commands ever run maintenance?
- **Settings.** What shape does the settings object take, and which legacy
  stores does it absorb? This belongs to the configuration decision area.
- **Library selection.** How does a request choose a library once more than
  one is hosted?
- **`Library.close()` during migration.** Keep it as a no-op or delegating
  method for compatibility?

### Decision log

- **2026-09-26:** Proposed that `Library` own the lifecycle of each open
  library, with Core owning the process.
- **2026-09-28:** Revised by maintainer decision:
  - Core owns lifecycle.
  - `LibraryServices` (the core lifecycle object) is separate from `Library`
    (the facade callers use).
  - Maintenance becomes a Core job.
  - Core resolves configuration.
  - Hosting multiple libraries stays possible.

## Future decision areas

These areas come from the architecture review's structural priorities. They
are not yet decided and should be added here as sections when they are:

- whole-package layer rules and cycle enforcement;
- a single write path into bibliographic records;
- a single catalogue read model;
- configuration precedence and the legacy preference stores (ownership is
  decided: core resolves settings; see section 1);
- containment of legacy Calibre code.

## Related documentation

- [Top-level structure](<02 - Top Level Structure.md>)
- [Responsibility boundaries](<04 - Seperation of Concerns.md>)
- [Whole-repository architecture review, 2026-09-26](architecture-review-2026-09-26.md)
- [Top-level architecture review, 2026-09-17](top-level-architecture-review.md)
- [Core API](core-api.md) and [core workflow ownership](core-program-workflows.md)
- [Maintainability quality gates](maintainability-quality-gates.md)
