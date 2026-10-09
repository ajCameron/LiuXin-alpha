# Library lifecycle and opening-path review

Date: 2026-10-09

## Decision implemented

`core.LibraryServices` is the owner of one database-backed library graph. It
opens the database with implicit storage and maintenance disabled, then opens
storage, maintenance when requested, Core cache/Catalog services and the
non-owning `Library` facade. Startup failure closes every resource already
opened. Normal close attempts every owned closer in reverse dependency order and
is idempotent.

`Database` now defaults to persistence-only construction. `Library` no longer
accepts a path or closes collaborators. `create_core` owns `LibraryServices`
unless the caller supplies a complete borrowed `Library`.

Foundation imports are also lifecycle-neutral: path imports do not create
directories, global preferences do not rewrite their INI, and executable legacy
tweaks are loaded only on first use. Explicit preference/tweak operations retain
their existing persistence behavior.

## Six alternate opening paths

| Path | Previous ownership | Rationalized state | Assessment |
|---|---|---|---|
| `core/workflow_jobs.py` | Six independent `Library(path=...)` contexts | A shared `_open_library_services` helper opens one-shot graphs with maintenance disabled | Correct owner and policy; retain the helper until job settings become a resolved settings object |
| `ingest/mixed_application.py` | Opened `Database` and `StorageManager` itself | Durable ingest requires an injected open `Library`; the CLI surface owns `LibraryServices` | Correct dependency direction: ingest consumes handles and creates no durable graph |
| `ingest/remote_html.py` path wrappers | Opened `Database` directly | Compatibility wrappers delegate lifecycle to `LibraryServices`; Core jobs use the handle-based functions directly | Resource ownership is fixed, but the wrappers introduce a deferred ingest-to-core dependency; move/remove these compatibility APIs |
| `storage/reconcile/store_db_sync.py` path wrappers | Opened `Database` directly | Compatibility wrappers and its standalone legacy main delegate to `LibraryServices`; Core jobs use handle-based functions | Resource ownership is fixed, but the wrappers belong at a Core/surface boundary and should be retired from `storage` |
| `storage/backup/prototype_pipeline.py` | Opened `Library` internally | `run` requires an injected open `Library`; the durable job handler owns `LibraryServices` | Correct dependency direction; the prototype is now a workflow over borrowed services |
| `surfaces/cli/storage_audit.py` | Dynamically imported and opened `Library` | The surface explicitly opens `LibraryServices` with maintenance disabled | Correct owner; the dynamic-import boundary is gone |

## Remaining compatibility debt

The two `*_with_database_path` families are no longer lifecycle roots, but their
location is still wrong. They are retained temporarily for callers and tests.
New code must call a Core path operation or pass an open database/library to the
handle-based lower-layer operation. Once external compatibility is no longer
required, remove these wrappers and the standalone `storage.reconcile` main.

`library/caches/memory_sqlite.in_memory_db_factory` still constructs a Database
around a purpose-built in-memory driver. It is part of the quarantined legacy
cache island rather than an application opening path; it should move with that
island instead of being folded into `LibraryServices`.

The old `Database` opt-in flags and `bootstrap_storage_manager` method remain for
compatibility and focused low-level tests. Their defaults are now off. Production
composition must not opt into them; final removal can follow deletion of the
path-wrapper compatibility APIs.
