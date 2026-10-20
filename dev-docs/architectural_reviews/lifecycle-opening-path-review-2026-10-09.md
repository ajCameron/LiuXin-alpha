# Library lifecycle and opening-path review

Decision implemented: **2026-10-09**. Source reviewed **2026-10-20** against the
working tree based on `fc5de7af`. This is a lifecycle/caller review; transaction
safety and maintenance scheduling have separate remaining work.

## Decision implemented

[`core.library_services.LibraryServices`](../../src/LiuXin_alpha/core/library_services.py)
owns one database-backed library graph. For a database it creates, it disables
implicit storage and maintenance startup, then composes storage, optional
maintenance, Core cache/Catalog services and the non-owning Library facade.
`Database` defaults to persistence-only construction. `Library` accepts supplied
collaborators, not a path, and does not own their shutdown.

`create_core` uses LibraryServices when constructing a graph, including around
a supplied database. A supplied complete Library is borrowed instead. The
[decision record](target-architecture.md#1-lifecycle-ownership) explains why
one-shot jobs can use LibraryServices without a full CoreRuntime.

| Resource / event | Contract |
| --- | --- |
| Newly opened database, storage or maintenance | Register cleanup with the owner after successful construction. |
| Injected database, existing storage/maintenance | Borrowed; ownership of newly constructed collaborators is separate. |
| Injected cache | Borrowed by CoreServices; a cache it constructs is owned. |
| Startup failure | Unwind registered owned resources. A constructor failing before registration must clean up its own partial work. |
| Normal close | Attempt every registered closer in reverse registration order; preserve cleanup failures. |
| Repeated close | No-op after teardown was attempted, including after a failing closer. `closed` does not mean every resource closed successfully. |
| Default utility job manager | Process-shared; CoreRuntime does not shut it down by default. Explicit shutdown ownership can be requested. |

A CoreRuntime currently hosts one Library facade, not a registry of independently
selected libraries. Resource ownership is not a transaction or a guarantee that
prior external writes can be undone.

## Six alternate opening paths

| Path | Previous ownership | Current state | Remaining work |
| --- | --- | --- | --- |
| [`core/workflow_jobs.py`](../../src/LiuXin_alpha/core/workflow_jobs.py) | Independent `Library(path=...)` contexts | Shared `_open_library_services` opens one-shot graphs with maintenance disabled. | Keep job-specific lifecycle policy explicit; resolved job settings can replace repeated options. |
| [`ingest/mixed_application.py`](../../src/LiuXin_alpha/ingest/mixed_application.py) | Opened Database and StorageManager itself | Durable ingest requires an injected open Library; the calling CLI/Core boundary owns the graph. | Continue migrating callers through this handle-based contract. |
| [`ingest/remote_html.py`](../../src/LiuXin_alpha/ingest/remote_html.py) path wrappers | Opened Database directly | Compatibility wrappers delegate to LibraryServices; Core jobs use handle-based functions. | Move/remove the wrappers and their deferred upward Core imports. |
| [`storage/reconcile/store_db_sync.py`](../../src/LiuXin_alpha/storage/reconcile/store_db_sync.py) path wrappers | Opened Database directly | Wrappers and standalone legacy main delegate to LibraryServices; Core jobs use handles. | Move entry-point composition to Core/surfaces and remove the lower-layer wrappers. |
| [`storage/backup/prototype_pipeline.py`](../../src/LiuXin_alpha/storage/backup/prototype_pipeline.py) | Opened Library internally | `run` requires an injected Library; its durable job handler owns LibraryServices. | Keep prototype workflow and publication/recovery guarantees explicit. |
| [`surfaces/cli/storage_audit.py`](../../src/LiuXin_alpha/surfaces/cli/storage_audit.py) | Dynamically imported/opened Library | Explicitly opens LibraryServices with maintenance disabled. | Retain focused startup/close coverage when command policy changes. |

## Remaining compatibility debt

The two `*_with_database_path` families delegate cleanup to the correct owner,
but their placement still introduces lower-layer dependencies on Core. Migrate
callers and tests to Core path operations or supplied handles before removing
them. Deferred imports are still architectural dependencies.

The October 9 review also listed
`library/caches/memory_sqlite.in_memory_db_factory`. That module is **absent** at
the October 20 checkpoint; it is no longer a pending opening-path migration.
Other `library/caches` compatibility implementations remain and need their own
caller/behaviour evidence before retirement.

Database's explicit storage/maintenance opt-ins and `bootstrap_storage_manager`
remain for compatibility and focused low-level use. Production composition
should use LibraryServices. The `db.storage` back-reference still has live
consumers; removing it requires migration, not just changing the default flags.

LibraryServices can own and stop the existing maintenance thread. It has not
converted that thread/queue system into durable Core jobs. Similarly,
`Database.__exit__` closes the facade without exception-sensitive rollback and
can commit pending work on an exceptional exit. Use the explicit transaction
contract; do not infer rollback from a resource context manager. Existing-file
creation/backup safety also remains a separate issue.

## Foundation imports and configuration

The path resolver avoids creating directories on import. Application preferences
are lazy, and the current [configuration contract](../core/configuration-ownership.md)
uses literal-only tweak input. These fixes do not establish universal import
neutrality: `utils.ptempfiles` still imports `startup_scripts`, whose initializer
modifies `sys.meta_path` and builtins. Remove that dependency before claiming
foundation imports have no process-wide effects.

## Evidence and completion criteria

[Lifecycle regressions](../../tests/core/test_library_services_lifecycle.py)
exercise reverse-order/idempotent close, failed startup, failing closers,
maintenance shutdown, Core-owned teardown and an injected database. They do not
prove rollback, safe overwrite, thread-race freedom or every constructor's
internal cleanup. [Layer-direction checks](layer-direction-ratchet.md) expose
remaining upward dependencies; cold-import budgets cover selected passive APIs.

Closing the remaining migration requires caller inventory and behaviour checks,
removal of the unwanted dependency edges, and explicit ownership for resources
opened by replacement entry points. Maintenance scheduling needs restart,
cancellation and progress semantics in addition to the existing shutdown test.
