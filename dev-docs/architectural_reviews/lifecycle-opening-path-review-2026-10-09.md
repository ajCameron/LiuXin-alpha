# Library lifecycle and opening-path review

Decision implemented: **2026-10-09**. Source reviewed **2026-10-20** against the
working tree based on `fc5de7af`. This is a lifecycle/caller review; transaction
safety and maintenance scheduling have separate remaining work.

**Caller migration update, 2026-11-23:** the four remaining ingest/storage path
helpers and their repository callers have moved to Core. The table and migration
instructions below describe that updated placement.

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
| [`ingest/remote_html.py`](../../src/LiuXin_alpha/ingest/remote_html.py) path wrappers | Opened Database directly | Path helpers moved to `core.store_registration`; only borrowed-handle operations remain here. | No path-wrapper migration remains in this module. |
| [`storage/reconcile/store_db_sync.py`](../../src/LiuXin_alpha/storage/reconcile/store_db_sync.py) path wrappers | Opened Database directly | Path helpers moved to `core.store_registration`; standalone main moved to `surfaces.cli.store_registration`. | Keep domain registration handle-based. |
| [`storage/backup/prototype_pipeline.py`](../../src/LiuXin_alpha/storage/backup/prototype_pipeline.py) | Opened Library internally | `run` requires an injected Library; its durable job handler owns LibraryServices. | Keep prototype workflow and publication/recovery guarantees explicit. |
| [`surfaces/cli/storage_audit.py`](../../src/LiuXin_alpha/surfaces/cli/storage_audit.py) | Dynamically imported/opened Library | Explicitly opens LibraryServices with maintenance disabled. | Retain focused startup/close coverage when command policy changes. |

## Registration caller migration

Import these synchronous Python helpers from
[`LiuXin_alpha.core.store_registration`](../../src/LiuXin_alpha/core/store_registration.py):

- `register_existing_disk_with_database_path`
- `register_rclone_http_readonly_with_database_path`
- `register_wget_html_readonly_with_database_path`
- `register_native_html_readonly_with_database_path`

The names, options and report types are preserved. Old ingest/storage imports
are removed, with no reexports. Each helper opens an existing catalogue through
LibraryServices with creation, backup and maintenance disabled, invokes its
handle-based domain operation, and closes the graph before returning a report.
Callbacks run while the graph is open and retain the domain operation's existing
error policy. Startup, registration and cleanup failures propagate; this adds
neither an all-run transaction nor automatic retries. These remain filesystem
path helpers; use LibraryServices directly for server connection selections.

The standalone local-disk command is now:

```bash
python -m LiuXin_alpha.surfaces.cli.store_registration \
  --database catalogue.sqlite --disk /books --json
```

Its flags, text/JSON reports and `--fail-on-errors` exit status are preserved.
The old `storage.reconcile.store_db_sync` module no longer supplies a CLI.
Already-open callers continue using the functions in `ingest.remote_html` and
`storage.reconcile`; Core workflow jobs already followed that contract.

The dependency ratchet removes the storage-to-Core exception and now forbids
ingest-to-Core/surfaces imports in all import contexts. Deferred imports count.

## Remaining lifecycle work

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
the [transaction contract](../databases/transactions.md) now separates explicit
commit/rollback from resource cleanup, which discards pending work. Earlier
committed registration writes can survive a later failure. Existing-file safety
is covered by the separate [creation contract](../databases/creation-and-replacement.md).

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

[Registration lifecycle tests](../../tests/core/test_store_registration.py) cover
all four helpers on success, startup failure, registration failure and cleanup
failure, including progress before close. Existing registration suites exercise
the moved imports against temporary catalogues; the
[CLI tests](../../tests/surfaces/test_cli_store_registration.py) cover flags,
reports, exit status and failure output. Broader maintenance scheduling still
needs restart, cancellation and progress semantics beyond the shutdown tests.
