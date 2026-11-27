# Core metadata and lifecycle review

Reviewed **2026-11-23**, on `core-review`, starting from the reviewed cache
implementation at `21539083`. The existing ownership split is appropriate:
LibraryServices owns a library graph, Catalog owns metadata rules and persistence,
and CatalogCache coordinates native metadata operations with derived cache state.
Core supplies composition, named operations, authority checks and wire results.

This review covers service composition, metadata routing and one complete public
search/select/edit/save/readback workflow. It repairs lifecycle and error-reporting
gaps without creating another metadata authority. The bounded integration review
is complete; wider Core jobs, configuration and transport remain separate scopes.

## Metadata routes

| Route | Owner and cache behavior |
| --- | --- |
| `metadata.search`, `metadata.read_many`, `metadata.read_for_edit`, `metadata.update` | Shared `CoreServices.metadata`: MetadataAPI without a cache, CatalogCache with a native cache. The service owns its read snapshot, optimistic revisions and reconciliation. Core must not refresh its receipt again. |
| `catalog.field.write`, `catalog.field.write-one` | `CoreServices.field_writer` selects the native metadata bridge, uncached Catalog, or an explicitly injected external writer. Failures do not cause a fallback write. |
| Existing Catalog CRUD, Agent, relationship, merge and repair commands | Catalog repositories/services retain domain policy. Their existing wire receipts pass through explicit Core reconciliation after the operation. These older routes are not silently redefined as the newer metadata receipt protocol. |
| `rows.*`, `relations.list`, Item-shaped `metadata.get` and OPF export | The selected Core read source, including an explicit injected reader, owns retrieval. This older projection contract remains separate from native WEMI metadata containers. |
| `search.global` | Full selected-table scan through the read source. Required read failures now propagate instead of producing successful partial/empty results. It is not the typed, single-capture native metadata search API. |
| Metadata file and online-source commands | Metadata/file/plugin owners perform those workflows. File edits do not become catalogue edits merely because both use the `metadata.*` namespace. |

Native metadata commands use the bound Catalog/cache composition. Injecting a
legacy `read_source` changes the row/Item projection routes; it does not replace
the native metadata service's snapshot source. Application code should use the
named native workflow for WEMI editing and retain revisions supplied by
`metadata.read_for_edit`.

Core commands are the application operation boundary. Use the metadata service's
batch and managed-transaction contracts when composing several domain operations;
the older post-operation Core receipt adapter does not supply a transaction around
its caller. Raw database writes outside the coordinated owners still require
explicit cache invalidation.

## Repaired boundaries

| Finding | Previous behavior | Result |
| --- | --- | --- |
| Startup ownership | Invalid injected bindings were checked after cache preparation; initial loads and later runtime installation could leak newly created resources or leave displaced storage bindings. | Validate identities first, retain a created cache before loading it, and roll back resources that have not been transferred to a successful owner. LibraryServices and the factory preserve rollback semantics through Catalog and runtime installation. Borrowed collaborators remain open. |
| Cleanup continuation | A failed storage unbind skipped cache close. Durable-job stop could skip graph close; manager shutdown could replace the earlier error. | Attempt each owned closer independently and retain all errors. Factory rollback stops its new job service before unwinding the library graph. |
| Lazy lifetime | Most lazy properties could resolve services after close; construction could race cleanup. | Guard service acquisition and construction against closure. Database mutex precedes the Core lifecycle lock; a waiting reader does not hold a lock needed by the SQL owner. |
| Replacement binding | Closing an older Core could unbind a replacement session's cache on a shared database. | Detach only the binding still referring to this owner's cache. Construction rollback restores the previous binding. |
| Committed receipts | Cache state/generation lookup could fail after successful refresh and lose the committed-write classification. | Status-reporting errors retain the authoritative receipt through `CoreServiceReconciliationError`, like reload failures. |
| Required search reads | `search.global` silently skipped a table when its initial read failed. | Local and RPC callers receive a structured failure. Unicode matching, counts and paging retain their existing semantics. |

`close()` remains idempotent: a second call does not retry failed cleanup.
Startup errors remain distinguishable from rollback errors. Multiple cleanup
failures are reported as an exception group after all applicable closers run.
Cache inspection remains available through the retained cache reference; service
properties reject acquisition through a closed CoreServices owner. References
already borrowed by callers retain their own owners' lifetime contracts.

Library switching uses a newly composed graph. Mutating a live CoreServices
database/cache reference would leave its retained semantic services and readers
bound to the old graph and is not a supported switching operation.

## Verification

The initial lifecycle regression reproduced 20 failures. Four additional cases
reproduced lost prior storage bindings when Catalog or runtime installation failed
after the Core service bundle had opened. The final focused lifecycle selection
passed 31 cases, including borrowed/created cache rollback, concurrent lazy
construction/close and database-before-service lock ordering.

The public workflow matrix covers SQLite and APSW, uncached operation and all
three cache backends, through both local calls and HTTP RPC. It checks search
provenance, selection, revision-checked saves, stale-edit rejection,
subsequent search/readback, and durable-write receipts after publication failure.
Snapshot modes inject refresh failure; live and uncached modes use a real
after-commit callback failure. An in-memory generation failure is not a suitable
substitute because that counter can also change before commit and during recovery.
The complete Core suite, surface boundary enforcement and existing metadata
workflow regressions passed **405 tests**, with **one skip**, in **1,021.46 s**.
The skipped existing durable-maintenance case needs `LIUXIN_TEST_POSTGRES_URL`;
this review did not rerun live PostgreSQL. The fixture tests use disposable
catalogues and loopback HTTP listeners and do not change an application library.

The final repository quality gate passed: formatting, lint, complexity, import
cycles, layer direction, import budgets, basedpyright and mypy. The negative type
contracts rejected all 127/123 invalid examples respectively. Both new regression
files also pass explicit Ruff lint and formatting checks. Developer navigation
and public documentation boundary checks passed **22 tests**.

Reproduce the behavioral selection with `PYTHONPATH=src .venv/bin/python -m
pytest -q tests/core tests/surfaces/test_core_boundary_enforcement.py
tests/databases/caches/test_metadata_workflow.py`; loopback socket access is
required. Run `bash scripts/run_type_checks.sh` separately from pytest, then
`tests/scripts/test_developer_documentation_links.py` and
`tests/scripts/test_public_documentation_boundaries.py`. Run outputs were saved
under the local `/tmp/liuxin-core-review-` prefix (`regressions`, `quality-final`,
`docs`); they are disposable evidence, not repository dependencies.

## Next scope

Keep the existing owners. Extend the public native metadata routes as consumers
need picker, shared-record and hierarchy operations; do not push bibliographic
policy into the generic cache or Core dispatch. Preserve explicitly injected
legacy services during those caller migrations.

Population-scanning pickers and conservative snapshot refreshes remain measured
performance work under the [cache review](../caches/metadata-followups-2026-11-22.md).
The separate lifecycle caller migration is now committed in `04d6fd7d`.
The subsequent [whole-Core review](review-and-completion-plan-2026-11-23.md)
extends this bounded assessment to jobs, transport, discovery, configuration,
events and functional completion. It supplies the full improvement sequence;
[Core tasks](next_tasks.md) retains the implementation routing.

Return to [Core](README.md) or the [Catalog/cache boundary](../catalog/catalog-cache-boundary.md).
