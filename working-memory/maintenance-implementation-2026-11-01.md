# Maintenance implementation — 2026-11-01

Implemented user goal: the complete prompt/lazy contract from
`dev-docs/databases/maintenance-modes.md`, with transactional effects, durable
delivery, actual Catalog consumers and operator interfaces. The declaration-only
sketch was a separate earlier task and is now superseded.

Branch `maintenance-plugin-modes`, based on `b2343096`; sketch and implementation
remain uncommitted. Preserve user comments in builtin_plugins.py and
dummy_maintenance_bot.py, plus the dirty data submodule.

## Delivered contract

- Prompt effects share the originating connection/write scope, finish before
  successful operation/commit, propagate selection/handler/deferred failures,
  and respect nested savepoints and dependent reads. Explicit dependencies and
  bounded recursion; required plugins cannot silently be disabled/missing.
- Lazy obligations commit with source changes, survive restart, retain newer
  dirty revisions during acknowledgement, handle deletion, and support bounded
  claims, retries/backoff, visible failures and disabled/missing implementations.
- Catalogue-local capture/outbox with explicit migration and introspection.
  Jobs remains the execution owner; Core composes the cross-database handoff.
- At least one actual domain-owned consumer per mode, migration/retirement of
  legacy creator registrations, status/drain/retry operations and documentation.
- SQLite and live PostgreSQL regression evidence for rollback, restart, crash
  boundaries, dependencies, readonly access and all supported writer routes.

## Implementation

Use transactional database change capture instead of Python dirty queues as the
source of obligations. Persist plugin subscriptions/versions so temporarily absent
implementations do not lose lazy work; required prompt subscriptions fail closed.
Connect write completion to driver connections and transaction_scope so portable
macros, Catalog, Row and direct writers share the same transaction boundary.

Implemented modules: connection_hooks, maintenance/{schema,capture,declarations,
journal,bound_connection,runtime}, catalog/maintenance and core/maintenance_jobs.
Completion hooks run on native commit/context exit, transaction_scope savepoint
completion, SQL commit/cursors and dependent SELECT. Runtime binds all supported
driver handles. Prompt handlers get an expiring row-SQL-only connection, no general
Database facade. Required code/version/enabled guards live in persistent triggers.
Lazy effects and exact-revision acknowledgement share one transaction. Core bridges
the journal to existing JobRepository/JobWorker with stable keys, claim fences,
exponential retry/backoff and explicit cancellation/retry. No second lease owner.

Catalog owns prompt Work revision receipts and lazy distinct-expression counts.
Library attaches those implementations without migration or worker startup.
Legacy creator plugins no longer register by default. Explicit old queue adapters
remain for compatibility; they do not supply durable/prompt contracts.

CLI/Core status, install, drain, retry, pause/resume and bounded pruning implemented.
Core start action supports continued polling in a long-lived host. CLI drain is
bounded and synchronous. Core durable service now available independently of the
storage manager. Installation tracks subsequent writes, not historical backfill.
Plugin version upgrades intentionally require an explicit deployment migration.

Additional boundaries: installed SQLite scripts run in owned/nested write units,
without native implicit pre-commit; embedded transaction controls are rejected.
PostgreSQL statement chaining cannot conceal a commit. Savepoint exception cleanup
does not dispatch unrelated outer obligations. Stale pre-installation handles fail
closed. Uninstalled runtimes preserve existing script behavior. Repeated worker
crashes count once per run against retry limits; missing plugins do not starve
independent ready work.

## Final evidence

These selections overlap; do not sum them as a unique-test total:

- Broad matrix: **219 passed, 1 backend-specific skip, 2 existing xfails** in
  `/tmp/liuxin-maintenance-final.log`. Includes SQLite/APSW/PG real writers,
  transaction ownership, legacy callback compatibility, durable audit, Core,
  Library and CLI. Existing xfails are the legacy 4/5-argument interlink UDF.
- Final transaction-boundary checks: **103 passed, 2 backend-specific skips** in
  `/tmp/liuxin-maintenance-boundaries.log`; includes PostgreSQL statement chaining
  and pending outer work during failed savepoint cleanup.
- Real Core install/pause/retry/drain/restart and long-lived polling: passed in
  `/tmp/liuxin-maintenance-core-final.log` (its initial PG composition failure was
  fixed). Final live PG Core delivery plus lifecycle checks: **7 passed** in
  `/tmp/liuxin-maintenance-core-pg.log`.
- Documentation/hygiene/proxy contracts: **27 passed**. All **25 cold-import
  budgets**, lint on new implementation/tests, formatting across **246 protected
  files**, modern import-cycle checks and layer direction passed (1866 modules,
  unchanged reviewed edges). Manual page renders and diff whitespace checks pass.
- Broad test process-tree RSS peaked at **153.2 MiB**; telemetry recorded **zero
  additional OOM kills**. Scratch/cache/telemetry stayed in /tmp, with test scratch
  retention disabled. C: had about 10 GiB free during validation.

The isolated PG16 cluster under /tmp/liuxin-maintenance-pg has been stopped and
removed. Its log is retained at /tmp/liuxin-maintenance-pg-server.log. The system
PostgreSQL instance was not touched. No test sessions remain running.

New files are intent-to-add for tracked-link checks; no contents are staged and
no commit or PR was requested. Shell formatting needed auto-reviewed escalation
because of the case-mapped workspace mount; apply_patch works normally.

Follow-on work, outside this completed delivery: historical projection backfill,
explicit plugin-version upgrade tooling, more domain-owned projections, and
benchmarking reduced PostgreSQL serialization. Installation and worker operation
remain deliberate opt-ins; see docs/maintenance.md.

## Review follow-up — 2026-11-02

User requested implementation of all six findings from the overall review.
Changes remain on maintenance-plugin-modes, uncommitted, preserving the original
dirty data submodule and user TODO comments.

- SQLite CTE DML now opens the same implicit unit as ordinary DML; explicit
  autocommit still requires a transaction. CTE reads remain read-only.
- REPLACE conflict handling is refused on installed source handles and borrowed
  plugin connections. Explicit UPSERT captures both old and new endpoints.
- Cursor class factories are wrapped from creation, including pre-installation
  cursors; non-class factories are refused.
- Physical interlink merge uses one serialized macro transaction, validates row
  identities, and deletes only proven duplicate payloads. Non-duplicate failures
  propagate; differing roles/priorities/payloads stop the merge. Operational
  datestamps are excluded from payload comparison on both database backends.
  This remains a limited physical repair, not a complete semantic metadata merge.
- Jobs claims accept an optional kind filter. Maintenance drain claims only
  maintenance deliveries, leaving queued audits for the normal worker.
- Selective cleanup raises NotImplementedError without writes; Core translates
  this into capability_unavailable. Docs, API descriptions and man page updated.

Validation complete (overlapping selections; do not sum as unique tests):

- Final real-writer/repair selection: **22 passed** across SQLite, SQLite_apsw
  and live PostgreSQL. Covers CTE commits/rollback, mid-transfer/final-delete/
  prompt failure rollback, exact duplicate handling with differing datestamps,
  conflicting payload refusal, actual unique-priority failure, and unavailable
  cleanup. Log: /tmp/liuxin-maintenance-repairs-final-tests.log.
- Broad maintenance/transaction/jobs/CLI matrix: **202 passed, 9 backend-specific
  skips**, plus the PG datestamp failure discovered during the run. That process
  had loaded the earlier code; the failure is fixed and its exact test passed in
  the final selection above. Log: /tmp/liuxin-maintenance-review-fixes-tests.log.
- Additional plugin-contract/custom-cursor checks: **20 passed, 2 backend-specific
  skips** in /tmp/liuxin-maintenance-extra-tests.log.
- Documentation/hygiene: **23 passed**. Import cycles (252 modules), layer
  direction (1866 modules), all 25 cold-import budgets, the 246-file formatting
  scope, formatting/lint on touched implementation/tests, man-page rendering
  and diff whitespace checks passed.

The isolated PostgreSQL 16 cluster under /tmp/liuxin-maintenance-fixes-pg has
been stopped and removed; its server log remains in /tmp. The system PostgreSQL
instance is untouched. All test scratch/logs were under /tmp with retention
disabled. During final runs C: remained near 4.1 GiB free and each pytest process
used about 110–120 MiB RSS. Disk waits made the integration selections slow.
No test processes remain running. No commit or PR was requested.

## Legacy retirement — 2026-11-02

User approved the maintenance.legacy review recommendations and requested notes
on removed capabilities for later reimplementation. Implemented without a commit
or PR; the earlier maintenance work remains in the same dirty worktree.

- Removed the 1,440-line legacy module and public re-exports, creator rename/sort
  plugins, both SQLite TA_UPDATE registrations/driver hooks, the abstract hook,
  and the unused preference/default INI keys. The generic transient engine and
  counting plugin remain. User TODO comments were preserved.
- Catalog's `duplicates.find_duplicates` reads current identity declarations,
  respects scopes (including derived-key columns), returns only repeated groups,
  and skips NULL/blank values. Modes: identity (default), nocase and exact.
  A portable page query enforces max_rows (10,000 default, 100,000 ceiling),
  refusing incomplete results. Core/CLI support equality filters and scan budgets;
  CLI inspection enables neither storage nor maintenance services.
- Legacy Genre lookup refuses ambiguity instead of merging. Selective cleanup
  still returns capability_unavailable, implemented directly in Maintainer.
- The Catalog no-SQL boundary test exposed raw SQL in the earlier projection
  implementation. Extracted it to databases.maintenance.projection_sql, keeping
  bound connections and Catalog dependency/payload/freshness policy unchanged.
- Added dev-docs/databases/legacy-maintenance-retirement-2026-11-02.md with all
  removed symbols, former capabilities, failures, surviving alternatives and
  concrete requirements for restoring aggregates, Agent sorting, semantic merge,
  orphan cleanup and Calibre-shaped views. Updated docs, man, API docstrings and
  database navigation/tasks.

Final evidence (overlapping selections, not a unique total):

- `/tmp/liuxin-legacy-retirement-final.log`: **39 passed, 1 PostgreSQL skip**;
  Catalog/Core duplicate inspection on SQLite/APSW, legacy ambiguity refusal,
  maintenance modes, architecture ownership and durable Core workflows.
- `/tmp/liuxin-legacy-retirement-writers.log`: **4 passed, 2 PostgreSQL skips**;
  production prompt commits/rollback and installed read-only behavior after SQL
  extraction. No PostgreSQL test connection was configured for this turn.
- `/tmp/liuxin-legacy-retirement-docs-final.log`: **33 passed**, including the
  final CLI service-disable assertion and all documentation links.
- The initial docs/CI run passed its CI workflow tests; its only failure was
  new links absent from the Git index. Four new files are now intent-to-add,
  with no contents staged, and the final link checks pass.
- New/changed implementation lint, 246-file formatting, 252 protected-module
  import checks, layer direction (1,867 modules, unchanged 15 reviewed backward
  edges/28 dynamic imports), man rendering and diff whitespace checks passed.

Scratch was under /tmp with retention disabled. C: stayed near 4.0 GiB free.
No full suite or PostgreSQL provisioning was run. No test processes remain.

## Post-retirement review and testing — 2026-11-02

The subsequent active goal requested another overall maintenance review and
thorough tests. The baseline matrix passed 207 tests, skipped nine backend-only
cases and retained two old expected failures. New regressions reproduced six
defect groups before the following fixes:

- `runtime.flush` wraps the entire drain in a savepoint, preventing effects and
  acknowledgements from a caught failure leaking into a later successful retry.
  Tests include multiple handlers, 300 source rows, multiple batches, and real
  SQL integrity failures on both engines.
- SQLite `isolation_level=None` and `autocommit=True` settings honor the completion
  hook. Failed effects roll back; explicit autocommit writes require a unit.
  Success/failure tests cover native legacy, False and True transaction modes.
- PostgreSQL subscribed tables have a persistent BEFORE TRUNCATE guard, including
  cascades, lazy-only subscriptions and raw external cursors. DELETE is supported.
- Install/deliver acquire the database writer/advisory lock before the runtime
  Python lock. Event-coordinated concurrency tests reproduce the old lock timeout
  and prove the writer and maintenance operation both finish after the fix.
- Published delivery/failure receipts check ledger UUID and run ID. Late workers
  cannot consume a successor's retry budget or execute superseded work. Repeated
  failure receipts count once/run; ledger replacement clears the previous
  ledger's failure identity even when numeric run IDs collide. Direct non-jobs
  delivery remains supported. Core supplies the jobs ledger identity.
- SQLite and SQLite_apsw interlink UDFs dynamically resolve the active maintainer,
  instead of retaining the startup dummy. Removed the obsolete arity xfail; the
  previously hidden queue-empty failures now pass normally.

New tests: `test_maintenance_failure_boundaries.py` and
`test_maintenance_process_recovery.py`. The latter exits real subprocesses without
Python cleanup before source commit, during lazy effects, and after receipt commit.
Both SQLite and live PG roll back or replay without duplicate effects. These do
not test host power loss. New files are intent-to-add; no contents staged by this
turn and no commit/PR requested. Existing user comments and data-submodule edits
are preserved.

Final evidence:

- `/tmp/liuxin-maintenance-review-final.log`: **277 passed, 28 backend-specific
  skips**, no failures/xfails, 354.24 seconds. PostgreSQL 16 was actually configured
  and exercised, including Core worker reopen. This is a focused matrix, not the
  entire repository suite. Exact selection below.
- `/tmp/liuxin-maintenance-review-docs.log`: **28 passed** (developer links,
  repository hygiene, public documentation and Catalog ownership).
- `/tmp/liuxin-maintenance-review-final-links.log`: **16 passed** after adding
  the final results and working-memory navigation (overlaps the docs selection).
- `/tmp/liuxin-maintenance-review-format.log`: all 246 scoped files pass. Changed
  runtime/bridge/hooks/capture and new tests pass Ruff lint and formatting.
- `/tmp/liuxin-maintenance-review-imports.log`: 252 protected modules pass;
  `...-layers.log`: 1,867 modules, unchanged 15 reviewed backward edges and 28
  unresolved dynamic imports. `...-footprint.log`: all 25 cold-import budgets pass.
- `/tmp/liuxin-maintenance-review-final-telemetry`: peak process-tree RSS
  **146.6 MiB**, at least **4,425 MiB available RAM**, zero OOM-kill delta.
  C: remained near 3.9 GiB free. All scratch was under /tmp with retention disabled.

Reproduce with `LIUXIN_TEST_POSTGRES_URL` pointing at an isolated live database:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -n0 \
  tests/databases/test_maintenance_*.py tests/core/test_durable_maintenance.py \
  tests/catalog/test_duplicate_inspection.py tests/jobs/test_durable_job_claims.py \
  tests/jobs/test_jobs_worker.py tests/jobs/test_jobs_repository.py \
  tests/databases/database/database_contract/test_db_transactions_locking.py \
  tests/databases/database/database_contract/test_db_dirty_queue_and_maintenance.py \
  tests/databases/test_sqlite_progress.py tests/surfaces/test_cli_operational_families.py \
  --telemetry-dir=/tmp/liuxin-maintenance-review-final-telemetry --telemetry-interval=2 \
  --basetemp=/tmp/liuxin-maintenance-review-final \
  -o cache_dir=/tmp/liuxin-maintenance-audit-cache -o tmp_path_retention_policy=none
```

The private instance `/tmp/liuxin-maintenance-audit-pg.15iiQp` on socket port
55433 was stopped after the final run; its server log is retained separately.
No system PostgreSQL instance was changed. No test processes remain running.
The operator/developer guides record the fixes, tested boundaries and remaining
explicit limits: no automatic backfill or declaration upgrades, bounded drains,
database-only handler effects and a nondurable transient queue. The retirement
inventory remains the restoration plan for removed domain capabilities.

## Restored Agent rename and sort maintenance — 2026-11-02

Goal: determine which retired capability has a defensible current implementation
and restore it. Selected Agent rename/sort maintenance: the Agent schema, metadata
formatter and Catalog transaction owner already exist. Duplicate inspection was
already restored; semantic merges, orphan cleanup and inherited aggregates need
additional domain policies and were not silently reintroduced.

- `AgentRepository.rename` atomically updates canonical name and sort, with a
  dry-run proposal. Default preserve keeps every nonblank stored sort (including
  literal `none`) because historical rows have no derivation provenance. Derive
  regenerates explicitly; a nonblank sort_name supplies an override.
- `repair_sort_names` takes 1–500 explicit positive IDs, deduplicates in encounter
  order, validates all rows before writing and changes only sorts. Missing IDs,
  blank names, write errors and prompt failures abort the whole batch. Repository
  methods apply by default; dry_run=True is advisory and performs no writes.
- `metadata.author_names.agent_name_to_sort` owns pure value policy. Persons use
  copy/comma/nocomma explicitly; organisations/groups/pseudonyms retain display
  order. No locale inference. Aliases, subtype fields, Agent identity and credit
  links are preserved. Generic CRUD does not infer these semantics.
- `AgentsSQLMixin.lock_agent_rows` is the 66th table SQL operation. Apply reserves
  the serialized macro unit, prepares the maintenance write boundary, then takes
  ordered PostgreSQL row locks before reading. SQLite uses the reserved writer.
  Nested composition requires an already serialized macro transaction.
- Core commands `catalog.agent.rename` and `catalog.agent.sort.repair` default
  to dry_run=True; applied changes use existing cache reconciliation. CLI:
  `liuxin catalog agents rename ID NAME` and `... repair-sort ID [ID ...]` preview
  by default, with --yes to apply. Neither opens storage or starts maintenance.
- Updated API declarations/docstrings, operator/dev guides, man page, table-SQL
  inventory and retirement decision. No catalogue schema migration, no restored
  legacy plugin, and no new dependency. Changes are uncommitted; the new test file
  is intent-to-add. Existing user edits remain preserved.

Final evidence:

- `/tmp/liuxin-agent-names-final.log`: **88 passed, two backend-specific skips**,
  192.54 seconds; live PostgreSQL 16 plus SQLite/APSW. Includes
  `tests/catalog/test_agent_name_maintenance.py`, aggregate mutations, Core write
  routes, existing author formatting, metadata-SQL table contracts, Catalog API
  docs/no-SQL ownership, and CLI operation routing. It is not a full repo run.
- New regressions prove type-specific sorting, manual/literal-none preservation,
  explicit regeneration, complete-batch preflight, late-write rollback, enclosing
  transaction rollback, prompt effect/source rollback, no-write previews, real
  Core/CLI routing, and PostgreSQL waiting for a concurrent raw row writer before
  choosing a sort. The two skips are the PostgreSQL-only row-lock test on SQLite.
- `/tmp/liuxin-agent-names-docs.log`: **23 passed** (developer links, public
  documentation boundaries and repository hygiene).
- `/tmp/liuxin-agent-names-final-links.log`: **16 passed** after the final
  evidence/navigation updates; these overlap the documentation selection.
- `/tmp/liuxin-agent-names-cli-smoke-final.log`: the installed executable with
  its actual session factory preserves preview state, persists rename and repair,
  preserves reserved Agent row 0 and creates no jobs ledger. An initial smoke
  assertion accidentally selected the reserved row; the independent probe confirmed
  correct persistence and the final smoke addresses rows by explicit ID.
- `...-lint-final.log`: all touched Python files pass Ruff. `...-format-final.log`:
  all 246 scoped files pass. Import cycles (252 modules), layer direction (1,867,
  unchanged reviewed exceptions), all 25 cold-import budgets, man rendering and
  git diff whitespace checks pass. Only two pre-existing CLI lint nits were also
  corrected (import spacing and one equivalent f-string).
- `/tmp/liuxin-agent-names-final-telemetry`: 151.2 MiB peak process-tree RSS,
  zero OOM-kill delta. Test scratch stayed under /tmp with retention disabled;
  C: had about 9.6 GiB free near completion.

The isolated `/tmp/liuxin-agent-names-pg.fl3hend2` server on Unix socket port 55434
was stopped and removed after validation. Its server log remains at
`/tmp/liuxin-agent-names-postgres-server.log`. No system PG instance was touched.
No commit, push or PR was requested.
