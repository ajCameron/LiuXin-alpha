# Maintenance consistency modes

Implemented **2026-11-01**, reviewed **2026-11-03**. This replaces the earlier
declaration-only sketch.
The optional journal is installed explicitly; opening a Library attaches Catalog
implementations without migrating the database or starting a worker.

| Mode | Required boundary | Failure behavior |
| --- | --- | --- |
| `PROMPT` | Same connection and transaction as the source write, before successful write-unit completion or a dependent read. | Source and effects roll back together. A nested failure rolls back its savepoint, preserving independent outer work. |
| `LAZY` | Obligation commits atomically with the source. A jobs worker subsequently applies effects and acknowledges that exact obligation in one catalogue transaction. | Failed effects roll back; pending work, attempt count, backoff and error remain inspectable. |

Priority orders ready work; it does not change consistency. Lazy becomes current
while an available implementation and worker make progress. Paused, missing,
exhausted or cancelled work remains pending and visible.
The publisher orders eligible obligations by descending plugin priority, then
obligation ID; jobs claims use descending job-definition priority with FIFO ties.
Dependencies still determine eligibility. Already published jobs retain their
captured scheduling configuration, including handoffs recovered across deployment.

## Owners and concrete consumers

[MaintenanceRuntime](../../src/LiuXin_alpha/databases/maintenance/runtime.py)
owns declaration validation, capture configuration and transaction dispatch.
[Core's bridge](../../src/LiuXin_alpha/core/maintenance_jobs.py) publishes to the
existing [jobs ledger and worker](../jobs/README.md). Jobs owns claims, lease
renewal, cancellation and execution records; the catalogue owns obligations and
completion receipts. There is no second maintenance lease table.

[Catalog](../../src/LiuXin_alpha/catalog/maintenance.py) supplies two consumers:

- `catalog.work-revision` is prompt. Work and expression/Work-link changes publish
  a Work revision receipt, including both endpoints when a link moves. Deletion
  records a tombstone.
- `catalog.work-summary` is lazy and depends on that receipt. It counts distinct
  Expressions through physical `expression_work_links`. A deleted Work gets a
  deleted/zero-count summary.

`work_summary(database, work_id)` returns the cached value, source and summary
revisions, and `current`. It never schedules repair. Missing summaries report
`current=False`. Installation captures subsequent writes; it does not backfill
existing Works automatically. The explicit Catalog `rebuild_work_projections`
operation schedules bounded source/key pages with durable checkpoints, invalidates
selected cached values and reuses normal prompt/lazy delivery. It repairs missing
or corrupt entries and tombstones deleted identities. `maintenance rebuild` resumes
the scan; `--restart` requests a new one. Status separates scheduling from pending
completion. Validation, normalized identities and mutation policy remain
with their existing Catalog/metadata/schema owners.

## Write boundaries

The SQLite and SQLite_apsw connection classes, PostgreSQL adapter and
`transaction_scope` run completion hooks. Supported portable macro, direct driver
and Row/Catalog writers use those connections. Nested work uses
`transaction_scope(..., owns_transaction=False)`; native connection contexts
retain whole-connection semantics and do not replace nested savepoints.

Raw row SQL on an attached connection can be batched inside a write unit. A
dependent `SELECT`, `runtime.flush(connection)`, savepoint completion or commit
flushes prompt work. A returned raw `execute` cursor alone is not the end of a
batched write unit. SQLite autocommit writes require an explicit transaction;
scripts run as explicit owned/nested write units and reject embedded transaction
controls when maintenance is installed. Uninstalled scripts keep their old behavior. SQL
`COMMIT` and cursor execution also use completion hooks.
SQLite settings that implicitly commit (`isolation_level=None` and
`autocommit=True`) run the same completion hook and roll back on failure. Both
SQLite autocommit APIs require an explicit transaction before captured writes.
SQLite CTE writes open the same implicit transaction as ordinary DML when the
connection uses implicit transactions; CTE reads do not start one. Cursor class
factories are wrapped from creation, including cursors made before installation.
Callable factories that are not cursor classes are refused.
SQLite replacement conflict handling (`REPLACE`, `INSERT OR REPLACE`,
`UPDATE OR REPLACE`) is refused on installed write handles and handler connections.
Use an explicit UPSERT or DELETE/INSERT unit so capture retains both endpoints.
PostgreSQL execute calls require one statement once maintenance is installed;
embedded transaction controls cannot bypass the completion boundary.
Subscribed PostgreSQL tables reject `TRUNCATE`, including cascades and external
SQL connections, through a persistent statement trigger. Use `DELETE` so every
removed identity is captured. Trigger/schema administration remains an explicit
deployment operation, outside the row-write contract.

Each prompt drain has its own savepoint. If the caller catches a dependent-read
or explicit-flush failure, all effects, generated changes and acknowledgements
from that drain roll back, including earlier successful batches. The source work
remains in the caller's transaction and can be retried at the next boundary.
An uncaught completion failure still rolls back the owning write unit.

PostgreSQL source writes and deliveries take the same catalogue advisory
transaction lock. This preserves revision order across concurrent writers.
SQLite's single writer provides the corresponding serialization. This initial
implementation favors a clear transaction contract over parallel catalogue writes.
Installation and delivery acquire that database lock before the runtime's Python
lock; a waiting worker must not prevent an existing writer from completing.

Triggers capture identities and immutable change IDs, without invoking arbitrary
Python handlers inside a SQL statement. Subscriptions persist when code is absent.
Missing, disabled or incompatible prompt implementations block source mutations.
A bare external connection cannot supply prompt code: use Library or explicitly
attach the required runtime before writing.

## Plugin contract

Implement `MaintenancePluginBase` or its protocol. Declare stable `name`, string
`version` (default `"1"`), `mode` (default `LAZY`), physical `tables`, optional
`dependencies`, `priority`, `retry_limit` and `retry_backoff_s`. Capture requires
one integer primary key. Install explicitly with
`MaintenanceRuntime(database, plugins).install()`. Library already attaches the
Catalog implementations to its driver.

Names and versions cannot contain NUL. `enabled_by_default` must be a boolean;
priority and retry limit must fit portable database integer ranges, with room for
an exhausted attempt. Backoff must be finite, nonnegative and representable in
the stored floating-point column. The six maintenance-owned tables cannot be
subscription sources. Declarations, synchronous hook callability and upgrade
policies are validated before attachment; execution rechecks attached declarations.

Dependencies precede priority. Missing dependencies, cycles and prompt
dependencies on lazy completion are rejected. Persisted declarations cannot be
silently replaced. Version/configuration changes use
`runtime.upgrade(plugin_id, expected_version=..., dry_run=True)` and an explicit
apply. The new implementation declares a `MaintenancePluginUpgrade` for the old
version, optionally with `replay_pending=True` and a database-only migration
callback. Without replay, old pending work must drain first. Pending dependents
and mode changes always require draining. Apply validates the resulting dependency
graph, replaces subscriptions, invalidates the plugin's and dependents' entries in
`maintenance_projections`, and fences replayed obligations with a new jobs
generation. Completed history keeps
its captured version. All changes and the callback share one serialized transaction.
Removed final subscriptions also remove only their owned capture triggers.
Changing a declaration requires a new version; automatic upgrades are not provided.
Mutating an attached declaration also fails execution.

The callback migrates any other plugin-owned stored state; automatic invalidation
is limited to the standard projection store. It receives a borrowed context and
the previous declaration. It must run synchronously and return `None`.
Its connection expires on return and cannot commit,
change schema or open another
transaction. Use explicit external schema migrations for DDL. Rebuild projections
after upgrading; old checkpoint declarations report `obsolete` and require an
explicit restart. The latest upgrade receipt per plugin is retained in maintenance
metadata, without introducing another jobs log. See the
[operator sequence](../../docs/maintenance.md#upgrade-an-installed-plugin).

Durable handlers use `context.connection`; `context.db` is None to prevent
accidentally opening a second connection through the general facade. The borrowed
connection accepts single row statements, refuses transaction/schema controls,
and expires on return. Plugins are trusted Python extensions, not sandboxed code:
external effects, independent connections and deliberately bypassing the supplied
API violate the contract. Filesystem/network effects cannot participate in rollback.

Selection, coalescing and handler exceptions share the same rollback boundary.
`wants_event` must return an actual boolean: `False` deliberately skips that event;
`None`, integers and other truthy/falsey values are errors. Skipped events are
acknowledged only when the entire invocation succeeds. Async/generator callbacks,
including deferred results hidden behind synchronous wrappers, are refused.
Coalescing replaces invalidations only with an explicitly supplied key; distinct
actions must remain distinct. A key must be hashable, or `None` for no coalescing.
Return `MaintenancePluginResult` with `handled`
equal to the selected/coalesced count and zero deferred/errors. Incomplete or
malformed results fail the batch.

`RowChangeEvent` retains a durable change ID and before/after identity columns
(`*_id` plus the primary key), including deleted relationship endpoints. Rebuild
values are read through the bound connection. A plugin's own writes suppress its
own repeated invalidation, so it must leave its output consistent. Cross-plugin
writes retain ancestry; revisiting an earlier plugin fails. Prompt drains are
bounded to 64 batches of 128 obligations and ancestry to 32 plugins. Large imports
should use bounded write units.

## Durable delivery and recovery

The explicit migration creates six optional helper tables: metadata, installed
plugins, subscriptions, changes, obligations and projections. A persistent journal
UUID identifies the catalogue generation. Source rollback, including savepoint
rollback, removes corresponding obligations.

Core publishes committed obligations with stable keys containing journal UUID,
obligation ID and explicit retry generation. Publication can commit before the
catalogue stores the job ID; repeating the handoff uses the same jobs request key.
Workers reopen their own Library and check library/journal/plugin identities and
the live jobs claim before effects and before commit.

Effects and catalogue acknowledgement commit together. A crash before commit
rolls both back. A crash after commit but before the jobs success receipt can be
replayed without repeating effects: completed or safely pruned identities do no
work. Acknowledging selected obligations leaves newer revisions pending. Jobs
fences stale claim owners; catalogue serialization and receipts prevent duplicate
committed effects.

Failures retain an error and exponential retry delay, capped at one day.
A delay that rounds to zero milliseconds remains immediately eligible even if
the wall clock moves backwards. Positive delays use persisted wall-clock deadlines.
Diagnostic messages are bounded to 2,000 characters and made safe for backend
text storage. A broken exception formatter cannot replace the original exception
or prevent retry/worker failure evidence from being recorded.
`retry_limit` counts retries after the initial attempt. The bridge reconciles
expired jobs and submits explicit successors through the existing repository API.
Its acknowledgement of possible effects is justified by the atomic catalogue
receipt. Missing/paused implementations are not newly submitted; cancelled work
requires an explicit retry.
Queued deliveries encountered while their attached plugin or dependency is paused
retire with `handled=0` and a
`deferred` count, without acknowledging obligations or spending handler attempts.
They clear the catalogue job association and advance its generation in one
transaction, so resume can publish a new delivery even if the old jobs receipt was
interrupted. Direct unavailable delivery raises without recording a failed attempt.
Core's generic jobs retry refuses maintenance runs and directs callers to
maintenance retry. Publication can recover a successor created by a lower-level
jobs caller without abandoning the rest of the publication page.
The jobs handler supplies both ledger UUID and run ID. Superseded deliveries
cannot apply effects or change the successor's retry state, even when a replacement
ledger reuses a numeric run ID. Failure accounting is idempotent per current run;
repeated failure reports do not spend additional retries. Direct, non-jobs
deliveries remain explicit calls and each failed attempt counts separately.

## Operator interfaces

See the [user maintenance guide](../../docs/maintenance.md). Commands preview by
default; `--yes` applies them:

```text
liuxin maintenance install --database /path/catalog.sqlite --yes
liuxin maintenance status --database /path/catalog.sqlite
liuxin maintenance drain --database /path/catalog.sqlite --max-batches 128 --yes
liuxin maintenance retry catalog.work-summary --database /path/catalog.sqlite --yes
liuxin maintenance pause catalog.work-summary --database /path/catalog.sqlite --yes
liuxin maintenance resume catalog.work-summary --database /path/catalog.sqlite --yes
liuxin maintenance prune --database /path/catalog.sqlite --max-events 128 --yes
```

Core `maintenance.run` actions are `install`, `drain`, `retry`, `pause`, `resume`,
`prune` and `start`. `start` enables continued polling in a long-lived Core
process. CLI drain leases only maintenance jobs, leaving queued storage audits
untouched. It performs bounded synchronous job passes; repeat or schedule it
for eventual progress. SQLite uses the existing adjacent jobs ledger; PostgreSQL
needs a separately configured durable jobs path (`--job-ledger PATH` in the CLI).

Status reports schema/journal identity, declarations, pending totals and age,
attempts, retry time, last error and job IDs. It opens no jobs ledger and starts
no worker. Explicit retry advances the delivery generation. Pruning removes only
fully acknowledged changes and preserves monotonic delivery identities.

`maintenance prune-jobs` separately retires bounded, age-limited jobs history.
Core holds the catalogue writer boundary while checking library/journal identity
and completion (or prior safe journal pruning). Jobs then atomically rechecks
terminal state, active definitions and retained retry descendants before deleting
approved runs, events, request keys and orphaned definitions. Only completed domain
deliveries may retire their idempotency keys. A crash after the jobs deletion but
before returning the catalogue transaction cannot cause re-publication: pending
obligations were never eligible. Ledger identity and monotonic run IDs survive.
The CLI defaults to seven days and 128 inspected runs, supports descending keyset
pagination, and previews without opening a ledger. Cleanup makes pages reusable;
it does not compact files or prune other job kinds. See the operator guide for
the retirement and scheduling policy.

## Legacy route and evidence

The old queue engine is an explicit transient compatibility adapter, not the
durable lazy path. Prompt subscriptions use MaintenanceRuntime. Obsolete creator
rename/sort plugins, title aggregates and destructive duplicate repair were
removed on 2026-11-02; the default rename route directs callers to Catalog.
The transient engine and counting fixture plugin remain for explicit consumers.
Catalog owns read-only duplicate inspection. See the
[retirement inventory](legacy-maintenance-retirement-2026-11-02.md) for removed
capabilities and requirements for future replacements.

The physical merge helper uses one serialized portable-macro transaction for
registered interlink transfers and source deletion. It verifies duplicate identity
and matching non-timestamp payloads before deleting a redundant link; other
conflicts and all write failures propagate and roll back the merge. This is not a
complete semantic metadata/intralink/tree merge. Selective cleanup remains
unimplemented and is exposed as `capability_unavailable` through Core rather than
raising a misleading `TypeError` or claiming success.

[Runtime regressions](../../tests/databases/test_maintenance_runtime.py) cover
SQLite and live PostgreSQL rollback, dependent reads, missing implementations,
cycles, restart, publication/receipt failures, claim loss, newer revisions,
tombstones and pruning. [Journal tests](../../tests/databases/test_maintenance_journal.py)
cover capture and completion hooks; [mode tests](../../tests/databases/test_maintenance_modes.py)
retain checks for the legacy queue boundary.

## Review checkpoint — 2026-11-02

The follow-up review reproduced and fixed six defects:

1. A caught prompt failure retained partial effects; retry could commit duplicates.
   The savepoint now covers the whole drain, across handlers and batches.
2. SQLite's implicit-commit settings bypassed completion. Both native transaction
   modes and successful/failed settings changes now have regression coverage.
3. PostgreSQL `TRUNCATE` silently bypassed row capture. Persistent guards now
   refuse it for prompt and lazy subscriptions, including external cursors.
4. Installation/delivery could deadlock a source writer on the same runtime by
   acquiring the Python and database locks in opposite orders. Deterministic
   concurrent writer tests now cover both operations on both engines.
5. Stale jobs could overwrite retry diagnostics and consume a successor's budget.
   Delivery and failure accounting now check the current ledger/run association;
   operator retry, ledger replacement and duplicate reports are covered.
6. The transient interlink UDF captured the startup dummy maintainer permanently
   on older SQLite handles. Both drivers now resolve the active callback on each
   invocation. The two old expected failures are ordinary passing tests.

[Failure-boundary tests](../../tests/databases/test_maintenance_failure_boundaries.py)
also exercise a 300-row, two-plugin drain with Python and SQL exceptions, checking
that effects and acknowledgements from earlier batches disappear before retry.
[Process-recovery tests](../../tests/databases/test_maintenance_process_recovery.py)
terminate real subprocesses without Python cleanup during source writes, during
lazy effects and after acknowledgement. They verify rollback or safe replay on
SQLite and PostgreSQL. These establish process-crash behavior, not host power-loss
or filesystem-corruption guarantees.

Final validation on **2026-11-02**: **277 passed, 28 backend-specific skips** in
the combined maintenance/production-writer/Core/jobs/transaction/progress/CLI
matrix, with live PostgreSQL 16 plus SQLite and SQLite_apsw. No failures or
expected failures remain in that selection. A separate documentation/ownership
selection passed **28 tests**. Formatting, lint, import cycles, architectural
layer direction, all 25 cold-import budgets and diff whitespace checks passed.
This was the maintenance regression matrix, not the entire repository suite.
Telemetry recorded a 146.6 MiB process-tree RSS peak and no OOM kills; the private
PostgreSQL instance was stopped after validation.

Remaining deliberate limits: historical backfill requires an explicit rebuild;
plugin upgrades require an implementation-authored migration policy and an explicit
apply; prompt drains are bounded;
handlers must keep effects in the borrowed database transaction. The transient
queue remains nondurable. Catalog now owns explicit semantic merge and selected
cleanup through its [repair policies](../catalog/maintenance-repair-policies.md);
maintenance's older physical interlink merge retains its narrower contract.
The [retirement inventory](legacy-maintenance-retirement-2026-11-02.md) distinguishes
current replacements from capabilities that still need a supported policy.

The subsequent [2026-11-03 failure validation](maintenance-adversarial-validation-2026-11-03.md)
records malformed-plugin, callback, diagnostic and process-recovery checks, along
with the full-suite outcome when available. It supersedes the older matrix above
as the current validation checkpoint.

Related: [transaction ownership](transactions.md),
[Catalog/database ownership](../catalog/catalog-database-ownership.md).
Return to [Databases](README.md).
