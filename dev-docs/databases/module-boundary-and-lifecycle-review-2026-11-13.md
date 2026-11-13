# Database module boundary and lifecycle review

Date: **2026-11-13**. LC01–LC04 repaired and validated.
The observations below describe the pre-repair code.

## Ownership decision

Databases owns persisted representation and structural integrity. Catalog owns
bibliographic meaning, validation, matching, selection and coordinated semantic
changes. Efficient low-level SQL can reasonably belong to either layer; there is
a **slight preference for databases**, which already owns backend differences,
binding, batching and transaction machinery. Responsibility and a concrete
efficiency benefit take priority over a method's former location or the fact that
its body contains SQL. See [Catalog/database ownership](../catalog/catalog-database-ownership.md).

Physical-table helpers may know about Works, Items, assets and other stored tables.
Their SQL implements requested persistence and queries; domain owners determine
which entities to change and what the result means. Shared pure value policies
remain in metadata. Native custom-field storage belongs to its feature package;
Calibre calling conventions and projections belong to explicit compatibility
adapters, as described in [custom-column ownership](custom-column-ownership.md).

## Package assessment

The namespace itself is lightweight and the persistence contracts have useful
separation. Schema inspection, table SQL, native custom fields and maintenance
are legitimate database components. The remaining architectural work is:

1. Give ordinary composed writes one predictable transaction boundary. Portable
   macros join managed units; generic Row and direct wrapper writes still have
   their own documented commit points. Preserve the distinction between a
   resource context and a SQL transaction.
2. Reduce Database's runtime assembly responsibilities. Library/Core should
   compose storage services; open, create and repair should be deliberate
   lifecycle operations. Database-owned integrity maintenance remains appropriate
   here, with explicit activation and shutdown ownership.
3. Continue the broader Calibre extraction. Legacy series numbering, exports,
   cache types and connection-installed compatibility functions still occur in
   the wider package. Classify their responsibilities and migrate callers; useful
   general capabilities can have native owners.
4. Split large implementations along responsibility boundaries, such as
   transactions, rows, relationships, identities and repair. File size alone does
   not establish an ownership problem.

The preceding custom-field work passed a 2,273-case database gate, with final
targeted reruns documented in its working-memory handoff. That evidence supports
the persistence foundation; it does not certify every lifecycle failure path.

## Lifecycle review scope and sign-off criteria

Review opening and creation, implicit bootstrap writes, partially failed
construction, optional service startup, collaborator replacement and shutdown.
Use disposable databases, independent readers for durable effects and controlled
failure injection for resource cleanup. Existing repair contracts remain in force.

For sign-off, construction failures must release owned resources predictably;
shutdown must account for owned services before invalidating their dependencies;
public rebinding operations must preserve a coherent database owner; and opening,
creation, repair, resource cleanup and transaction completion must have distinct,
documented meanings. Architectural improvements without a reproduced failure
should be recorded separately from correctness blockers.

## Findings

All four findings were reproduced on fresh generated catalogues through both
`SQLite` and `SQLite_apsw`. They are existing lifecycle gaps, including documented
legacy behaviour, rather than regressions attributed to the custom-column work.
The two setter routes produce separate observations of LC04.

| ID | Priority | Trigger | Observed result |
| --- | --- | --- | --- |
| LC01 | P2 | Storage loading raises during strict construction after maintenance starts | Constructor raises while its worker and database connection remain usable |
| LC02 | P2 | Maintenance dispatch takes longer than the close timeout | Database close returns normally, clears its bindings, and leaves the worker running |
| LC03 | P2 | Close a Database that constructed and bootstrapped its own StorageManager | Manager and Store shutdown are never invoked |
| LC04 | P2 | Install another Database's macros or wrapper through a public setter | Writes through the first facade reach the second catalogue while other aliases still target the first |

### LC01. Construction failure has no complete resource cleanup

[`Database.__init__`](../../src/LiuXin_alpha/databases/database/__init__.py)
initializes the driver and maintenance before calling storage bootstrap. An
exception from strict storage loading escapes without closing resources already
owned by the partially constructed object.

The probe injects a `load_from_database` failure in a real StorageManager after
starting the real maintenance worker. After the constructor raises, the worker
is alive and `driver.conn.execute("SELECT 1")` succeeds. Finalization is not a
predictable replacement for constructor cleanup; the worker's running context
also retains the database. The probe explicitly closes both owners afterward.

Repair direction: stage construction under an explicit cleanup owner and unwind
completed resource acquisitions on failure. Preserve the original exception and
report cleanup failures. Distinguish resource cleanup from rollback of earlier
committed bootstrap writes: closing resources does not undo those writes.

### LC02. Shutdown invalidates a worker's dependencies before it stops

[`Maintainer.close`](../../src/LiuXin_alpha/databases/maintenance/service.py)
correctly raises when its worker is still alive after the join timeout.
`Database.close` catches that failure and retains the worker for retry, but then
closes the driver and clears the facade's collaborators anyway. It returns
without reporting the incomplete shutdown.

The probe blocks a real maintenance dispatch on an event, calls `Database.close`,
then releases the event. Close returns while the worker is alive and the driver
binding is gone; resumed dispatch raises `DatabaseIntegrityError` when accessing
the now-closed macro owner. This needs no overlapping caller-owned transaction.

Repair direction: distinguish closing, incompletely closed and fully closed
states. Keep dependencies needed by outstanding owned work until it has stopped,
and make an explicit close report timeout/failure so the caller can retry. A
best-effort destructor can remain separate from that public close contract.

### LC03. Database-created storage has no matching shutdown owner

[`bootstrap_storage_manager`](../../src/LiuXin_alpha/databases/runtime.py)
constructs a manager when `db.storage` is absent. `Database.close` never calls its
close method, and `break_cycles` deliberately retains the storage reference.
Library borrows this manager and likewise does not close it, so the ordinary
Database context does not complete the lifecycle of the manager it created.

The probe persists a `single_file_sqlite` Store configuration, opens Database
with storage bootstrap enabled and verifies that the real Store is started.
After database close, `manager._closed` remains false and the Store driver's
`_started` flag remains true. Explicit manager close changes both states. This
establishes a missing shutdown call; the SQLite Store uses per-operation handles,
so this probe does not claim it leaked a persistent BLOB connection.

Repair direction: move storage construction to an application owner that also
closes it, as the preferred boundary already suggests. If Database retains this
option, distinguish owned from injected storage and close only what it owns,
before releasing database dependencies needed during storage shutdown.

### LC04. Public collaborator setters bypass complete driver binding

`set_driver` validates ownership and prepares a complete binding. The public
`set_macros`, `set_driver_wrapper` and `set_metadata_sql` methods instead assign
individual collaborators. Their docstrings acknowledge that they do not rebuild
the owner graph; `set_driver_wrapper` even assigns before validating the candidate.
These shortcuts remain callable after construction and bypass the guarded route.

With two independent catalogues, `first.set_macros(second.macros)` allows
`first.macros.insert_row` to commit into the second file. With the second wrapper
installed instead, `first.driver_wrapper.add_row` also writes to the second file,
while `first.execute` remains bound to the original wrapper. Independent SQLite
readers confirm zero matching rows in the first catalogue and one in the second
for each route. The test restores both owner graphs before cleanup.

Repair direction: remove unused public assembly setters or make any supported
replacement use one validated, atomic binding operation. Prefer private assembly
for construction. Rejection must precede mutation, foreign owners and closed
facades must be rejected, and aliases, locks and transaction providers must agree.
The existing guarded `set_driver` rejection tests are useful positive controls.

## Implemented lifecycle contract

- **LC01:** construction now unwinds acquired resources on any escaping exception.
  A driver constructed internally is also closed when binding preparation rejects
  it; a rejected caller-supplied driver stays with its caller. Successful binding
  transfers ownership. Cleanup preserves the original exception and adds a note
  if shutdown itself fails. Earlier committed bootstrap writes remain committed.
- **LC02:** `Database.lifecycle_state` distinguishes `initializing`, `open`,
  `closing` and `closed`. Close stops all owned maintenance before touching
  storage or database dependencies. A timeout propagates, retains the owner graph
  and leaves `closing`; finish the outstanding work and call `close()` again.
  Driver replacement and service startup are refused once shutdown begins.
  Existing worker access remains available so it can finish. Callers must stop
  starting their own work and serialize lifecycle operations with ordinary use.
- **LC03:** managers created by database bootstrap are tracked by identity and
  closed before database handles, even if the public `storage` attachment is
  subsequently replaced. Detaching storage does not permit switching catalogues
  while that ownership remains. Injected managers remain caller-owned. A failed Store
  shutdown leaves its manager eligible for retry; errors propagate through
  database close without dropping database dependencies. Storage orchestration
  can still move to Library/Core separately.
- **LC04:** `Database.set_macros`, `set_driver_wrapper` and `set_metadata_sql`
  are removed from both the concrete class and `DatabaseAPI`. Their properties
  remain read-only. Use the complete guarded `set_driver` operation for supported
  replacement; there are no forwarding compatibility aliases.

An explicit close reports shutdown failure. The destructor remains best effort.
If a resource-context body already raised, cleanup failure is attached as an
exception note so the body error remains primary. A failed close is incomplete
shutdown, not permission to resume normal use. Resource cleanup is still distinct
from a transaction: pending work is discarded when connections close, and earlier
commits are never undone.

Permanent regressions live in
[`test_database_lifecycle_repairs.py`](../../tests/databases/test_database_lifecycle_repairs.py)
and its
[PostgreSQL execution module](../../tests/databases/test_database_lifecycle_repairs_postgres_live.py).
They exercise real workers, primary and auxiliary handles, bootstrapped SQLite
object Stores, retained dependencies, cleanup failures, retries and caller-owned
storage. The older review probes intentionally assert the original defects and
remain historical evidence rather than tests of the repaired contract.

## Deliberate contracts and follow-ups

- Missing file paths can still create a catalogue with `create=False`, and
  `repair_bootstrap_rows=True` can repair rating/sentinel rows during opening.
  Those behaviours are explicit in the current docs/tests. Separating open,
  create and repair is an API improvement, rather than an additional reproduced
  defect from this review.
- Resource contexts roll back pending primary work on close, while previously
  committed work remains committed. Ordinary direct Row/wrapper writes still
  have the boundaries listed in [transaction ownership](transactions.md).
  Lifecycle cleanup must preserve that distinction.
- Keep storage orchestration above databases and continue the remaining Calibre
  extraction. Splitting large internal modules can follow these ownership fixes;
  it is not itself a prerequisite for correctness sign-off.

## Original review validation

The existing lifecycle, driver-rebinding and transaction suite passed **76 tests**;
creation and replacement safety passed another **38**, for **114 existing checks**.
The additional disposable probes reproduced **10 observations**: five routes
on each SQLite driver name, covering the four findings above. They used real
database connections and workers, a real bootstrapped SQLite Store, independent
readers for committed writes, and controlled failure/event injection. All probe
workers and stores were explicitly closed and temporary catalogues removed.

Local evidence is retained under `.tmp/database-lifecycle-review-2026-11-13/`:
`probes.py`, `probe-results.json`, `probes.log`, `baseline-tests.log` and
`creation-tests.log`. The
probes assert the observed defects for review reproduction; they are not passing
regression tests for repaired behaviour. Documentation checks are recorded in
the working-memory handoff.

No live PostgreSQL run was performed for this review. The findings concern shared
facade code, but this evidence does not establish every PostgreSQL failure path.
No production implementation was changed during the review.

**Original sign-off recommendation:** repair LC01–LC04 and add permanent failure-path
regressions before signing off the lifecycle surface. Existing normal-path
coverage and the recent full database gate are encouraging, but do not cover
these reproduced gaps. The architectural follow-ups can be scheduled separately.

## Repair validation — 2026-11-13

The 44 new mandatory regressions cover 30 SQLite/SQLite_apsw cases and 14 real
PostgreSQL cases. They retain all 2,162 earlier manifest cases, bringing the
mandatory set to 2,206. Validation for this change comprises:

- Final SQLite lifecycle/rebinding selection: 82 passed; the final detached-store
  ownership guard and full rebinding module then passed 24 cases (22 overlap).
- Final PostgreSQL lifecycle module: 14 passed.
- Creation, transaction, Row state, metadata SQL and facade lifecycle integration:
  172 passed.
- Storage-manager reload/contracts and Library lifecycle integration: 60 passed.
- Documentation/navigation: 22 passed; regression-gate tooling: 21 passed.
- Full formatting, lint, architecture, import and typing runner: passed, including
  both checkers and their negative contracts.

These are targeted and integration checks of the repaired surface, not a new full
2,206-case database-gate run. Earlier full-gate evidence remains recorded in the
custom-column handoff. Final logs and preservation evidence are retained under
`.tmp/database-lifecycle-fixes-2026-11-13/`. The private PostgreSQL test cluster has
been stopped. All changes remain uncommitted.

The four reviewed lifecycle findings are resolved. The separately listed
open/create/repair API separation, wider Calibre extraction and internal module
splits remain architectural follow-ups.
