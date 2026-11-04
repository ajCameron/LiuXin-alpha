# Full database package typing

Completed **2026-11-04**. The objective was to fast-forward merged work, branch,
and properly type the entire databases package. This handoff supersedes the
active-process and pending-work instructions in earlier checkpoints.

Worktree: `/tmp/liuxin-databases-typing`; branch: `databases-full-typing`.
The reflog records the fast-forward from `6d54293c` to merged integration head
`d8d52587`, then branch creation. #153/#154 landed on `database-review` after #152
had reached main; do not drop that integration work by blindly resetting to main.
The original `/mnt/c/dev/liuxin-alpha` source/preferences/data remain preserved.
The user subsequently requested delivery of the complete pass: commit, push and
open a regular PR targeting `database-review`. No merge was requested. This base
contains both prerequisite merges; current main at `23aa55c9` does not.

Read the [implementation and completion audit](../dev-docs/databases/package-typing-2026-11-04.md)
for contracts, behavior changes and detailed historical evidence.

## Final evidence

- **225 modules, 2,482 annotated function definitions, zero incomplete signatures.**
  Both checker scopes include the whole database root. Strict mypy settings,
  existing basedpyright strict areas and import budgets are retained. No new
  database exclusions or blanket suppression were introduced.
- Full quality runner: **exit 0**, 452 mypy sources, zero basedpyright diagnostics,
  303 formatting targets, all 25 cold-import budgets, cycles/layers/lint pass.
  Valid calls pass and negative probes produce the expected **95 basedpyright /
  93 mypy** diagnostics. Log: `...-quality-final.log`.
- Whole database directory: **2,577 passed, 377 skipped, eight existing xfails**,
  two outdated lifecycle assertions and one packaging-related teardown error.
  The traceback proves the `build/` creation/removal race. The two assertions now
  verify the retained wrapper is cleaned and closed facade access raises.
  Log: `...-all-databases.log`; exec **23252 exited 1** normally.
- Final regressions: **191 passed, two unconfigured native-PG skips**. Every
  broad-run failure/error was rerun successfully, alongside the new preference
  regression, Catalog/Core/Library consumers, API signatures, docs and tooling.
  Log: `...-signoff.log`; exec **49416 exited 0**.
- After the final documentation edits, all **16 documentation checks** passed;
  log `...-final-docs.log`, exec **76568 exited 0**.
- Live PostgreSQL final selection: **46 passed**, including the two native cases
  skipped above. Server stopped and removed; its path record is historical.
- Native backend docstrings: **119 examples passed** after removing obsolete
  constructor-output expectations. Log: `...-native-doctests-final.log`;
  exec **92343 exited 0**. No runtime implementation change was needed.
- Final isolated wheel contains `databases/py.typed` and all 225 database modules;
  the current content verifier and exact-source comparison pass. Use
  scripts/verify_wheel_install.py, not the obsolete scripts/verify_wheel.py.
- Broad telemetry: **258.1 MiB peak test-tree RSS**, **3.81 GiB minimum available
  memory**, **zero OOM kills**. Per-test scratch is discarded under /tmp. The
  failed in-checkout build output was removed after pytest ended; later packaging
  used `/tmp/liuxin-databases-typing-wheel-source-5gnwmok_`.

Log/report prefix is `/tmp/liuxin-databases-typing-`. Additional reports:
`final-inventory.json`, `wheel-final-inspection.json`, `signoff-resources.json`.
Selections overlap. The full-project suite and real-file import were not run;
the broad database invocation was not repeated after the focused fixes.

## Contracts and review notes

The facade has honest nullable internal state and rejects closed/unbound public
access with DatabaseIntegrityError. Standalone runtime initialization reuses the
constructor wiring and rebinds actual `.db` owners. Closed preference writes fail
before changing the cache. DatabaseMacrosAPI describes common operations;
MacrosAPI supplies legacy SQLite operations through an explicit capability check.
PostgreSQL does not claim that legacy API. Native cursors preserve selected row
containers and close owned resources without ending borrowed transactions.

Rows, shared mixins, generators, custom-column collaborators, maintenance plugins
and table SQL have typed contracts. Any remains at dynamic SQL-cell, serialization,
proxy-extension and arbitrary native-row-factory boundaries. A final input audit
also replaced normalization's source Any with object and typed named PG row reads.
The typing extra includes types-psycopg2; runtime optional imports stay type-only.

All implementation and regression work belongs in this delivery, including new
files, cross-package callers, typing probes, packaging metadata and documentation.
Preserve user TODOs and the original dirty checkout. Remaining legacy SQL xfails
and runtime reinitialization atomicity are outside this typing objective and are
documented in the implementation record. Publish from this worktree; do not
recommit the original checkout's already-published maintenance/wrapper changes.

The previous PR-status-only goal turn was **no progress**. The completion turn
verified the existing broad process to termination, resolved/reran its failures,
refined two annotation boundaries, fixed two stale doctest expectations, validated
packaging and completed the full-scope audit. There are no remaining goal tasks.

## Earlier checkpoint: custom-column service contracts

Current inventory: **225 database modules, 21 incomplete signatures**. PostgreSQL
accounts for 19 remaining signatures and the main Database class for two. Full
configured checks report **60 mypy errors and 36 basedpyright errors**, with zero
basedpyright warnings. All custom-column service, manager, mixin, wrapper storage
and custom-column API modules now check cleanly. The entire-package goal remains
active; Database/runtime lifecycle and PostgreSQL still need work.

Custom-column collaborators now have explicit cache-row, preference, notification,
dirtying and descriptor-registration protocols. A missing cache is represented by
None and cached reads raise a clear RuntimeError. The facade resolves positional
cache rows to persistent IDs and refreshes its numeric label map with definition
maps. Default callbacks remain inert only outside embedding, are replaceable, and
honor the declared default commit argument. Embedded hosts still provide callbacks.

The shared storage mixin is generic over its connection contract. The service
uses physical SQLConnection; DriverWrapper retains QueryConnection and its mutable,
rebindable local table-name cache. The service retains live owner discovery. CRUD
bookkeeping inherits the actual storage implementation and accesses sibling
service operations through one typed composition adapter. Public API bodies remain
declarations. Definition edits return bool, value inventories return lists of
pairs, reference lookups return row lists, field maps accept string/integer keys,
and bulk extras retain sequence or position-keyed mapping support.

Four initial regressions failed before corrections: deletion used the wrong cache
keyword, one-pass owner IDs disappeared before dirtying/cache/notification, and
cleanup leaked its fresh connection on both success and failure. Those now pass.
Case-change regressions cover text and series: resolved IDs and replacement values
are passed in the correct positions, and missing IDs raise DatabaseIntegrityError
before link creation. Historical explicit commits and separate SQL/cache failure
boundaries remain; these changes do not make whole value mutations atomic.

The database service consumes a narrow local descriptor protocol, removing one
reviewed database-to-Catalog type dependency rather than adding two more. Existing
runtime FieldMetadata construction remains. Catalog's full FieldMetadataAPI also
accepts its concrete container: accepted datatype constants are read-only facts,
and its mutable label map has an explicit type. The layer baseline removes the
obsolete edge and refreshes four unchanged registry dynamic-import line locations;
no new dependency allowance or graph budget was added.

Validation (selections overlap; do not sum as unique tests):
- Initial service regressions: **four reproduced failures**, then the first
  service/facade lane passed **18 tests**;
  `...-cc-service-repro.log`, `...-cc-service-initial.log`.
- SQLite/APSW service, custom storage, Catalog descriptors and API matrix:
  **251 passed, two skips**; `...-cc-service-matrix.log`. Skips cover absent live
  PostgreSQL configuration and a PostgreSQL-shaped SQLite harness limitation.
- Wrapper boundary/features and compatibility callers: **181 passed, one skip**;
  `...-cc-integration.log`. The skip is the explicitly disabled legacy Calibre
  cache bootstrap module.
- Final callback/descriptor refinements, API and gate-tooling checks:
  **134 passed**; `...-cc-final-tests.log`.
- **52 doctest examples passed**; `...-cc-doctests.log`. The earlier pass exposed
  a wrapper example using an incomplete SimpleNamespace host; the example now
  constructs the actual storage adapter.
- Basedpyright accepts valid calls and rejects **88 marked invalid cases**;
  `...-cc-pyright-probes.log`. Mypy still fails on unfinished package diagnostics,
  with no missing expected or unexpected fixture diagnostics;
  `...-cc-mypy-probes.log`. Do not claim the whole mypy probe gate passes.
- All 25 cold-import budgets, cycle protection and layer checks pass;
  `...-cc-imports.log`, `...-cc-cycles.log`, `...-cc-layers.log`. The layer inventory
  has 1,898 modules, **14** reviewed backward edges and 28 unresolved dynamic
  imports; cycle protection covers 255 modules.
- Scoped F lint and formatting pass; `...-cc-lint.log` (13 formatting targets).
  Whitespace checks pass. Catalog's small annotation edits were not a whole-file
  formatting pass.

Log prefix: `/tmp/liuxin-databases-typing`. Authoritative checker outputs:
`...-mypy-cc-final.log`, `...-pyright-cc-final.json`; coverage inventory:
`...-cc-inventory.json`. The five runtime telemetry samplers stopped normally;
`...-cc-resource-summary.json` records **122.3 MiB** peak test-tree RSS, **3.96 GiB**
minimum available memory and zero OOM kills. All test/checker/gate processes are
terminal. C: retains 5.4 GiB free, /tmp 921 GiB. Original checkout, preferences and
data remain untouched. No full quality run, full repository suite, live PostgreSQL,
commit or push is claimed for this checkpoint.

Next: finish Database/runtime lifecycle and PostgreSQL driver/connection contracts,
including psycopg2 stubs. Database private driver/wrapper/macros fields currently
claim non-nullability despite close/break_cycles assigning None through setattr;
do not hide that with casts. Runtime helpers wire concrete state missing from the
public DatabaseAPI and duplicate some constructor wiring. They were inspected in
this turn but not changed. Full configured checkers, complete quality gates,
representative invalid-call probes, lifecycle/runtime/API regressions and live
PostgreSQL evidence remain necessary before marking the goal complete.


## Earlier checkpoint: schema builders, table SQL and helper boundaries

Current inventory: **225 database modules, 25 incomplete signatures**. The remaining
signatures are in PostgreSQL (19), the custom-column service (four), and the main
Database class (two). Full configured checks report **130 mypy errors and 85
basedpyright errors**, with zero basedpyright warnings. Registry, schema builders,
shared SQL custom-column DDL, table SQL, preferences, notifications, schema specs,
normalization preparation, utility helpers and the wrapper have zero diagnostics
in both checkers. The entire-package goal remains active and unfinished.

The utility builder now separates configured schema state from driver-only
execution/cache hooks. The FRBR generator implements its own script hook on its
caller-owned connection. Three pre-fix regressions failed: SQLite errors were
masked by e.message, a one-pass column iterable was consumed before DDL generation,
and the generator's declared direct-link operation called a missing execution
hook. All are corrected and covered. Parsed link specs use TypedDict records,
fixed endpoint pairs and explicit column choices. The intralink facade exposes
all shared builder options; allowed-type generation correctly declares list[str]
in both APIs. Staged build commits and the known explicit intralink
sequence_number/is_required duplication remain unchanged.

Calibre fixture builders describe path-like inputs, dynamic scalar conversions,
JSON display maps and inserted IDs. Missing insertion IDs have explicit integrity
guards; resource paths decode filesystem bytes. Aggregate scripts execute through
the connection contract. The collision checker accepts the mappings and one-pass
name iterables it actually consumes. PostgreSQL schema inference no longer reuses
a table-definition variable for a string name. Its link-column parser declares
Literal['all'] versus a set. Legacy SQLite version strings retain their exact
spelling while unused format arguments are removed.

The SQL custom-column DDL mixin now uses the shared driver state/API boundary.
Its dispatcher accepts existing boolean/string modes and returns the actual table
name. Cache absence resolves table names through the driver before checking for
an existing custom table. SQL formatting cleanup leaves rendered indexes unchanged.

Table readers require an existing oriented physical relationship before calling
portable macros. Missing relationships still raise InputIntegrityError, now with
endpoint context; declared empty relationships still return empty groups. The
new regression covers every relationship reader and proves absent specs never
reach the query helper. Agent locking takes a borrowed RowSQLConnection and a
narrow backend qualification capability, retaining its transaction and lock order.
Annotation filters declare mixed integer/text values.

DBPrefs has string keys and dynamic serialized values, object-to-JSON decoding and
text filesystem paths. Notification hooks require an embedding-state protocol.
Series numbering overloads distinguish nested SQL rows from flat numbers, and the
custom-column forwarding contract now expects rows. Legacy export services have
explicit row-map/path/format protocols, with a regression covering custom series,
missing formats and path remapping. Dynamic dataclass field annotations are typed
as runtime objects; database cell values remain dynamic. Normalization-source
callbacks have explicit reader contracts and retain their built-in fallback.

Registry loaders return typed driver classes and ModuleType caches without eager
backend imports. Namespace packages without a file location require explicit
package_dir. Third-party builder results remain object-valued and fallback to the
module is preserved. Tests cover alias cache sharing, lazy resolution, namespace
configuration and arbitrary builder results. Driver schema-version declarations
now include PostgreSQL's string fingerprint as well as SQLite's integer counter.
Wrapper macro wiring preserves the documented dynamic setter/fallback behavior;
unchecked Row inputs retain their explicit rejection diagnostic.

Validation (selections overlap; do not sum as unique tests):
- Initial schema/Calibre matrix: 89 passed, one skip and one API-return mismatch.
  The mismatch was corrected and is covered by all final API lanes below;
  `...-builders-tests.log`.
- Builder/helper/Calibre/link/API boundaries: **126 passed**;
  `...-builder-boundaries-tests.log`.
- Custom-column DDL, PostgreSQL adapter fakes and API checks: **58 passed**;
  `...-custom-ddl-tests.log`.
- Table SQL, WEMI reads, Catalog Agent maintenance and APIs: **85 passed, 10 skips**;
  `...-metadata-contracts-tests.log`. Skips are PostgreSQL configuration/backend
  restrictions; this is not a live PostgreSQL run.
- Wrapper, registry, schema/API and driver lifecycle/surface: **189 passed, two
  Windows-only skips**; `...-builder-signoff-tests.log`.
- Final normalization, wrapper, registry, table SQL, API and probe-tooling checks:
  **98 passed**; `...-builder-final-tests.log`.
- **88 doctest examples passed** across the updated builders/helpers;
  `...-builder-doctests.log`.
- Basedpyright accepts valid calls and rejects all **83 marked invalid cases**;
  `...-builder-probes-signoff.log`. The mypy probe has no missing expected or
  unexpected fixture diagnostics, but still fails on the 130 unfinished package
  errors; `...-builder-mypy-probes-signoff.log`. Do not describe it as a passing
  whole-package gate.
- All 25 cold-import budgets, cycle and layer checks pass. Logs:
  `...-builder-imports-final.log`, `...-builder-cycles.log`, `...-builder-layers.log`.
  The layer inventory contains 1,898 modules, 15 reviewed backward edges and 28
  unresolved dynamic imports; cycle protection covers 255 modules.
- Scoped F lint and formatting pass for **68 files**; the exact selection is
  `...-builder-targets.json`, with output in `...-builder-lint.log`.

Log prefix: `/tmp/liuxin-databases-typing`. Authoritative checker outputs:
`...-mypy-builder-final.log` and `...-pyright-builder-final.json`. Fresh inventory:
`...-builder-inventory.json`. Telemetry across these lanes recorded **121.5 MiB**
peak test-tree RSS, **3.90 GiB** minimum available memory, and zero OOM kills.
All six runtime samplers stopped normally; `...-builder-resource-summary.json`.
All checker, probe and gate processes for this checkpoint are terminal. C: had
5.4 GiB free and /tmp had 921 GiB free at the final check.
Scratch/config/cache stayed under /tmp; the original checkout/preferences/data
remain untouched. No full quality, full repository suite, live PostgreSQL,
commit or push is claimed for this checkpoint.

Next: type custom-column service state/collaborators and mixin dispatch, main
Database/runtime lifecycle, and PostgreSQL connections/driver methods. Mypy also
needs the psycopg2 stub dependency addressed without suppressing that backend.
The typed registry now checks constructors at callers. Full checker/quality,
representative negative probes, API/runtime and live PostgreSQL evidence remain
required for completion.

## Earlier checkpoint: macro services and query indexes

Current inventory: **224 database modules, 32 incomplete signatures**. Remaining
signatures are in PostgreSQL (19), registry (four), custom-column services (four),
main Database (two), SQL utility mixins (one), notify (one) and utils (one).
Full configured checks report **290 mypy errors and 189 basedpyright errors**, with
zero basedpyright warnings. All **11 shared SQL macro modules**, MacrosBase,
MacroHostState, both macro APIs and query_indexes now have zero diagnostics in
both checkers. Package-wide typing and validation remain unfinished; no commit,
push, full-suite or full-quality signoff is claimed.

Transaction contexts expose SQLConnection, with typed per-thread nesting,
connection and serialization state. Explicit/borrowed connection ownership,
nested savepoints and legacy get_connection/primary fallbacks are preserved.
The concurrent regression confirms each thread sees only its own active handle.
Composition adapters in _macro_state resolve canonical macro operations and the
private connection hook without importing implementations or making services Any.
The documented unbound get_link_data structural host remains supported.

Search compiler, bulk-link staging, normalized-value batches, fingerprints and
physical repair helpers now have typed collaborators and containers. SQL cells
retain backend-defined values. find_rows_matching has always returned a tuple of
row mappings; its API now declares that result and accepts the existing None
limit. PhysicalReference describes ordered columns, target columns, schema and
FK/polymorphic kind. Invalid JSON receipt roots now raise ValueError: the eight
new cases failed before that guard. PhysicalReference is a type-only API import,
so the unchanged 25 cold-import budgets remain satisfied.

Legacy macro contracts describe set/list/typed-map link results, nullable scalar
IDs, row-list lookup modes, actual cursors and SQL binding containers. Custom
reads honor a project get hook or use a native borrowed cursor, closing that
cursor without committing the caller's work. One native SQLite regression failed
before the adapter; project-connection behavior passed before and after. Pair
readers retain explicit (id, value) tuples and missing pairs still raise IndexError.
Missing-row/link guards provide explicit integrity errors where indexing was
previously unchecked. Historical legacy write commit points remain documented.

Batch owner deletion now accepts scalar IDs and one-cell parameter tuples on both
connection paths; two of the four cases failed before the correction. It preserves
pending transactions and facade executemany overrides. Metadata hooks, temporary
SQL helpers and column-definition transactions now use useful connection/callback
contracts. The macro AST verifier accounts for contextmanager's public return
while preserving the yielded type; regression cases distinguish decorated and
undecorated generators. API bodies remain declarative.

Validation (selections overlap):
- Transaction/search/link/receipt initial lane: **121 passed, one fixture skip**;
  `...-macro-core-tests.log`.
- Custom-column/wrapper/API/transaction lane: **116 passed**;
  `...-cc-final-tests.log`.
- Final macros, query indexes/search, normalization, table SQL, Catalog repair,
  concurrency/cursor boundaries and API lane: **158 passed, 11 skips**;
  `...-macro-signoff-tests.log`. PostgreSQL is not configured; other skips cover
  the pg_temp-less SQLite harness and PG-only lock cases.
- Updated macro doctests: **260 examples passed**;
  `...-macro-doctests-final.log`.
- Basedpyright accepts valid examples and rejects all **73 invalid probes**;
  `...-macro-probes-final.log`. Mypy's probe runner still reports the unfinished
  imported package errors; do not describe it as a passing full gate.
- All 25 cold-import budgets, import cycles (255 protected modules) and layer
  direction (1,897 modules; 15 backward edges, 28 unresolved imports) pass.
  Logs: `...-macro-imports-final.log`, `...-macro-cycles.log`, `...-macro-layers.log`.
- Scoped F lint, formatting and whitespace checks pass for this macro/index pass.

Log prefix: `/tmp/liuxin-databases-typing`. Authoritative whole-checker outputs:
`...-mypy-macro-final.log` and `...-pyright-macro-final.json`. The fresh signature
inventory is `...-macro-inventory.json`. Final/custom-column telemetry recorded
154.4 / 98.2 MiB peak test-tree RSS, 3.87 / 3.95 GiB minimum available memory and
zero OOM kills. Both samplers stopped normally; resource summary is
`...-macro-resource-summary.json`. Scratch/config/caches stay under /tmp. C: has
5.6 GiB free. Original checkout/preferences/data remain untouched.

Next: PostgreSQL and registry constructor/connection contracts, main facade
lifetime/runtime composition, custom-column service ownership, SQL utility and
schema-generator helpers, dbprefs/schema types/notify/utils. SQL utility mixins
still lack host contracts, use e.message and incorrectly narrow requested_cols;
they were inspected but not edited in this checkpoint. Their stripped scratch
copy is `/tmp/liuxin-utility-code.py` (never copy it back: it omits comments/docs).
No live PostgreSQL server is running; that lane and full quality/runtime checks
remain required before completion.


## Previous checkpoint: shared SQL helpers and SQLite backends

Current inventory: **223 database modules, 149 incomplete signatures** (93 driver
plugins, 44 API, four query indexes, four custom columns, two main Database, one
notify and one utils). Both configured checkers still cover the package root:
**640 mypy errors and 310 basedpyright errors**, with no basedpyright warnings.
All **21 shared SQL/databasedriver modules** and both concrete SQLite databasedriver
modules have zero diagnostics in both checks. The full goal remains unfinished.
No commit, push, full-suite or full-quality signoff is claimed.

The shared tree helpers now use the existing column/ancestor methods and actual
physical identity columns. Missing parents and parent cycles terminate with
integrity errors. Derived path writes close owned handles. Direct dirty-record
notifications use the queue the facade actually wires. Search contracts describe
False missing rows, accept nested DDL column specifications and preserve input
heading generators across multiple results. Script execution closes owned handles
on failure too. API declarations match those behaviors.

SQL adapters and callback factories now have typed JSON, state, callback and
collation contracts. SQLite text collation no longer tries bytes-only decoding.
The registered concat and identifiers aggregates now supply stdlib aggregate
objects; native APSW receives its state/step/final callback triples. Registration
helpers adapt only the native stubs' arity limitations and still check factory
shape. The installed sqlite3 stubs describe only an integer SUM-style aggregate;
do not weaken all aggregate signatures to fit that stub. APSW type aliases exist
only in its stubs, so casts containing those aliases must use strings at runtime.

Both SQLite backends now use explicit imports and typed constructors, progress
state, regex callbacks, connection results and rebuild callbacks. Fetch helpers
close cursors and preserve SQLite OperationalError instead of reading the obsolete
exception.message. Native APSW scalar reads use fetchone, and execute/executemany
support the base API's binding/options contract plus the existing sql and
sequence_of_bindings keyword aliases. Failed batches roll back. The placeholder
last_modified_epoch_k is honestly None-returning; it remains unimplemented.

`MacroHostState` in `database_driver_plugins/_macro_state.py` supplies a nullable
public db and a checked `_macro_database` for facade-dependent operations. All
macro families use that state; the larger macro implementation still needs typing.
Macro construction with db=None remains supported. Bound get/execute/executemany
properties use specific callable protocols, and DatabaseAPI/main Database declare
the dynamically bound nullable get reader. A facade get override remains effective;
the pre-existing get contract test passed before this annotation change, so this
is not evidence of a formerly broken runtime get operation. Main driver/wrapper/
macros lifetime declarations remain an outstanding separate issue.

Basedpyright now resolves the repo .venv explicitly. Interpreter autodiscovery in
this environment supplied no site-packages path even with --pythonpath, hiding
APSW's installed stubs. Both native backend signatures are now checked against
those stubs without import suppressions or new package exclusions.

Validation (selections overlap):
- Initial helper reproductions: 12 failures; corrected early helper/API lane:
  **44 passed**. Collation reproduction: two failures.
- Registered concat/identifier reproduction: **four failures**, caused by tuple
  factories lacking stdlib step/finalize methods; corrected in the broader lane.
- Native APSW scalar reproduction: AttributeError on cursor.next; corrected.
- Final search/tree/script/progress/API/dump-restore lane: **101 passed**;
  `...-helper-integration-tests.log`. Both SQLite variants, 14 existing deprecation
  warnings, scratch discarded between tests.
- Macro/portable/transaction/wrapper/efficiency lane: **170 passed, one skip**;
  `...-macro-host-tests.log`. The skip is the PG-shaped SQLite harness's absent
  pg_temp schema, not a live PostgreSQL result.
- New detached-host/facade-override/native-APSW rollback boundaries: **four passed**;
  `...-helper-boundary-tests.log`.
- After the final custom-column host migration: **70 passed**;
  `...-helper-last-tests.log` (custom-column, wrapper and API contracts).
- Utility doctests: **77 examples passed**. Basedpyright accepts valid probes and
  rejects **68 invalid examples**; `...-helper-probes-final.log`.
- All 25 cold import budgets, import cycles (255 protected modules) and layer
  direction (1,896 modules) pass; `...-helper-{imports,cycles,layers}.log`.
- Scoped F lint for completed helper/backend/state/API files, formatting and
  whitespace pass. Wider legacy macro lint is still unfinished.

Log prefix: `/tmp/liuxin-databases-typing`. Authoritative full checker outputs:
`...-mypy-helper-checkpoint-final.log` and
`...-pyright-helper-checkpoint-final.json`. The signature inventory is
`...-helper-inventory.json`. Test telemetry directories are
`...-helper-integration-telemetry` and `...-macro-host-telemetry`: peak test-tree
RSS 89.0 / 116.8 MiB, minimum available memory 3.97 / 4.88 GiB, zero OOM kills.
All listed processes completed. Scratch, caches, config and telemetry stay under
/tmp; C: had 5.9 GiB free. Original checkout/preferences/data remain untouched.

Next: finish macro services and their APIs, especially custom-column queries and
portable transaction collaborators (some still use Any), then PostgreSQL and
schema/generator utilities. Both SQLite package version formatters still pass an
unused fifth argument; do not confuse those package-level errors with the clean
databasedriver modules. The main facade still needs an honest post-close lifetime
contract. Query indexes, remaining custom columns, notify/utils and full runtime/
negative-probe/quality checks remain in scope. No live PostgreSQL server is running.

## Previous checkpoint: rows and shared SQL drivers

Current inventory: **222 modules, 191 incomplete signatures** (129 driver-plugin,
50 API, two main Database, four custom columns, four query indexes, one notify,
one utils). Full configured checks: **776 mypy errors**, **392 basedpyright errors**,
three warnings. The entire package remains in scope and the goal is unfinished.
No commit/push or full-suite/full-quality signoff yet.

All ten facade mixins and Row/RowAPI are clean. Twelve shared SQL driver modules
also have zero errors in both checkers: `_state`, `__init__`, add/delete/update,
metadata, table names, math, triggers, new-book groups, views and value casting.
`SQLDriverMixinState` declares caches/owned connections; `driver_operations(self)`
is the single cast to canonical DriverAPI for sibling calls. Use the function,
not a new self property: existing unbound helpers support SimpleNamespace hosts.
SQL backends are structurally composed and do NOT inherit DatabaseDriverAPI.

Row retains False/None for failed loads, None table for drafts, live keys views,
and read-only instance sync rebinding. API now declares row_dict/row_id setters
and abstract `_require_row_dict`; its concrete body belongs only in Row (API parity
excludes inherited operational API bodies). Relationship/tree consumers guard
unidentified tables. Tests cover missing-payload sync/no insert, readonly deepcopy,
facade string/column lookup corrections and duplicate validation before allocation.

Shared SQLConnection now includes positional-only context exit and executescript;
SQLCursor execute/executemany return SQLCursor. Concrete SQLite and PG adapter
conformance plus borrowed maintenance cursors pass the basedpyright probes.
DriverAPI conn is SQLConnection|None and declares db, callbacks and dirty queue.
Wrapper API declares its real lock/category/queue state; categories use abstract
Set to accept facade frozensets. SchemaDriver.conn is read-only in its protocol.
Connection tracking is generic and preserves native handle identity. The optional
create_function registration is resolved at that dynamic capability boundary.
FRBR generator connection parameters now use SQLConnection; SQLite SQL semantics
remain required. SQLBaseDriver._build_database_file casts its concrete class to
the canonical driver constructor at one composition boundary.

API return contracts now admit backend-specific results. Shared-SQL batch inserts
return True if empty, otherwise None; ID-only updates return True; bulk deletes
return None while PostgreSQL returns True. Nonempty column updates reject no field.
No-ID insert failures are inside the transaction. Facade set_driver_wrapper now
uses wrapper.set_macros, fixing assignment to a read-only property. The parity
checker now parses outer unions and verifies covariance; eight regression examples
reject widened returns and changed nested elements. Do not restore its old unsafe
actual-union-membership shortcut. Basedpyright accepts valid examples and rejects
**65 invalid calls**, including new driver/connection cases.

Validation evidence (selections overlap):
- Row4 lane: **192 passed, two existing xfails**, log `...-row4-tests.log`.
- Initial SQL lane: **114 passed, two skips**, `...-sql-state-tests.log`.
- Broad SQL lane: 229 passes, two skips, five failures (API return declarations /
  unbound structural hosts), `...-sql-final2-tests.log`. All five repaired.
- Corrected API/boundaries: **60 passed**, `...-sql-boundary3-tests.log`.
- Final row/API/creation/transactions/maintenance: **142 passed, 50 skips**,
  `...-sql-signoff-tests.log`. No configured live PostgreSQL server.
- 65-probe log: `...-sql-probes-verified.log`.
- All 25 cold import budgets, cycles255, layers1895/15backward/28unresolved pass;
  logs `...-sql-{imports,cycles,layers}.log`.
- Scoped F lint (finished SQL helpers, facade, new tests and API) and whitespace
  pass. Wider changed-files F scan has legacy wildcard imports / unfinished
  cleanup; do not claim the whole lint surface is clean. Edited DB sources formatted.

All log prefixes are `/tmp/liuxin-databases-typing`. Full checker logs:
`...-mypy-sql-checkpoint.log`, `...-pyright-sql-checkpoint.json`. No processes need
restarting; runtime/checker/probe/gate runs listed above have completed. A collection
failure from a transient bad import indentation was repaired before the final lanes.
No full quality runner or live-PG/full-suite run yet. Keep scratch under /tmp;
C: had 6.2GiB free at checkpoint. Original checkout remains untouched.

Next: shared SQL search/tree/script helpers and concrete backend composition,
then macro/API gaps. Main Database still has untyped macros getter/setter and
incorrect public post-close non-None driver/wrapper/macros declarations. Fix the
lifetime contract honestly; do not hide it with casts or silently change public
None-after-close behavior. Raw dirty-record helper still spells its queue
`dirtied_records_queue`, unlike constructed drivers' `dirty_records_queue`;
investigate with a regression instead of just declaring a fictitious attribute.
The undefined Aby/byte-only ICU comparison, missing APSW re import, and SQLite
datetime annotation remain backend follow-ups. Tree helpers also call stale
get_full_column_name / __get_linear_index_of_columns names. Prior known priority
swap / link-row-return semantics remain unchanged.

## Previous checkpoint: facade mixins

All ten `database/*_mixin.py` files now have complete signatures and check cleanly
in both full-scope checkers. The main Database class and Row are still unfinished.
The inventory now contains 220 modules (new `_state.py`) and 238 incomplete
signatures: 143 driver plugins, 50 API, 30 main facade, five row, four query indexes,
four custom columns, one notify and one utils. Full mypy still reports **1,117
errors**; the latest basedpyright run reports **593 errors**, three warnings.
The goal remains active and neither checker is green across the package.

`DatabaseMixinState` supplies typed cache attributes and one `_database` property
cast to canonical DatabaseAPI. Concrete Database implements that API; partial
mixins remain instantiable with explicit collaborators. There are no new public
collaborator properties/setters and no ABC inheritance on the state class.

New/finished work since the maintenance checkpoint:

- Metadata uses real nullable UUID/version contracts, a scalar library ID lookup,
  canonical ColumnOptions, and cursor closure without committing the caller's
  transaction. Missing/NULL library IDs generate and persist a UUID.
- SearchIndex describes comparison triples, search values remain SQL-defined,
  unique-value overloads distinguish sets/iterators, and an empty multi-column
  backend result becomes an empty list. Unknown grouping columns are rejected.
- Tree, null-row, custom-column and dirty-record helpers are typed. Telemetry
  events use a TypedDict; ObservedDirtyRecordsQueue is generic, and callback sinks
  are structural protocols. Cache table groups now accept their actual immutable
  sets. Persistence uses a scalar count and preserves drained records on failure.
- Relationship helpers return generic RowAPI records, with overloads for single
  versus multiple links. Endpoint lists honestly preserve None for missing rows,
  propagated into linked-row and chunked-search contracts. Consumers that require
  existing endpoints raise integrity errors. Explicit unlink/update of a missing
  link raises InputIntegrityError; bulk unlink skips links already removed.
- Priority APIs accept numbers/text/None, payload dictionaries stay separate from
  Row objects, and default-collision probes use scalar query reads. Reordering
  consumes any ID iterable once. Incoming-only intralink deletion now uses the
  secondary endpoint; its former xfail is removed and endpoints are checked intact.
  Legacy get_intralinked_rows still returns link rows; the broken priority swap is
  documented and unchanged. Do not claim those semantics are corrected.
- Added the existing get_allowed_tables_snapshot method to the wrapper API, keeping
  its body declarative. Both new negative facade/queue probes and valid overload
  assertions pass: **59 invalid examples** are rejected by basedpyright.
- Confirmed merged PR #154's CI failed in two stale tests. In this typing branch,
  the PostgreSQL fake now supplies physical schema results and cursor close; its
  view assertion checks the current bounded query and cursor cleanup. Pagination
  measures actual cursor consumption/closure instead of the retired mapping hook.
  The remote merged PR has not been modified or rerun.

Validation: the final facade/driver selection passed **104 tests**, 40 skips and
two existing linked-row xfails. Maintenance/row/macro callers passed **96 tests**,
58 skips. Final wrapper/API/incoming-unlink checks passed **51 tests**. Earlier broad selection: 179 passed, 40 skipped, four xfails and one
stale PostgreSQL fake failure, repaired and included in the final 104-pass lane.
The intermediate focused incoming-unlink/pagination run passed four cases; its PG
cursor-ownership assertion was subsequently corrected and retested. Selections
overlap. No live PostgreSQL server was configured in these runs.

Logs: `/tmp/liuxin-databases-typing-facade-{final-tests,collaborators,probes-final}.log`;
checker checkpoint logs are `/tmp/liuxin-databases-typing-{mypy,pyright}-facade-checkpoint.*`.
The last wrapper/API run is `/tmp/liuxin-databases-typing-facade-last-tests.log`.
Scoped lint/formatting, whitespace, import cycles, layer direction and all 25
cold-import budgets pass. No full quality runner, full suite, commit or push yet.
New source/tests/docs remain untracked; retain them and add intent-to-add before
tracked-document checks. All scratch and telemetry remain under `/tmp`.

Next: Row/RowAPI contracts and main Database initialization, then SQL driver
composition/macros. Row.table really can be None. Row.row_dict/int_row_dict retain
False/None after missing loads; model those honestly without silently discarding
compatibility. Row.keys returns a keys view, and read-only mode rebinds sync on the
instance. Avoid broad Any/casts or API changes that conceal these states. The ten
mixins are currently clean against the old RowAPI.table/row_dict contracts;
correcting those contracts will require adding callers' guards.

## Previous checkpoint: foundations and maintenance

Foundations and maintenance have advanced; **do not stop at this subset**. The
latest full-scope runs report 1,385 mypy errors and 828 basedpyright errors (three
warnings). No errors remain in maintenance, its API, connection_hooks, transactions
or the new SQL/maintenance types. Fresh inventory: 219 modules, 256 incomplete
signatures (145 driver plugins, 52 API, 44 facade, five row, four query indexes,
four custom columns, one notify and one utils). Baseline was 340.
Fixing missing DatabaseAPI type imports exposed another set of driver/wrapper
errors formerly hidden behind unresolved types; do not treat raw counts as a
like-for-like improvement measure or remove these imports to reduce diagnostics.

Changes include typed SQL execution/borrowed-row protocols in driver_types;
structural MaintenancePluginAPI; plugin context/upgrade/delivery signatures;
TypedDict journal rows and rebuild checkpoints; typed connection hooks and generic
transaction scopes. SQLiteCompletionConnection.cursor(None) now preserves default
hook behavior, covered by the existing custom-cursor transaction regression.
The separate QueryCursor fetchall contract permits sequences; wrapper list APIs
preserve existing lists and materialize only non-list results. API parity accepts
SQLConnection vs native sqlite3.Connection, with structural type probes covering it.

Validation: foundation selection 97 passed / 32 skipped; maintenance selection
258 passed / 143 skipped; final hook/checkpoint selection 98 passed / 104 skipped.
Final runtime/API selection: 61 passed / 50 skipped. Physical merge after the
type-column guard: 12 passed / six skipped (six deselected).
Selections overlap. PostgreSQL was unconfigured, so no live PG claim here. Full
basedpyright contract probe rejects 55 invalid calls. Mypy's contract probe still
requires the full configured package to be clean. Logs are
`/tmp/liuxin-databases-typing-{mypy,pyright}-maintenance-checkpoint-final.*`,
`/tmp/liuxin-databases-typing-maintenance-{tests,final-tests,probes-final}.log`.
Import-cycle and layer-direction gates pass. Cold-import checking exposed one new
type-only RowPredicate import; it has been moved behind TYPE_CHECKING. Its final
rerun is `/tmp/liuxin-databases-typing-maintenance-import-final.log`. Scoped lint
and whitespace pass; all 25 cold-import budgets pass. The final cycle log is
`/tmp/liuxin-databases-typing-maintenance-cycles-final.log`. No full quality
signoff, full suite or commit yet. All checker/test handles have finished; final
logs are complete. New docs remain untracked, so add intent-to-add before the
eventual tracked-document-link check.

Remaining backend defects observed but not fixed: SQL driver utils icu_collator
annotates undefined Aby and claims bool although returning int; SQLite
last_modified lacks a datetime type import; SQLite_apsw REGEXP uses re without an
import. Inspect and exercise actual backend behavior when fixing these.
