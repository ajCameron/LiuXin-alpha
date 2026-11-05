# Database package typing

Follow-up: [runtime fixes and the required regression ratchet, 2026-11-05](regression-ratchet-2026-11-05.md).

Completed **2026-11-04** on `databases-full-typing`.

All **225 database modules** are covered by both configured type checkers, with
**2,482 fully annotated function definitions** and no incomplete signatures.
The full quality runner passes. The [final verification](#final-verification)
records runtime results, resolved failures and validation limits. The dated
checkpoints below preserve intermediate results; their remaining-work statements
are historical, not the current completion status.

The goal is to type the entire `LiuXin_alpha.databases` package and keep that
coverage enforced. The initial inventory contains **219 Python modules** and
**340 functions with incomplete signatures**. Widening mypy's existing strict
scope to the package reports **1,464 errors** before repairs; this is the baseline,
not an accepted exclusion list. Both checker configurations now name the package
root rather than a selection of wrapper modules.

The work starts at `d8d52587`, the merged `database-review` integration head.
PRs #153 and #154 merged there after #152 had already merged to main; main at
`23aa55c9` does not yet contain those later merges. The branch is
`databases-full-typing`, in `/tmp/liuxin-databases-typing`. The original dirty
checkout, preferences and data submodule are preserved.

## Completion requirements

- Both configured checkers cover every Python module under `databases`, including
  drivers, generators, APIs, facade mixins, rows, custom columns, maintenance and
  table SQL. No package exclusions or blanket diagnostic suppression substitute
  for fixing contracts.
- Function signatures, containers, callbacks, optional results and shared mixin
  state describe the implementation and accepted callers. Database cell values
  and genuinely dynamic extension boundaries may remain dynamic, with that
  boundary explicit; database services must have useful interfaces.
- Existing strict checking and import/layer budgets remain enforced. Public API
  declarations stay separate from implementations. Annotations must not add
  runtime import cycles or eagerly load optional backends.
- Invalid-call probes cover representative new facade/row, driver, maintenance
  and batch SQL contracts. Meaningful runtime tests cover any behavior corrections
  exposed by typing, plus the affected SQLite/APSW/PostgreSQL callers.
- Final evidence includes both full configured checker runs, the quality runner,
  API contracts, regression results and a fresh package-wide coverage audit.
  A green subset is an intermediate checkpoint, not completion.

## Final verification

- **Branch and scope:** the worktree reflog records the fast-forward from
  `6d54293c` to `d8d52587`, followed by creation of `databases-full-typing`.
  Both checker configurations name the database package root. A fresh AST audit
  covers all 225 modules and 2,482 function definitions. No database exclusions,
  blanket diagnostic suppression or weaker checker settings were added.
- **Static contracts:** the complete quality runner passes, including 452 mypy
  sources, zero basedpyright diagnostics, 303 formatting targets, all 25 cold-import
  budgets, import cycles and layer direction. Positive calls pass; the negative
  probes produce the expected 95 basedpyright and 93 mypy diagnostics. The differing
  counts reflect overload reporting. Full-package F lint and diff whitespace pass.
- **Broad runtime coverage:** all 2,957 collected database cases ran, producing
  2,577 passes, 377 skips and eight existing expected failures. Two assertions
  still accessed the wrapper after closing its facade; they now retain the actual
  wrapper, verify its lock was cleared, and check closed access raises. One
  teardown error was caused by a concurrent wheel build creating `build/` while
  the test guard removed it. The traceback confirms that directory race; subsequent
  wheel builds use an isolated source copy.
- **Final regressions:** 191 tests pass, including every affected broad-run case,
  closed preference writes, lifecycle/API contracts, Catalog field metadata,
  Core services, Library ownership, documentation links and packaging/tooling.
  The two skipped native PostgreSQL cases had already passed in the separate
  live-backend run, whose final selection passed 46 tests. All 119 SQLite, APSW
  and PostgreSQL connection docstring examples pass after removing two obsolete
  constructor-output expectations. Test selections overlap; their counts are not
  a unique-test total. The broad invocation was not repeated after these fixes.
  After updating this completion record, all 16 documentation-navigation checks
  passed again (`final-docs.log`).
- **Distribution:** an isolated wheel passes the current content verifier and
  includes `databases/py.typed`; all 225 packaged database modules match the final
  source bytes. The typing extra supplies `types-psycopg2`; optional native backend
  imports remain type-only. No installed conversion smoke is claimed.
- **Resource ownership:** the broad run peaked at 258.1 MiB test-process-tree RSS,
  retained at least 3.81 GiB available memory and recorded zero OOM kills. Scratch,
  caches and telemetry stayed under `/tmp`. The private PostgreSQL server was
  stopped and removed. The original checkout, preferences and data are preserved.

Logs under `/tmp/liuxin-databases-typing-`:
`quality-final.log`, `final-inventory.json`, `all-databases.log`, `signoff.log`,
`pg-final.log`, `native-doctests-final.log`, `wheel-final.log`,
`wheel-final-inspection.json` and `signoff-resources.json`.

The typing scope includes facade state, row payloads, SQL connections/cursors,
backend and generator operations, macros, custom columns, maintenance callbacks
and table SQL. Dynamic SQL cells, serialization values and arbitrary native row
factories retain explicit dynamic boundaries; service collaborators have useful
contracts. In particular, closed facade access raises `DatabaseIntegrityError`,
and legacy macro operations require the explicit `MacrosAPI` capability.

This completes the package typing objective. It does not claim a new full-project
test run, a real-file import, or removal of the documented legacy SQL limitations.
The follow-up delivery request authorizes committing and publishing the complete
pass. The PR targets `database-review`, which contains the maintenance and wrapper
prerequisites; `main` at `23aa55c9` still lacks those two merged PRs.

## Work sequence

1. Shared value/connection contracts and API declarations.
2. Rows, facade state and database mixins.
3. Shared SQL driver and concrete backend/generator contracts.
4. Portable macros, table SQL and custom columns.
5. Maintenance capture/runtime/plugin/worker contracts.
6. Cross-package callers, negative type probes and full validation.

Runtime behavior should remain stable unless a reproduced defect requires a
correction. Preserve user comments and TODOs; update inaccurate contract docs
alongside annotations.

## Checkpoint: PostgreSQL, macro capabilities and facade lifecycle

The full configured quality runner passes: 303 formatting targets, lint and
complexity, annotation coverage, protected imports, layer direction, all 25
cold-import budgets, zero basedpyright diagnostics, and 452 mypy source files.
The negative-call fixtures accept valid calls and reject all marked mistakes:
95 basedpyright diagnostics and 93 mypy diagnostics. The difference includes
the checkers' different reporting of an invalid overloaded cursor factory.
The source inventory finds zero incomplete signatures across 225 modules.
These are static results, not a substitute for runtime validation.

The generic driver, wrapper and facade now expose `DatabaseMacrosAPI`, which
extends `PortableMacrosAPI` with the owner binding and shared SQL shortcuts.
`MacrosAPI` adds SQLite's legacy custom-column/cache operations. Legacy callers
use `require_legacy_macros()` to check this capability explicitly. PostgreSQL
does not claim the legacy surface; unsupported operations raise
`DatabaseDriverError`. An extension providing that surface must explicitly
implement `MacrosAPI`. The common declaration lives beside the portable API,
avoiding an extra eager module import and retaining existing import budgets.

Facade driver, wrapper and macro references are optional internally. Public
accessors require a live binding and raise `DatabaseIntegrityError` before
binding or after cleanup; they no longer return None under a non-null contract.
`metadata_sql` retains its explicitly optional result. SQL convenience aliases,
storage, telemetry and the mutex describe their absent/closed states. Preference
writes reject a closed database before updating their cached value.

The standalone runtime initializer now uses the constructor's wiring path and
assigns the actual `db` owner on collaborators. Its previous `catalog` assignments
did not rebind them. Callback factories and preferences have explicit contracts;
storage bootstrap operates on the concrete runtime facade and narrows absent
metadata before logging a load failure. Reinitialization still does not shut down
an earlier maintainer or guarantee atomic replacement.

PostgreSQL uses the native psycopg2 stubs from the typing extra. Native mapping
checker cursors, schema connections and managed action return types are explicit.
The adapter preserves a selected cursor's row container: default physical reads
are positional, while custom factories expose positional-or-mapping `SQLRow`.
Its generic cursor forwards native description, row count, iteration and fetch
results without copying individual rows. A documented cast at the native factory
boundary handles psycopg2's non-generic cursor hierarchy and newly owned result
lists. Optional imports remain under TYPE_CHECKING.

Materialized adapter fetches and script execution now close their temporary
cursors on success and failure without ending the borrowed connection. Native
direct executors expose cursor/lastrowid results and bound parameter containers;
integer binding shorthand is supported. Default inserts omit absent bindings.
Search-index function registration now requires the SQLite or APSW registration
capability instead of an untyped connection.

The sqlite3 convenience connection classes in both backend modules retain dynamic
single-row results because native row factories may return arbitrary objects.
Their all-row overloads now preserve the known list container, with positive
type probes covering both classes.

Validation so far (selections overlap):
- Adapter/backend/wrapper/API checks: **116 passed**.
- Lifecycle reproduction: **eight failures** across SQLite/APSW before the
  accessor and owner-wiring changes; capability checks already passed.
- Updated lifecycle, custom-column, wrapper, API and storage-bootstrap matrix:
  **188 passed, two PostgreSQL-related skips**.
- Live PostgreSQL row writes, scans, transactions, macros and native cursor
  factories: **64 passed, two test-assumption failures**. The two legacy-DDL
  tests now declare their injected capability or expect the explicit unsupported
  capability error. The final adapter/backend and corrected live tests passed
  **46 cases**. The private server had no leftover test schemas and is removed.
- An isolated wheel contains all **225 database modules** and `databases/py.typed`.
  The current wheel-content verifier passes, including runtime assets and manuals;
  it now enforces that typing marker. No installed conversion smoke is claimed.

The completed runtime lanes peaked at **131.4 MiB** test-tree RSS, retained at
least **4.66 GiB** available memory and recorded zero OOM kills. All scratch and
logs remain under `/tmp/liuxin-databases-typing-*`.

The broad run and focused reruns are now complete. The
[final verification](#final-verification) supersedes the pending validation from
this checkpoint and records all three broad-run issues and their successful
reruns. It also covers the closed-preference regression and packaging checks.

Current authoritative logs: `...-quality-containers.log`, `...-final-inventory.json`,
`...-lifecycle-initial.log`, `...-pg-live-allowed.log`, `...-pg-final.log`,
`...-wheel-isolated.log`, `...-wheel-content.log`,
`...-lifecycle-resource-summary.json`, and `...-all-databases.log`.

## Checkpoint: foundations and maintenance

The complete package remains in both checker scopes. The latest inventory has
**256 incomplete signatures**, down from 340. All maintenance and connection-hook
functions now have complete signatures, and neither checker reports errors in
those modules. This is an intermediate checkpoint: the full configured checks
still report **1,385 mypy errors** and **828 basedpyright errors**. Restoring missing
DatabaseAPI imports exposed additional driver/wrapper errors previously hidden
behind unresolved types, so these counts are not a simple baseline subtraction.
Most remaining
work concerns facade composition, row payloads, legacy macros and backend drivers.

- Driver contracts now describe lazy row iterators, nullable book-group IDs,
  missing table identities and structured search predicates. Actual timestamp
  results use datetime annotations. Exception constructors are also checked so
  imported exception classes no longer conceal untyped calls.
- SQL cell values remain backend-defined. SQLCursor and SQLConnection describe
  physical execution with the default positional row factory; QueryConnection
  remains the wrapper's broader read contract. RowSQLConnection exposes borrowed
  row operations without commit, rollback or connection-close methods. The plugin
  context uses that borrowed contract; BoundConnection continues enforcing the
  runtime restrictions and expiry.
- MaintenancePluginAPI is a structural protocol with typed lifecycle, selection,
  coalescing and handler methods. Existing base classes keep their defaults;
  implementations need not inherit an API class. Upgrade callbacks, delivery IDs,
  claim checks, journal records and persisted rebuild checkpoints now have
  explicit types. Service plugin snapshots return tuples rather than None.
- Transaction scopes preserve the concrete connection type supplied by callers.
  SQLite hooks accept native indexed binding containers as well as ordinary
  sequences/mappings. Explicit cursor(None) now selects the default hooked cursor,
  matching SQLite's factory convention. The existing test also checks that this
  cursor cannot bypass required completion after maintenance is installed.

Validation so far: **258 regression passes / 143 skips**, then **98 passes / 104
skips** after the connection and checkpoint changes; these selections overlap.
Final runtime/API checks passed **61 tests / 50 skips**, and physical-merge
regressions passed **12 / six skips** after a nullable link-column guard.
PostgreSQL was not configured for these runs, and some cases are backend-specific.
Basedpyright's contract probe accepts valid SQLite/PostgreSQL structural examples
and rejects all **55 invalid examples**. A live PostgreSQL lane and both full
checker/quality runs remain required before final signoff. Scratch, configuration,
checker caches and telemetry remain under `/tmp/liuxin-databases-typing-*`.
Import-cycle and layer-direction checks pass, as do all 25 cold-import budgets
and scoped lint. Type-only predicate imports no longer increase runtime loading.

## Checkpoint: facade contracts

The package now contains **220 modules** and **238 incomplete signatures**. All
ten facade mixins check cleanly; the main Database class and Row still need work.
The full configured checkers still report **1,117 mypy errors** and **593
basedpyright errors**, plus three warnings. Coverage has not been narrowed.

Partial mixins share typed state and call sibling operations through the canonical
DatabaseAPI at one explicit composition boundary. This preserves standalone
metadata helpers without adding public collaborator properties or making them
abstract. SQL values remain backend-defined; service collaborators have contracts.

Metadata reads now distinguish nullable UUID/version fields from the persisted
scalar library ID. Missing/NULL library IDs generate a UUID. Borrowed version reads
close their cursor without committing the caller's transaction. Search overloads
expose set versus iterator results; comparison triples and callback queues carry
explicit types. Empty multi-column search results produce an empty list.

Relationship readers distinguish one link from a list of links. Their generic Row
results and possible missing endpoint positions are reflected in the API rather
than claiming specialized link objects or always-present records. Priority inputs
and ID iterables match the implementation. Incoming-only self-link deletion now
uses the supplied secondary endpoint and retains endpoint records; its previous
expected-failure marker has been removed. Existing link-row return behavior and
the documented priority-swap defect remain separate follow-ups.

Final regression selections passed **104 tests** (40 skips, two existing xfails)
and **96 tests** (58 skips), followed by **51 wrapper/API checks**; selections
overlap and PostgreSQL was unconfigured.
Both CI failures found on merged PR #154 are corrected locally: the PostgreSQL
view fake models physical schema/cursor ownership, and the pagination assertion
measures actual cursor consumption instead of a retired mapping helper. No new
remote CI or full-suite result is claimed.

The basedpyright contract probe now accepts valid facade/queue usage and rejects
all **59 invalid examples**. Scoped formatting/lint, import cycles, layer direction
and all 25 cold-import budgets pass. The full checker/quality runs, remaining row,
facade-initialization and backend contracts, and live PostgreSQL validation remain
required for completion.

## Checkpoint: row state and shared SQL drivers

The current inventory is **222 modules** and **191 incomplete signatures**. Both
checkers still cover the whole package: **776 mypy errors** and **392 basedpyright
errors**, plus three warnings, remain. These are intermediate results, not a
package signoff. Twelve shared SQL implementation/state modules now have no
reported errors in either checker, including lifecycle, insert/update/delete,
metadata, name discovery, conversion and view reads.

Row contracts now preserve the distinction between an empty draft and a failed
lookup. `table` can be None; the local payload can be a dictionary, False or None.
`keys()` exposes a live keys view. Mapping operations explicitly reject missing
payloads, while a missing loaded row retains its cached identity and does not
become an insert. Read-only instance binding survives changes to the public flag;
a copied populated row retains the existing writable-base-Row behavior. Facade
callers guard unidentified rows before table-specific operations. A missing
payload is rejected before duplicate-row allocation. Tests also cover corrected
facade column lookup and Python text rendering.

SQL mixins share typed cache/connection state. Calls supplied by sibling mixins
pass through one canonical DriverAPI composition adapter. It also accepts the
existing structural hosts used to call unbound helpers; no new inheritance is
required of those hosts. Backend loading and macro composition still need work.

Physical connections now describe context management and script execution;
physical cursors return a cursor from execute/executemany. These declarations are
checked against native SQLite, PostgreSQL's adapter and borrowed maintenance
cursors. The registry of open connections preserves the concrete connection type.
Schema readers require only read access to the primary handle, so both broader
query handles and positional SQL handles satisfy the same reader contract.

Bulk insert/update/delete return declarations now describe the backend differences,
including True for an empty shared-SQL insert batch, None after its writes, and
PostgreSQL's successful boolean returns. Insert/update paths reject unidentified
rows; an insert without a returned identity fails inside its transaction. Nonempty
column updates require a field. Wrapper replacement uses the declared macro setter
instead of assigning to its read-only property. Wrapper table groups accept the
immutable sets actually supplied by the facade. Schema generators use the physical
connection protocol while retaining their documented SQLite SQL requirements.

The API parity check now parses outer unions and checks return covariance: each
concrete result must fit the declared API. It rejects widened results and changed
container elements. It remains an AST surface check, supplemented by both type
checkers. The basedpyright contract probe now rejects **65 invalid examples** and
accepts valid row, driver and connection examples.

Validation: row/facade regressions passed **192 tests**, with two existing xfails.
The first SQL CRUD/schema selection passed **114 tests**, with two fixture skips.
A broader selection passed 229 tests but exposed five API/structural-host failures;
all five were fixed and the API/wrapper-boundary rerun passed **60 tests**. The
final row, API, creation, transaction and maintenance selection passed **142 tests**,
with 50 backend skips. Selections overlap; PostgreSQL was not configured. All 25
cold-import budgets, cycle/layer gates, focused lint and whitespace checks pass.
Formatting was applied to edited database files. A wider lint scan still reports
legacy backend wildcard imports and other unfinished cleanup, and neither full
checker nor the full quality runner is green. No full-suite or live-PostgreSQL
signoff is claimed.

Next work includes shared SQL search/tree/script helpers, concrete backend and
macro composition, the remaining API annotations, custom columns and query
indexes. Main Database lifecycle typing must still expose the actual None states
after cleanup; its current driver/wrapper declarations and macros API are not a
finished lifetime contract. No nullable state should be hidden with casts merely
to make these callers pass.

Return to [Databases](README.md).

## Checkpoint: shared SQL helpers and SQLite backends

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

## Checkpoint: macro services and query indexes

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


## Checkpoint: schema builders, table SQL and helper boundaries

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


## Checkpoint: custom-column service contracts

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
