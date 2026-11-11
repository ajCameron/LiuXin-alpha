# Database driver plugins review

Reviewed **2026-11-05** against integration commit `07fc3326`, after the merge
of [PR #155](https://github.com/ajCameron/LiuXin-alpha/pull/155).
The package is `databases.database_driver_plugins`. This is a review of the
merged implementation. All six findings now have local fixes and regression
coverage; the implementation checkpoint below distinguishes them from the
original evidence. Structural recommendations remain separate follow-up work.

The persistence boundary is substantially clearer after the wrapper,
maintenance and typing work. The main remaining risks are in older direct
driver operations: backup, connection lifetime, recovery and SQL adaptation.
Passing type checks does not establish those runtime guarantees.

**Follow-up:** the [second review](database-driver-plugins-follow-up-review-2026-11-05.md)
records DDP07–DDP10. In particular, DDP08 identifies an additional operator-recognition
case beyond the original DDP04 regressions. All four follow-up findings now have
local repairs/retirement and 50 additional mandatory cases. The first repair
validation remains valid for its tested boundaries; it is not a claim that all
adapter SQL is correct.

## Scope and current ownership

This was a risk-led review of registration, connection setup and cleanup,
direct SQL execution, backup/restore, backend adaptation and the shared macro
transaction path. Schema generation and the larger legacy mixins were sampled,
not exhaustively audited statement by statement. It is not a deployment signoff
or a claim that every method in the package is correct.

| Owner | Current responsibility | Assessment |
| --- | --- | --- |
| `registry.py` | Lazy backend registration, aliases, builders and direct-access modules | A useful single entry point. Keep imports lazy and validate capabilities at their boundary. |
| `SQLite/databasedriver` | Configured stdlib SQLite connections and file operations | Supported path; backup and connection ownership need more work. |
| `SQLite_apsw/databasedriver` | Another stdlib-based driver plus a separate native APSW connection and shell restore path | Its ordinary `get_connection()` returns `sqlite3.Connection`. An `apsw`-labelled driver test does not establish native APSW parity. |
| `PostgreSQL` | Configuration, connection/cursor adapters, server schema and backend-specific driver operations | Real backend coverage exists. Recovery and SQL translation still have correctness defects. |
| `SQL/databasedriver` | Shared physical SQL helpers and historical direct methods | The current-table accelerators belong here; transaction and resource conventions remain uneven. |
| `SQL/macros` | Portable transactions, physical-table operations, links, normalization mechanics and SQLite compatibility helpers | The portable transaction boundary is much stronger than the remaining direct execution surfaces. Keep that distinction explicit. |
| `SQL/database_generator_frbr` | Current WEMI schema construction and seed data | Current schema authority. Empty-schema round trips alone do not establish populated restore safety. |
| `SQL/calibre_database_generator` (relocated to `utils/calibre_compat/database_generator` on 2026-11-11) | Explicit legacy schema generation and fixture/library construction | Distinct compatibility scope, not the current catalogue schema owner. |

Recent improvements remain valuable: common versus legacy macro APIs are
explicit, nested portable units use savepoints, direct row batches have failure
coverage, custom-value cache changes wait for successful commits, and TEMP
helpers no longer introduce implicit script commits. See the
[transaction contract](transactions.md), [typing checkpoint](package-typing-2026-11-04.md)
and [regression ratchet](regression-ratchet-2026-11-05.md).

## Original findings

The line references and observations in this section describe `07fc3326`,
before the repairs. They are retained as the evidence for the regression tests.

### DDP01 — High: SQLite backups can omit committed rows

[SQLBaseDriver.direct_backup](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/__init__.py)
at lines 392–395 opens a connection but delegates to a plain file copy.
[backup_local_file](../../src/LiuXin_alpha/utils/storage/local/file_backup.py)
hashes and copies only the main database file. A connection context does not
checkpoint WAL or create a consistent backup snapshot.

Reproduced on both SQLite driver names: create a table, enable WAL, disable
automatic checkpoints, insert and commit one row, then call `direct_backup`.
The source contains **one row**; the successful, readable backup contains
**zero**. This is reachable through the database/Core backup operations, not
just a private helper. Hash equality cannot detect omitted WAL content.

Use SQLite's backup mechanism into a privately prepared destination, validate
it, and publish it under the existing replacement/backup policy. Add committed
WAL-content and concurrent-writer regressions, checking rows rather than just
schema presence or `integrity_check`.

### DDP02 — High: PostgreSQL recovery silently replaces an aborted transaction

[DatabaseDriver._primary_connection](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/databasedriver/__init__.py)
at lines 2778–2787 treats every failed `SELECT 1` as a dead connection. It closes
the current handle and opens another. PostgreSQL also rejects that probe when
the connection is healthy but its transaction is aborted after a statement
error.

Reproduced on PostgreSQL 16: insert an uncommitted row, establish a recovery
savepoint, encounter division by zero, then call `direct_execute` with a count
query. The query succeeds on a
replacement connection, the old connection is closed, and the pending row is
gone. The caller loses its transaction/savepoints without an explicit recovery
decision. A rollback-to-savepoint routed through this entry point would also
lose the connection it needs to recover.

Distinguish closed/broken transport from an aborted or cancelled transaction.
Propagate the latter and retain the handle for caller-owned recovery. Reconnect
only at an explicit safe lifecycle boundary. The portable macro transaction
path is separate; this finding is not a failure of its nested-savepoint tests.

### DDP03 — Medium: direct helpers retain live connections, and tracking never shrinks

[MathFunctionsMixin](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/math_mixin.py)
at lines 43–48 and 69–74 opens a connection for each maximum/minimum query and
never closes it. The same normal-return leak exists in
[direct_get_all_values](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/search_mixin.py)
at lines 557–563 and
[direct_execute_sql](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sql_execution_mixin.py)
at lines 93–99.

Reproduced for both SQLite drivers: ten maximum lookups leave ten additional
usable connections; ten value lookups leave another ten. Garbage collection
does not release them because `_open_connections` holds strong references.
Even correctly closed temporary handles remain in that list until driver close,
so repeated macro units also grow the tracking collection and subsequent scans.

Close owned handles in `finally`/`closing`, including failure paths. Give the
registry a bounded live-handle lifecycle without weakening close-all behavior.
Test both open descriptors and retained entries while the driver remains alive;
existing close-at-shutdown checks do not establish bounded import-time usage.

### DDP04 — Medium: PostgreSQL SQL adaptation changes literals and rejects valid input

[connection.py](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/connection.py)
contains character-level placeholder translation at lines 671–692, unconditional
translation at lines 1133–1141, and semicolon splitting at lines 919–922.
It does not distinguish all SQL lexical contexts or psycopg parameter escaping.

Live PostgreSQL 16 probes produced:

| Input through the adapter | Observed result |
| --- | --- |
| `SELECT '100%', ?` with `(7,)` | `IndexError` from parameter interpolation. |
| `SELECT ? -- why?` with `(7,)` | The comment becomes another placeholder; `IndexError`. |
| `SELECT $$what?$$` | Successfully returns **`what%s`**, altering literal data. |
| A native JSONB `?` operator | Rewritten into invalid SQL. |
| `executescript("SELECT 'a;b';")` | Splits inside the string and raises a syntax error. |

The scanner limitations are documented, but silent literal rewriting remains
an unsafe outcome for the public SQL surface. Define the supported dialect
explicitly, preserve literal/comment bodies, handle parameter styles deliberately,
and let PostgreSQL execute native scripts without naive splitting. Until then,
ordinary bound macro values are the safer supported path; these probes do not
show that bound catalogue text itself is being rewritten.

### DDP05 — Medium: consuming a SQL dump controls the primary transaction

Both [SQLite.sql_dump](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite/databasedriver/__init__.py)
at lines 796–802 and
[SQLite_apsw.sql_dump](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite_apsw/databasedriver/__init__.py)
wrap iteration in the primary connection context. Exhaustion commits pending
writes; closing a partly consumed generator rolls them back.

Reproduced on both driver names: insert a pending row, request the first dump
line, then close the generator. The row disappears. The docstrings accurately
describe this, so it is an existing hazardous contract rather than an undisclosed
typing regression. Export cancellation should not decide the fate of unrelated
application writes.

Choose and document dump snapshot ownership, then cover full iteration, early
close and sink failure while a caller has pending work. Coordinate this with
DDP01 without assuming that dumping SQL is equivalent to a verified backup.

### DDP06 — Medium: SQLite restore checks foreign keys before parent tables exist

[SQLite.dump_and_restore](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite/databasedriver/__init__.py)
enables foreign keys on the restore connection at line 930, then replays the
dump at lines 947–957. SQLite's dump order is not a dependency ordering.

Reproduced with a populated `a_child` referencing `z_parent`: replay fails with
`no such table: main.z_parent`. The original file survives this failure. The
APSW shell restore succeeds for the same fixture, so the two driver names have
different supported restore behavior. A current WEMI database containing one
Manifestation and its Item also fails under SQLite with
`no such table: main.manifestations`; the APSW restore succeeds. Fresh empty WEMI
databases round-trip on both drivers, illustrating why an empty-schema test
misses the defect.

Restore privately with a deliberate foreign-key loading policy, then run foreign
key and integrity validation before publication. Include populated WEMI links,
cycles, extension tables and required SQL callbacks in the round-trip tests.

## Implementation checkpoint — 2026-11-05

All six reproduced defects are repaired on
`database-driver-plugins-review-2026-11-05`. The three new regression modules
are [SQLite safety](../../tests/databases/test_driver_plugin_sqlite_safety.py),
[populated restore](../../tests/databases/test_driver_plugin_sqlite_restore.py)
and [live PostgreSQL safety](../../tests/databases/test_driver_plugin_postgres_safety.py).

| Finding | Implemented behavior | Regression coverage |
| --- | --- | --- |
| DDP01 | A separate read-only SQLite connection pins committed state, including WAL, and backs it up privately. Integrity and foreign keys are checked before create-only publication. | Committed versus pending rows; a writer committing during copying; existing/source/sidecar/symlink destinations; validation failure and publication races. |
| DDP02 | A failed primary-connection probe propagates without closing or replacing the handle. Recovery uses the existing transaction; explicit `reopen()` or `refresh(reconnect=True)` deliberately discards it. | Live PostgreSQL aborted transactions and savepoints through execute, executemany and refresh; closed transport; cancelled SQLite probes. |
| DDP03 | Direct helpers close owned connections and cursors on success and failure. Successfully closed supported handles deregister immediately; failed closes remain owned for retry. Partial connection setup also cleans up. | Actual retained and usable handles, supplied-connection ownership, shutdown, concurrent temporary closes, and injected SQL/setup/close failures. Tree failure coverage also caught and repaired a chunk-reader leak. |
| DDP04 | Lexical adaptation preserves quoted/comment bodies, JSON operators and percent data while deliberately handling qmark/native bindings. Native scripts remain whole unless installed statement hooks require boundaries. | Live literals, identifiers, comments, JSON operators, parameter styles, single-use batches, procedural bodies, script errors, savepoint recovery and hook ordering. |
| DDP05 | SQL dump borrows the primary connection without completing its transaction. | Exhaustion, generator close and sink failure retain pending writes on both driver names. |
| DDP06 | SQLite restores with foreign-key enforcement disabled during loading, then validates before replacement. Both restore paths refuse tracked pending transactions and validate without reseeding missing language rows. | Populated WEMI Item/Manifestation links, forward references, cycles, extension constraints/collations/functions, schema version, and SQL/FK/validation/publication failures. Native APSW shell restore is exercised explicitly. |

### Contracts and remaining limits

- File backup includes committed state only. SQL dump includes changes visible
  on the caller’s connection, including its pending writes. Serialize use of that
  connection during iteration; a caller-owned read transaction is needed for a
  stable snapshot across the dump’s statements. Neither operation completes the
  caller’s transaction.
- Rebuild/restore still requires exclusive application access. Tracked pending
  transactions are refused, but this is not coordination with other processes
  or an online replacement protocol. Keep the existing explicit recovery-backup
  procedure when replacing a database.
- The PostgreSQL adapter accepts standard-conforming strings, explicit E-string
  escapes, dollar quotes, nested comments, backtick identifiers, qmark parameters
  and native pyformat parameters. It rejects mixed binding styles. It is not a
  general SQLite-to-PostgreSQL grammar translator. Hooked scripts remain subject
  to maintenance’s supported-statement policy; native unhooked scripts are sent
  whole to the server.
- Close callbacks cover the supported SQLite and PostgreSQL adapter handles.
  Arbitrary foreign handles without callback attributes remain tracked until
  driver shutdown. Lifecycle operations still require caller serialization.
- Ordinary `SQLite_apsw.get_connection()` remains stdlib SQLite. Native APSW
  coverage here specifically exercises its shell restore and existing callback
  contracts; this does not introduce a native APSW implementation of every API.

The [regression manifest](../../tests/databases/regression_ratchet.json) adds
**110 named cases** in six invariants, preserving all previous 240 cases. Its CI
gate rejects skipped, missing, deselected, xfailed or xpassed required cases.
Both new implementation helpers and all three regression modules are included
in the existing formatting/lint checks; production code remains checked by
both mypy and basedpyright. Final combined validation is recorded below.

## Structural recommendations and implementation order

1. **Implemented:** repair DDP01–DDP03 first: reliable backup, explicit recovery and bounded
   connection lifetime are prerequisites for confidence in long-running use.
2. **Implemented:** add the new invariant regressions to the existing ratchet. Keep native APSW,
   stdlib SQLite and live PostgreSQL cases identifiable; backend labels alone
   are insufficient.
3. **Implemented:** repair SQL adaptation and dump/restore contracts before widening their API.
   These operations need failure and cancellation tests as well as happy paths.
4. **Still open:** consolidate the duplicated stdlib SQLite driver construction. Retain an
   explicit APSW-specific restore/native adapter where needed; do not advertise
   a native APSW backend merely because the plugin imports APSW.
5. **Still open:** break the large portable-macro and PostgreSQL-driver modules into focused
   implementation units while keeping the public driver/macro contracts stable.
   Physical SQL acceleration stays in databases. WEMI selection and Calibre
   projection policy continue to belong in Catalog, as already agreed in the
   [ownership guide](../catalog/catalog-database-ownership.md).

6. **Measured follow-up:** profile full-schema PostgreSQL inspection. The existing
   `test_postgres_current_folder_hierarchy_and_view_filter` passed but took
   **140.2 seconds** in the repair integration run while walking physical tables
   and views. The server was actively querying schema metadata, with no blocking
   locks. This is one local test observation, not a general throughput benchmark;
   measure repeated metadata discovery before changing caching or query policy.

This is not a recommendation to rename or replace the whole driver architecture.
The correctness fixes precede further structural work. Keep these reproduced
boundaries covered permanently while reducing implementation duplication.

## Original review validation and limits

The existing driver-plugin, registry and adapter selection produced **320 passed,
seven skipped**. One teardown error detected `LiuXin_scratch` created by a
concurrent standalone restore probe; that probe's scratch was subsequently
redirected to `/tmp`. The affected lifecycle cases, live PostgreSQL cursor
factories, direct row-write and transaction ownership contracts then passed in
a separate **32-pass, zero-skip** run. These selections overlap; their counts
are not a unique-test total. The initial teardown error was review-probe
interference, not an attributed driver test regression.

The skipped cases were Windows deletion semantics (two), unavailable usable
view rows (two), WEMI's read-only `books` view (one), and live PostgreSQL cursor
factories before a server was configured (two). This was not a full-repository
run. Runtime implementation and test expectations were not changed for this review.

All **16 documentation navigation/link checks** passed, and the staged diff
passes `git diff --check`. The private PostgreSQL server was stopped and its
cluster removed after the live checks; only review logs remain.

Review evidence is held outside the checkout in
`/tmp/liuxin-driver-review-tests.log`, `/tmp/liuxin-driver-review-repro.log`,
`/tmp/liuxin-driver-review-postgres.log`, `/tmp/liuxin-driver-review-restore.log`,
`/tmp/liuxin-driver-review-wemi-restore.log`,
`/tmp/liuxin-driver-review-wemi-populated.log` and
`/tmp/liuxin-driver-review-live-tests.log`. The corresponding standalone probe
scripts are `/tmp/liuxin-driver-review-*.py`. These local paths are evidence for
this session, not permanent test artifacts; the invariant descriptions above
are the specification for follow-up regressions.

## Repair validation — 2026-11-05

- The final mandatory gate passed **350 cases, zero skips/xfails**, with a
  monotonic baseline comparison against `07fc3326`. This includes the strengthened
  concurrent-backup regression, which forces a writer commit while pages remain
  to copy. The first expanded gate also passed all 350 cases.
- Backend, creation/replacement, portable API, maintenance, Core/Catalog consumer,
  wrapper and documentation/tooling integration: **1,275 passed, 37 skipped,
  zero failures**. The skips are Windows-only deletion behavior, unavailable view
  rows, read-only WEMI compatibility behavior, a PostgreSQL-shaped SQLite harness
  without `pg_temp`, and explicitly backend-specific maintenance/locking cases.
  No required ratchet case is excused by these integration skips.
- All formatting, lint, architecture, import-footprint and typing gates passed:
  **455 mypy sources**, zero basedpyright errors/warnings, and both internal
  contract checks (95/93 rejected invalid examples). Final production edits
  after this signoff only clarify docstrings; the strengthened test passes Ruff
  and formatting checks.
- All **121 native driver/adapter doctest examples** and **22 final documentation
  checks** passed. Counts describe separate selections and overlap; they are not
  a unique-test total.
- Tests ran serially with external scratch and telemetry. The integration run
  recorded about **229 MiB** peak test-process-tree RSS and no OOM-counter increase.
  This is local test evidence, not a production import or full-repository run.

The disposable PostgreSQL server was stopped and its cluster removed after
validation. Changes remain local on the review branch; no production import or
new full-repository run is claimed.

The reproducible regression owners and CI policy are linked above. Local repair
logs use `/tmp/liuxin-driver-fixes-*`, including the ratchet JSON receipts,
integration selection and telemetry, quality output and doctest results.
