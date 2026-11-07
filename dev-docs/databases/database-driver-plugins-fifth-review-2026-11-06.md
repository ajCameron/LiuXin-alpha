# Database driver plugins — fifth review

Reviewed **2026-11-06**, at commit
`47e089d4fd6179444d687922f7649ef1a7910a88` on
[PR #156](https://github.com/ajCameron/LiuXin-alpha/pull/156), following the
[fourth review and repairs](database-driver-plugins-fourth-review-2026-11-06.md).
The original pass changed review/navigation/handoff documentation only.
The subsequent authorized mitigations are recorded in
[Implementation and regression coverage](#implementation-and-regression-coverage).

**The review found three runtime defects: two high priority and one medium.** All
applicable CI checks on this head are green, including the full suite,
confidence tests, architecture/typing and mandatory database regression ratchet.
The new engine probes exercise cases outside that coverage.

| Finding | Priority | Observed consequence |
| --- | --- | --- |
| DDP20 | High | Divergent transaction-keyword parsing permits partial SQLite script commits and PostgreSQL commits without required prompt effects. |
| DDP21 | High | A retained SQLite SELECT cursor leaves an old WAL across file replacement; the reopened database loses a table present in the validated candidate. |
| DDP22 | Medium | Schema-qualified PostgreSQL JSON operators are mistaken for parameter locations, including with native bindings. |

## DDP20 — High: completion guards disagree with engine tokenization

[statement_keyword](../../src/LiuXin_alpha/databases/maintenance/bound_connection.py),
lines 31–42, skips only space, tab, CR, LF and semicolons, and ends a block
comment at its first closing delimiter. It does not recognize form feed, a
SQLite BOM, or PostgreSQL nested block comments. The newer SQLite row classifier
and PostgreSQL SQL tokenizer already recognize more of these forms, but these
completion paths still use the older scanner.

### SQLite installed-maintenance scripts

[SQLiteCompletionCursor.executescript](../../src/LiuXin_alpha/databases/connection_hooks.py),
lines 113–126, preflights transaction controls with that scanner, then executes
the accepted script in one transaction. On both **SQLite and SQLite_apsw**:

1. Create a primary-key source table and install a prompt plugin that writes
   one effect row per source event.
2. Call `driver.direct_executescript` with a successful INSERT, a prefixed
   COMMIT and a duplicate INSERT that fails its primary-key constraint.
3. Inspect committed source/effect rows using an independent sqlite3 reader.

The script is equivalent to this Python string, where `\f` is a form feed:

```python
"INSERT INTO review_items VALUES (1); \fCOMMIT; INSERT INTO review_items VALUES (1);"
```

The call raises `DatabaseDriverError`, but the first source row and its prompt
effect are already durable. Substituting `\ufeffCOMMIT` reproduces the same
result. Plain COMMIT is rejected before executing any statement; neither row is
durable in that control. The SQLite per-statement hook recognizes the prefixed
COMMIT and flushes maintenance, but cannot restore the script's atomicity after
the native commit.

This finding concerns the explicitly atomic **installed-maintenance** script
path. It does not redefine unmaintained sqlite3 scripts, which retain their
documented native behavior.

### PostgreSQL source commits

[MaintenanceRuntime.before_statement](../../src/LiuXin_alpha/databases/maintenance/runtime.py),
lines 191–215, uses the same scanner to recognize SQL COMMIT/END and flush
required prompt work. Against a disposable **PostgreSQL 16** server, insert a
source row through the configured connection, then execute either:

```python
connection.execute("\fCOMMIT")
connection.execute("/* outer /* inner */ tail */ COMMIT")
```

Each call succeeds and commits the source row with **zero handler calls and
zero effect rows**. The scanner returns an empty keyword or `TAIL`, respectively.
A plain COMMIT calls the handler and commits both rows.

With a deliberately failing prompt handler, plain COMMIT invokes it, raises,
and rolls back the source row. Both prefixed variants still succeed and commit
the source row without invoking it. All durability observations use a separate
native connection, avoiding a dependent read that could flush the pending work.
The Python `connection.commit()` method is not the failing path here.

**Repair:** give each backend one consistent lexical policy for statement
boundaries and leading operations. Use it for script preflight, direct writes
and maintenance dispatch; include engine-valid whitespace/comments. Preserve
the distinction between SQLite's comments and PostgreSQL's nested comments.
Regressions must check independent-reader durability, failing handlers and
late script failures, alongside ordinary controls. Do not treat an unrecognized
control as permission to skip a required completion boundary.

## DDP21 — High: rebuilding can retain WAL from the replaced database

[require_idle_sqlite_connections](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sqlite_files.py),
lines 15–25, checks `connection.in_transaction`. An unfinished SELECT can hold
a read statement while that property is false. Closing its Python connection
does not necessarily finalize the retained cursor's native statement.

[SQLite.DatabaseDriver.dump_and_restore](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite/databasedriver/__init__.py),
lines 927–932, then closes tracked connections, renames the validated candidate
over the main file and reopens it, without checking that the old WAL is gone.

Reproduced with the real **SQLite** driver, using only its own tracked primary
connection and no concurrent database user:

1. Enable WAL and commit a table containing 50 rows.
2. Execute a SELECT, fetch one row, and retain the unfinished cursor.
3. Confirm `connection.in_transaction is False`.
4. Call `driver.dump_and_restore(sql="CREATE TABLE restore_marker(id INTEGER);")`.
5. Inspect the replacement through an independent sqlite3 connection.

The method returns successfully, but `restore_marker` is absent. An observation
wrapper around the real rename confirms that the validated candidate contains
the marker and the old destination WAL still exists at publication. The reopened
file retains the original 50 rows and reports `integrity_check = ok`, despite
not preserving the candidate's schema. Releasing the old cursor afterwards does
not recover the marker. This is loss of a demonstrated rebuild change, not a
claim that arbitrary catalogue rows were corrupted in the probe.

Without the cursor, or after explicitly closing it before rebuilding, the WAL
is absent at rename and the marker survives. The same probe with
**SQLite_apsw** also preserves the marker, including with the retained cursor;
the observed defect is in the stdlib dump/restore route.

**Repair:** establish that owned read statements and WAL state are quiescent
before publication, or refuse the rebuild while that cannot be established.
Closing a tracked connection and checking `in_transaction` are insufficient.
Keep candidate validation, but also protect the final publication boundary.
Do not blindly unlink a live WAL. Add retained-reader/closed-reader cases and
check the reopened schema/data, rather than integrity status alone.

## DDP22 — Medium: qualified JSON operators are translated as parameters

[_classified_tokens](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 251–281, treats a question mark following a qualification dot as an
expression binding. It does not recognize PostgreSQL's `OPERATOR(schema.?)`
syntax.

On PostgreSQL 16, native psycopg2 returns `(True,)` for:

```sql
SELECT %s::jsonb OPERATOR(pg_catalog.?) %s
```

with bindings `('{"key":1}', 'key')`. The configured adapter raises
`ValueError: Cannot mix qmark and PostgreSQL pyformat bindings` for that same
native statement. With portable parameters:

```sql
SELECT ?::jsonb OPERATOR(pg_catalog.?) ?
```

it binds the second value inside the operator name and leaves the final qmark
unchanged, producing a PostgreSQL syntax error. The unqualified `?` operator
works through both native and portable binding styles.

**Repair:** preserve schema-qualified operator syntax as operators, and cover
native and portable operands with live comparisons. Switching these statements
to native `%s` does not avoid the current classifier, so it is not a workaround
for this finding. Unqualified operators work in the tested context; callers
that need explicit operator qualification should not silently lose it.

## Review-time validation and recommended order

The current head's
[full CI suite](https://github.com/ajCameron/LiuXin-alpha/actions/runs/42262422549/job/128045515803)
completed with **8,799 passed, 1,147 skipped and 15 xfailed**.
The [mandatory database ratchet](https://github.com/ajCameron/LiuXin-alpha/actions/runs/42262422549/job/128045515801),
[confidence job](https://github.com/ajCameron/LiuXin-alpha/actions/runs/42262414303/job/128045486880),
[architecture/typing checks](https://github.com/ajCameron/LiuXin-alpha/actions/runs/42262422549/job/128045515653)
and remaining applicable PR checks passed. Those results establish the previous
repairs' regression baseline; they do not cover the new counterexamples above.

New evidence comes from isolated real-engine probes, with ordinary SQL,
native-parameter and closed-cursor controls. Configuration, preferences,
databases and scratch files were confined to `/tmp`. The private PostgreSQL
server was stopped and its owned cluster removed. No production import or
source-file trial was performed, and the full suite was not repeated locally.
At review time, permanent regressions and production repairs remained to be implemented.
All **22 documentation link/boundary checks passed** in 11.47 seconds; the staged
documentation diff also passed the whitespace check.

Repair DDP20 and DDP21 before merging, then DDP22. The repeated parser findings
now justify consolidating lexical ownership as part of the correctness work,
with backend-specific grammar and shared consumers. Keep the broader connection
setup consolidation and measured inspection performance work separate.

## Implementation and regression coverage

Implemented **2026-11-06**, following authorization to mitigate these findings.
The original evidence above describes the reviewed head, before these changes.

- **DDP20:** statement boundaries and leading operations now share the SQLite
  token stream or the existing PostgreSQL tokenizer. Installed SQLite script
  preflight, PostgreSQL completion/dependent-read dispatch, handler connections
  and upgrade callbacks use the appropriate backend rules. Native PostgreSQL
  dollar/escaped strings remain opaque while chained statements remain refused.
  SQLite replacement conflict syntax retains its explicit portable-handler
  refusal. New tests check actual durability and handler invocation across
  prefixed COMMIT/END, failed drains, savepoint release and script controls.
- **DDP21:** configured SQLite connections weakly track their cursors, including
  custom cursor classes, and finalize them before closing the native connection.
  Both rebuild routes refuse publication after failed closes or if old WAL,
  shared-memory or journal files remain. They reuse the file-creation sidecar
  guard and never unlink a live journal. Regressions check the reopened schema
  and rows, primary/auxiliary readers, custom/closed/abandoned cursors, unwrapped
  native reads, external WAL readers, failed-close ownership and successful retry.
  Exclusive access by other database users remains the caller's responsibility.
- **DDP22:** explicit `OPERATOR([schema.]symbol)` names stay opaque operators during
  binding classification. Identifier translation still reaches schema names
  inside them. Live tests compare native results for four JSON operators,
  both operand binding styles and five qualification/quoting/comment forms.

The mandatory manifest adds **156 cases**, increasing the required set from
652 to **808**, without removing or moving an existing case. The additions are
96 maintenance-completion cases, 18 rebuild/publication cases, two resource
lifetime cases and 40 qualified-operator cases. The new test module also enters
the formatting and lint gates.

Final local validation:

- **808/808 mandatory cases** passed setup, execution and teardown, with no
  failures, skips or xfails. The full selected files passed **839 tests** in
  313.08 seconds. Baseline comparison against `47e089d4` preserved every prior
  case and its invariant membership.
- The affected driver/maintenance/upgrade integration selection passed
  **460 tests**, with 29 backend/setting-specific skips, in 82.21 seconds.
  Its selection overlaps the mandatory gate.
- The complete confidence stream passed **313 tests**, with three existing
  skips: two views without usable ID rows, and the cssutils-absent scenario
  when cssutils is installed. Telemetry records exit status zero and no failed
  reports. All **121 doctest examples** pass.
- Full quality passes: formatting, lint, complexity, annotation coverage,
  dependency direction/cycles, cold-import budgets, both type checkers and
  both invalid-call contract probes. **58 documentation/runner checks** pass.

The first integration run caught a changed error type for unsupported PostgreSQL
`INSERT OR REPLACE`; the portable handler refusal was restored and the expanded
selection rerun successfully. An initial quality runner reached successful checks
but encountered a shell read error after its file was edited while it was running;
the unchanged final script passes syntax validation and a complete fresh run.

All runtime state stayed in temporary Linux scratch. The private PostgreSQL 16
cluster is stopped and removed. The previously published head's green CI is not
validation of these changes; require fresh PR checks on the delivered commit.
No full local suite or production import trial is claimed.
