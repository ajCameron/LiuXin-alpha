# Database driver plugins — eighth review

Reviewed **2026-11-11** against `bab2b682cfa642fd3ecd431c1c395fbac0ecad00`
plus the 23-file staged sixth/seventh-review changes. The previous DDP25–DDP27
repairs remain covered by passing regressions. **Both new PostgreSQL findings
now have implemented repairs**, described below. The original finding evidence
is retained. No new SQLite defect was reproduced in this pass.

The reviewed checkout is
`/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`, branch
`database-driver-plugins-review-2026-11-05`. Its origin is the original local
checkout, not a GitHub publication destination. Remote PR/CI was not refreshed.
The original review left production code, tests, the required manifest and staged
contents unchanged. The authorized follow-up implementation is recorded below.

| Finding | Priority | Reproduced consequence |
| --- | --- | --- |
| DDP29 | Medium / P2 | A connection configured with nonstandard string escapes can silently rewrite and commit literal text. |
| DDP28 | Medium / P2 | Valid expression positions in six SQL forms are mistaken for JSON operators, rejecting qmark execution. |

## DDP29: establish or honor the connection's string-literal mode

[`sql_tokens`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 134–139, handles backslash escapes for explicit `E'…'` literals only.
Its docstring assumes standard-conforming strings, but
[`connect_postgres`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/connection.py)
and [`get_connection`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/databasedriver/__init__.py)
neither establish that setting nor refuse incompatible connections. A supported
connection URL can supply `options=-c standard_conforming_strings=off`.
The registered driver's live connection retains `off`, verified with SHOW.

On that connection, this native-valid statement exposes literal contents to
backtick identifier conversion:

```sql
SELECT 'prefix\'`literal`\'';
```

Native psycopg2 returns ``prefix'`literal`'``. The adapter returns
`prefix'"literal"'`, silently substituting double quotes for the backticks.
Equivalent INSERTs were committed through the registered driver's connection;
an independent native reader returned:

| Source | Persisted body |
| --- | --- |
| Native SQL | ``prefix'`literal`'`` |
| Adapter SQL | `prefix'"literal"'` |
| Bound-value control | ``prefix'`literal`'`` |

Explicit `E'…'` syntax preserves the expected value with the setting both on
and off. The unprefixed example is invalid with the setting on and both paths
reject it; that control is not counted as a valid-query failure. Bound values
also preserve the value with the setting off.

This requires a nondefault connection setting. PostgreSQL documents the setting
as controlling backslash interpretation in ordinary literals and permits
applications to inspect it; its default is on. See the
[PostgreSQL 16 compatibility settings](https://www.postgresql.org/docs/16/runtime-config-compatible.html#GUC-STANDARD-CONFORMING-STRINGS).
The demonstrated defect is persisted value alteration; no injection or
transaction-boundary exploit is claimed.

Make the lexer's precondition real: honor the current native setting, or
explicitly reject incompatible connections/settings before adaptation. Merely
documenting an assumption while accepting such connections leaves silent
rewriting possible. Cover startup options and subsequent setting changes,
ordinary and continued literals, script splitting, and independently read
committed values. Explicit E-strings and bound values are verified workarounds
for this example. This is separate from DDP26: the repaired continued E-string
behavior passes with the default setting.

## DDP28: recognize qmarks at the remaining expression boundaries

[`_classified_tokens`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 344–345 and 376–379, relies on a limited prefix set and treats every
closing parenthesis as ending an operand. The prefix set at lines 40–43 omits
FOR, LEADING, TRAILING, ROWS and VARIADIC; closing DISTINCT ON also fails to
recognize the start of the SELECT target. Required bindings become operators.

| SQL with qmark parameters | Parameters | Native result |
| --- | --- | --- |
| `SELECT SUBSTRING(? FROM ? FOR ?)` | `('abcdef', 2, 3)` | `'bcd'` |
| `SELECT TRIM(LEADING ? FROM ?)` | `('x', 'xxvalue')` | `'value'` |
| `SELECT TRIM(TRAILING ? FROM ?)` | `('x', 'valuexx')` | `'value'` |
| `SELECT DISTINCT ON (?) ?` | `(1, 'result')` | `'result'` |
| `SELECT SUM(n) OVER (ORDER BY n ROWS ? PRECEDING) FROM generate_series(1,3) n` | `(1,)` | `1, 3, 5` |
| `SELECT CONCAT(VARIADIC ?::text[])` | `(['a', 'b'],)` | `'ab'` |

The first four adapter calls raise `TypeError: not all arguments converted
during string formatting`; the last two send the question mark to PostgreSQL
and raise syntax errors. All six reproduce through registered-driver
`direct_execute`, where they are wrapped as `DatabaseDriverError`. Replacing
the binding marks with `%s` succeeds both natively and through that same
registered driver. These forms are valid PostgreSQL grammar, including the
SQL-standard keyword-separated string functions; see its
[string-function reference](https://www.postgresql.org/docs/16/functions-string.html)
and [SELECT grammar](https://www.postgresql.org/docs/16/sql-select.html).

A SUBSTRING INSERT also fails through `direct_executemany`. The native-binding
version commits both expected rows, verified independently; the failed qmark
batch contributes no rows. Existing JSON `?` binding and `ROWS BETWEEN ?
PRECEDING AND CURRENT ROW` are passing controls. No partial write is claimed.

Recognize these expression positions contextually while retaining identifier
and JSON-operator behavior. Simply adding unreserved words to the global prefix
set could reintroduce earlier bare-column failures. Add native/adapter comparisons
for single and batch calls, plus operator/identifier controls. Native `%s`
bindings are a verified workaround and are already preferred by the runbook for
PostgreSQL-specific SQL. This finding concerns binding recognition, not a request
to translate these SQL forms into SQLite grammar.

## Verification and limits

- Existing focused suite: **945 passed, zero skips**, in 177.04 seconds. Eight
  modules cover SQLite restore/safety/value preservation, all-driver parity,
  live PostgreSQL adaptation/resources, completion, adapter and registry contracts.
- Registered-driver probes reproduce both findings against private PostgreSQL
  16. Native execution and independent committed-value reads provide controls.
- A deterministic SQLite export/replay sweep checked **10,000 finite floating
  point values with zero mismatches**. Five FTS5 declaration variants, including
  comments/newlines/tabs, preserve row ID 42 and the MATCH result after replay;
  SQLite canonicalizes the stored virtual-table declarations.
- Focused telemetry: 82.0 MiB peak test-process-tree RSS, at least 6.70 GiB
  available memory and zero OS OOM kills.
- Documentation link/navigation and public-boundary checks: **22 passed** in
  189.92 seconds. Staged and unstaged whitespace checks pass.
- All 23 staged files match their review-start hashes. Existing implementation,
  tests and configuration are unchanged; only review/navigation/handoff docs
  were edited. Original protected preference/source files, the old worktree
  index, recovery patch and all 19 recovered files match their saved hashes.
- The owned private PostgreSQL 16 cluster is stopped and removed; its server log
  and cleanup receipt are retained. It used max_locks_per_transaction=256.

Persistent evidence:
`/mnt/c/dev/LiuXin-alpha/.tmp/driver-eighth-review-2026-11-11/`.
Files include `review-source.json`, `review-start-preservation.json`,
`focused.log`, `focused-result.json`, `focused-telemetry/`,
`resources-summary.json`, `probes.py`, `probe-results.json`,
`grammar-public.py`, `grammar-public-results.json`, `connection-mode.py`,
`connection-mode-results.json`, `sqlite-values.py`, `sqlite-values-results.json`,
`pg-server.log`, `pg-cleanup.json` and the final documentation/preservation results.
The first focused invocation named a nonexistent test module and ran no tests;
the corrected eight-module run above is authoritative. An initial connection
probe incorrectly encoded URL spaces as `+`; percent encoding corrected the
probe before the actual findings were reproduced.

The previous **1,180/1,180 mandatory gate**, full quality checks, doctests and
integration evidence remain documented in the
[seventh-review repairs](database-driver-plugins-seventh-review-2026-11-10.md).
This pass reruns the focused suite, not the full ratchet, repository suite or
remote CI. The follow-up below addresses both findings and adds their regressions.

## Implemented repairs — 2026-11-11

- **DDP28:** reserved expression prefixes cover SUBSTRING/OVERLAY, TRIM and
  VARIADIC. Parenthesis context identifies the SELECT target after DISTINCT ON,
  including nested groups. ROWS/RANGE/GROUPS remain unreserved column names at
  expression starts and introduce offsets in window frame positions. Both
  parameter styles execute and commit matching native results through single
  and batch driver calls; JSON-operator controls retain their meaning.
- **DDP29:** adaptation reads the current connection setting from libpq's
  ParameterStatus without issuing SQL or starting/ending a transaction. Startup
  options, SET, SET LOCAL, savepoint rollback and transaction completion are
  reflected when existing cursors are reused. Standalone native cursor adapters
  use their cursor's owning connection. Ordinary and continued strings share
  that mode; explicit E-strings retain their explicit escape semantics.
- Maintenance statement guards, borrowed handler connections and script splitting
  use the same mode. Hooked scripts lazily refresh it between executed statements;
  unhooked scripts still go to the native server as a whole. Apparent SQL inside
  literals stays data, real statement chaining is rejected, and later failures
  roll back both source changes and prompt-maintenance effects.

The connection reader uses the documented
[psycopg parameter-status API](https://www.psycopg.org/docs/connection.html#connection.get_parameter_status).
Syntax-only helpers without a connection still default to standard-conforming
strings; their explicit mode option is available when the setting is known.

The [required manifest](../../tests/databases/regression_ratchet.json) retains all
1,180 prior cases and adds **147**, taking the gate to **1,327**. New invariant
groups contain 66 expression-context cases and 81 connection-string-mode cases;
the [ratchet guide](regression-ratchet-2026-11-05.md) records the coverage.

The initial 145-case run reproduced **69 failures** with **76 passing controls**.
After the repair, all **145 passed**, zero skips, in 15.58 seconds. Two further
statement-chaining regressions and commit-mode restoration assertions were then
added for the final gate. The native keyword sweep makes **742 comparisons with
zero mismatches**; 100 identifiers rejected natively are excluded.

Final mandatory gate: **1,327/1,327 required cases** passed setup, call and
teardown, with `failures=[]` and exit 0. All **1,358 selected tests passed**, zero
skips, in **347.08 seconds**. All previous 1,180 invariant memberships are retained.
Full quality passed, including both type checkers (458 mypy sources), the 95/93
invalid-call probes, formatting/lint, annotations, architecture, complexity and
import budgets. The first quality attempt found one formatting difference,
corrected before its successful rerun.

The first full gate passed 1,356 selected tests and failed two extended lifecycle
cases. Those tests issued SET LOCAL after raw SQL ROLLBACK without opening a new
transaction; native PostgreSQL left the setting unchanged and rejected the
subsequent literal too. Adding explicit BEGIN corrected the fixture while
retaining the commit-restoration assertion. Both corrected cases passed alone,
then the complete final gate above passed. No production change or assertion
weakening was needed; the original failed run and native probe are retained.

Affected maintenance/callback/transaction integration: **285 passed, 29 existing
backend-specific skips**, in 150.05 seconds. The skipped variants exercise SQL
and connection behavior belonging to the other backend; both live PostgreSQL
and SQLite variants ran. All **121 native-driver/adapter doctest examples** and
**85 documentation/runner checks** passed (the latter in 94.16 seconds).

Final source hashes match the tested tree. Original protected preference/source
files, the old worktree index, recovery patch and all 19 recovered files retain
their saved hashes. The earlier SQLite fixes remain unchanged. The owned private
PG16 server is stopped and removed, with its log and cleanup receipt retained.
Final validation recorded zero OS OOM kills. Changes are staged, uncommitted;
no push or merge. No full-repository or remote-CI rerun is claimed.

Follow-up evidence:
`/mnt/c/dev/LiuXin-alpha/.tmp/driver-eighth-fixes-2026-11-11/`.
The original review evidence above remains separate and unchanged.
Key follow-up files are `baseline-manifest.json`, `new-cases.json`, `red.log`,
`focused.log`, `quality-final.log`, `lexer-sweep-results.json`, `ratchet.log`,
`lifecycle-debug.log`, `lifecycle-fixed.log`, `ratchet-final/ratchet.json`,
`new-case-coverage.json`, `integration.log`, `tooling.log`, `doctests-results.json`,
`tested-source.json`, `checks-complete.json`, `preservation-checks.json`,
`resources-summary.json`, `validation-summary.json`, `final-changes.patch`
and `pg-cleanup.json`.
