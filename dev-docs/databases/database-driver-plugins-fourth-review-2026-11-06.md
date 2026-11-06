# Database driver plugins — fourth review

Reviewed **2026-11-06**, at commit `11b6f320301b2a8f4631dfb9659c70cc50cf6e38`
on [PR #156](https://github.com/ajCameron/LiuXin-alpha/pull/156), following the
[third review and repairs](database-driver-plugins-third-review-2026-11-06.md).
The original pass changed review/navigation/handoff documentation only. The
subsequent authorized repairs are recorded in
[Implementation and regression coverage](#implementation-and-regression-coverage).
The findings below retain the original evidence and repair rationale.

**The review found two runtime defects and one CI fixture defect.** The earlier
repairs substantially improved the driver contract, but the SQL classifiers
accepted a narrower language than the engines do.

| Finding | Priority | Observed consequence |
| --- | --- | --- |
| DDP17 | High | An unquoted non-ASCII SQLite CTE name bypasses write recognition; a failed statement can persist a partial update. |
| DDP18 | Medium | Valid PostgreSQL time/interval queries with qmark bindings fail for omitted suffix/clause forms. |
| DDP19 | Medium | The confidence job fails because the PostgreSQL index test double still supplies the old row shape. |

## DDP17 — High: valid SQLite CTE names bypass transaction ownership

The shared [row classifier](../../src/LiuXin_alpha/databases/sql_statements.py),
lines 13–17, tokenizes unquoted words using `[A-Za-z_][A-Za-z_0-9]*`. SQLite
accepts unquoted non-ASCII identifiers. For a CTE named `名`, the scanner drops
the name and returns `("", False)` instead of identifying the outer write.

[SQLExecutionMixin.direct_execute](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sql_execution_mixin.py),
lines 184–196, uses that result both to start a transaction and to finish a
result-producing write before returning. With an idle primary connection and no
installed maintenance boundary, neither protection runs for these statements.

Reproduced on **both SQLite and SQLite_apsw**, using real configured connections,
an independent sqlite3 reader and this table/trigger arrangement:

```sql
CREATE TABLE review_items (id INTEGER PRIMARY KEY, label TEXT);
INSERT INTO review_items VALUES (1, NULL), (2, NULL), (3, NULL);
CREATE TRIGGER reject_second BEFORE UPDATE ON review_items
WHEN OLD.id = 2 BEGIN SELECT RAISE(FAIL, 'second row refused'); END;
```

Commit setup, then call `driver.direct_execute` with:

```sql
WITH 名(value) AS (VALUES ('changed'))
UPDATE review_items SET label = (SELECT value FROM 名)
```

The call raises `DatabaseDriverError`, but the independent reader sees
`[(1, 'changed'), (2, None), (3, None)]`. The first change is durable.
Identical statements using `source` or quoted `"名"` roll back the first change,
leaving all labels NULL. `RAISE(FAIL)` preserves the statement's earlier row
changes; the missing enclosing transaction makes them durable despite the
driver's error path.

The same classification defect also qualifies DDP13's completion guarantee:

```sql
WITH 名(id) AS (VALUES (2))
INSERT INTO review_items(id) SELECT id FROM 名 RETURNING id
```

On a fresh table without that ID, `direct_execute` returns a live cursor while
the new row is still invisible to another connection. Closing the cursor makes
it visible. ASCII and quoted-name controls publish before returning, as promised.
This is a single-statement defect; DDP11's unconditional batch transaction fix
continues to protect `direct_executemany`.

**Maintenance limit:** complete WEMI databases with the Work revision prompt
plugin installed rejected the unquoted-name write with
`Required prompt maintenance is unavailable`; no Work persisted. ASCII and
quoted-name controls reached an injected handler failure and rolled back.
The maintenance trigger therefore failed closed in this tested fresh-connection
path. Do not generalize the direct-driver partial update into a demonstrated
maintenance bypass.

**Repair:** make the shared classifier respect SQLite identifier tokenization
and ensure an unrecognized write cannot silently lose the transaction contract.
Retain streaming SELECT behavior. Regressions should cover unquoted/quoted
non-ASCII CTEs, other legal identifier characters, independent-reader durability,
late row failures, RETURNING completion and installed maintenance behavior on
both driver names. Share the recognition policy between driver and maintenance.

## DDP18 — Medium: typed-literal bindings depend on an incomplete suffix list

[_typed_literal_binding](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 146–153, recognizes a qmark after TIME/INTERVAL only when the following
word belongs to a short list. Valid interval fields and several clause starters
are absent. The adapter consequently leaves the parameter as a binary `?`
operator and sends SQL with the wrong meaning to PostgreSQL.

Against a disposable **PostgreSQL 16** server:

| SQL passed to the configured connection | Bindings | Adapted result |
| --- | --- | --- |
| `SELECT INTERVAL ? DAY` | `('2',)` | `UndefinedColumn: column "interval" does not exist` |
| `SELECT INTERVAL ? DAY TO SECOND` | `('2 days 03:00:00',)` | Syntax error near `TO` |
| `SELECT TIME ? WHERE TRUE` | `('12:34:00',)` | Syntax error near `WHERE` |
| `SELECT INTERVAL ? OFFSET 0` | `('2 days',)` | Syntax error near `OFFSET` |

For each case, the same raw psycopg2 connection executes the corresponding
native `%s` statement successfully and returns the expected time/timedelta.
`SELECT INTERVAL ?` succeeds through the adapter as a control. The rewritten
failing statements retain `?` unchanged, so this is parameter classification,
not unsupported PostgreSQL syntax. No persisted-data corruption was observed.

**Repair:** define a deliberate native/portable parameter policy or recognize
the necessary grammar context. Another isolated keyword patch should not break
JSON operators whose operands are named `time` or `interval`. Add live cases for
field qualifiers and subsequent clauses alongside those operand regressions.
Native `%s` is a working alternative for the statements reproduced here.

## DDP19 — Medium: a stale PostgreSQL fixture breaks the confidence gate

The [confidence job](https://github.com/ajCameron/LiuXin-alpha/actions/runs/42146470069/job/127659036657)
failed at `test_postgresql_driver_connects_and_introspects`, with 94 passed,
one skipped and one failed before `--maxfail=1` stopped the stream.
Running that test alone locally reproduces the failure: one failed in 1.46 s.

[_FakeDriverCursor.fetchall](../../tests/databases/database_driver_plugins/PostgreSQL_database_driver/test_postgresql_backend.py),
lines 992–993, still returns a one-column array tuple for `pg_index` queries.
The shared [physical index reader](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/physical_schema.py),
lines 207–218, now consumes nine fields. Accessing `row[7]` raises `IndexError`.
The test also asserts old SQL text excluding partial/expression indexes; those
decisions now belong to interpretation of the shared physical facts.

**Repair:** update the fake rows to the actual query contract and assert the
resulting unique-key behavior, including exclusion of partial/expression/invalid
indexes and correct treatment of included columns. Retain live PostgreSQL
coverage. Then run the complete confidence stream: the first failing fixture
currently hides any later failures. Do not modify production SQL merely to
accommodate the obsolete fake.

## Validation, confidence and next order

At **02:10 UTC on 2026-11-06**, the published head's database regression ratchet,
architecture/typing, installed-wheel and file-format checks had passed. The
confidence job had failed as above; the full PR suite was still running. The
previous local repair run's 548 mandatory passes remain useful evidence but do
not exercise DDP17/DDP18. This review did not repeat the full local suite.
All 22 documentation link/boundary checks passed in 8.74 s, and the staged
documentation diff passed the whitespace check.

New runtime evidence uses real SQLite engines and PostgreSQL 16 with ASCII,
quoted-name and native-parameter controls. Runtime configuration, preferences,
scratch databases and logs were isolated under `/tmp`. The private PostgreSQL
server was stopped and its owned cluster removed after the probes. No production
import or source-file trial was performed.

Fix the transaction-classification defect first, then the parameter policy and
confidence fixture. Rerun their new regressions and the complete confidence
stream, and require completed PR checks before merging. Connection-setup
consolidation and measured PostgreSQL inspection performance remain worthwhile
follow-ups after these correctness fixes.

## Implementation and regression coverage

Implemented **2026-11-06**, following authorization to repair these findings and
closely related defects. The manifest adds **104 mandatory cases**, increasing
the required set from 548 to 652 without removing or reassigning any prior case.

- **DDP17:** SQLite tokenization now preserves every non-ASCII identifier
  character, combining marks, dollar continuations and otherwise significant
  punctuation. It recognizes BOM/whitespace and leading empty statements.
  The shared driver/maintenance classifier therefore recognizes the outer write
  for these legal CTE names. Seventy new cases cover all three row operations
  with a second-row FAIL trigger, independent-reader rollback, RETURNING
  completion, streaming SELECTs, prompt success/failure and REPLACE rejection on
  both driver names. Quoted names and ordinary ASCII remain controls.
- **DDP18:** execution supplies the parameter count when resolving temporal
  qmarks. Temporal grammar includes field qualifiers, precision, time-zone
  modifiers, aliases and subsequent clauses. Same-named JSON operands retain
  their role. Where multiple assignments remain possible, execution raises an
  explicit error before sending SQL; native `%s` parameters or `CAST(? AS type)`
  disambiguate the expression. Batch execution inspects the first row once and
  streams the full iterator without replay. Thirty-three new live PostgreSQL
  cases compare native results, protect JSON operands and verify late batch
  failure rolls back. See the [parameter contract](postgresql-backend.md#sql-parameters-and-native-operators).
- **DDP19:** the fixture supplies all nine physical-index fields, and the test
  asserts unique-key results and schema/table scope instead of obsolete query
  filters. Partial, expression, invalid and nonunique indexes are excluded;
  INCLUDE columns remain outside the key. The previously failing test is now a
  named requirement in the mandatory gate, alongside existing live index cases.

The old head's full CI suite subsequently completed with **8,763 passed, two
failed, 1,078 skipped and 15 xfailed**. Besides DDP19, it exposed a missing
optional-APSW guard in the catalogue-wrapper CTE test. That test now follows the
other optional-backend cases in an ordinary suite. The mandatory regression job
still installs APSW and requires this exact case to pass; its coverage is not
optional there. An isolated missing-module probe verified the ordinary skip.

Final local verification:

- Mandatory gate: **652/652 required cases passed all phases**, no skips/xfails
  or manifest reductions against `11b6f320`; **683 tests passed** in the full
  selected files (237.57 s).
- Complete confidence stream: **313 passed, three skipped** (240.90 s). Two
  fixture views lacked usable ID rows; the CSS-utils-absent case skipped because
  CSS-utils is installed. The stream reported 86 warnings.
- All **121 driver/adapter doctest examples** passed.
- Documentation and regression-runner checks: **43 passed** (22.25 s).
- Full formatting/lint/architecture/import and typing checks passed. Both type
  checkers report no issues; mypy checked 457 sources, and the negative-contract
  suites rejected all 95/93 invalid examples. The final diff has no whitespace
  errors.

An initial confidence attempt overlapped another local pytest process using
default numbered scratch directories with retention disabled. That mode does
not create pytest cleanup locks; the other process's exit removed the active
directory, causing a disk-I/O failure and 25 setup errors. The complete rerun
above used its own explicit `--basetemp` and passed. The mandatory gate already
owns a separate scratch root. Use distinct explicit roots for concurrent runs.

The disposable PostgreSQL server was stopped and its owned cluster removed.
Fresh PR CI must still complete before merge; these local results are not a
claim that the full suite or a production import has passed on the repaired head.
