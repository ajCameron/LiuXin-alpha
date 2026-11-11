# Database driver plugins — third review

Reviewed **2026-11-05–06**, after the repairs in the
[first review](database-driver-plugins-review-2026-11-05.md) and
[second review](database-driver-plugins-follow-up-review-2026-11-05.md).
The reviewed tree is the staged, uncommitted implementation on
`database-driver-plugins-review-2026-11-05`, based on `07fc3326`.
The original review changed documentation only. The subsequent authorized repair
is recorded in [Implementation and regression coverage](#implementation-and-regression-coverage);
the findings below retain the original evidence and repair rationale.

**The review found six reproducible defects: two high priority and four medium priority.**
At review time the existing 400 mandatory cases passed. These were additional inputs and
paths beyond their coverage, including two qualifications to earlier fixes.

| Finding | Priority | Observed consequence |
| --- | --- | --- |
| DDP11 | High | A failing SQLite `WITH … INSERT` batch leaves its first row committed. |
| DDP12 | High | PostgreSQL binary values become memory-address text; a row round trip persists that text over the payload. |
| DDP13 | Medium | SQLite `direct_execute`/wrapper `execute` cannot return rows from an ordinary `INSERT … RETURNING`. |
| DDP14 | Medium | A failed legacy book-group deletion retains an owned transaction and blocks other writers. |
| DDP15 | Medium | SQLite uniqueness discovery misses a rowid primary key and rejects valid quoted index names. |
| DDP16 | Medium | Unqualified keyword-shaped identifiers still cause valid PostgreSQL JSON operators to be rejected. |

## DDP11 — High: CTE batches can commit a prefix despite reporting failure

[SQLExecutionMixin.direct_executemany](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sql_execution_mixin.py),
lines 253–262, relies on `with conn:` for its transaction. Entering that context
does not itself begin a SQLite transaction. The implicit transaction recognition
used by these stdlib-backed connections does not cover leading `WITH` writes.

With an initially idle primary connection and no installed maintenance boundary,
the following operation raises `DatabaseDriverError`, preserving the input error,
but an independent connection still reads `(1, "prefix")`:

```python
def values():
    yield (1, "prefix")
    raise ValueError("late input error")

db.driver_wrapper.executemany(
    "WITH supplied(id, value) AS (VALUES (?, ?)) "
    "INSERT INTO review_items SELECT * FROM supplied",
    values(),
)
```

Reproduced on **both SQLite driver names**, first with small driver fixtures and
then through complete WEMI `Database` instances and their public wrappers.
Ordinary `INSERT` rolls back correctly. Starting `BEGIN` before the CTE batch also
prevents a durable prefix. The new retry removal is effective; this is a separate
missing-transaction path, and the method reports failure rather than silent success.

The installed [maintenance boundary](../../src/LiuXin_alpha/databases/maintenance/runtime.py)
already explicitly starts transactions for this SQLite limitation. That conditional
protection does not establish the driver contract when maintenance is absent.
This qualifies DDP07's earlier completion for ordinary leading-DML batches.

**Repair:** explicitly establish the batch's transaction before consuming bindings.
Preserve or deliberately revise the documented treatment of an existing primary
transaction; do not infer ownership from SQL's first word or the presence of a
maintenance hook. Mandatory regressions should cover idle/existing transactions,
CTE INSERT/UPDATE/DELETE, late input and SQL failures, and independent-reader state.

## DDP12 — High: PostgreSQL binary row round trips replace payloads with address text

PostgreSQL's [_row_to_dict_from_db_row](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/databasedriver/__init__.py),
lines 2593–2613, delegates to the shared SQLite-oriented
[_coerce_db_value](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/value_casting_mixin.py).
The declared PostgreSQL type `bytea` falls into its NUMERIC conversion bucket.
Psycopg2 supplies a `memoryview`; that bucket stringifies it instead of returning
its bytes. Table reads and the shared view reader expose `"<memory at 0x…>"`.

This affects an existing generated table, not just a custom native extension:
`conversion_options.conversion_option_data` is declared BLOB in the
[current schema](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/table_sql/db_metadata_tables/conversion_options.sql),
and the [PostgreSQL builder](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/schema.py)
translates BLOB to `bytea`.

Reproduced in a complete PostgreSQL 16 WEMI database:

1. Insert conversion options containing nontext bytes, including NUL and `0xff`.
2. Read the row using `direct_get_row_dict_from_id`.
3. Change only `conversion_option_format` from EPUB to PDF in that dictionary.
4. Pass the dictionary to `direct_update_row_dict`.
5. Read the persisted payload with raw psycopg2: it is now the bytes of
   `"<memory at 0x…>"`, replacing the original binary content.

A raw read before conversion returns the correct bytes. A small BYTEA view also
reproduces the bad dictionary value. No fabricated row object or dependency
replacement is involved.

**Repair:** make conversion backend-aware and preserve binary contents across all
table/view/Row paths. In particular, BYTEA must not use SQLite NUMERIC fallback.
Add mandatory binary round-trip regressions against the real conversion-options
table, including NUL/non-UTF-8 data, view reads and an unrelated-field update.
Review other native PostgreSQL value types under the same policy; this pass did
not establish equivalent corruption for every non-SQLite type.

## DDP13 — Medium: the direct cursor-returning API commits before consuming RETURNING

[SQLExecutionMixin.direct_execute](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sql_execution_mixin.py),
lines 175–181, exits the connection transaction context before returning its cursor.
For an ordinary SQLite result-producing write, that cursor still has pending rows:

```sql
INSERT INTO review_items VALUES (2, 'returning') RETURNING review_item_id
```

Both SQLite driver names raise a `DatabaseDriverError` wrapping
`OperationalError: cannot commit transaction - SQL statements in progress`.
The inserted row is rolled back. This also reproduces through
`Database.driver_wrapper.execute` on complete WEMI databases. The configured
native connection succeeds when the caller fetches and closes the cursor before
leaving its transaction context.

The physical SQLite capability report correctly advertises engine support for
RETURNING; the failure is in the higher-level execution lifecycle. This is an
availability/contract defect, not observed silent data corruption.

**Repair:** define a result-producing-write path whose results are consumed before
commit, or expose caller-owned transaction/cursor lifetime explicitly. Preserve
ordinary SELECT streaming deliberately; do not indiscriminately materialize every
query or silently change when writes become durable. Test INSERT/UPDATE/DELETE
RETURNING, result consumption/closure, failure and independent-reader visibility.

## DDP14 — Medium: a failed book-group delete leaves a write lock held

[BookGroupMixin.direct_delete_book_group](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/new_book_mixin.py),
lines 112–124, opens a connection but commits/closes only on success. It catches
only `sqlite3.ProgrammingError`. A real `BEFORE DELETE` trigger using
`RAISE(ABORT, 'protected')` raises `IntegrityError` and leaves the owned connection
in an active transaction.

On both SQLite driver names, the driver then retains **two connections**, with
the failed deletion connection still in a transaction. An independent writer to
an unrelated table gets `database is locked`. Closing the driver releases the
retained handle and the independent writer succeeds. No mocked close failure or
resource exhaustion is needed.

The method remains in the public driver API and contract tests, but no application
call site was found in `src`. That makes retirement worth considering, as with
the removed scratch API. Its neighbouring `direct_get_next_book_group` also lacks
exception-safe connection cleanup; the reproduced lock finding concerns deletion.

**Repair:** retire the unused surface and retain capability notes, or give both
helpers explicit owned-resource cleanup and transactional deletion. A regression
must show that an independent writer succeeds immediately after a refused delete,
without requiring driver shutdown, while the protected group remains intact.

## DDP15 — Medium: SQLite's older uniqueness reader disagrees with physical schema

[MetadataMixin._get_unique_column_groups](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/metadata_mixin.py),
lines 362–381, reads only `PRAGMA index_list` and permits only alphanumeric/underscore
index names. Two valid SQLite layouts consequently lose their uniqueness evidence:

- `left_id INTEGER PRIMARY KEY` is a unique rowid alias without a separate index
  entry. The helper returns no groups despite physical inspection reporting that
  primary key.
- `CREATE UNIQUE INDEX "a valid index" ON review_links(left_id)` is legal, but the
  helper raises `DatabaseIntegrityError` for the name. The physical reader quotes
  it correctly and returns its unique key.

For either layout, wrapper `_link_constraint_shape` reports **UNKNOWN**, whereas
an ordinary underscore-named unique index on the same column reports
**MANY_TO_ONE**. All three layouts reject duplicate `left_id` values in SQLite.
Reproduced on both driver names using actual constraints and both readers.
These are valid native/extension layouts; this is not a claim that generated
WEMI links currently use a rowid endpoint or a spaced index name.

**Repair:** derive SQLite uniqueness from the shared physical facts, including
primary keys, and quote native identifiers instead of rejecting valid names.
Retain the existing exclusion of partial/expression indexes. Add parity tests for
rowid/composite primary keys, quoted names and downstream cardinality. The DDP09
PostgreSQL consolidation is repaired; the corresponding SQLite duplication remains.

## DDP16 — Medium: unqualified JSON operands remain ambiguous to the SQL adapter

The qualification-dot fix in
[_classified_tokens](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 124–130, correctly handles `t.first`. It still treats the valid unqualified
column `first` as an expression-introducing keyword:

```sql
SELECT first ? %s
FROM (SELECT '{"key":1}'::jsonb AS first) t
```

With `("key",)` bindings, raw psycopg2 returns `True`; the adapter raises
`ValueError: Cannot mix qmark and PostgreSQL pyformat bindings`. Naming the column
`payload` or quoting `"first"` makes both paths succeed. This is an additional
DDP08 input, not evidence that its qualified-name regression has returned.

**Repair:** make the native-versus-portable parameter contract explicit, or use
enough SQL context to handle the supported grammar. Repeated keyword exceptions
will leave this ambiguity fragile. Preserve literal/comment/percent handling and
genuine mixed-style rejection. Add raw-versus-adapted controls for unqualified and
qualified operands under both binding styles.

## Priority and verification

Address **DDP11 and DDP12 first**: they have demonstrated durable-data consequences.
Then settle the direct result/transaction contract, close or retire the legacy
resource leak, and consolidate the remaining physical-schema and SQL adaptation
policies. The previously documented module-size, duplicated connection setup and
PostgreSQL introspection performance work remains separate.

The unchanged mandatory gate passed **400 cases, zero skips/xfails**, with baseline
comparison against `07fc3326`, in **129.84 seconds**. All required setup/call/teardown
phases passed. These six findings are uncovered cases, not failures in that run.
No production/test fixes or new mandatory cases are included in this review.
All **22 documentation link/boundary checks passed** in 15.04 seconds; the staged
diff has no whitespace errors.

Reproductions used disposable SQLite files and a PostgreSQL 16 Unix-socket server.
The complete-catalogue probes used `enable_maintenance=False` and no installed
prompt-maintenance hooks. Both SQLite driver names use their ordinary stdlib
connections here; this pass does not claim separate native APSW cursor parity.

Session evidence is under `/tmp/liuxin-driver-third-review-`:

- `sqlite.py` / `.log`: batch failure, explicit-BEGIN control, RETURNING and lock recovery.
- `postgres.py` / `.log`: native BYTEA/view/round-trip controls and JSON operator adaptation.
- `catalogue.py` / `.log`: complete WEMI wrapper and current conversion-options reproductions.
- `indexes.py` / `.log`: native uniqueness, physical specs and wrapper cardinality.
- `ratchet/ratchet.json` and telemetry, with `ratchet.log` for the unchanged baseline.

These external probes are review evidence, not permanent regression coverage.
The PostgreSQL service is stopped and its owned cluster removed; its server log
and status receipt are retained. This is a focused failure-path review, not an
exhaustive audit of every legacy macro, a full-suite run or a production-import signoff.

## Implementation and regression coverage

**2026-11-06:** all six findings have local repairs and permanent regressions.
The original evidence above describes the pre-repair tree.

- **DDP11:** SQLite batches explicitly start a transaction before consuming inputs
  when the primary is idle. Existing primary transactions retain the documented
  whole-context completion policy: success commits earlier work, failure rolls it
  back. No retries or implicit-transaction inference are involved.
- **DDP12:** PostgreSQL overrides the SQLite conversion policy. Native decoded
  values are preserved and byte buffers become immutable bytes. Table dictionaries,
  the shared view reader and Row loads use that policy. Actual generated
  `conversion_options` round trips preserve binary data while format changes;
  native Decimal, date and boolean reads also retain their types.
- **DDP13:** result-producing SQLite writes finish and close their native cursor
  before commit. A detached query result exposes their rows afterward. Ordinary
  reads retain streaming cursors. Write-result buffering uses memory proportional
  to the returned rows; callers needing explicit cursor/transaction ownership can
  use a configured connection and consume its cursor inside their transaction.
  The CTE row-operation classifier is shared with maintenance and skips CTE names,
  bodies and optional materialization syntax before identifying the outer command.
- **DDP14:** both book-group helpers retain their API but close owned cursors and
  connections on every path. Deletion uses a transaction; a refused delete rolls
  back and releases its write lock before the error reaches the caller.
- **DDP15:** SQLite uniqueness groups now derive from the physical table spec,
  including rowid/composite primary keys and quoted native indexes. Duplicate
  groups are omitted; partial, expression and nonunique indexes remain excluded.
- **DDP16:** contextual SQL words acquire their expression-prefix role only inside
  phrases such as `FETCH FIRST`, `ORDER BY` and `AT TIME ZONE`. Unqualified JSON
  attributes remain operands; TIME/INTERVAL typed literals retain their bindings.
  The adapter remains lexical rather than a complete
  dialect parser; existing quote/comment handling and mixed-binding rejection stay
  covered by mandatory tests.

The pre-repair regression run had **54 failures and 72 passing controls**. After
repair, the initial focused run passed **283 tests**. Additional consumption,
public-wrapper and keyword-named CTE checks are included in the final gate.

The manifest adds **148 mandatory cases**, increasing **400 to 548**, with no
previous case removed or reassigned. The
[ratchet guide](regression-ratchet-2026-11-05.md#third-driver-review-extension--2026-11-06)
maps the new cases to their invariants.

### Repair validation

- Final mandatory gate: **548 passed, zero skips/xfails**, in 225.85 seconds.
  All required setup/call/teardown phases passed; baseline comparison used
  `07fc3326`. No existing case was removed or reassigned.
- Full quality runner: formatting (310 files), annotations (463), import cycles
  (255 protected modules), layer direction (1,903 modules), 25 cold-import budgets,
  lint and complexity checks passed. Basedpyright reported zero errors/warnings;
  mypy passed 457 source files. Both negative-contract suites passed (95/93 cases).
  An initial mypy inference error in the contextual-keyword table was corrected
  with an explicit dictionary/set/tuple annotation before this final run.
- All **121 driver/adapter doctest examples passed**.
- Affected integration: **495 passed, 13 skipped and 2 expected failures**, in
  594.13 seconds, including all 22 documentation link/boundary checks. Skips are
  two fixture-view cases, one PostgreSQL-shaped SQLite harness and ten explicitly
  SQLite-only maintenance cases on other backends. The two existing legacy scalar
  return expectations remain xfailed. Two existing Python 3.12 SQLite timestamp
  converter deprecation warnings were reported; there were no unexpected failures.
- The owned PostgreSQL 16 server was stopped and its cluster removed after checks;
  its log and status receipt remain outside the checkout. No full-suite run or
  production-import validation is claimed.

Current repair evidence uses the `/tmp/liuxin-driver-third-fixes-` prefix:
`red.log`, `focused.log`, `sql-context.log`, `ratchet-final/ratchet.json` and its
telemetry, `quality-final.log`, `doctests.log` and `integration.log`. Pytest uses
`-n0`; scratch, telemetry and mypy cache are outside the checkout. The regression
run's sampled test-process tree peaked at 83.2 MiB and the host OOM-kill counter
stayed unchanged; that tree excludes the separately owned PostgreSQL server and
quality processes. These measurements are not a full-machine memory guarantee.
