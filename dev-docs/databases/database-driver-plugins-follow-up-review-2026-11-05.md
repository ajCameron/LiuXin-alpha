# Database driver plugins — follow-up review

Reviewed **2026-11-05**, after the local repairs in the
[first driver-plugin review](database-driver-plugins-review-2026-11-05.md).
The reviewed implementation is the staged work on
`database-driver-plugins-review-2026-11-05` in the isolated review checkout,
based on integration commit `07fc3326`. The findings below record that original
reviewed state. The subsequent repair on the same branch is recorded in the
repair checkpoint.

**Later review:** the [third pass, 2026-11-06](database-driver-plugins-third-review-2026-11-06.md)
records six additional inputs/paths. DDP11 qualifies the batch guarantee for
leading CTE writes without an established transaction; DDP16 extends the operator
ambiguity to unqualified keyword-shaped columns. The original regression cases
remained repaired in the unchanged 400-case review gate. Those additional findings
now also have repairs; the third review records the expanded 548-case validation.

The previous repairs substantially improve backup, connection ownership and
transaction recovery. This pass found one additional silent partial-write path,
a remaining corner case in the new PostgreSQL translator, inconsistent native
index interpretation, and a broken obsolete scratch API. The passing regression
baseline covered the earlier demonstrated cases; it did not cover these inputs.

## Repair checkpoint — 2026-11-05

All four findings now have local changes and permanent regression coverage:

- **DDP07:** normalize the documented outer scalar tuple before SQL, stream other
  binding rows once and execute without retries. Input, adapter and SQL failures
  escape the connection context, roll back and retain their original exception
  as `DatabaseDriverError.__cause__`. Fourteen cases cover both SQLite drivers,
  failed generators, a repeatable adapter that fails only once, constraints,
  supported binding shapes and durable state observed independently.
- **DDP08:** qualification dots retain identifier context across whitespace and
  comments. Twenty-four live PostgreSQL cases exercise `first`, `next` and `time`
  with native/qmark bindings and all four JSON operators. Existing mixed-style,
  literal, comment and percent protections remain. Direct-script docstrings now
  describe native scripts and lexical boundaries for installed hooks accurately.
- **DDP09:** the physical schema and uniqueness readers share one index-fact
  query. Only valid, nonpartial, nonexpression unique keys prove uniqueness;
  `INCLUDE` payload is separate. Seven live cases cover endpoint/type inclusion,
  ordered composite keys, partial/expression/nonunique indexes, primary keys and
  a real invalid index left by a failed concurrent build. Link cardinality,
  typed identity, owned-handle cleanup and pending primary work are checked.
  Groups now have deterministic index-name order, matching physical inspection.
- **DDP10:** removed `lock_writing`/`make_scratch` from concrete and abstract
  database/driver APIs, including PostgreSQL's unsupported stub. No database
  callers remain. Five cases guard retirement and verify a separate working copy
  containing committed WAL, independent writes and unchanged source ownership.
  The [replacement workflow and retired capability](creation-and-replacement.md#independent-sqlite-working-copies)
  are documented; this does not promise source locking or read-only access.

The new tests reproduced **14 failures** before the first three repairs; the
retirement guard separately reproduced **3 failures** before removing the API.
A strengthened adapter case also failed on both drivers against the exact staged
pre-fix method: auto-assigned IDs allow a retry to duplicate an earlier row, and
the old method incorrectly returned success.
The first focused run after repairs passed **142 cases**. All **50 new cases**
are named in the mandatory regression ratchet, increasing it from 350 to **400**.
Existing case IDs and invariant membership are preserved. Final validation is
recorded below after the original review evidence.

## Findings

### DDP07 — High: a failed input iterator can become a successful partial batch

[SQLExecutionMixin.direct_executemany](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sql_execution_mixin.py)
at lines 263–272 retries every `ValueError` as if it were a binding-shape problem.
The retry happens inside the same transaction, after execution may already have
inserted rows or consumed part of the input iterator.

Reproduced with real connections under **both SQLite driver names**:

```python
def rows():
    yield (1, "prefix")
    raise ValueError("input conversion failed")

driver.direct_executemany("INSERT INTO review_items VALUES (?, ?)", rows())
```

The method returns normally. An independent connection reads the committed row
`(1, "prefix")`. Once the generator raises, it is exhausted; `tuple(values)` is
empty, so the retry succeeds and context exit commits the prefix. The original
input error disappears. The driver-wrapper `executemany` surface delegates here,
so this is reachable without invoking a private helper. PostgreSQL's override
has no equivalent retry loop; the already-repaired row-dictionary batch writers
are also separate paths.

**Repair:** normalize supported binding shapes before any SQL executes, then
execute the batch once and propagate input/execution errors through rollback.
Do not infer a retryable binding problem from an arbitrary exception after
execution starts. Add regressions for a generator that fails after a prefix,
conversion failures, and repeatable input that fails during adaptation. Verify
independent-reader durable state and that the original error survives.

### DDP08 — Medium: valid PostgreSQL JSON operators are mistaken for mixed parameters

The new [_classified_tokens/adapt_sql](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py)
at lines 123–127 and 150–154 treats every word in its expression-prefix set as a
keyword, including a qualified identifier such as `t.first`. The following native
JSONB operator is then classified as a qmark binding.

On the disposable **PostgreSQL 16** server, raw psycopg2 accepts this query and
returns `True`, with `("key",)` as its bindings:

```sql
SELECT t.first ? %s
FROM (SELECT '{"key":1}'::jsonb AS first) t
```

The LiuXin adapter raises `ValueError: Cannot mix qmark and PostgreSQL pyformat
bindings`. Changing the column name to `payload`, or quoting it as `"first"`,
makes the adapter return `True`. This is valid native SQL using one parameter
style; it is not a request to translate unsupported SQLite grammar. It does not
silently rewrite the returned JSON data, but it incorrectly refuses the query.

**Repair:** retain enough identifier/expression context to distinguish qualified
names from keywords. Consider an explicit native-parameter mode so native SQL
need not be guessed as qmark syntax. Add native and qmark regressions with
qualified keyword-shaped identifiers and JSON operators; keep the existing
literal/comment, percent-escaping and mixed-style rejection cases.

This qualifies the first review's DDP04 completion: its demonstrated failures
are repaired, but broader operator recognition still has this uncovered defect.

### DDP09 — Medium: INCLUDE columns change the reported uniqueness and link cardinality

[PostgreSQL.DatabaseDriver._get_unique_column_groups](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/databasedriver/__init__.py)
at lines 1104–1122 unnests all of `pg_index.indkey`, without limiting positions
to `indnkeyatts`. It consequently counts non-key `INCLUDE` columns as uniqueness
columns. The newer [physical schema reader](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/physical_schema.py)
already makes that distinction correctly.

Reproduced with this real index:

```sql
CREATE TABLE review_links (left_id INTEGER, right_id INTEGER);
CREATE UNIQUE INDEX review_covering_key
    ON review_links(left_id) INCLUDE (right_id);
```

| Reader or operation | Observed result |
| --- | --- |
| Physical index specification | Key `left_id`; included column `right_id`. |
| `_get_unique_column_groups` | Claims the key is `(left_id, right_id)`. |
| Driver-wrapper `_link_constraint_shape` | Reports `many_to_many`, rather than its `many_to_one` classification for a unique left endpoint. |
| Insert `(1, 2)`, then `(1, 3)` | PostgreSQL rejects the second row with `UniqueViolation`. |

This feeds public interlink/intralink specifications. It can misdescribe
cardinality or whether a relationship type participates in identity, even while
the physical index specification is correct. Ordinary indexes without `INCLUDE`
are unaffected by this specific defect; no claim is made that the generated
WEMI schema currently creates this index shape.

**Repair:** derive unique groups from the physical index reader, or at least
apply the same key-position filter. Add parity tests for included payload/type
columns, composite keys, and partial/expression/invalid indexes. This is a
concrete example of the remaining duplicate ownership problem.

### DDP10 — Medium: the obsolete scratch redirection surface is unusable and unsafe to revive piecemeal

[SQLBaseDriver.make_scratch](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/__init__.py)
at lines 113–116 remains exposed through
[Database.lock_writing](../../src/LiuXin_alpha/databases/database/__init__.py).
In the current environment both SQLite driver names raise `AttributeError`
because [get_scratch_folder](../../src/LiuXin_alpha/utils/ptempfiles.py) calls the
removed `CompatLogger.dump_to_file` at line 349. It creates a scratch directory
before raising. This unmodified call **does not reach copying or redirect writes**.

A second, explicitly conditional probe replaced **only** that temp-directory
provider with a private test directory, leaving the driver method unchanged:

- The source had one committed WAL row. The scratch copy contained zero rows.
- `database_path` changed, but `PRAGMA database_list` on the primary connection
  still named the original file.
- A subsequent `direct_execute` inserted into the original file. The scratch
  copy remained empty.

These are latent lifecycle/WAL defects behind the immediate helper failure,
not evidence that the currently failing unmodified call silently writes through.
The facade docstring already says this is not a write lock. Nevertheless, fixing
only the logger call would restore a misleading and internally split operation.
The repaired `direct_backup` is not used here.

**Repair:** prefer retiring `lock_writing`/`make_scratch` if their public
compatibility surface is no longer needed; no other in-tree database consumer
was found. Otherwise define an explicit clone operation using a verified SQLite
snapshot and a separate owner, with a deliberate handle-switching policy. Test
that committed WAL is copied and that all subsequent operations use the intended
file. Do not use this API as a read-only guarantee.

## Other observations and repair order

1. Fix DDP07 first: silent partial success is the highest data-integrity risk.
2. Finish DDP08 and extend the existing SQL-adaptation invariant group.
3. Consolidate index interpretation while fixing DDP09; avoid another independent
   catalog query that can diverge from the physical schema reader.
4. Decide whether to retire the obsolete scratch surface before repairing its
   temp helper. Retain the capability notes if removing it.
5. The PostgreSQL direct-script docstrings still describe naive semicolon
   splitting, although the repaired adapter preserves native scripts. Update
   those comments alongside DDP08. This is documentation drift, not another
   reproduced runtime failure.

The previously recorded duplicated SQLite setup, oversized implementation
modules and slow full-schema PostgreSQL inspection remain follow-up work.
Schema-spec callers still need the documented explicit refresh after DDL;
registry re-registration also explicitly retains its module caches. This pass
did not establish a new built-in-backend failure from those documented limits.

## Original review evidence and limits

The SQLite probes use real drivers, independent readers and private temporary
files. The PostgreSQL probes compare the adapter with raw psycopg2 on the same
server, and compare both index readers with actual uniqueness enforcement.
The scratch probe's single dependency replacement is stated above. No production
files, existing test expectations, or regression manifest entries were changed
during the original review; the subsequent repairs are recorded above.

Local reproduction scripts/logs are:

- `/tmp/liuxin-driver-second-review-probes.py` and `.log`.
- `/tmp/liuxin-driver-second-review-postgres.py` and `.log`.
- `/tmp/liuxin-driver-second-review-ratchet.log` and the corresponding result and
  telemetry directory.

These `/tmp` artifacts are session evidence, not permanent regression coverage.
The examples and expected test boundaries in each finding are the handoff for
new regressions. This was a risk-led code review, not an exhaustive audit of all
legacy SQL or a production-import signoff. Final verification is recorded below.

### Original review verification

- Existing mandatory regression gate: **350 passed, zero skips/xfails**, with
  baseline comparison against `07fc3326` (129.18 seconds). The four findings are
  additional coverage gaps, not failures in that run.
- Both SQLite driver names reproduced DDP07 and the immediate/conditional DDP10
  outcomes. Live PostgreSQL 16 reproduced DDP08 and DDP09, with raw-driver and
  native-index controls.
- The disposable PostgreSQL server was stopped and its cluster removed. Logs and
  the status receipt remain under `/tmp`.
- All **22 documentation navigation/boundary checks** passed; the staged diff
  has no whitespace errors.
- No full-repository or broad integration rerun was performed for this review;
  the earlier repair validation remains separately documented.

## Repair validation — 2026-11-05

- Final mandatory ratchet: **400 passed, zero skips/xfails**, in 145.42 seconds,
  including the strengthened adapter regression. All setup/call/teardown phases
  passed. Baseline comparison used `07fc3326`; the earlier 350 local cases also
  retain their IDs and invariant membership.
- Affected integration: **319 passing cases**, three fixture/backend skips and
  two existing expected failures in `DriverWrapper.get(all=False)`. Selection
  covered SQL execution, bulk operations, schema/link introspection, database
  execution, API parity/portable macros, wrapper/physical readers and tooling.
  The initial run also had two teardown errors because concurrent mypy created
  `.mypy_cache`, which the repository-artifact guard correctly removed. Both
  affected cases were rerun after typing finished: **2 passed**, including
  teardown. No code or test expectations were changed to hide these errors.
- Driver/adapter doctests: **121 examples passed**.
- Final documentation navigation/boundary checks: **22 passed**. The disposable
  PostgreSQL service was stopped and its owned cluster removed; its log and
  receipt are retained with the other local evidence.
- Complete quality runner passed: 308 formatted files; annotation coverage across
  463 files; import-cycle, layer-direction, cold-import budget and Ruff gates;
  basedpyright **zero diagnostics**; mypy **455 clean sources**. Both negative
  contract suites also passed, rejecting 95/93 deliberately invalid examples.
  The final test-only strengthening passed formatting/lint and the mandatory gate.
- Final ratchet and integration telemetry recorded no OOM-counter increase.
  Sampled test-process-tree RSS peaked at approximately **82 MiB** and **139 MiB**,
  respectively; these figures exclude the separate PostgreSQL service.

Evidence uses the `/tmp/liuxin-driver-followup-fixes-` prefix: `ratchet-final`
contains the authoritative JSON and telemetry, and `integration-selection.json`,
`integration.log`, `teardown-rerun.log`, `quality.log` and `doctests.log` record
the remaining checks. Counts overlap; no full-repository run or production import
is claimed. Run typing separately from pytest, or direct `MYPY_CACHE_DIR` outside
the checkout, to avoid concurrent cache creation tripping its hygiene guard.
