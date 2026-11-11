# Database driver plugins — seventh review

Reviewed **2026-11-10**, after recovering the interrupted sixth-review fixes.
The reviewed tree is `bab2b682cfa642fd3ecd431c1c395fbac0ecad00` plus the
19-file staged recovery patch (SHA-256
`e190dfc53e3c6f86447ca43b31f3328b31d38c768aa457abb5a6d68048cec47b`).
This includes the DDP23/DDP24 implementation and 959-case manifest. It is not
the published PR #156 head alone. Remote PR/CI state was not refreshed.

The persistent isolated checkout is
`/mnt/c/dev/LiuXin-alpha/.tmp/database-driver-review-2026-11-10`, on
`database-driver-plugins-review-2026-11-05`. It is an independent local clone
sharing the original Git object store. Its `origin` points to the original
local checkout; it is not a GitHub publication destination.

**All three findings have implemented repairs**, with 221 new mandatory
regressions. The final mandatory gate and quality checks pass; see below. The
finding descriptions retain the original pre-fix evidence and recommendations.

| Finding | Priority | Reproduced consequence |
| --- | --- | --- |
| DDP25 | High | SQLite export/rebuild renumbers hidden row IDs while preserving FTS shadow indexes, publishing a database whose full-text queries fail. |
| DDP26 | Medium | PostgreSQL continued escape strings lose their lexical context, allowing literal text to be rewritten and committed. |
| DDP27 | Medium | Four legal bare column names cause JSON `?` operators to be classified as parameter bindings. |

## DDP25: preserve hidden row IDs when copying FTS content tables

[`sqlite_dump._table_rows`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sqlite_dump.py),
lines 58–71, derives INSERT columns exclusively from `PRAGMA table_info`.
Hidden `rowid` is omitted for ordinary tables without an INTEGER PRIMARY KEY
alias, so replay assigns fresh IDs. The serializer independently copies the
FTS shadow tables, which still contain the original document IDs.

Minimal source:

```sql
CREATE TABLE documents(body TEXT);
INSERT INTO documents(rowid,body) VALUES(42,'alpha');
CREATE VIRTUAL TABLE search USING fts5(body,content=documents);
INSERT INTO search(search) VALUES('rebuild');
```

Before export/rebuild, `SELECT rowid,body FROM search WHERE search MATCH 'alpha'`
returns `(42, 'alpha')`. After replay, `documents` contains `(1, 'alpha')`,
`PRAGMA integrity_check` reports `ok`, and the same search raises
`database disk image is malformed`.

This reproduces through **both** public `sql_dump()` implementations and
**both** `dump_and_restore(callback=None)` implementations. Each rebuild
returns successfully and replaces the original file. Independent native readers
confirm the bad persisted state. `direct_backup()` preserves both the row IDs
and search results on both driver names, providing a verified control.

This is consistent with [SQLite's external-content FTS contract](https://www.sqlite.org/fts5.html#external_content_tables):
the default content key is the underlying table's `rowid`. No change to the
source data or its search index was requested by the rebuild.

Repair the shared serialization policy to preserve accessible hidden row IDs,
distinguishing true INTEGER PRIMARY KEY aliases and WITHOUT ROWID tables.
Handle shadowed `rowid`/`_rowid_`/`oid` names deliberately; refuse a rebuild
before replacement if a lossless mapping cannot be established. Add public
export/rebuild regressions with gaps in ordinary row IDs and external-content
FTS, comparing document IDs and MATCH results through independent readers.
Structural integrity checks alone do not detect this defect.

## DDP26: retain escape mode across PostgreSQL string continuations

[`sql_tokens`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 117–122, enables backslash handling only for a token that begins with
`E'`. PostgreSQL permits a newline-separated continuation whose later quote
does not repeat the `E` prefix. The lexer treats that continuation as an
ordinary string and closes it at an escaped quote. Literal backticks can then
reach identifier conversion.

```sql
SELECT E'prefix'
'\'`literal`\'';
```

With PostgreSQL 16's `standard_conforming_strings=on`, native execution returns
the characters `prefix'` followed by backtick-delimited `literal` and a final
apostrophe. Adapter execution silently replaces those backticks with double
quotes. Equivalent INSERTs were committed and read through a separate native
connection: the native and adapter values differ. A simpler continuation
containing `with\'quote` instead raises a false `Unterminated SQL quoted text`.

Standalone E-strings, ordinary newline string continuations and bound values
are passing controls. A trial with an intervening block comment was rejected
by the native server too and is not part of this finding.

The [PostgreSQL 16 lexical specification](https://www.postgresql.org/docs/16/sql-syntax-lexical.html#SQL-SYNTAX-STRINGS-ESCAPE)
explicitly retains escape semantics across these continuations. Consume the
whole continued literal with its original mode, sharing that token boundary
with adaptation and statement splitting. Regressions should cover escaped
quotes, backticks, question marks, percent signs and apparent semicolons, plus
committed values and installed-maintenance script handling. Bound values avoid
the demonstrated rewriting while this remains open.

## DDP27: distinguish remaining keyword-shaped identifiers from syntax

[`_classified_tokens`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 348–349, treats every word in `_EXPRESSION_PREFIXES` as introducing an
expression unless covered by an earlier contextual exception. PostgreSQL also
accepts bare columns named `between`, `escape`, `set` and `values`.

```sql
SELECT values ? %s
FROM (SELECT '{"key":1}'::jsonb AS values) t;
```

Binding `('key',)` with native psycopg2 returns `True`. The public adapter raises
`Cannot mix qmark and PostgreSQL pyformat bindings`. Replacing `%s` with portable
`?` makes it bind the JSON operator position and generates a server syntax error.
All four names reproduce both failures. Quoting the operand or qualifying it
as `t.values` preserves the expected result with either binding style.

A native-server sweep tested 371 accepted keyword identifiers in both binding
styles: 742 comparisons, 734 matches and these eight failures. Another 100
candidate identifiers were rejected natively and excluded. A public-adapter
confirmation reproduced all eight failures and passed all 16 quoted/qualified
controls. This extends the earlier contextual-keyword finding; the previous
`first`/`next`/`time` repairs do not cover these names.

Recognize these words according to their statement/expression context, without
breaking actual BETWEEN, SET, VALUES or ESCAPE syntax. Add the native comparison
matrix to permanent regressions. Native `%s` by itself is not a workaround;
quote or qualify the affected operand.

## Original review verification and limits

- Existing focused regressions: **724 passed, zero skips**, in 127.17 seconds.
  The selection covers value preservation, SQLite restore/resource/transaction
  safety, live PostgreSQL adaptation/resources, completion boundaries, adapter
  row contracts and driver registration.
- New probes exercise actual SQLite/APSW rebuild publication, native FTS,
  PostgreSQL 16 lexical behavior and independently read committed values.
- The keyword sweep and public-adapter controls above are separate probes,
  not added permanent tests or an enlargement of the required manifest.
- Documentation link/navigation and public-boundary checks: **22 passed** in
  116.39 seconds. Staged and unstaged whitespace checks pass.
- Focused-run telemetry recorded 70.6 MiB peak test-process-tree RSS, at least
  6.64 GiB available memory and zero OOM kills.
- The original checkout's three protected preference/source hashes remain
  unchanged. No source implementation or existing regression test was edited.
- The owned private PostgreSQL 16 cluster was stopped and removed. Existing
  server processes were not changed.

Persistent evidence directory:
`/mnt/c/dev/LiuXin-alpha/.tmp/driver-seventh-review-2026-11-10/`.
It contains `review-source.json`, `run-focused.py`, `focused.log`,
`focused-result.json`, `focused-telemetry/`, `resources-summary.json`,
`probe.py`, `probe.log`, `probe-results.json`, `lexer-sweep.py`,
`lexer-sweep-results.json`, `keyword-controls.py`, `keyword-controls.log`,
`keyword-controls-results.json`, `documentation.log`, `preservation-checks.json`,
`pg-server.log` and `pg-cleanup.json`.

The prior full 959-case gate and final quality run were interrupted by WSL;
their temporary evidence was lost. This focused review does not establish
completion of those runs, the full repository suite, current CI or a production
import trial. Resolve the three findings and rerun the required gate before
signoff. Broader connection-setup consolidation and measured schema-inspection
performance remain later structural work.

## Implemented repairs — 2026-11-10

- **DDP25:** shared SQLite serialization includes an accessible hidden row ID
  unless an INTEGER PRIMARY KEY alias already carries it. `table_xinfo`,
  `table_list` and primary-index metadata distinguish generated/shadowing
  columns, WITHOUT ROWID tables and the nonalias INTEGER PRIMARY KEY DESC form.
  Fully shadowed, inaccessible IDs raise before rebuild publication; export
  failure leaves the caller's transaction intact. Both SQLite names now have
  real FTS MATCH checks after export/rebuild and native-backup controls.
- **DDP26:** the shared PostgreSQL lexer consumes all newline-separated chunks
  with the initial string's escape mode, including SQL line comments and CR/LF.
  Adaptation, script splitting and maintenance completion use the same opaque
  token. Tests compare committed single/batch values with native execution,
  preserve SQL-looking literal text and prove later failures remain atomic.
  A bounded probe exposed excessive backtracking in the initial continuation
  pattern for trailing line comments. The final pattern permits one comment
  before the first newline; four required subprocess-bounded regressions cover
  EOF and continued comments after both ordinary and escape strings.
- **DDP27:** contextual keyword handling distinguishes the four bare JSON
  operands from infix/clause grammar. Tests cover all four names, four JSON
  operators, both binding styles and actual BETWEEN/ESCAPE/SET/VALUES syntax,
  including unary/infix NOT, SYMMETRIC/ASYMMETRIC and nested tuple bindings.

The [mandatory manifest](../../tests/databases/regression_ratchet.json) grows
from 959 to **1,180**, with no previous case removed or moved. Its 221 additions
include a [portable parity matrix](../../tests/databases/test_driver_value_parity.py)
covering all three driver names, text/binary values, single/batch writes and
commit/rollback observed by an independent reader. The
[ratchet guide](regression-ratchet-2026-11-05.md) records invariant membership.

Focused new regressions: **217 passed**, zero skips, in 28.17 seconds. The
initial red run reproduced the reviewed defects; separate test-fixture errors
(a reserved PG column name and JSON braces in template formatting) were fixed
before the green run. After tightening the comment scanner, **70** affected
cases passed, including its four new regressions.

Final validation:

- Mandatory gate: **1,180/1,180** required cases passed all three phases,
  `failures=[]`, exit 0; **1,211** selected tests passed, no skips, in 609.79 s.
  All 959 recovered cases retain their invariant membership, checked separately
  from the runner's comparison against the published base commit.
- Native PostgreSQL keyword sweep: **742 comparisons, zero mismatches**;
  100 names rejected by PostgreSQL were excluded.
- Full quality: format/lint, annotation/import/layer/complexity checks,
  basedpyright and mypy (458 source files), plus 95/93 invalid-call probes pass.
- **121** driver/adapter doctest examples and **58** documentation/runner checks
  pass. Affected integration: 177 test calls passed; its one schema-cleanup
  error passed an isolated retry (one passed, including cleanup, in 11.36 s).
- The initial 1,176-case gate and overlapping integration each exhausted the
  disposable PostgreSQL server's shared lock pool during DROP SCHEMA. The
  initial gate remains recorded as failed; the complete final gate above is
  the passing signoff. No production behavior, assertions or gate rules were
  weakened to address that test-server contention.
- Final-gate telemetry: 90.5 MiB peak test-process-tree RSS, at least 5.60 GiB
  available memory and zero OS OOM kills. PostgreSQL's shared-lock allocation
  error above is separate from OS memory exhaustion.
- The original checkout's three protected preference/source files, surviving
  worktree index, recovery patch and all 19 recovered files remain unchanged.

This complete local gate supersedes the interrupted 959-case run. The full
repository suite and remote CI were not rerun; no publication is claimed.

Repair evidence is retained in
`/mnt/c/dev/LiuXin-alpha/.tmp/driver-seventh-fixes-2026-11-10/`.
See the [repair handoff](../../working-memory/database-driver-plugins-seventh-fixes-2026-11-10.md)
for validation details. The private PostgreSQL cluster is stopped and removed.
Changes are staged, uncommitted; no push or merge was requested.
