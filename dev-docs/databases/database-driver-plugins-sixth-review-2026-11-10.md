# Database driver plugins — sixth review

Reviewed **2026-11-10** at commit
`bab2b682cfa642fd3ecd431c1c395fbac0ecad00` on
[PR #156](https://github.com/ajCameron/LiuXin-alpha/pull/156), following the
[fifth review and mitigations](database-driver-plugins-fifth-review-2026-11-06.md).
The PR remains open against `database-review`. All applicable checks on this
commit have passed, including the full suite, confidence tests, database
regression ratchet, architecture/typing and installed-wheel checks.

**The review reproduced two defects: a lossy SQLite rebuild/export and incomplete
PostgreSQL lexical recognition.** Both now have implemented repairs and new
regressions, recorded in [Implementation and validation](#implementation-and-validation).
The original evidence below describes `bab2b682` before those repairs.

| Finding | Priority | Observed consequence |
| --- | --- | --- |
| DDP23 | High | SQLite rebuild silently truncates TEXT at an embedded NUL and publishes the shortened value. Both SQLite driver names also produce lossy SQL exports. |
| DDP24 | Medium | Valid PostgreSQL Unicode identifiers confuse JSON parameter adaptation; Unicode dollar-quote tags expose literal contents to rewriting. |

## DDP23 — High: SQL serialization can lose stored text before validation

[SQLite.sql_dump](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite/databasedriver/__init__.py),
line 811, and
[SQLite_apsw.sql_dump](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite_apsw/databasedriver/__init__.py),
line 1049, both delegate to the stdlib connection's `iterdump()`.
The SQLite rebuild also uses `dump_conn.iterdump()` at lines 902–905, then
validates and publishes the reconstructed database at lines 929–937.

Reproduction against both configured driver names:

1. Create `payloads(id INTEGER PRIMARY KEY, text_value TEXT, blob_value BLOB)`.
2. Bind and commit `"before\x00after"` as TEXT and its encoded bytes as BLOB.
3. Export with `sql_dump()` and replay it into a separate database.
4. Independently test `direct_backup()` and `dump_and_restore()`.
5. Read the stored values through native sqlite3, outside driver conversions.

| Path | SQLite | SQLite_apsw |
| --- | --- | --- |
| Original committed TEXT | `"before\x00after"` | `"before\x00after"` |
| Replayed `sql_dump()` TEXT | `"before"` | `"before"` |
| Verified file backup TEXT | `"before\x00after"` | `"before\x00after"` |
| Reopened `dump_and_restore()` TEXT | `"before"` | `"before\x00after"` |

The BLOB remains `6265666F7265006166746572` in every case. Both rebuilt files
report `integrity_check = ok`. The stdlib rebuild returns success and replaces
the source containing the full text with one containing only its prefix.
The APSW shell rebuild preserves this value; do not infer an APSW rebuild
failure from the common SQL-export failure.

The failure occurs during value serialization, before the new WAL/publication
guards. Integrity and foreign-key checks establish structural consistency,
not equality with the original stored values. More publication checks alone
will not repair a candidate whose contents were already shortened.

**Proposed repair:** provide one lossless value-serialization policy for the
public SQL exports and stdlib rebuild, preserving SQLite storage classes.
If a value or schema cannot yet be serialized safely, refuse before replacing
the original. A byte-preserving TEXT expression is possible; it must remain
TEXT after replay, rather than silently becoming a BLOB. Do not remove the NUL
or normalize the value as a persistence-layer repair.

Add permanent round-trip cases for NUL at the beginning, middle and end,
multiple NULs, ordinary Unicode/quotes/newlines, TEXT versus BLOB and empty
versus SQL NULL. Check independent-reader values after publication and preserve
the original on refusal. Keep the existing foreign-key, callback, pending-write
and WAL protections. Verified file backup preserved the sample and remains the
appropriate independent-copy workflow for this case.

## DDP24 — Medium: PostgreSQL lexer uses narrower identifier/tag rules

[sql_tokens.py](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/sql_tokens.py),
lines 20–21, recognizes words and dollar tags with Python Unicode `\w`.
Its whitespace branch at lines 91–94 uses `str.isspace()`. Real PostgreSQL 16
accepts the following names/tags that those choices split or discard:

- `e\u0301`: Latin `e` followed by a combining acute accent.
- `📚`: a book emoji.
- `\u00a0`: a non-breaking space, used as an identifier or within a dollar tag.

### JSON operators after identifiers

For each name, native psycopg2 successfully executes:

```sql
SELECT 📚 ? %s FROM (SELECT '{"key":1}'::jsonb AS 📚) t
```

with `("key",)` and returns `(True,)`. The configured adapter rejects the
same SQL with `ValueError: Cannot mix qmark and PostgreSQL pyformat bindings`.
Replacing the operand's `%s` with `?` causes it to bind the value at the
operator position and leave the actual parameter untranslated, yielding a
server syntax error. The combining-accent and non-breaking-space identifiers
produce the same failures. Ordinary `plain` and `名` controls pass with both
binding styles.

### Dollar-quoted text is silently changed

Native PostgreSQL returns ``"before `quoted` ? after"`` for:

```sql
SELECT $📚$before `quoted` ? after$📚$
```

The adapter successfully returns `'before "quoted" ? after'` instead. The
unrecognized dollar tag leaves the body exposed to backtick-identifier
translation. Tags containing `e\u0301` and `\u00a0` reproduce this change;
ASCII and `名` tags preserve the value. No SQL parameters are needed for
the silent literal change. This observation concerns the returned expression;
the probe did not test a persisted PostgreSQL write with that literal.

**Proposed repair:** align word starts/continuations, dollar-tag recognition
and whitespace with PostgreSQL's lexical rules, retaining each recognized
quoted body as one opaque token. Apply the fix in the existing shared scanner,
which also serves statement splitting and maintenance boundaries. Regressions
should compare actual server results for both binding styles, quoted and
unquoted names, Unicode tags and literal contents containing backticks,
semicolons, percent signs and question marks. Add a persisted-value comparison
as well as SELECT comparisons; do not claim it is covered by the current probe.

The demonstrated workarounds are quoted identifiers for affected column names,
ASCII or empty dollar tags, and bound values instead of SQL literal bodies.
Native `%s` alone does not avoid the identifier failure, because the adapter
classifies the complete SQL before native execution.

## Scope, verification and next work

The pass revisited connection/cursor ownership, transaction completion,
backup/rebuild publication, physical-schema readers and SQL adaptation.
Detailed new probes focused on value preservation and PostgreSQL lexical
boundaries. It was not an exhaustive audit of every legacy SQL helper, a new
local full-suite run, or a production import trial.

The existing CI results are green at the exact reviewed commit. New probes
used configured SQLite/SQLite_apsw drivers and an isolated PostgreSQL 16 server,
with native readers and native-execution controls. All runtime preferences,
databases and scratch files stayed under temporary Linux storage.

The focused existing restore, WAL-publication, dump-ownership, backup and
qualified-operator selection passed **82 tests**, with 476 deselected and no
skips, in 24.40 seconds. These existing passing cases do not cover the new
counterexamples. Quoted identifiers, empty/ASCII dollar tags and ordinary
bound-value controls also passed the native comparison probes. The private
PostgreSQL server was stopped and its owned cluster removed.
All **22 documentation link/boundary checks passed** in 19.52 seconds; the
staged documentation also passes the whitespace check.

Fix DDP23 first because it can replace the only original database with a lossy
reconstruction. Then repair DDP24 with server comparisons, rather than another
list of special identifier spellings. Add both to the mandatory regressions.

After those repairs, the structural follow-ups remain useful: share ordinary
stdlib SQLite connection setup between the two driver names; separate the
native APSW shell capability explicitly; measure PostgreSQL inspection costs;
and split large driver modules by responsibility while preserving their
transaction contracts. None of those refactors substitutes for these two
value-preservation fixes.

## Implementation and validation

Implemented **2026-11-10**, following authorization to fix both findings.

**DDP23:** the shared
[SQLite serializer](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/sqlite_dump.py)
now supplies both public SQL exports and both rebuilds. It reads actual storage
values without declared-type conversion, decodes TEXT using the source encoding
and emits text expressions that also work in a different destination encoding.
NUL-containing values use one replacement expression, including when a value
contains thousands of NULs. Original text that resembles its temporary marker
is preserved. BLOBs remain BLOBs; invalid encoded TEXT raises before publication.

Rows stream and the borrowed connection's factories and transaction remain
unchanged. Standard `str`/`bytes` text factories are supported; custom
transforming factories are refused before export rather than allowing them to
change the serialized schema or values. Generated columns, indexes, views,
triggers, numeric values and
AUTOINCREMENT high-water marks survive the tested round trips. Virtual tables
use their shadow-table data and reload their schema after restore; FTS5 row
contents and actual MATCH results are checked. The APSW backend still restores
through its native shell, using the same serialized SQL as the stdlib backend.
All existing pending-write, foreign-key, integrity, failed-close and WAL guards
remain in place.

The first expanded run found the old APSW formatter exceeded SQLite's
expression-depth limit on 10,000 NULs. Sharing export serialization with its
rebuild removed that additional limitation; the complete SQLite selection
then passed.

**DDP24:** PostgreSQL identifier starts/continuations, dollar tags and whitespace
now follow the corresponding rules in
[PostgreSQL 16's scanner](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/parser/scan.l).
Non-ASCII characters stay within identifiers/tags rather than being split by
Python's Unicode character classes. Dollar bodies remain opaque to adaptation,
script splitting and maintenance boundaries. Existing portable/native
parameter-style rules remain unchanged. Keyword recognition folds ASCII only,
so identifiers such as `ſelect` and `ıN` cannot become SELECT/IN through Python
Unicode uppercasing. Four additional native comparisons reproduced that
classification error before the correction.

New live comparisons cover quoted and unquoted names, combining characters,
emoji, CJK, Unicode spaces and BOMs. Unlike the original review probe, permanent
regressions also commit literal values and compare them through an independent
native reader. Installed-maintenance scripts retain quoted COMMIT/semicolon
text as data and roll back all source/effect rows after a later failure.

The mandatory manifest adds **151 cases**, from 808 to **959**, preserving every
previous node ID and invariant assignment:

- 60 SQLite value-preservation cases.
- 80 PostgreSQL identifier/literal cases.
- 11 PostgreSQL maintenance and script-boundary cases.

Focused validation passed **76 SQLite tests**, **403 adapter/completion tests**
and **121 doctest examples**. The broader dump/restore, pure-SQLite and
maintenance/upgrade selection passed **116 tests**, with 10 backend-specific
skips. All **58 documentation/runner checks** passed. These selections overlap
and are not a unique-test total.

Earlier complete gates passed 943 and then 945 required cases. The completed
959-case set also includes the Unicode keyword comparisons; its final results
are recorded below after validation.
