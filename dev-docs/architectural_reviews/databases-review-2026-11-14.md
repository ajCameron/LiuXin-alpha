# Databases package review — 2026-11-14

Reviewed checkpoint: the working tree on `main`, with `HEAD` at
**`7780b7eb62020d8b1cf6891b3bb95a47d9f7fcfb`**. Uncommitted changes were not
inspected, because no shell was available on the review machine. The top-level
modules in `src/LiuXin_alpha/databases/` were reviewed on 2026-11-13. The
subpackages were reviewed on 2026-11-14, and a source check confirmed the files
had not changed between the two passes. No production code, real catalogue or
test was changed.

**The package's ownership, transaction and publication contracts are in
substantially better shape than the October baseline. Recent review rounds on
the drivers, facade, maintenance, custom columns and generator show in the code.
The remaining risks are mostly not inside one subsystem. They sit where
subsystems meet: persisted declarations against code defaults, link deletion
against schema cascades, typed reads against view metadata, transaction wrappers
against SQLite pragmas, and in-process locks against multi-process access.** Four
findings can silently merge or delete catalogue data. Fix those before running
the affected operations on a library that matters.

## Scope and evidence

The package contains about 76,000 lines of Python. Coverage was not uniform:

| Depth | Areas |
| --- | --- |
| Read in full | All 20 top-level modules; SQLite driver read/write paths, value casting and dump/restore; portable macros for ensure/find, link replacement, link batching and orphan pruning; normalized identities. |
| Read selectively | Maintenance, custom columns, PostgreSQL driver and privileges, database facade, driver wrapper, metadata SQL mixins, FRBR generator. Each already had several dedicated review rounds in `working-memory/`. |
| Skimmed | `api/` and `db_types/`, which are mostly protocols and documentation. |

Evidence comes in three kinds, and each finding names its kind:

- **Executed:** a disposable reproduction, run in a separate sandbox against the
  package's own code (lifted functions or stubbed collaborators) and real SQLite
  3.x with Python 3.13.
- **Schema-backed:** facts read from the July 2026 benchmark database,
  `LiuXin_data/benchmarks/provisioned/liuxin-benchmark-cxn0imsp/benchmark_db_smoke/benchmark_db_smoke.test_db`.
  The current schema `.sql` files could not be read for this review, so check
  these facts against them before relying on them.
- **Source-traced:** a code path followed by reading, without execution.

The repository test suite was **not** run, and no live PostgreSQL server was
used. Evidence scripts are kept locally in the ignored
`.tmp/databases-top-level-review-2026-11-13/` and
`.tmp/databases-full-review-2026-11-14/` directories. The results that matter are
reproduced below.

## Findings and priorities

P1 means resolve before running the affected operation on a library that
matters. P2 means a reproducible correctness or operational gap to close in a
focused change. P3 means contained hardening or consistency debt.

| ID | Priority | Finding | Evidence |
| --- | --- | --- | --- |
| DBR01 | P1 for existing libraries | Legacy normalized-identity profiles silently merge distinct series, genres, subjects, tags and labels, and there is no supported upgrade path. | Schema-backed declarations; executed lookup against a copy of the benchmark DB. |
| DBR02 | P1 | `delete_unreferenced_rows` deletes rows still referenced by links it was not given, and the schema then cascades those deletions. | Executed against a copy of the benchmark DB, using the API's own example. |
| DBR03 | P1 for scripts run through `executescript` | With maintenance installed, `executescript` runs inside a transaction. `PRAGMA foreign_keys` is then ignored, and SQLite's documented table-rebuild recipe cascade-deletes child rows. | Executed with `SQLiteCompletionConnection`. |
| DBR04 | P1 for archival or tool-interoperability claims | Search indexes depend on LiuXin's custom SQL functions. Other tools cannot insert, update, VACUUM or integrity-check those tables, and a change in the functions' output corrupts the indexes undetected. | Executed with `install_sqlite_search_indexes`. |
| DBR05 | P2 | The SQLite typed-read path turns text in untyped view columns into numbers: "1984" becomes `1984` and "007" becomes `7`. | Executed with the lifted casting code; 170 untyped view columns in the benchmark DB. |
| DBR06 | P2 | The usual macro write pattern (start a deferred transaction, read, then write) fails instantly under a second writer, and nothing retries. | Executed with two SQLite connections, in DELETE and WAL journal modes. |
| DBR07 | P2 | `preferences.preference_key` is not unique. Concurrent `DBPrefs` writers create duplicate rows, and later changes appear to revert. | Schema-backed; executed with deterministic interleaving. |
| DBR08 | P2 | `DBPrefs.write_serialized` truncates the previous backup before serializing. | Executed. |
| DBR09 | P2 | Reading a column on a draft `Row` makes `sync()` write NULL over the column DEFAULT. | Executed. |
| DBR10 | P2 | Neither backend checks how many rows an update affected, so `Row.sync()` on a missing ID reports success. | Executed (SQLite shape); source-traced (PostgreSQL). |
| DBR11 | P3 | Identity-column metadata disagrees with the identity registry, and the registry is maintained in two places. | Executed. |
| DBR12 | P3 | SQLite adapters and converters are registered for the whole process, and the PYSET codec cannot read back some of the values it writes. | Executed (codec); source-traced (registration). |
| DBR13 | P3 | Durability and backup gaps in SQLite file publication and in `dump_and_restore`. | Source-traced. |
| DBR14 | P3 | Smaller contract gaps listed below. | Mixed; marked individually. |

### DBR01. Give existing libraries a path off the lossy legacy identity profiles

[`NORMALIZED_IDENTITY_DEFAULTS`](../../src/LiuXin_alpha/databases/normalized_identities.py)
(line 122) now uses `unicode_nfkc_whitespace_casefold` for the nine identity
bindings. The benchmark database's `normalized_identities` table, however,
persists the historical profiles. Series, genres and subjects use
`title_search_term`, which truncates at the first hyphen and drops stop words and
punctuation. Tags and labels use `tag_search_term`, which removes all
whitespace. [`metadata/text_identity.py`](../../src/LiuXin_alpha/metadata/text_identity.py)
documents both as lossy.

Three behaviours keep existing libraries on those profiles:

- [`_existing_normalized_identity_specs`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/portable_macros_mixin.py)
  (line 4408) lets persisted declarations override the code defaults.
- [`seed_normalized_identities`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/database_generator_frbr/database_generator.py)
  (line 659) inserts with `INSERT OR IGNORE`.
- [`_validate_normalized_identity_metadata`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/value_casting_mixin.py)
  (line 470) rejects any profile change for a declared identity.

`_column_metadata` (line 3092) then applies the persisted profile to both lookup
and insertion. The lookup in `_ensure_table_value` (line 3334) therefore returns
an existing row whose key merely collides.

Executed against a copy of the benchmark database:

```text
persisted profiles: {'series': 'title_search_term', 'genres': 'title_search_term', 'tags': 'tag_search_term'}
  ensure series 'Star Trek - Deep Space Nine'        -> reuses existing 'Star Trek - The Next Generation'
  ensure series 'Spider-Woman'                       -> reuses existing 'Spider-Man'
  ensure series "Warhammer 40,000 - Gaunt's Ghosts"  -> reuses existing 'Warhammer 40,000 - Horus Heresy'
  genre keys 'Science-Fiction' / 'Science-Fantasy': 'science' / 'science'
  tag keys   'The Rapist' / 'Therapist': 'therapist' / 'therapist'
  current code default for the same pairs: False
```

The merge is silent: the caller receives a valid ID for the wrong value. Every
library created before the default changed is affected, until its declarations
are migrated.

**Next:** add an explicit profile migration that runs as one transaction. It
should:
- compute new keys under the target profile;
- report collisions (rows that would merge, or that already wrongly share a key)
  before writing anything;
- update the declarations and the derived columns together.

Then decide whether opening a library with legacy profiles should warn, refuse
writes or offer the migration. Add a check (for example in `audit_query_indexes`
or the upgrade path) that reports legacy profiles still in use.

### DBR02. Make orphan pruning derive references from the schema

[`delete_unreferenced_rows`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/portable_macros_mixin.py)
(line 5366) and `_unreferenced_ids` (line 5243) treat a row as unreferenced if no
row in the *supplied* link specs points to it. The docstring says this does not
prove global unreferenced status. The schema, however, makes the gap
destructive. In the benchmark database, every foreign key to `tags` (3 link
tables), `agents` (15 tables) and `works` (15 tables) is `ON DELETE CASCADE`. The SQL link
helpers in [`utility_mixins.py`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/utility_mixins.py)
(lines 507 and 514) generate the same cascade. The method's own example (line
5385) passes only the work-to-tag spec for `tags`.

Executed on a copy of the benchmark database, using the SQL these helpers
generate:

```text
tag 6 is used by item 1 - item_tag_links rows: 1
candidates judged 'unreferenced': [6]
after prune: tag exists = False | item_tag_links rows = 0
```

**Next:** compute the protecting references from the physical foreign keys. The
repair layer already introspects them. Failing that, refuse to run unless the
supplied specs cover every foreign key that references the target table. Fix the
docstring example either way. Treat pruning of `agents` and `works` with extra
care, because their cascades reach human and organization agent records and
15 referencing tables.

### DBR03. Keep `executescript` from silently changing SQLite pragmas

[`SQLiteCompletionCursor.executescript`](../../src/LiuXin_alpha/databases/connection_hooks.py)
(line 100) checks for a `maintenance_metadata` table. If it exists, the method
splits the script, rejects transaction-control keywords (line 126), and runs the
statements inside `transaction_scope` (line 134). SQLite ignores
`PRAGMA foreign_keys` inside a transaction and refuses `VACUUM`. The SQLite
driver turns foreign keys on at connect. The documented rebuild recipe
(`foreign_keys=OFF`, copy, `DROP TABLE`, `RENAME`, `foreign_keys=ON`) therefore
runs with enforcement still on, and `DROP TABLE` cascades.

Executed with the real connection class, with and without the maintenance
condition:

```text
== standard SQLite table-rebuild migration via executescript
  maintenance installed=False: items rows after migration = 2
  maintenance installed=True : items rows after migration = 0
== a script that enables foreign keys, then inserts an orphan
  installed=False: script FAILED -> FOREIGN KEY constraint failed
  installed=True : script ran; foreign_keys = 0 ; orphan rows = 1
      VACUUM: FAILS -> cannot VACUUM from within a transaction
```

**Next:** in the maintained branch, reject `PRAGMA`, `VACUUM`, `ATTACH` and
`DETACH` alongside the transaction keywords, with a clear error. Alternatively,
run `PRAGMA foreign_keys` outside the unit before it begins. Add the rebuild
recipe as a regression test.

### DBR04. Decide whether search indexes may depend on custom SQL functions

[`SQLITE_SEARCH_INDEXES`](../../src/LiuXin_alpha/databases/query_indexes.py)
(line 71) creates expression indexes on `liuxin_text_key_v1(...)` and
`liuxin_identifier_key_v1(...)`. [`search_functions.py`](../../src/LiuXin_alpha/databases/search_functions.py)
registers those functions only on LiuXin's own connections. Any other
connection can read the tables, but SQLite must evaluate the functions to
maintain or check the indexes. The functions use NFKC normalization and
casefolding, which depend on the Python interpreter's Unicode data. The module
docstring says a new function version is required when that data changes.
Nothing records the Unicode data version, and nothing compares it when a
library opens.

Executed:

```text
== a connection without LiuXin's functions
  SELECT: [('Dune',)]
  INSERT: FAILS -> unknown function: liuxin_text_key_v1()
  UPDATE: FAILS -> unknown function: liuxin_text_key_v1()
  integrity_check: FAILS -> unknown function: liuxin_identifier_key_v1()
  VACUUM: FAILS -> no such function: liuxin_text_key_v1
== the key function's output changes under the stored index
  indexed lookup  : []
  full-scan lookup: [(1,)]
  integrity_check : [('row 1 missing from index idx_works_title_key_v1',)]
```

**Next:** prefer plain key columns that the writer maintains, which is how
`tag_phash` and `series_name_norm` already work. Ordinary indexes on those
columns work in every tool and survive interpreter upgrades. Old keys then stay
consistent until a deliberate rekey. If expression indexes stay, record
`unicodedata.unidata_version` and the function version in the database. On a
mismatch, refuse writes or rebuild, and document that standard tools cannot
modify these tables.

### DBR05. Stop the SQLite read path converting text in untyped columns

[`_sqlite_affinity`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/value_casting_mixin.py)
(line 1657) returns `NUMERIC` for an empty declared type (line 1690). SQLite's
own rule gives such columns BLOB affinity, which means no conversion.
`_coerce_db_value` (line 1693) then turns numeric-looking text into `int` or
`float`, and `_row_to_dict` (line 1932) applies this to every row read through
the table-aware paths. Headings missing from the declared-type map take the same
route.

In the benchmark database, **170 view columns and no base-table columns** report
an empty type. They include `titles_v.title`, `titles_v.title_sort`,
`books.book_sort`, `wemi_rays_v.display_work_title`, `search_seed.seed_title` and
`duplicate_candidates_v.book_ids_csv`. Executed with the lifted casting code and a view
of the same `COALESCE` shape as `titles_v.title`:

```text
declared types SQLite reports for the view: {'id': 'INTEGER', 'title': ''}
  stored '1984'   -> returned 1984
  stored '007'    -> returned 7
  stored '1e3'    -> returned 1000.0
```

Three paths return the text unchanged: PostgreSQL's `_row_to_dict_from_db_row`,
which preserves native values; `direct_search_table` inside a macro transaction
(line 666), which returns raw rows; and the top-level `view_reads`. The same read
therefore yields different types depending on backend and transaction state. A
read-modify-write would store "7" back in place of "007".

**Next:** treat an empty or unknown declared type as "no conversion". Add
SQLite/PostgreSQL parity tests that read views as well as tables.

### DBR06. Make write units survive a second writer

[`_macro_transaction`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/portable_macros_mixin.py)
(line 455) starts a deferred `BEGIN` unless the unit is serialized (line 552).
Most ensure, upsert and replace units read first and then write. When a second
connection holds a read lock, SQLite refuses the second writer's lock upgrade
immediately to avoid a deadlock. Python's 5-second busy timeout is therefore
never applied. The macro lock serializes only units within one `Database`
handle. Background jobs on the `process` backend open their own handles.
Neither the package nor its callers retry. The package sets no `journal_mode`
and no `busy_timeout`.

Executed with two connections, each running the deferred read-then-write
pattern:

```text
journal_mode=delete: second writer -> database is locked after 0.00s (busy timeout not applied)
journal_mode=wal   : second writer -> database is locked after 0.00s (busy timeout not applied)
```

[`native_service.py`](../../src/LiuXin_alpha/databases/custom_columns/native_service.py)
already documents this and asks for an outer retry. Ordinary metadata writes
have no equivalent.

**Next:** use `BEGIN IMMEDIATE` for SQLite units that will write. This turns the
failure into a bounded wait. Add a bounded retry around the outermost unit for
`SQLITE_BUSY`. Choose and record a journal mode (WAL suits a UI plus background
processes). Then add a two-process test.

### DBR07. Make preference keys unique and writes idempotent

The benchmark schema declares `preferences(preference_id INTEGER PRIMARY KEY,
preference_key TEXT, preference_value TEXT)` and has no index on
`preference_key`. Calibre's equivalent table has `UNIQUE(key)`.
[`DBPrefs.__setitem__`](../../src/LiuXin_alpha/databases/dbprefs.py) (line 213)
searches for the key and then inserts or updates through the driver wrapper. The
wrapper opens a fresh connection for each write. The only guard is the facade's
in-process lock. After duplicate rows exist, `__setitem__` updates the first
row, while `load_from_db` (line 76) keeps the last decoded value.

Executed with two `DBPrefs` instances standing in for two processes. A barrier
made both look up the key before either inserted:

```text
preferences indexes: []
rows for page_size: [(1, '50'), (2, '75')]
UI sets 100; value after next load: 75
```

Which value wins depends on interleaving. The user's newest value is the one
lost.

**Next:** add a unique index on `preference_key`, after merging any existing
duplicates and keeping the newest row. Write with an upsert. Confirm this
against the current `.sql`, which this review could not read.

### DBR08. Write the preferences backup atomically

[`DBPrefs.write_serialized`](../../src/LiuXin_alpha/databases/dbprefs.py)
(line 326) opens the backup with mode `"w"` at line 346. Only after that does it
call `json.dumps` (line 347). If serialization fails, the previous backup is left
at zero bytes. The exception is logged and suppressed.

```text
backup after good write: 68 bytes
[log_exception WARN] Preferences did not write out.: TypeError("PosixPath('/tmp/x') is not JSON serializable")
backup after failed write: 0 bytes
read_serialized now fails: JSONDecodeError Expecting value: line 1 column 1 (char 0)
```

An unserializable value can reach the cache by three routes: in-place mutation
of a cached container, inherited `dict.update`, and assignment with
`disable_setting=True`. Each of these bypasses persistence by design.

**Next:** serialize first, write a temporary file in the same directory, fsync
it, then `os.replace` it into place.

### DBR09. Reads must not change what a draft `Row` writes

[`Row.__getitem__`](../../src/LiuXin_alpha/databases/row.py) (line 777) stores
`None` for an allowed column missing from the payload (line 799).
`Row.sync()` (line 929) inserts `dict(payload)` for drafts (line 958). Merely
reading a column therefore turns into an explicit NULL, which replaces the
column DEFAULT.

```text
peeked=False  stored work_language = 'und'
peeked=True   stored work_language = None
```

**Next:** do not insert the key when reading. Alternatively, track which keys
were added by reads and leave them out of the draft insert. Add a test that
reads a defaulted column before syncing a draft.

### DBR10. Treat zero-row updates as failures where callers expect a row

[`direct_update_row_dict`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/update_mixin.py)
(line 92) for SQLite, and the PostgreSQL
[`direct_update_row_dict`](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/databasedriver/__init__.py)
(line 2120), both return success without checking the row count.
`Row.sync()` (line 965) discards the result. A `Row` built with an explicit ID,
for example an import that preserves IDs, or a row deleted concurrently,
therefore syncs without any error:

```text
state: identified
sync() returned normally; rows with id 4242: 0
```

**Next:** return the affected-row count, and have `Row.sync()` raise when it is
not 1. Provide an explicit "insert with this ID" path for imports that preserve
IDs.

### DBR11. Derive identity-column metadata from the identity registry

[`_machine_role`](../../src/LiuXin_alpha/databases/column_metadata.py)
(line 704) gives the hash role to any column ending `_phash` (line 802). As a
result, `tag_phash`, `genre_phash` and `subject_phash` get the hash role and
validation profile. In fact they hold normalized text, which the
`normalized_identities` docstring states explicitly. `series_name_norm` and
`label_text_norm` get `normalized_key`. Separately, `COMPARISON_COLUMNS` (line
499) and `NORMALIZED_IDENTITY_DEFAULTS` (normalized_identities line 122) are two
hand-maintained copies of the same nine bindings. They are identical today, and
nothing checks that they stay that way.

**Next:** classify declared identity columns before applying the name
heuristics, and derive one registry from the other.

### DBR12. Scope SQLite adapters to LiuXin connections and fix the PYSET codec

The SQLite driver's
[`_open_connection`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite/databasedriver/__init__.py)
(line 521) calls `sqlite3.register_adapter` for `set`, `list` and `dict`. It also
calls `sqlite3.register_converter` for `PYSET`, `PYLIST`, `PYDICT` and `DATE`
(lines 530–543), and repeats this every time it opens a connection. These registries
belong to the whole process. Binding a list by accident is serialized silently
instead of raising, and other code in the process that parses `DATE` columns gets
LiuXin's converter.

The [`py_set_adapter`/`py_set_converter`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/utils.py)
pair (lines 150 and 81) also fails to round-trip some values:

```text
{"O'Brien"}  -> stored "'O''Brien'" -> read ERROR parsing a string into a python set has gone wrong
{''}         -> stored "''"         -> read set()
```

No `PYSET`-declared column exists in the benchmark schema, so the codec failure
is latent today.

**Next:** register adapters once at import, or better, convert values explicitly
at the binding sites. Replace the PYSET text format with JSON before any column
starts using it.

### DBR13. Close the SQLite durability and backup gaps

- [`creation.py`](../../src/LiuXin_alpha/databases/creation.py) fsyncs the
  candidate database file. It does not fsync the parent directory after
  `os.link` (line 87) or `os.replace` (line 158). A crash can therefore lose
  the newly published directory entry.
- [`dump_and_restore`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQLite/databasedriver/__init__.py)
  (line 815) validates the rebuilt file, then replaces the original without
  keeping it. `creation.replace_sqlite_file` keeps a backup. For an archival
  library, one rule for both is easier to reason about.

**Next:** fsync the directory on POSIX, and retain or require a backup path in
`dump_and_restore`.

### DBR14. Smaller contract gaps

| Gap | Evidence | Next |
| --- | --- | --- |
| [`transaction_scope`](../../src/LiuXin_alpha/databases/transactions.py) (line 91) does not check that the connection is idle before an owned `BEGIN`. On PostgreSQL, `BEGIN` inside a transaction only raises a warning, so the scope's commit publishes the caller's earlier work. Current callers pass fresh connections. | Source-traced | Raise when `owns_transaction` is set and a transaction is already active. |
| On PostgreSQL, [`_index_matches`](../../src/LiuXin_alpha/databases/query_indexes.py) (line 139) ignores direction, collation and operator class. The SQLite path checks the first two. | Source-traced | Compare `indoption`, `indcollation` and `indclass`. |
| [`bootstrap_storage_manager`](../../src/LiuXin_alpha/databases/runtime.py) reports "1 discovered / 1 failed" for any non-strict failure (line 153). It also changes an injected manager's `startup_on_add` before loading (line 133). | Source-traced | Report what is actually known, and restore the setting on failure. |
| [`Row.from_idless_row_dict(table=...)`](../../src/LiuXin_alpha/databases/row.py) (line 278) never passes the table hint to `add_row`. The driver inserts into the first matching table or view, while the wrapper demands a unique match, and the reload uses the hinted table. The method also opens a fresh connection on every call to probe `sqlite_master` (line 240). | Source-traced | Pass the hint through, or remove the parameter; reuse the existing connection. |
| `ensure_table_value` and `find_table_value` fall back to a full Python scan (`_find_ensured_id`, line 3218) for 74 columns that have a normalization profile but no comparison column, including `agents.agent_canonical_name`. | Executed (column count) | Add comparison columns where these lookups are used at scale. |
| The PostgreSQL [runtime grants](../../src/LiuXin_alpha/databases/database_driver_plugins/PostgreSQL/runtime_privileges.py) (line 158) do not include `TEMPORARY`. They rely on PUBLIC's default, but link batching creates temporary tables. | Source-traced | Grant `TEMPORARY` on the database to the runtime role explicitly. |
| [`persist_dirtied_records`](../../src/LiuXin_alpha/databases/database/dirtied_mixin.py) (line 537) returns 0 both for "nothing queued" and for "insert failed and was requeued". Nothing in the package calls it automatically. This complements CR07 in the [October 27 review](codebase-review-2026-10-27.md). | Source-traced | Return or raise distinct outcomes, as part of CR07. |
| `DBPrefs` values of type `bytes` reload as `str`. `__delitem__` (line 191) ignores `disable_setting` and the lock, and drops the cache entry before the database delete. | Executed | Use a tagged bytes encoding, and give deletion the same rules as assignment. |
| `replace_sqlite_file` and the other snapshot paths run `integrity_check` with the search functions registered. Without them, the check fails (DBR04). | Executed | Covered by DBR04. |

## Checked without new findings

These areas were examined for this review's failure modes and held up:

- SQL statement classification (`sql_statements`), including CTE and REPLACE
  detection.
- Savepoint nesting and owned/borrowed transaction branches.
- Ordering of after-commit callbacks, which run after the SQL commit because the
  completion frame wraps the transaction body.
- Create-only SQLite publication through a hard link.
- Link batching with staged temporary tables and read-back verification.
- Tree walks with cycle detection.
- The maintenance capture triggers, whose change IDs use `AUTOINCREMENT` and so
  are not reused.
- Custom-column typed value contracts.
- PostgreSQL role and identifier validation.

## Validation performed

- Static checks: Ruff's `E9` and `F` rules found no syntax errors or undefined
  names, only seven unused imports. A broader `B` and `E722` pass found 25
  maintenance-level hits: 13 `B904`, 6 `B009`, 3 bare `except` and one `B905`.
  Its two `B023` hits are one false positive at `search_queries.py:271`, a
  closure used immediately.
- Every executed reproduction above used disposable directories, in-memory
  databases or copies of the benchmark database. No real library, test database
  or source file was modified.
- The test suite, the quality gate and live PostgreSQL were **not** run. Treat
  the PostgreSQL statements as source-traced.

## Suggested next sequence

1. **Stop silent data changes:** DBR01, then DBR02 and DBR03. Each needs a
   regression test built from the reproduction above. DBR01 also needs a
   collision report before any declarations are rewritten.
2. **Restore type and write fidelity:** DBR05, DBR09 and DBR10, with SQLite and
   PostgreSQL parity tests that cover views.
3. **Make concurrency explicit:** DBR06 and DBR07 together, ending with a
   two-process test that runs the UI write path against a background job.
4. **Decide the index portability policy:** DBR04, before claiming that standard
   SQLite tools can maintain the archive.
5. **Hardening:** DBR08 and DBR11–DBR14 as small focused changes.

New fixes should cite the relevant DBR ID. Record implementation, verification
and inclusion in `main` separately, as the [review index](README.md) asks.

## Review-of-review and repairs — 2026-11-14, locally validated

The follow-up source check included the then-uncommitted DR01–DR03 repairs on top of
`7780b7eb`; the original checkpoint description above should not be read as proof
of a clean-tree review. Direct current-API probes confirmed DBR01, DBR02, DBR05,
DBR07–DBR10; actual connection/index/codec modules confirmed the other reproduced
mechanisms. The findings remain useful, with these corrections:

- DBR01: five of nine defaults use NFKC/whitespace/casefold; four use NFC/trim/casefold.
- DBR05: dedicated `view_reads` is affected too. Empty declarations differ from
  unfamiliar nonempty SQLite type names, which can legitimately have NUMERIC affinity.
- DBR04: changing function output was an injected corruption mechanism, not proof
  that a particular Python upgrade had already corrupted a catalogue.
- DBR06: deferred promotion failure is an expected SQLite conflict. Writer
  reservations help; arbitrary transaction bodies cannot safely be retried implicitly.
- DBR07: the highest row ID does not establish the latest user edit. Conflict
  resolution needs an explicit selection.
- DBR12: registering adapters once at import still changes process-wide behavior.

The [repair contracts](../databases/review-repairs-2026-11-14.md) record each finding's
disposition and explicit migration requirements. **DBR01–DBR13 are resolved at the
documented boundaries. DBR14 is partial:** its concrete contract fixes are complete;
broad comparison-column additions and automatic dirty-queue scheduling remain
explicit follow-ups. DBR04 retains expression indexes with version validation and
an offline rebuild; generic SQLite tool restrictions remain intentional.

The combined local checkpoint includes DR01–DR03 and these DBR repairs. Its exact
source is the commit containing this follow-up, whose parent is
`7780b7eb62020d8b1cf6891b3bb95a47d9f7fcfb`. All 55 new cases passed within the full
database gate, including four two-process SQLite cases and nine live PostgreSQL
cases. **The full gate passed 2,425 tests, including all 2,314 mandatory cases**,
with no skips, failures or xfails, in 2,312.91 seconds. Two existing logging
deprecation warnings were emitted. All 78 frozen source/test/configuration inputs
matched after completion.

Full quality and **96 tooling, storage and documentation tests** also passed.
Earlier focused selections overlap the full gate; their one pre-existing
PostgreSQL-shaped SQLite harness skip is not a skip in the full gate. The private
PostgreSQL server is stopped. No push or production catalogue migration was
performed. Implementation and local validation are complete; independent
re-review remains the next step.

## Independent re-review — 2026-11-14, evening

Re-reviewed checkpoint: **`5d5c0c88fb8f9e9e4a3ff14ef15c79b4b27806ba`**, the commit that
contains the repairs and the follow-up above. Nothing under `src/` or `tests/` had
uncommitted changes. Scope: DBR01–DBR14, and problems introduced by their repairs. The
native custom-field work bundled in the same commit was covered only by the test run.

The repair's regression tests build a fresh library and then edit it to look legacy.
This re-review also used the real July 2026 benchmark library named under *Scope and
evidence*, copied to a temporary directory. Each probe ran through public APIs:
`Database`, `macros`, `DBPrefs`, `Row` and the preference and index helpers. The probes
ran in a Python 3.13.18 sandbox (Unicode data 15.1.0, SQLite 3.53.1) without APSW or
PostgreSQL, so the PostgreSQL claims were **not** re-verified. Evidence is kept locally
in the ignored `.tmp/databases-rereview-2026-11-14/` directory; the results that matter
are reproduced below.

**Ten findings are resolved as claimed, and DBR14 is partial as stated. DBR01 and DBR07
are resolved for libraries generated by the current schema, but not for older ones.
DBR04's version check works, but carries an operational cost that has to be settled
before deployment.** The re-review adds RR01–RR06. Following the
[decision below](#decision-pre-release-libraries-are-not-upgraded--2026-11-14), RR01 and
RR02 no longer need upgrade work. They are replaced by one requirement: refuse
unsupported libraries when they are opened (RR06).

### Status of the original findings

| ID | Re-review status | Evidence |
| --- | --- | --- |
| DBR01 | Resolved for current-schema libraries. On the July library, ensure, find and add all refuse the lossy identities. The migration out of them fails there (RR01). | Executed |
| DBR02 | Resolved for declared foreign keys. On the July library, the API's own example keeps the item-only tag, and nested calls are refused before any effect. Undeclared references remain (RR05). | Executed |
| DBR03 | Resolved. Plain, comment-prefixed and lower-case `PRAGMA`, `VACUUM INTO` and `END TRANSACTION` are refused through both the connection and the cursor before any statement runs. Trigger bodies are still accepted. | Executed, using the repository test's harness |
| DBR04 | Resolved as a policy. The version check and offline rebuild work. A Python minor-version change triggers them (RR03), and the new table breaks an invariant test (RR04). | Executed |
| DBR05 | Resolved: `1984`, `007` and `1e3` read back through a view as text. | Executed |
| DBR06 | Resolved as contracted. A second writer waits the 5-second busy timeout and then fails cleanly, and a retry after release succeeds. Read macros take no writer lock. | Executed; reads source-traced |
| DBR07 | Resolved for current-schema libraries. Older libraries cannot write preferences (RR02). | Executed |
| DBR08 | Resolved. | Repository tests pass; source-traced |
| DBR09 | Resolved. | Executed |
| DBR10 | Resolved: `Row.sync()` on a missing ID raises `RowIntegrityError`. | Executed |
| DBR11 | Resolved. | Repository tests pass; not independently probed |
| DBR12 | Resolved: opening a library leaves the global `sqlite3` registries untouched. No column is declared `DATE` or `PY*` and `detect_types` is 0, so reads are unchanged. | Executed; source-traced |
| DBR13 | Resolved: `dump_and_restore` keeps a verified `before-rebuild` copy. The POSIX directory fsync is source-traced, and the Windows limitation is stated. | Executed; source-traced |
| DBR14 | Partial, as the repair contracts state. The PostgreSQL items were not re-verified. | Repository tests pass |

### New findings

| ID | Priority | Finding |
| --- | --- | --- |
| RR01 | Superseded by the decision (was P1 for existing libraries) | The identity migration fails on every library that needs it. |
| RR02 | Superseded by the decision (was P2) | Libraries generated before `5d5c0c88` cannot write preferences. |
| RR03 | P2, before deployment | A Python minor-version change stops every library from opening until an offline rebuild. |
| RR04 | P3 | The repair commit broke two tests that the ratchet gate does not run. |
| RR05 | P3, latent | Six reference columns have no declared foreign key, so the pruner cannot see them. |
| RR06 | P2, before deployment | Nothing refuses an unsupported library when it is opened. |

#### RR01. The identity migration fails on legacy libraries

The guard ([`portable_macros_mixin.py`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/portable_macros_mixin.py),
line 3211) refuses ensure, find and add for lossy profiles. On the July library,
`migrate_normalized_identities(target_profiles=...)` was called with the code default,
`unicode_nfkc_whitespace_casefold`, for each of the five legacy identities:

```text
legacy declarations: genres, series, subjects (title_search_term); labels, tags (tag_search_term)
audit: rows_needing_update = 1 | collisions = 0
migrate: IntegrityError: CHECK constraint failed: column_metadata_normalization_profile_allowed
```

The transaction rolled back, and the integrity check still passed. The `column_metadata`
CHECK only gained this profile in `5f085a13` (2026-10-16), the same commit that made it
the default; the `normalized_identities` table carries the same list. As far as the git
history shows, every library old enough to carry the lossy profiles predates the CHECK
that would allow the default target, and changing a CHECK in SQLite needs a table
rebuild. Targeting `unicode_nfc_trim_casefold`, which the old CHECK allows, does migrate.
Afterwards Deep Space Nine and The Next Generation, Spider-Woman and Spider-Man, and
"The Rapist" and "Therapist" all get separate rows.

**Disposition:** superseded by the decision; no upgrade path will be built. Tests that
start from a fresh library could not catch this. If old-library behaviour matters again,
use the July library as a fixture.

#### RR02. Pre-`5d5c0c88` libraries cannot write preferences

`DBPrefs` writes use an upsert with `ON CONFLICT (preference_key)`
([`dbprefs.py`](../../src/LiuXin_alpha/databases/dbprefs.py), line 267). That needs the
unique index that `5d5c0c88` added to `preferences.sql`. On the July library, which has
no duplicate keys:

```text
load: OK, keys = 0
write: OperationalError: ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint
migrate_preference_keys(db): OK
write after migration: OK
```

The migration is lossless when there are no duplicates, but nothing runs it and the error
does not mention it. This affects every library generated before `5d5c0c88`, including
those generated after 16 October.

**Disposition:** superseded by the decision; covered by RR06.

#### RR03. Decide what a Python upgrade means for search indexes

[`search_index_semantics`](../../src/LiuXin_alpha/databases/search_functions.py)
(line 24) includes `unicodedata.unidata_version`. `validate_search_index_semantics`
(line 47) runs every time the SQLite driver opens a connection (line 580 of its
`databasedriver/__init__.py`). It refuses any mismatch, including for read-only access.
The probe created a library under this interpreter (Unicode 15.1.0), then changed its
stamp to 15.0.0, which is what Python 3.12 writes:

```text
open after interpreter change: DatabaseIntegrityError: 'Search index semantics are unversioned
  or incompatible; explicitly run rebuild_sqlite_search_indexes before opening this catalogue'
rebuild_sqlite_search_indexes: ok
open after rebuild: OK
```

Refusing is the safe choice, because SQLite can use persisted expression results without
re-evaluating the function. The cost lands on operations:

- Every Python minor upgrade (3.12, 3.13 and 3.14 each ship different Unicode data)
  requires an offline rebuild of every library, taking time proportional to its size.
- Moving one library between two interpreters breaks it each time. Examples are WSL and
  Windows, or a development machine and CI.
- The error does not say which versions differ.

**Next:** before deployment, choose one of these and document it:

1. Keep the check. Put the recorded and current versions, and the exact command, in the
   error, and add the rebuild to the upgrade procedure.
2. Stop persisted keys depending on the interpreter's Unicode data. Either pin the
   normalization tables, or store keys in columns maintained by the writer, as DBR04
   originally suggested.

#### RR04. Repair the tests the commit broke and widen the gate

A full run of `tests/databases` showed two failures caused by `5d5c0c88`:

- `test_every_physical_main_table_has_a_concrete_owner` (`tests/databases/test_metadata_sql_tables.py`,
  line 19). The new `search_index_metadata` table counts as a main table but has no owner
  mixin: 37 tables against 36 owners.
- `test_database_api_signature_parity` (`tests/databases/api/test_database_api_signature_parity.py`,
  line 604). [`_coerce_db_value`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/databasedriver/value_casting_mixin.py)
  (line 1711) now returns `Any`, while
  [`DatabaseDriverAPI`](../../src/LiuXin_alpha/databases/api/driver_api/driver_api.py)
  (line 202) still declares `Optional[Union[...]]`.

Neither test is listed in `tests/databases/regression_ratchet.json`, so the 2,425-test
gate did not run them. Three older failures also sit outside the gate:

- `test_owner_batches_scope_identifiers_and_bound_queries` has been stale since the
  `entity_identifiers` trigger was added in `2823cd22` on 2026-11-11.
- The two `custom_columns_populated_db` fixture tests have been stale since `7780b7eb`
  added the custom-column key column.

**Next:**
- Register `search_index_metadata` as an owned or infrastructure table.
- Align the API annotation with the implementation.
- Update the stale owner-batches test and the two fixtures.
- Then run the full `tests/databases` suite as the gate, or add these files to the
  ratchet, so that the gate notices tests a repair breaks.

#### RR05. Declare the references the pruner cannot see

[`inspect_repair_references`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/portable_macros_mixin.py)
(line 669) finds declared foreign keys only. In both a fresh library and the July one, six
columns look like references but have no declared foreign key:

- `annotations.annotation_item_id` → `items`
- `annotations.annotation_device_id` → `devices`
- `backup_presence_links.backup_presence_link_source_file_id` → `files`
- `backup_workflow_sources.backup_workflow_source_file_id` → `files`
- `backup_workflow_sources.backup_workflow_source_store_id` → `stores`
- `backup_workflow_sources.backup_workflow_source_asset_replica_id` → `asset_replicas`

On the July library, pruning `items` deleted an item that only an annotation referenced,
leaving the annotation dangling. No production code calls the pruner today.

**Next:** declare these foreign keys, choosing each `ON DELETE` behaviour deliberately.
Alternatively, make the pruner refuse any table with a known undeclared reference.

#### RR06. Refuse unsupported libraries when they are opened

A library stores a `database_version` string assembled from code-module versions. A
fresh library contains `…_database_version_(0__3__0)_…`; the July library contains
`(0__2__0)`. Nothing compares this string on open, and `5d5c0c88` changed the generated
SQL without changing it. `PRAGMA user_version` is 0. A library from before `5d5c0c88`
therefore opens normally, then fails later inside ordinary writes (RR01, RR02).

**Next:** before deployment, add a schema revision that the generator writes and every
open compares. Bump it whenever the generated SQL changes, and refuse unsupported
revisions with a message telling the user to regenerate the library. The first revision
should be the `5d5c0c88` schema. After release, each bump needs either a migration or an
explicit refusal.

Once the gate exists, decide whether to keep the old-library helpers: the lossy-profile
guard, the `target_profiles` migration and `migrate_preference_keys`. The guard is cheap
insurance, but the migrations have no supported input.

### Decision: pre-release libraries are not upgraded — 2026-11-14

LiuXin has not been deployed. Libraries generated before `5d5c0c88` are unsupported and
will not be upgraded: regenerate them instead. The cut-off is `5d5c0c88` rather than
16 October, because that commit's preferences change (RR02) also affects libraries
generated after the identity-profile change. This puts RR01 and RR02 out of scope; RR06 is
the requirement that replaces them.

### Validation performed

- `pytest tests/databases`, excluding the `postgres_live` files, on Python 3.13.18 without
  APSW or PostgreSQL, with the package importable: **2,520 passed, 14 failed, 3 errors,
  1,619 skipped** in 675 seconds. The failures and errors break down as follows:
  - Two failures were caused by `5d5c0c88`, and three older failures were already present
    (both covered in RR04).
  - Nine failures are sandbox artefacts: seven `SQLite_apsw` parametrisations without
    APSW, and two tests that read `dev-docs` files missing from the snapshot.
  - Two errors also come from the missing `dev-docs` files.
  - One error depends on test order and passes when run alone.
- Twelve probes ran against disposable copies. No repository file, library or test
  database was modified. PostgreSQL was not run.

### Suggested next sequence

1. **Before deployment:** RR06's schema-revision gate, with `5d5c0c88` as the first
   supported revision, and the RR03 policy decision.
2. **Close the test gap:** fix the four tests in RR04, and make the gate cover the whole
   database suite.
3. **With the next storage or repair work:** RR05.
4. **After RR06:** decide whether to remove the old-library migration helpers.

New fixes should cite the relevant RR or DBR ID. Record implementation, verification and
inclusion in `main` separately, as the [review index](README.md) asks.

### Implementation follow-up — 2026-11-14

The worktree above `5d5c0c88` implements RR03–RR06. The detailed contracts and
upgrade procedure are in [the repair follow-up](../databases/review-repairs-2026-11-14.md#independent-re-review-follow-up--2026-11-14).
No new commit, merge or publication is claimed; this work is for another review.

- **RR03:** retain the policy/Unicode guard, report recorded and current stamps,
  and supply an offline rebuild CLI with the actual interpreter and catalogue path
  in the error. A change of Unicode/policy stamp is the trigger; an interpreter
  minor-version label alone is not. Writer-maintained columns would still require
  stable semantics or rekeying.
- **RR04:** classify both metadata tables as infrastructure, align the driver API,
  repair owner and custom-field fixtures, and collect the entire database test
  directory in the ratchet. Preserve all 2,314 prior mandatory cases and add 87;
  mandatory setup/call/teardown must pass without skips or xfails. Unlisted suite
  failures also fail the runner. Driver-unit fixtures carry explicit format metadata;
  catalogue integration tests continue to generate complete schemas.
- **RR05:** adopt the explicitly proposed refusal policy. Single and bulk pruning
  reject targets with any of the six known undeclared references until an actual
  matching FK defines its lifecycle. Bulk refusal rolls back earlier deletions.
  Adding constraints and choosing annotation/backup deletion behavior remains a
  separate schema decision, not an implicit cascade in this repair.
- **RR06:** generators write base schema revision 1; all three drivers check it
  before normal access or bootstrap. Creation has a private empty-target boundary;
  restores/backups validate their populated format before publication. Every
  unversioned pre-release catalogue must be regenerated, including files generated
  by `5d5c0c88`. Native custom-field DDL does not change the base revision.

RR01 and RR02 remain superseded by the recorded regeneration decision. The actual
July benchmark library was copied and independently checked: both SQLite backends
refuse it at admission and leave its bytes unchanged. Profile migration remains
supported for deliberate policy changes within current-format catalogues; the
claim above that it has no supported input is too broad. Explicit preference-key
repair and the lossy-profile guard remain available without admitting old formats.

Local verification completed on **2026-11-15**. The complete database suite ran
with APSW and live PostgreSQL: **5,000 passed, 90 skipped, two failed and eight setup
errors**, with 26 marker/deprecation warnings. All **2,401 mandatory cases fully
passed**, without missing cases, skips or xfails. The ten unsuccessful cases were
the progress-hook module's empty `:memory:` fixtures, which the new format gate
correctly refused. Those fixtures now carry explicit format metadata; all **10/10**
cases pass in a separate module run. No production code changed afterward. The
full suite was not repeated after this test-only correction, so its raw failed
status is preserved rather than described as a clean full gate. The earlier
unnamed order-dependent error was not reproduced in the complete local run.

The full quality checks passed. All 33 frozen implementation/test/configuration
inputs match the worktree; the separately verified progress fixture accounts for
the one later changed file. The tooling/documentation selection has one unrelated
existing failure because the index links to the user's untracked file-formats
review. Windows PowerShell recovery quoting remains source-checked only. The
private PostgreSQL server is stopped. Detailed commands, evidence and limits are
in the linked repair follow-up; no new commit or merge is claimed.

## Second independent re-review — 2026-11-15

Re-reviewed checkpoint: the uncommitted worktree on `5d5c0c88` described in the
implementation follow-up above. The SHA-256 hashes of all 33 changed or new files under
`src/`, `tests/` and `scripts/` were taken from the worktree when this section was written
(2026-11-15, evening). They match the snapshot reviewed at 01:21. Scope: RR03–RR06, plus a
re-check of the earlier DBR repairs. APSW and PostgreSQL were not available, so PostgreSQL
admission and the APSW variants were **not** re-verified.

The probes ran in the same sandbox as before (Python 3.13.18, Unicode data 15.1.0,
SQLite 3.53.1). Besides the July library, they used a library generated by the actual
`5d5c0c88` code, taken from the previous review's snapshot. Evidence is kept locally in the
ignored `.tmp/databases-rereview2-2026-11-15/` directory; the results that matter are
reproduced below.

**RR04, RR05 and RR06 are resolved. RR03 is partial: the policy and the rebuild command
work, but the command shown in the error cannot be run as displayed. The earlier DBR
repairs still hold.** This was also the first complete database-suite run of this
worktree. It had no failures apart from APSW cases this sandbox cannot run, and one
order-dependent error (RR09). Four new P3 items are recorded below as RR07–RR10.

### Status of the re-review findings

| ID | Status | Evidence |
| --- | --- | --- |
| RR03 | Partial. The error names the recorded and current stamps. `python -m LiuXin_alpha.databases.rebuild_search_indexes` repairs a library whose path contains a space, and refuses the July library because of its schema revision. The command embedded in the error cannot be run as displayed (RR07). | Executed |
| RR04 | Resolved. The four repaired tests and the whole `test_sqlite_progress.py` module pass within a full run. | Executed (full suite) |
| RR05 | Resolved. Pruning `items` is refused, naming `annotations.annotation_item_id`. A bulk call that reaches `items` rolls back its earlier `tags` deletion, and pruning `tags` alone still works. See RR10. | Executed |
| RR06 | Resolved for SQLite; detail below. | Executed; raw utilities source-traced |
| DBR03, 05, 06, 09, 10, 12, 13 | Still hold on the current code. All 12 disguised script commands are refused, and trigger bodies still run. | Executed |

RR06 detail. These were all refused, with every file in the directory byte-identical
afterwards:

- the July library;
- a library generated by the `5d5c0c88` code;
- an empty file;
- an unrelated SQLite file;
- a library whose revision was set to 2;
- a library whose marker table was dropped.

`create=True` over an existing file raises `FileExistsError` without changing it. A fresh
library records revision 1 and opens. The raw `sqlite3` maintenance utilities (backup
verification, lock probe and WAL checkpoint) do not check the revision, but they do not
read catalogue content. PostgreSQL admission was not verified.

### New findings

| ID | Priority | Finding |
| --- | --- | --- |
| RR07 | P3 | The rebuild command in the RR03 error cannot be run as displayed. |
| RR08 | P3, pre-existing | Opening a missing path creates a library, even with `create=False`. |
| RR09 | P3 | In a clean checkout, the suite leaks `LiuXin_scratch` into the repository root. |
| RR10 | P3 | No test pins RR05's reference list to the generated schema. |

#### RR07. Show the recovery command as runnable text

[`_rebuild_command`](../../src/LiuXin_alpha/databases/search_functions.py) (line 25)
quotes the command correctly, but
[`DatabaseIntegrityError.__str__`](../../src/LiuXin_alpha/errors.py) (line 506) returns
`repr(self.argument)`. The message also embeds the JSON stamps, which contain `"`. The repr
therefore delimits the message with `'`, escapes every inner `'` and doubles every
backslash. This is what a user sees and copies:

```text
POSIX path with a space: … --database-path \'/tmp/lib with space …/library.sqlite\'
  run verbatim -> exit 2: unrecognized arguments: with space …
PowerShell, any path:     & \'<python>\' \'-m\' … \'--database-path\' \'C:\\Users\\Alex\\Libraries\\library.sqlite\'
```

POSIX paths without spaces or quotes are unaffected. The PowerShell form had only been
source-checked.

**Next:** make the error display its message as plain text. Note that
`tests/surfaces/test_surface_read_errors.py` exercises this class. Alternatively, give the
command its own line or a structured field. Add a test that runs the displayed command on
a path containing a space.

#### RR08. Make `create=False` require an existing catalogue

[`Database.__init__`](../../src/LiuXin_alpha/databases/database/__init__.py) documents
`create` as an explicit request for new schema creation (line 325), and it defaults to
`False`. A path-backed target that does not exist is nevertheless generated (line 609). Now
that admission is strict, a mistyped path is the one way to open an unexpected empty
catalogue. The behaviour was the same at `5d5c0c88`.

**Next:** make `create=False` require an existing file, and have any surface that offers a
new library pass `create=True`.

#### RR09. Keep scratch files out of the repository root during tests

`tests/conftest.py` redirects the scratch folder for each test by monkeypatching module
attributes (from line 352). That does not reach code outside the patched scope.
[`ptempfiles`](../../src/LiuXin_alpha/utils/ptempfiles.py) defaults its base folder to
`<repo>/LiuXin_scratch` (line 52) and creates it on demand (line 455).

In each of three full runs on a clean snapshot, the repository-root leak guard
(`tests/conftest.py`, line 479) failed one teardown, naming a different test each time; each
of those tests passes alone. The development checkout has no `LiuXin_scratch` folder, and it
is not yet clear why local runs do not reproduce the error. A CI run on a fresh clone should
be expected to.

**Next:** run the suite on a fresh clone and find the caller, for example by making
`ptempfiles` fail loudly under tests when its base is the repository root. Alternatively,
point the default scratch base at the per-test runtime root for the whole session.

#### RR10. Pin the undeclared-reference list to the schema

[`UNDECLARED_REFERENCE_CANDIDATES`](../../src/LiuXin_alpha/databases/database_driver_plugins/SQL/macros/repair_storage.py)
(line 33) names six columns. The check skips any column that no longer exists (line 169), so
renaming one of them would silently remove its refusal. No test asserts that the six columns
are present in a generated catalogue.

**Next:** add a test that every candidate column exists in a freshly generated catalogue
and has no foreign key yet. It will fail deliberately when someone adds the foreign key,
which is the prompt to remove that entry from the list.

### Validation performed

- `pytest tests/databases`, excluding the `postgres_live` files, on Python 3.13.18 without
  APSW or PostgreSQL: **2,554 passed, 7 failed, 1 error, 1,644 skipped** in 548 seconds.
  - The seven failures are `SQLite_apsw` cases in `test_frbr_generator_integrity.py`, which
    fail because APSW is not installed.
  - The error is RR09; that test passes alone.
- Eight probes ran against disposable copies; the `5d5c0c88` library was generated with the
  previous review's snapshot. No repository file, library or test database was modified.
- The tooling and documentation selection was not run here. Its one failure recorded above
  comes from the review index linking to the untracked
  `file-formats-top-level-review-2026-11-14.md`; committing that file resolves it.

### Suggested next sequence

1. **RR07,** before anyone relies on the RR03 recovery path on Windows.
2. **Commit the RR03–RR06 worktree** once RR07 is fixed, and record that commit here as the
   checkpoint for these repairs.
3. **RR08,** before any surface offers to open a library by path.
4. **RR09,** before CI runs on fresh clones. **RR10,** with the next schema change to the
   annotation or backup tables.

New fixes should cite the relevant RR ID. Record implementation, verification and inclusion
in `main` separately, as the [review index](README.md) asks.

### Closure work following the second re-review — 2026-11-15

The second review's 33 changed source, script and test files were checked against
its archived snapshot and match byte-for-byte. Independent disposable-catalogue
probes reproduced RR07 and RR08. The following repairs are implemented in the
worktree; verification and PR status will be recorded separately below.

- **RR07:** `DatabaseIntegrityError` now displays plain message text. Recovery
  puts its command on a separate line, and the regression executes that displayed
  command on a catalogue path containing spaces and an apostrophe. The earlier
  regression inspected `.argument` and supplied a separately constructed argv,
  which missed the rendering defect. A new line alone would not fix `repr()`.
- **RR08:** ordinary facade/Core opens require an existing catalogue. Both SQLite
  driver names use `mode=rw` for ordinary connections, including a target removed
  between the facade check and connection. Explicit private creation retains
  `mode=rwc`. Existing creation callers and fixture expectations are being checked
  against this deliberate change in the open/create contract.
- **RR09:** `LIUXIN_SCRATCH_DIR` provides a native scratch default that the test
  harness sets at session initialization and overrides per test. Child processes
  inherit it. A subprocess regression covers collection, session setup/teardown
  and per-test use outside and inside the function-level monkeypatch scope.
- **RR10:** a schema regression checks all six candidates and their targets on
  the generated SQLite catalogue before fixtures can synthesize missing tables;
  adding a foreign key requires reviewing the corresponding refusal entry.

Qualifications to the review and scope:

- RR09's three reported sandbox failures are evidence of an isolation problem,
  but do not establish that a fresh checkout is the cause or that every CI run
  will reproduce it. The earlier local full run also used a snapshot and did not
  reproduce it. The exact original caller remains unidentified; the new check
  verifies the broader lifetime/child-process gap directly.
- RR10's column-rename rationale is too broad: renaming `annotation_item_id` in a
  disposable catalogue already makes the existing regression fail at insertion.
  The uncovered gap was fixture-created missing tables and no direct assertion
  of the generated FK state. PostgreSQL's optional storage relations are not
  required to exist merely because the complete SQLite schema contains them.
- No old-library conversion, custom-column compatibility facade, or speculative
  FK deletion policy is introduced. Refusal remains the policy for undeclared
  annotation/backup references until their owners choose deletion semantics.
- The second independent review did not execute APSW or PostgreSQL. Earlier
  local verification did, with the split full-run/progress-fixture results
  recorded above. Those histories must remain separate from the new checks.

Initial closure verification passed **110 focused tests** (creation, recovery,
schema inventory, temporary-directory fixtures and scratch lifetimes), followed
by **98 consumer/runner/recovery tests** with 46 deliberately deselected cases.
These selections overlap and are not a combined distinct-case count. The latter
run includes the Windows renderer regression added after the first collection.
The full quality runner passed formatting (380 files), annotations (463), lint,
architecture checks, 25 cold-import budgets and both type checkers; mypy checked
474 files and the invalid-contract suites rejected 95/93 invalid examples.

Native Windows PowerShell 5.1 also round-tripped the displayed command's arguments
with spaces, apostrophes and backslashes intact. This verifies Windows shell
argument handling; the complete SQLite rebuild was executed on Linux, not in a
native Windows Python environment.

The final source audit also caught a Windows UNC-path issue introduced by the
new existing-file connection mode: `Path.as_uri()` supplies a hostname authority
that SQLite normally rejects. RR08 now encodes `//server/share` as the filename
under an empty URI authority. Drive/UNC URI regressions cover opening and private
creation, including escaped spaces and `#`; no live Windows network share was
used. The 12 URI/missing-file/disappearance checks passed.

The first complete-gate attempt was stopped for that correction. It had also
identified four failures in two bootstrap tests that explicitly used
`create=False` to create fresh catalogues. Their creation calls now use
`create=True`; their reopening and bootstrap assertions remain intact. The
interrupted run is diagnostic evidence, not a complete result.

The corrected complete database gate will require **2,456 mandatory cases**,
retaining all 2,401 from the preceding worktree and adding 55. It also preserves
every requirement from the PR base `47156490`. The raw results of earlier runs
remain historical evidence. PR/publication and final gate status will be
recorded after verification; no merge is claimed.

### PR consumer follow-up — 2026-11-15

Closure commits `1b4c450a` and `e7b45407` are published in
[draft PR #161](https://github.com/ajCameron/LiuXin-alpha/pull/161), together with
the three preceding database commits. They are not merged into `main`.
The corrected bootstrap module passed all **34 tests**. The clean PR checkout,
without the unrelated file-formats draft link, passed **92 documentation,
tooling and scratch checks**. The full quality runner passed again after the
UNC correction.

CI exposed two consumer problems outside the original re-review probes: metadata
test doubles inferred `items` for `item_identifiers`, and the general suite could
not collect one PostgreSQL module without its optional driver. The doubles now
identify declared schemas and explicitly declare their edge-case columns. The
PostgreSQL module uses `pytest.importorskip`; the dedicated database ratchet still
requires its cases to execute. The absent-dependency probe passed, and all **nine
live PostgreSQL tests** passed with the driver installed. The local confidence
stream passed **313 tests**, with three nonmandatory skips.

The wider consumer check also exposed a real metadata adapter regression: cached
association records were wrapped as `Row` objects, but the cache's advertised
schema contains main tables. Strict row validation then rejected those records,
and hydration lost their relationships. The adapter now preserves the immutable
link mappings already permitted by `MetadataLinkRowSequence`. The regression
uses physical association columns and disables database fallback. This repair
belongs to the catalog adapter; native database validation remains strict.

One integration test selected an old unversioned prebuilt fixture from the local
data directory. It now generates its deterministic current-format fixture before
opening it. No stored fixture or unsupported library is relabelled or upgraded.
All **112 tests** in the four affected consumer modules now pass, including both
real-backend parity/round-trip cases. One pre-existing unregistered `catalog`
marker warning remains. The broader diagnostic run had 185 passes, three failures
and one unavailable-Tkinter skip; the three failures are covered by the repaired
selection, so that earlier run is not reported as green. The complete database
gate is still running at this checkpoint; its final outcome must be recorded
before claiming sign-off.

The full quality runner also passed after the catalog adapter correction,
including both type checkers and the 95/93 invalid-contract examples. This
consumer correction is outside the frozen `e7b45407` database-run snapshot;
the 112-test result above covers it separately. Published CI will exercise the
combined PR tree.

### Native Windows closure follow-up — 2026-11-16

Actual Windows execution exposed two additional portability gaps beyond the
independent review's probes:

- The offline index rebuild, replacement snapshot and standalone backup readers
  still used separate `Path.as_uri()` constructions. UNC filenames therefore
  retained a hostname authority that SQLite rejects. All four URI consumers now
  use `databases/sqlite_uri.py`, preserving their existing `ro`, `rw` and private
  `rwc` modes. Six integration cases exercise the three offline operations under
  both driver names, using UNC URI spelling with a local SQLite transport bridge.
- Native Windows creation failed before publication: `os.fsync()` on the
  read-only file descriptor raised `EBADF`. The private candidate's sync now
  opens it read/write on Windows without truncation; POSIX retains its existing
  read-only descriptor. The earlier Linux tests could not expose this failure.

Native **Windows Python 3.14.7, SQLite 3.50.4, Unicode 16.0.0** now passes real
catalogue creation, refusal of stale search semantics, the displayed PowerShell
rebuild command, reopening and normalized search. The catalogue path contains
spaces, an apostrophe and `#`. A missing-file open leaves no directories behind.
Standalone backup and explicit replacement also pass, and both recovery backups
reopen with the original metadata. Preferences, configuration and scratch were
isolated under a disposable task directory. Windows execution policy was not
changed. This supersedes the earlier shell-argument-only verification limit;
no live Windows network share or native Windows APSW execution is claimed.

These changes touch native code after `90f86685`. The earlier local full gate at
`e7b45407` was stopped and remains diagnostic evidence, not final validation. Its
private PostgreSQL server is stopped. The mandatory manifest now contains
**2,462 cases**, preserving all previous requirements and adding the six offline
URI cases. All **125 creation, recovery and restore tests** pass after these
repairs, including the six new cases. The full quality runner also passes:
381 formatted files, 475 files checked by mypy, both type checkers and the 95/93
invalid-contract examples. A fresh complete gate for the updated commit remains
required before sign-off; the focused results do not replace that result.
