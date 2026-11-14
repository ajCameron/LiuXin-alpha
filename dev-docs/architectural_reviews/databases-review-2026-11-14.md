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
