# Cache review — 2026-11-26

Status:

- **Fixed in this pass:** the torn-reload, raw-collision and stale-receipt bugs (CR04–CR06), silent cascading deletes (CR07) and picker cost (CR08).
- **Open:** the cost of a full snapshot reload on every changed write (CR09, CR10). This blocks ingest through a snapshot cache.

The review covers `main` at `95bb4fac`, after #213–#215 merged. It looks at the cache as ingest will use it, which is the next workstream. Earlier reviews ([2026-11-16](caches-review-2026-11-16.md), [2026-11-17](caches-review-2026-11-17.md)) and the dated cache notes under [caches](../caches/README.md) stay authoritative for what they cover. Their CR01–CR03 numbering continues here.

## Method

- **Differential workload.**
  - A 38-step workload ran against all four modes: uncached, `schema_backed`, `database_backed` and `numpy_vectorized`.
  - It covered:
    - exact reuse with case, whitespace and full-width variants;
    - tag membership and reordering;
    - colliding renames and deletes of linked values;
    - genre paths with a shared leaf name;
    - Series membership, credits, title edits and search.
  - After every step, each cache was compared with an uncached read of the same database.
- **Second writer.** A second `Database` was opened on the same file, standing in for another process.
- **Reload interleaving.** A commit from that second connection was injected between two table reads of a reload.
- **Timings and profiles.** Ingest-shaped writes and picker reads were timed and profiled at small scale.

Timings come from the local development VM, under Python 3.13 with the 3.14-only syntax adapted. They are relative evidence, not latency targets. CI on Python 3.14 is the authoritative test run.

## What holds

- No backend differences in the differential workload, and no stale reads after in-process writes.
- Revision preconditions catch edits based on a stale snapshot, including one held while another connection renamed the row.
- Deletes leave no orphan rows; foreign keys are enforced.
- `ensure_*` operations are indexed, at about 23 ms each locally.
- The metadata API's genre and subject ensures are parent-scoped. `ensure_genre("Fantasy")` returns the root Fantasy even when Fiction › Fantasy also exists.

## Findings

| ID | Finding | Status |
| --- | --- | --- |
| CR04 | Snapshot reloads could publish a torn state | Fixed |
| CR05 | Identity collisions surfaced as raw driver errors | Fixed |
| CR06 | Exact reuse of another connection's row returned an unreadable ID | Fixed |
| CR07 | Deleting linked flat vocabulary or Agents cascaded silently | Fixed |
| CR08 | Pickers cost about a millisecond per row on every backend | Fixed |
| CR09 | Every changed write reloads the whole snapshot | Open |
| CR10 | Each reload opens one connection per table | Open; the write path's share fixed 2026-11-28 |

### CR04. Torn snapshot reloads

**Problem.** `reload_data()` read about 191 tables one at a time, each on a fresh connection and outside any read transaction. A commit from another connection between two of those reads made the snapshot internally inconsistent until the next reload.

In the reproduction, a work, its tag and the link between them were committed just after the `works` table had been read:

- the snapshot held the tag and the link but not the work;
- `works.get` returned `None`, and `list_tags` raised "Missing referenced works row";
- both `schema_backed` and `numpy_vectorized` behaved this way.

**First attempt.** Running the whole reload inside `read_transaction()` made the snapshot consistent, but under SQLite's rollback journal the reload's shared lock made the other writer fail with "database is locked" after its 5-second busy timeout. Schema discovery during the reload opens further connections, so the two sides waited on each other until the writer gave up.

**Fix.** A new macro, `read_consistently(read, attempts=3)`, runs the reload.

- **PostgreSQL** uses one repeatable-read snapshot, which never blocks writers.
- **SQLite** reads optimistically. A witness connection records `PRAGMA data_version` before and after; a commit from any other connection in between repeats the read. After three optimistic passes, a final pass holds a deferred read transaction.

`ReadCache` full refreshes use this macro. Inside a macro transaction, `driver_wrapper.get_all_rows` now reads through that transaction, as `get_row_from_id` already did, so PostgreSQL's snapshot covers the whole reload.

In the same reproduction, the other writer finished in 0.13 s and the reload repeated once. The published snapshot had the work, the tag and the link.

Remaining limit: under constant cross-process writes, the final locked pass can still make a writer wait, and fail after its busy timeout if the reload takes longer than that. WAL mode would remove that contention. Its effect on backups and storage copies needs its own decision.

### CR05. Raw driver errors on identity collisions

**Problem.** These operations raised `sqlite3.IntegrityError` (or psycopg's error on PostgreSQL) straight through the metadata API:

- renaming a Tag, Label or Series to a name another row holds;
- moving a Genre beside a sibling of the same name;
- creating a duplicate.

Core reported them only as `handler_error`. The cache stayed consistent.

**Fix.** Exact and tree repositories now check each declared unique normalized identity before writing. The check:

- runs on inserts, and on updates that change the identity or its scope;
- compares rows in the same batch with each other.

A collision raises `CatalogIdentityConflictError(table, existing_id, identity)`, a `CatalogMutationError`. Core maps it to `identity_conflict`, with the table and the existing ID. That ID lets a surface offer calibre's behaviour, merging into the existing row, as an explicit choice.

The database's unique indexes remain the final guard. A concurrent writer outside the serialized boundary can still meet the raw error.

### CR06. Unreadable ID after exact reuse of an external row

**Problem.** Another connection created a Tag after the snapshot loaded. `ensure_tag` then returned that Tag's ID with `cache='current'`, but `tags.require(id)` raised not-found. `works.add_tag(text=...)` behaved the same way.

**Fix.** The metadata write coordinator accepts the rows a result promises exist. After a committed no-change write, a snapshot missing any of those rows is reloaded. This applies to every `ensure_*` operation and to `works.add_tag(text=...)`.

Remaining limit: a no-op link whose two endpoints are both cached, but whose link row is not, is still not detected. Snapshot caches still need explicit reloads for external writes in general, as the contract already states.

### CR07. Silent cascade when deleting linked values and Agents

**Problem.** Tree vocabulary refused deletion while in use unless `detach=True`. Tags, Labels, Ratings and Agents deleted their links silently. Deleting an author removed their credits at all four WEMI levels.

**Fix.** `metadata.tags`, `labels`, `ratings` and `agents` now refuse a linked deletion unless `detach=True`.

- The refusal raises `CatalogInUseError` with link-row counts per link table; Core maps it to `in_use`.
- `HierarchyInUseError` is now a subclass of `CatalogInUseError`.
- Core's `catalog.entity.delete` accepts an optional boolean `detach` and applies the same rule. It also forwards `detach` to tree repositories.
- `BaseRepository.link_references(entity_id)` counts interlink and intralink rows. It does not count owned extension rows, such as an Agent's person details.

Catalog's own repository `delete` is unchanged, so internal merge and repair workflows keep their semantics.

### CR08. Picker cost

**Problem.** `lookup` and `suggest` wrapped every row of the shared population in a `databases.row.Row`. Building each one ran link-table discovery, about 57 schema-version queries per row.

**Fix.**

- Pickers read plain row values: `MetadataReadSource.get_all_row_dicts`.
- Exact lookups with a text operand read only rows matching its shared text key, through the cache's text indexes or SQL's text-key expression indexes (`find_text_rows`). Exact comparison implies equal text keys, so this is a superset, and the exact rule still runs on it.
- `get_interlinked_tables` keeps its answer per table until the schema version changes. This also speeds up every other `Row` construction.

Results with 2,000 tags and 800 Agents, locally, in milliseconds (before → after). Every result was identical before and after.

| Mode | `tags.lookup` | `tags.suggest` | `agents.lookup` | `agents.suggest` |
| --- | --- | --- | --- | --- |
| uncached | 1,710 → 15 | 1,841 → 16 | 840 → 7 | 982 → 13 |
| `schema_backed` | 2,093 → 17 | 1,992 → 18 | 729 → 10 | 769 → 13 |
| `database_backed` | 1,942 → 64 | 1,945 → 67 | 924 → 32 | 977 → 38 |
| `numpy_vectorized` | 2,115 → 20 | 2,109 → 25 | 835 → 13 | 956 → 17 |

Suggestions still scan the whole population, now cheaply. A prefix index would make them independent of population size, if pickers ever need it.

### CR09. Full reload on every changed write (open)

Every changed metadata write on a snapshot backend calls `reload_data()`. Inside `metadata.batch()`, there is one reload per batch.

Locally, on a catalogue of 20–40 books, each changed write took:

| Mode | Time per changed write |
| --- | --- |
| `schema_backed` | about 1.2–1.5 s |
| `numpy_vectorized` | about 2.1–2.7 s |
| `database_backed` | 25–80 ms |
| uncached | 60–110 ms |

Snapshot times grow with catalogue size. An ingest that writes a stack, tags, a credit and a Series per book through a snapshot cache spends several seconds per book. [next_tasks](../caches/next_tasks.md) already names conservative snapshot refreshes as the next performance work; these numbers make it the blocker for ingest.

### CR10. One connection per table per reload (open)

`direct_get_all_rows` opens and configures a fresh connection for each table: about 190 per reload, roughly half of a reload's time locally. CR04's optimistic pass keeps this behaviour. A single pinned read connection for the reload would halve the time, and on SQLite it should be combined with the data-version witness.

**Update 2026-11-28: the write path's share is fixed.** Every catalogue write read its normalized-identity declarations through a fresh connection, about 13 times per book, and each new connection validated the schema revision. Inside a managed transaction they are now read on the transaction's own connection. Measured locally:

| Workload | Before | After |
| --- | --- | --- |
| A WEMI stack plus a Tag per book (two writes), `schema_backed` cache, 120 books | 0.19 s per book | 0.07–0.09 s per book |
| The ingest apply step from #234, 200 books | 131 ms per book | 30 ms per book |

The ingest catalogues were identical table by table, apart from generated IDs and paths. Each write transaction still opens its own connection. The reload's one connection per table, above, is still open.

## An add-only cache

There are two ways to read "add-only". Each supports the other.

**1. Incremental publication of additions.** A write that only inserts rows and link rows cannot make existing snapshot rows wrong. Only the derived state over them needs updating:

- text and membership indexes;
- query results;
- counts and sort positions.

Ingest is almost entirely this shape: new stacks, new links, and exact reuse of existing vocabulary.

Writers already know what they added. `create_wemi_stack` returns its IDs, `ensure` reports `created`, and link operations know their link table. Generic `cache.reconcile(receipt, tables=..., links=...)` already refreshes explicit IDs. Routing pure additions there, instead of `reload_data()`, would make ingest cost per book independent of catalogue size. This is the recommended fix for CR09. Updates and deletions keep the full reload until each has a proven narrower dependency.

**2. An append-only catalogue.** Records would be retired rather than deleted: a retired flag and its provenance would be kept, and reads would hide retired rows by default. This extends CR07's `detach=True` and matches the archival rule that originals are not deleted. Purge would stay an explicit, audited operation, as native custom fields already do.

This makes reading 1 much simpler, because deletion is the case that forces full invalidation. It is a schema and policy decision, though: every repository, search and export path has to agree on what "retired" hides. Reading 1 can ship first and does not depend on it.

## Implications for ingest

- **Writes stay in the process that owns the cache.** Worker processes only read files, because snapshot caches do not observe external writes.
- **Writes go in batches** inside `metadata.batch()`, with one savepoint per book, until CR09 is fixed. Alternatively, ingest writes uncached and reloads once at the end.
- **Known tags come from paging `tags.list`.** It is fast: 1,000 rows in 9 ms.
- **Author matching uses `agents.lookup`,** which is now indexed by text key.
- **Ambiguous genre and subject names** go through the metadata API's parent-scoped `ensure_genre` and `ensure_path`, not `write_container`'s text resolution. That resolution still takes the lowest-ID match when a leaf name is shared.

## Validation

- **New regressions.** These files add 28 regressions; the mandatory manifest goes from 3,139 to 3,167 cases:
  - `tests/databases/caches/test_reload_consistency.py`
  - `tests/databases/caches/test_picker_reads.py`
  - `tests/databases/caches/test_identity_conflicts.py`
  - `tests/databases/caches/test_delete_detach.py`
- **Updated test.** `test_metadata_trees.py` now expects `CatalogIdentityConflictError` where it expected `sqlite3.IntegrityError`.
- **Remaining checks.** The full suite and both database ratchets run in CI.
