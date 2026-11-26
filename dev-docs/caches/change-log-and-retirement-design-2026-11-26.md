# Change log and retirement — design note

Date: **2026-11-26**. Status: **proposal for review; nothing here is implemented.**

This note answers [CACHE06](next_tasks.md#cache06-publish-additions-without-a-full-reload) and the add-only question in the [2026-11-26 cache review](../architectural_reviews/caches-review-2026-11-26.md). The direction chosen in discussion is:

- the catalogue becomes append-only: records are retired rather than deleted;
- the cache never applies a write itself. It copies the rows the database says changed.

The section headed "Decisions needed" lists what has to be settled before any code.

## Why not keep cache and database in step by writing to both

Applying each write twice, as SQL and again inside the cache with its own logic, would need a second, matching copy of every rule the database applies:

- **Derived values:** normalized identity keys, defaults and timestamps, new IDs, sibling positions.
- **Trigger side effects:**
  - 29 timestamp triggers update `*_modified_timestamp_ep_k` after updates;
  - `entity_identifiers` rows are deleted after their owner is deleted;
  - storage folders and digital assets have triggers too.
- **Cascades:** `ON DELETE CASCADE` on link tables, in both SQLite and PostgreSQL.
- **Partial failure and savepoint rollback:** the two copies must succeed and fail together, including when a nested savepoint rolls back.
- **Other processes:** writers in other processes would never pass through this cache.

Every divergence would be a silent stale read. This is why the cache has so far been rebuilt from the database rather than written alongside it.

## Principles

1. **The database is the only writer.** The cache copies rows by ID and never interprets an operation.
2. **The database records what changed, in the same transaction as the change.** The record therefore cannot disagree with the data, and it includes trigger side effects and writes from other processes.
3. **Nothing ordinary deletes.** Retirement is an update. Physical removal is an explicit, audited purge.
4. **Full reload stays the recovery path.** Any doubt about the change log means a full reload, never a guess.

## What already exists

- **Trigger-based capture.** [`databases/maintenance/capture.py`](../../src/LiuXin_alpha/databases/maintenance/capture.py) already installs per-table `AFTER INSERT/UPDATE/DELETE` triggers on SQLite and PostgreSQL. They write row identity, not values, into `maintenance_changes` inside the source transaction. Its pruning and scale behaviour are measured in [maintenance-scale-2026-11-02](../databases/maintenance-scale-2026-11-02.md).
- **Row-level refresh hooks.**
  - `StorageCacheAPI.reload_ids` exists, but its default reloads the whole table.
  - `schema_backed` overrides it.
  - Fields have `refresh_ids` on `schema_backed` and `numpy_vectorized`.
  - `ReadCache.reconcile` is table-granular today.
- **Consistent reads.** `macros.read_consistently` (CR04) and SQLite's `PRAGMA data_version` witness tell a cache whether any other connection has committed.
- **Retire and purge precedent.** Native custom fields already retire by default and purge only explicitly; see [custom-column cleanup](<../databases/03 - TEMP triggers for custom-column cleanup.md>).
- **Identity rules.** `CatalogIdentityConflictError` (CR05) checks declared unique identities before writing. The root-genre key is already a partial unique index.
- **Deletion guard.** `detach=True` (CR07) already makes deletions of linked rows explicit.

## Part 1. The change log

### Table

There is one catalogue table, `catalog_changes`, with these columns:

| Column | Meaning |
| --- | --- |
| `change_id` | Identity primary key; the log's order. |
| `table_name` | Physical table changed. |
| `row_id` | Row changed. |
| `operation` | `insert`, `update`, `retire`, `revive` or `purge`. |
| `txid` | PostgreSQL only: `pg_current_xact_id()`, for ordering (below). |
| `created_ms` | Time of the change. |

The log stores no values; the cache re-reads current rows by ID. It is installed with the catalogue, at a new schema revision, and covers every table a cache loads: main tables, link tables, extension tables and native custom-field tables. The triggers are generated from the same templates as maintenance capture.

Recommended: a dedicated table, separate from `maintenance_changes`.

- **Different lifecycle.** The maintenance journal is optional, installed per subscription, and pruned once durable plugin obligations complete. A cache's cursor is per process and not durable, so it should not hold up or depend on obligations.
- **Merging later is possible.** The two logs could be merged later if pruning rules align.

### Catching up

1. **Starting point.** A full load reads `max(change_id)` inside the same `read_consistently` pass as the data. That becomes the cache's cursor.
2. **When to catch up:**
   - after this process's own committed writes, replacing today's `reload_data()` in the metadata write coordinator;
   - before a read, when SQLite's data-version witness has moved, or when a throttled `max(change_id)` poll moves on PostgreSQL.
3. **Applying changes:**
   - read entries after the cursor, grouped by table;
   - refresh those rows by ID through `reload_ids`, plus `refresh_ids` on affected fields and links;
   - rebuild only the derived entries for those rows, using the same code a full load uses;
   - advance the cursor and generation.
4. **Falling back to a full reload when:**
   - entries were pruned past the cursor;
   - a gap cannot be resolved (below);
   - more than a set fraction of a table changed;
   - the backend lacks row-level refresh for that table;
   - any refresh fails.

### Ordering

**SQLite** has one writer at a time, so `change_id` order is commit order. A rolled-back transaction's entries never become visible.

**PostgreSQL** assigns identity values before commit, so a later `change_id` can commit first. Advancing the cursor past a not-yet-committed entry would miss it permanently. The recommended rule:

- record each entry's `txid`;
- on catch-up, take `pg_snapshot_xmin(pg_current_snapshot())`;
- apply entries from transactions older than that horizon, which are all finished;
- keep newer entries for the next pass;
- the cursor is the horizon, not the highest `change_id` seen.

The alternatives, re-reading a trailing time window or logical decoding, are listed under "Decisions needed".

### Cost and pruning

Each changed row costs one small extra insert, against today's full reload of about 191 tables per changed write. Entries older than every live cache's needs can be pruned by age or count; a cache behind the pruned range reloads in full. If Part 2 keeps the log as history, pruning becomes archiving instead.

### Verification

- **Differential tests:** "catch up from the log" equals "full reload" on all three backends, after randomized workloads including other-connection writes, trigger side effects and rollbacks.
- **Interleaving tests:** the CR04 interleaving tests, extended to catch-up.
- **PostgreSQL ordering tests:** two writers committing out of `change_id` order.
- **Debug mode:** an option that compares sampled rows against the database after each catch-up.
- **Ingest benchmark:** per-book write cost must not grow with catalogue size.

## Part 2. Retirement instead of deletion

### Schema

Each main and vocabulary table gets nullable columns, `NULL` meaning live:

- `<prefix>_retired_ms`
- `<prefix>_retired_reason`
- `<prefix>_replaced_by_id`, set by merges.

Whether link rows retire too is an open decision (below). Existing rows are live, so adding the columns rewrites no data.

### Unique identities

- Each declared unique identity index becomes partial: `WHERE <prefix>_retired_ms IS NULL`.
- The CR05 identity check ignores retired rows.
- Creating a name a retired row holds makes a new row by default. `revive(id)` brings the old one back, and fails if a live row has since taken its identity.

### Reads

Retired rows are hidden by default everywhere:

- repository reads;
- cache loads and catch-up;
- search predicates and projections;
- exports and calibre-compat views.

The risk is a read path that forgets. Two defences:

- **One definition:** a single shared "live" predicate per table, so no path re-implements it.
- **A contract test:** create, retire, then assert absence through every public read surface.

Explicit history reads opt in with `include_retired=True`.

### Operations

- **`delete(id)` becomes retire.** It keeps the `detach=True` rule for linked rows.
- **`purge(id)` removes rows physically.** It is a separate audited plan/apply operation, as native custom fields already do. It is the only path where cascades run; for the cache it is a `purge` entry, handled by reloading the affected tables.
- **Merges retire the loser,** with `replaced_by_id` pointing at the winner.
- **calibre-compat deletes become retirements.**

### Upgrade

- **SQLite:** add the columns with `ALTER TABLE ... ADD COLUMN`, then rebuild each unique identity index as partial.
- **PostgreSQL:** extend the plan/apply service in [postgres-catalog-upgrade](../databases/postgres-catalog-upgrade.md).

Both run as previewable plan/apply operations; opening a catalogue never upgrades it.

## Phasing

1. **Change log and catch-up** for inserts and updates; deletions and purges fall back to reloading the affected tables.
   - This removes the full-reload cost from ingest (CR09).
   - Writes from other processes become visible.
   - The single-connection reload (CR10) belongs here.
2. **Retirement for Tags, Labels, Ratings, Agents and tree vocabulary.** `delete` becomes retire; the identity indexes become partial; `purge` is added.
3. **Retirement for WEMI entities and, if chosen, link rows.** After this, ordinary operations never cause a delete-driven reload.
4. **Retention and history:** pruning or archiving the log, and history reads.

Ingest can start after phase 1.

## Decisions needed

1. **Where the log lives.** A dedicated `catalog_changes` table (recommended), or the maintenance journal made mandatory.
2. **Link rows.** Should they retire too (full membership history, partial unique keys on link tables), or be physically removed with a log entry?
3. **Re-creating a retired name.** A new row (recommended) or automatic revival.
4. **Log retention.** Prune once caches no longer need it, or keep it as the catalogue's history.
5. **Retired rows in caches.** Held but filtered (cheap history reads), or excluded (smaller caches; recommended to start).
6. **PostgreSQL ordering.** The transaction-ID horizon (recommended), a trailing re-read window, or logical decoding.

## Risks

- **A forgotten read path** shows retired rows. The shared predicate and contract test above defend against it.
- **Row-level refresh is incomplete on some backends.** The default `reload_ids` reloads whole tables. Each backend's per-row derived state, especially `numpy_vectorized`'s sorted arrays, needs measurement, or catch-up gains nothing there.
- **Log write overhead on large imports** must be measured against the ingest benchmark.
- **Purge is the remaining full-invalidation case.** It should stay rare and explicit.
