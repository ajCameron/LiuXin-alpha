# Change log and retirement — design note

Date: **2026-11-26**. Status: **all six decisions settled on 2026-11-26. Phase 1 (the change log) is in progress.**

This note answers [CACHE06](next_tasks.md#cache06-publish-additions-without-a-full-reload) and the add-only question in the [2026-11-26 cache review](../architectural_reviews/caches-review-2026-11-26.md). The direction chosen in discussion is:

- the catalogue becomes append-only: records are retired rather than deleted;
- the cache never applies a write itself. It copies the rows the database says changed.

The section headed "Decisions" records what was settled.

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
| `operation` | `insert`, `update` or `delete`. Phase 2 adds `retire` and `revive`, which the update trigger reports when a row's retired timestamp is set or cleared. |
| `old_ids` | For updates and deletes: JSON of the old row's ID column and its `*_id` columns. |
| `txid` | PostgreSQL only: `pg_current_xact_id()`, for ordering (below). |
| `created_ms` | Time of the change. |

The log stores no bibliographic values; the cache re-reads current rows by ID. `old_ids` holds only identities, as the maintenance journal does. It lets a removed link keep both of its ends (book and tag, say), both as history and so a cache knows which records to refresh. The triggers are generated from the same templates as maintenance capture and use only standard SQL, so writers outside LiuXin are logged too.

It covers every table a cache loads: main tables, link tables, allowed-type tables and native custom-field tables. When a custom field creates its tables, their triggers are installed with them.

**Installing it.** The log is an optional helper table rather than a new schema revision, so existing catalogues and test fixtures keep opening:

- new catalogues get it at creation, on SQLite and PostgreSQL;
- existing catalogues add it with an explicit, previewable install; opening a catalogue never installs it;
- a cache uses the log only when it covers every table that cache loads; otherwise it keeps today's full reload.

Decided: a dedicated table, separate from `maintenance_changes`.

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
- the cursor is the horizon, not the highest `change_id` seen.

In practice each catch-up reads every visible entry at or above the stored horizon, applies them all, and stores the new horizon, taken in the same snapshot as the read. Entries from transactions that committed early are therefore applied straight away and applied again on the next pass. That is harmless because applying an entry only re-reads the row by ID. Waiting for the horizon to pass them instead would let one long-running writer elsewhere hold back the cache's view of this process's own writes.

Decided: the transaction-ID horizon. The alternatives were re-reading a trailing time window, or PostgreSQL's logical decoding.

### Cost and pruning

Each changed row costs one small extra insert, against today's full reload of about 191 tables per changed write.

Decided: the log is kept as the catalogue's history, with no pruning for now. Growth will be measured during ingest. Limits, such as archiving old entries or compacting repeated updates, are phase 4 work. If entries are ever removed, a cache whose cursor falls behind the oldest remaining entry reloads in full.

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

Link rows do not retire: they are physically removed, and the log entry keeps both ends (decision 2). Existing rows are live, so adding the columns rewrites no data.

### Unique identities

- Each declared unique identity index becomes partial: `WHERE <prefix>_retired_ms IS NULL`.
- The CR05 identity check ignores retired rows.
- Decided (decision 3): creating a name a retired row holds makes a new row. `revive(id)` brings the old one back explicitly, and fails if a live row has since taken its identity. The reasons:
  - links are physically removed, so automatic revival would return the old ID with none of its links;
  - a row retired by a merge points at its replacement, and reviving it would silently undo the merge;
  - revival would bring back attributes the caller did not supply, such as a genre's parent, which may itself be retired;
  - a name retired more than once has several candidates;
  - it matches the ingest rule that likely duplicates become new, flagged records rather than silent matches.
- A create that matches a retired row reports it, with the retired row's ID and any replacement, so a UI or the ingest report can offer to revive it or, for a merged name, to use its replacement. The create itself never revives or redirects.

### Reads

Retired rows are hidden by default everywhere. Decided: caches exclude them entirely, rather than holding them behind a filter.

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
3. **Retirement for WEMI entities.** Link rows are still removed physically, and the cache applies those removals from `old_ids` without reloading.
4. **Retention and history:** limits on the log's growth, such as archiving, and history reads.

Ingest can start after phase 1.

## Decisions

Settled on 2026-11-26.

1. **Where the log lives:** a dedicated `catalog_changes` table, not the maintenance journal made mandatory.
2. **Link rows:** physically removed, with a log entry that records both ends. They do not retire.
3. **Re-creating a retired name:** a new row; the create reports the retired match so a UI or ingest report can offer revival or the replacement. See "Unique identities".
4. **Log retention:** kept as the catalogue's history. Limits on its growth are to be decided later, informed by ingest measurements.
5. **Retired rows in caches:** excluded for now.
6. **PostgreSQL ordering:** the transaction-ID horizon.

## Risks

- **A forgotten read path** shows retired rows. The shared predicate and contract test above defend against it.
- **Row-level refresh is incomplete on some backends.** The default `reload_ids` reloads whole tables. Each backend's per-row derived state, especially `numpy_vectorized`'s sorted arrays, needs measurement, or catch-up gains nothing there.
- **Log write overhead on large imports** must be measured against the ingest benchmark.
- **Purge is the remaining full-invalidation case.** It should stay rare and explicit.
