# Catching up from the change log

Date: **2026-11-26**. Phase 1, part 2 of the [change log and retirement design](change-log-and-retirement-design-2026-11-26.md). Part 1 is the [catalogue change log](../databases/change-log.md) itself.

A snapshot cache (`schema_backed`, `numpy_vectorized`) used to reload every table after each committed metadata write (CR09). It now re-reads only the rows the change log names.

## How it works

**Cursor.** A full load or reload takes the log's cursor before it reads any data. The cursor is kept only when the log covers every table the snapshot read: its main tables and the physical tables behind its link views. Otherwise the cache keeps no cursor and behaves as before. Live backends (`database_backed`) never take one.

**`ReadCache.catch_up()`.** This reads the log after the cursor and groups the entries:

| Logged change | Refresh |
| --- | --- |
| Main-table row | `reload_ids(table, ids)`, plus the old ID if an update changed it |
| Entry with no row ID (text-keyed table, PostgreSQL `TRUNCATE`) | `reload_main_table(table)`, or `reload_link_table` for every view of a link table |
| Link-table row | `reload_link_rows(table, ids)`: on `schema_backed`, every directed view re-reads those rows by ID and each relation field on it re-projects only the sources whose links changed (2026-11-27); other backends reload each view |
| Any other table | Nothing; the snapshot does not read it |

The refreshes reuse the backends' existing hooks, so every derived field they maintain is rebuilt the same way a full reload rebuilds it.

**One read transaction.** The whole catch-up runs in one short read transaction (`_read_snapshot`). Every refresh borrows its connection, so the log and the rows it names come from one committed state, and no further connection is opened; I checked this with a connection counter on both backends. On SQLite, holding the snapshot makes another writer's commit wait until it ends. A catch-up is short. The full reload, which is long, still reads optimistically (CR04).

**Falling back to a full reload.** The cache reloads in full, as before, when:

- the cache has no cursor;
- the log no longer covers the snapshot (coverage is checked on every catch-up, so a dropped trigger is noticed);
- the log or its triggers were reinstalled;
- more than `CATCH_UP_LIMIT` (5000) entries are pending, as after a bulk import elsewhere;
- any refresh fails.

## Who calls it

- **The metadata write coordinator** (`MetadataAPI._write`):
  - A committed change calls `catch_up()` where it used to call `reload_data()`.
  - A change inside a caller's transaction marks the cache to catch up on its next read once the transaction completes. If the transaction rolls back, the log holds nothing, so nothing is applied. Previously any pending change forced a full reload.
- **Batches** no longer force a full reload. A child that changes the schema, such as creating a custom field, still requires one, and the outer batch honours it.
- **Core command reconciliation** (`CoreServices.reconcile`) prefers `catch_up()` to `reload_data()`.
- **Callers** may call `cache.catch_up()` directly to pick up writes from other connections or processes; it re-reads only what they changed.

Writes from another process become visible at this cache's next write or explicit `catch_up()`. Polling before every read is not done yet: it would change the snapshot contract, and its cost per read needs measuring first.

## Measurements

Local, Python 3.13, SQLite, one process. Each "book" is two metadata writes:
a WEMI stack, then a Tag added to its Work. Costs are averaged over the first
and last quarter of each run.

| Backend | Books | `main` (full reload) | Catch-up |
| --- | --- | --- | --- |
| `schema_backed` | 30 | 3.15 s | 0.26 s |
| `numpy_vectorized` | 30 | 5.18 – 5.68 s | 0.25 s |
| `schema_backed` | 150, first → last quarter | not run | 0.26 → 0.41 s |
| `numpy_vectorized` | 150, first → last quarter | not run | 0.24 → 0.47 s |

On a 30-book catalogue a write is 12–20 times cheaper. The cost still grows
with catalogue size, so CACHE06's done criterion (per-book cost flat with size)
is not met yet. Profiling `schema_backed` at about 20 books puts most of the
remaining catch-up time in reloading whole link tables and rebinding their
relation fields (about 70%), the rest in re-reading rows by ID. Pinning the
catch-up to one read transaction cut it from about 245 ms to 175 ms per write
by not opening a connection per refresh.

The comparison script creates a catalogue, loads a cache and times
`create_wemi_stack` plus `works.add_tag` per book.

## Row-level link refresh (2026-11-27)

The first version reloaded a whole link table, and rebound every field on it, for each changed link row. On `schema_backed` that cost grew with the link table. Now:

- **`SchemaBackedLinkTable.refresh_rows(ids)`** re-reads only those link rows by ID and repairs its record, endpoint, pair and row-ID indexes in place.
  - A changed row keeps its ordering position; a new row is placed after every existing one, as a full read in ID order would place it. A removed row is found through the cache's own copy, so the log's `old_ids` are not needed here.
  - Cardinality is reclassified from maintained counts of repeated endpoints, so there is no scan and no database query.
  - It returns the sources and destinations whose links changed. A table without an ID column returns None, and the view reloads in full.
- **`SchemaBackedStorageCache.reload_link_rows`** does that for every directed view of the table. It then calls `refresh_table_ids` on each relation field of the view, for just those sources. A stale view or field reloads in full as before.
- **`StorageCacheAPI.reload_link_rows`** is the default for other backends: it reloads each view in full.

The differential test now also compares every field's projected values with a fresh load. That covers ordered memberships with removals in the middle, and multiple credits. On `schema_backed` it also checks that no link table is reloaded. Skipping the field repair makes it fail.

| `schema_backed` | First quarter | Last quarter |
| --- | --- | --- |
| 30 books | 0.26 s | 0.20 s |
| 300 books | 0.22 s | 0.23 s |

Per-book cost is now flat with catalogue size on `schema_backed`, which is the default backend. That closes CACHE06 for it. About 120 ms of each write is catch-up: re-projecting fields, many of which deep-copy row snapshots, scales with the number of fields, not catalogue size. About 65 ms is opening connections (CR10).

## Remaining work

- **`numpy_vectorized`** still reloads a whole main table for `reload_ids`, and each view of a changed link table, so its cost still grows with size. It needs row-level refresh of its arrays.
- **Field re-projection** copies row snapshots; projecting without a deep copy would cut the catch-up's constant cost.
- **Connections on the write path.** Fixed on 2026-11-28 for the main cause: normalized-identity declarations are now read on the open transaction's connection instead of about 13 fresh connections per book. Each write transaction still opens one connection, which validates the schema revision. See CR10 in the [caches review](../architectural_reviews/caches-review-2026-11-26.md).
- **Automatic catch-up before reads,** as above.
