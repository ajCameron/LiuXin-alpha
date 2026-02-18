# Metadata dirtied queue + sidecar write-out

## What problem does this solve?

LiuXin needs a reliable way to notice *metadata-changing* operations (edits, imports, trigger-driven normalizations)
and later write updated sidecar files (e.g. OPF/JSON/YAML alongside content files) without re-scanning the entire library.

Two constraints drive the design:

1. Dirtied events can happen frequently and from many contexts (including triggers that bounce into Python),
   so enqueueing needs to be cheap.
2. Sidecar write-out may run later (or in a different process), so we also want an optional *persistent* record
   of dirtied events.

## Two layers

### 1) In-memory queue: `Database.dirty_records_queue`

- Type: `queue.Queue`
- Payload: `(table: str, row_id: int, reason: str)`
- API:
  - `Database.dirty_record(table, row_id, reason="...")` enqueues an event (only for `db.dirtiable_tables`)
  - `Database.get_dirtied_count()` returns the current queue size (approximate)

This is the "fast path" and is what triggers/tests use.

### 2) Persistent table: `metadata_dirtied_books` (historic name)

The helper table name is inherited from Calibre-era schema, but the intent is generic:
store enough information to later write sidecars.

Recommended logical fields are:

- dirtied_record_id (TEXT PK)          -> unique id
- dirtied_table (TEXT)                -> which table changed
- dirtied_row_id (INTEGER)            -> which row changed
- dirtied_reason (TEXT)               -> optional reason / provenance
- created/modified timestamps

The current FRBR-first schema includes a helper table named `metadata_dirtied_books`.
Column names may vary across fixture DB generations, so persistence code probes columns at runtime.

### Moving from queue -> table

`Database.persist_dirtied_records(limit=None)` drains queued events and inserts rows into `metadata_dirtied_books`.

Why a separate step?
- SQLite connections are typically not safe to use concurrently across many Python threads.
- Enqueueing is cheap and thread-friendly; persistence is best done in a single controlling loop.

### Counting persisted records

`Database.get_persisted_dirtied_count()` returns `COUNT(*)` from the helper table if it exists.

`Database.get_dirtied_count(include_persisted=True)` returns queue size + persisted count.

## How this fits sidecar write-out

A typical future sidecar writer can:

1. Periodically call `db.persist_dirtied_records()` (or on shutdown) to ensure durable tracking.
2. Read persisted events from `metadata_dirtied_books` in a stable order.
3. For each event, re-hydrate the metadata payload for `(table, row_id)` and write a sidecar file.
4. Delete or mark-complete the processed dirtied records (a follow-up API will formalize this).

## Notes / TODOs

- The helper table name should eventually be generalized (e.g. `metadata_dirtied_records`) with a migration shim.
- Consider a uniqueness constraint on `(dirtied_table, dirtied_row_id, dirtied_reason)` if you want dedupe.
- Consider a small retention policy (e.g. purge processed rows older than N days).
