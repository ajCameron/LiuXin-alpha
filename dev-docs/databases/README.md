# Databases

Reviewed **2026-10-20**. Current schema, connections, backend status and historical notes have distinct authority. See [next tasks](next_tasks.md) for the proposed implementation order.

Databases owns persistence mechanics and structural integrity. Bibliographic
meaning belongs to metadata/Catalog; physical storage policy belongs to storage.

## Working guides and operational contracts

- [Schema overview](<../03 - Database Schema.md>): current WEMI graph, assets,
  replicas, generated relations, compatibility views and schema source map.
- [Worked link examples](wemi-link-examples.md): WEMI rows and paths, novels with
  introductions, omnibuses, shared editions, copy counts, primary selection,
  credits and payload attachments.
- [Fields](what-are-fields.md) and [column metadata](column-metadata.md).
- [Link capabilities](link-capabilities.md).
- [Creation and replacement](creation-and-replacement.md): create-only publication,
  explicit offline replacement, recovery backups and server-schema refusal.
- [Transaction ownership](transactions.md): resource cleanup, owned transactions,
  nested savepoints, borrowed connections and the remaining direct commit points.
- [Backend strategy](liuxin_database_backend_strategy.md) and
  [PostgreSQL runbook](postgresql-backend.md).
- [Connection ownership](<02 - Connection ownership and stale connections.md>) and
  [TEMP trigger cleanup](<03 - TEMP triggers for custom-column cleanup.md>).

## Schema design and historical notes

- [Database field guide](LiuXin_database_field_guide.md): current inventory of all 67
  explicit tables, checked against generated SQLite columns and linked to their DDL.
- [Schema draft](<00 - Schema Draft.md>): earlier design material.
- [Asset/replica policy](<04 - Digital assets, replicas and storage policy.md>):
  design history with a current implementation checkpoint.
- [Category terminology](<01 - Categories and the Tag Browser Misnomer.md>) and
  [WEMI graph](WEMI_as_a_Graph_ASCII.md).
- [Schema notes](liuXin_schema_notes.md), [metadata tables](liuXin_metadata_tables_notes.md),
  [triggers](liuXin_triggers_notes.md) and [tightened triggers](liuXin_triggers_tightened_notes.md).

The installed schema is owned by the packaged SQL tree described in
[packaging](../hacking/packaging.md); design sketches do not replace it.
For fixtures and validation, use [testing](../hacking/testing/README.md).
Return to the [developer index](../README.md).
