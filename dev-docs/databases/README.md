# Databases

Databases owns persistence mechanics and structural integrity. Bibliographic
meaning belongs to metadata/Catalog; physical storage policy belongs to storage.

## Working guides and operational contracts

- [Database field guide](LiuXin_database_field_guide.md), [fields](what-are-fields.md)
  and [column metadata](column-metadata.md).
- [Link capabilities](link-capabilities.md).
- [Backend strategy](liuxin_database_backend_strategy.md) and
  [PostgreSQL runbook](postgresql-backend.md).
- [Connection ownership](<02 - Connection ownership and stale connections.md>) and
  [TEMP trigger cleanup](<03 - TEMP triggers for custom-column cleanup.md>).
- [Asset/replica policy](<04 - Digital assets, replicas and storage policy.md>).

## Schema design and historical notes

- [Schema overview](<../03 - Database Schema.md>) and [draft](<00 - Schema Draft.md>):
  earlier design material with outstanding rewrite work.
- [Category terminology](<01 - Categories and the Tag Browser Misnomer.md>) and
  [WEMI graph](WEMI_as_a_Graph_ASCII.md).
- [Schema notes](liuXin_schema_notes.md), [metadata tables](liuXin_metadata_tables_notes.md),
  [triggers](liuXin_triggers_notes.md) and [tightened triggers](liuXin_triggers_tightened_notes.md).

The installed schema is owned by the packaged SQL tree described in
[packaging](../hacking/packaging.md); design sketches do not replace it.
For fixtures and validation, use [testing](../hacking/testing/README.md).
Return to the [developer index](../README.md).
