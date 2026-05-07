# Metadata Facade Workflows

Date: 2026-05-07
Status: working guide / current caller contract

This note describes the public workflow surface exposed from
`LiuXin_alpha.metadata`. It is deliberately about caller behavior, not the full
container internals.

Use the top-level facade when code wants to:

- hydrate metadata from a database or cache
- choose WEMI, LiuXin, or Calibre-shaped metadata views
- serialize metadata to OPF
- hydrate metadata from OPF
- write supported metadata changes back to a database

## Main entry points

The main read entry point is:

```python
from LiuXin_alpha import metadata

md = metadata.metadata_from_database(db, item_id=1)
```

By default this returns an item-centred `LiuXinWEMIMetadata` object hydrated
directly from the database.

Use `kind` when a caller needs another view:

```python
wemi_md = metadata.metadata_from_database(db, item_id=1, kind="wemi")
liuxin_md = metadata.metadata_from_database(db, item_id=1, kind="liuxin")
calibre_md = metadata.metadata_from_database(db, item_id=1, kind="calibre")
```

The accepted public kind names are:

- `wemi`
- `liuxin_wemi`
- `liuxin`
- `calibre`

`calibre` is a compatibility shape. It is useful at OPF and Calibre-adjacent
boundaries, but it is not the richest native object.

## Database and cache sources

Direct database hydration is explicit:

```python
md = metadata.metadata_from_database(db, item_id=1, source="database")
```

Cache-backed hydration is also explicit:

```python
md = metadata.metadata_from_database(db, item_id=1, source="cache")
```

The convenience wrapper is:

```python
md = metadata.cache_metadata_from_database(db, item_id=1)
```

Pass an already-loaded cache when the caller owns cache lifetime:

```python
md = metadata.cache_metadata_from_database(
    db,
    item_id=1,
    cache=storage_cache,
    allow_database_fallback=False,
)
```

`allow_database_fallback=False` is useful in tests where the cache path itself
must be pinned.

## Lazy metadata

Lazy hydration is for object inspection or workflows where only some metadata
families are needed:

```python
md = metadata.lazy_metadata_from_database(db, item_id=1)
```

Accessing a lazy field loads that field on demand. A caller can also force a
small set of fields up front:

```python
md = metadata.lazy_metadata_from_database(
    db,
    item_id=1,
    force_hydrate=("tags", "labels", "identifiers"),
)
```

Use `force_hydrate=True` only when the caller deliberately wants the full lazy
object materialized.

## OPF exchange

OPF helpers are available through the same facade:

```python
raw = metadata.metadata_to_opf_bytes(md)
metadata.metadata_to_opf_file(md, "metadata.opf")
from_opf = metadata.metadata_from_opf(raw, kind="wemi", database=db, item_id=1)
```

OPF is an exchange format here, not a full persistence format for the WEMI
graph. It can preserve common book metadata such as title, authors, tags,
series, publisher-ish fields, and identifiers, but it does not carry every
database relation edge or internal provenance field.

When hydrating WEMI from OPF, pass `database` and `item_id` when possible. That
lets the OPF fields overlay an existing WEMI chain instead of creating a thin
detached metadata object.

## Database write-back

Writing metadata back to the database is explicit:

```python
report = md.write_to_database(
    db,
    fields=("tags", "series", "identifiers"),
    item_id=1,
)
```

Supported write-back is relation/identifier focused. Current expected fields
include:

- `tags`
- `labels`
- `genre` / `genres`
- `subject` / `subjects`
- `series`
- `notes`
- `comments`
- `synopses`
- `identifiers`

Append mode is the default. It adds missing relation terms, links, and entity
identifier rows. Replace mode treats requested values as authoritative for the
target row:

```python
report = md.write_to_database(
    db,
    fields=("tags", "identifiers"),
    item_id=1,
    replace=True,
)
```

The write report exposes `changed`, `rows_added`, `rows_updated`,
`rows_removed`, `links_added`, `links_removed`, `skipped`, and `errors`.

## OPF round-trip smoke commands

Read-oriented smoke:

```bash
python scripts/metadata_opf_round_trip_smoke.py \
  LiuXin_data/test_databases/isfdb_mysql_55_2026_04_18/isfdb_mysql_55_2026_04_18.test_db \
  --lazy \
  --item-id 1 \
  --opf-dir .tmp/metadata-opf-smoke \
  --json-out .tmp/metadata-opf-smoke/report-large-lazy.json
```

Write-back smoke should normally run against a scratch copy:

```bash
python scripts/metadata_opf_round_trip_smoke.py \
  LiuXin_data/test_databases/isfdb_mysql_55_2026_04_18/isfdb_mysql_55_2026_04_18.test_db \
  --lazy \
  --item-id 1 \
  --write-back \
  --write-back-field tags \
  --add-tag smoke-writeback \
  --scratch-db .tmp/metadata-opf-smoke/writeback-smoke.test_db \
  --opf-dir .tmp/metadata-opf-smoke \
  --json-out .tmp/metadata-opf-smoke/report-writeback.json
```

The smoke script refuses write-back against the original database unless
`--allow-write-original` is passed. Prefer `--scratch-db` for real library or
generated ISFDB databases.

By default the smoke script opens the database without storage bootstrap and
without bootstrap repair writes. Pass `--repair-bootstrap-rows` only when the
test is meant to include startup rating/null-row repair behavior.

## Naming rule

Use `tags` for user/library-facing descriptive facets.

Use `labels` for operational or system-facing labels such as workflow states,
automation markers, import flags, command routing, or UI state.
