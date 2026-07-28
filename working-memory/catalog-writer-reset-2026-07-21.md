# Catalog Writer Reset

Date: 2026-07-21

## Decision

The legacy catalog-only `BaseCatalogWriter` and cardinality-named hierarchy are
not the foundation for future catalog writes. They were incomplete cache-writer
migration code with no live catalog consumers, no direct tests, mismatched
`CatalogAPI` assumptions, blocked construction, and broken runtime paths.

A new minimal `BaseCatalogWriter` now reuses the old name but none of the old
implementation. It owns the bulk and single-pair build-then-apply lifecycles.

The replacement architecture is recorded in
`dev-docs/catalog-link-writer-architecture.md`.

## Stable footing retained

- `StorageLinkSpec` declares physical relation capabilities and identity.
- `LinkUpdate` accepts and normalizes complete and incremental relation
  instructions.
- `LinkUpdateLink` provides immutable per-link inspection and lazy destination
  display-value loading.
- `Catalog.write_link_update(...)` applies a normalized update through the
  catalog database's portable macros.
- Portable macros own atomic persistence and backend-specific execution.

## Catalog-only legacy removal scope

Remove the legacy implementations of:

- `catalog/write/base_writer.py`;
- `catalog/write/generic_writers/`;
- `catalog/write/uuid_writer.py`.

Preserve:

- `catalog/write/link_update.py`;
- `catalog/write/__init__.py` and its `LinkUpdate` exports;
- every file under `caches/write/`.

The catalog UUID writer was removed with the hierarchy. Scalar fields may
inherit the new storage-neutral lifecycle and value-preparation base, but they
do not belong in the link-writer specialization. `CatalogColumnUpdate` and
`CatalogColumnWriter` now provide that separate scalar mutation path.

## Writer responsibility

A future catalog link writer translates metadata intent into a normalized
catalog mutation:

```text
adapt/validate -> resolve destination IDs -> LinkUpdate
    -> Catalog.write_link_update -> LinkRow results
```

It must not own SQL, locking, transactions, cache mutation, schema discovery,
matching algorithms, implicit garbage collection, or partial per-link writes.

## Implemented foundation

`src/LiuXin_alpha/catalog/write/base_writer.py` now provides regular abstract
`BaseCatalogWriter` and `CatalogValueWriter` classes. The first owns
`build_update -> apply_update` and
`build_one_update -> apply_update`; the second adds `adapt -> validate` and
the default one-entry update construction.

`src/LiuXin_alpha/catalog/write/link_writer.py` provides abstract
`CatalogLinkWriter`. Concrete field writers implement `adapt` and
`resolve_destination` and may override `validate`. `StorageLinkSpec` remains
explicit constructor configuration rather than being discovered during a
write.

`CatalogColumnWriter` is the concrete same-table leaf.
`CatalogOwnedRowOneToOneWriter` is the concrete separate-table leaf for an
owned one-to-one destination. Its `CatalogOwnedRowUpdate` is applied through
`Catalog.write_owned_row_update(...)` and one portable transaction: update the
linked row in place, create-and-link when missing, or unlink on `None` without
implicit destination-row deletion.

`CatalogTableValueLinkWriter` is the shared separate-table leaf. Replacements
and additions delegate destination matching/creation to `ensure_table_value`;
deletions use the policy-equivalent, non-creating `find_table_value` and ignore
an absent value. The public
`create_catalog_writer(catalog, src_table, dst_column)` factory resolves the
destination table and directed link specification from schema metadata. It
selects owned-row policy for `ONE_TO_ONE` and shared-value policy for the other
cardinalities. It rejects ambiguous columns and missing links rather than
depending on iteration order.

The concrete `Catalog` facade and `CatalogAPI` protocol expose the factory as
`create_writer`, with `write` and `write_one` conveniences which forward to the
selected writer. Normalized update application remains on the existing
`write_column_update`, `write_link_update`, and `write_owned_row_update` seam.

The abstract link writer's `build_update(...)` constructs an inspectable
`LinkUpdate.from_legacy(...)` without writing. The shared-value leaf tightens
that boundary with `LinkUpdate.from_values(...)`; the owned-row leaf constructs
`CatalogOwnedRowUpdate` instead. `write(...)` constructs the applicable update,
calls its catalog application method once, and returns the resulting link rows
unchanged.

Raw metadata methods run as `adapt -> validate -> resolve_destination`.
Existing integer IDs and rich `LinkValue` instructions bypass that path on the
abstract legacy-compatible link base. The column-oriented concrete link writer
treats raw integers as destination-column values; `LinkValue(id)` is the
explicit already-resolved form.
Singular-source cardinalities reject multiple destinations in one operation;
plural-source cardinalities accept them. Configuration errors fail during
construction, and write failures propagate without hidden logging, retries,
cache updates, or cleanup.

Every concrete writer exposes `write_one(src_id, dst_value, **kwargs)`. It is
the one-entry form of the normal write, returns the usual source-keyed result
mapping, and traverses the same adapter, validator, normalized update, catalog,
and database paths. On a link writer it is authoritative replacement;
incremental linking remains explicit through `write(additions=...)`.

Link writers now validate every explicit type form before resolving or
creating destination values. Untyped links reject named types; named values
must be non-blank strings and must satisfy the static
`StorageLinkSpec.allowed_types` tuple and the live `allowed_types_table` when
present. `None` remains the valid SQL-null link type. `LinkUpdate` repeats the
static checks and portable macros repeat the live check at persistence time.
The registry is read through `DatabaseDriverWrapperAPI` without writer-level
caching, so an allowed type added to the database is immediately usable by an
existing writer.

The writer workflow is also available from every `StorageCacheAPI` backend:
`create_writer(src_table, dst_column)` returns a cache-bound concrete catalog
writer, while `write(...)` and `write_one(...)` are construction-and-apply
conveniences. Persistence remains catalog-owned. After success, snapshot
caches reload the affected table/link objects and the live database-backed
cache observes the result naturally. Cache-created writers reject use after
detach/reattach, and invalid writes fail before either database mutation or
cache reconciliation.

The cache writer surface is exercised identically across all three registered
cache backends. The focused cache-writer slice passes `15` cases and the wider
storage-cache regression lane passes `90` cases in `86.80s`, including a real
schema-discovered scalar and typed-link round trip.

The next build slices are metadata-specific adapters, validators, and resolver
policies where the two generic separate-table policies are insufficient,
followed by deliberate field-family migration and class update/change work.

## Full-pass correction, 2026-07-22

The owned-row operation and the final normalized link replacement are atomic.
The complete shared raw-value workflow is not: `build_update()` calls
`ensure_table_value()` in separately committed transactions before the later
link replacement. It also performs cardinality validation after those ensures.
An invalid or later-failing request can therefore leave newly created,
unlinked destination rows. Statements above that describe shared-value update
construction as non-writing or the complete workflow as all-or-nothing are
superseded by this correction. The durable finding and acceptance criterion
are recorded in `dev-docs/catalog-fitness-review.md`.

## Verification

The catalog-only legacy sources were deleted, including generated bytecode
directories. `catalog/write/` now contains `__init__.py`, the retained
`link_update.py`, the new base/link foundations, normalized column and owned-row
updates, three concrete schema-backed leaves, and the writer factory.

Source-reference audit:

```text
rg -n "LiuXin_alpha\.catalog\.write\.(base_writer|generic_writers|uuid_writer)|BaseCatalogWriter|OneToOneCatalogWriterBase" src tests scripts -g '*.py'
```

Result before introducing the replacement base: no stranded legacy imports or
references. Current `BaseCatalogWriter` references target the new foundation.

Public import and compile checks passed for `Catalog`, `CatalogAPI`,
`CatalogLinkWriter`, `LinkUpdate`, `LinkUpdateEntry`, and `LinkUpdateLink`.

```text
.venv/bin/python -m pytest -q \
  tests/catalog \
  tests/databases/api/test_portable_macros.py \
  tests/databases/api/test_portable_macros_real_db.py
```

Result: `256 passed, 1 skipped in 29.78s`. The skip is the existing
PostgreSQL-shaped SQLite harness case without a `pg_temp` schema.

### Foundation verification

Focused command:

```text
.venv/bin/python -m pytest -q \
  tests/catalog/test_link_writer.py tests/catalog/test_catalog_imports.py
```

Initial configured-writer result: `20 passed in 31.43s` under branch
instrumentation.

After the inheritance refactor, the focused suite covers the abstract base,
same-table scalar extension seam, other-table one-to-one link behavior, and all
four link cardinalities: `25 passed in 15.82s`.

Coverage result for `catalog/write/link_writer.py`: all `37` statements and
all `8` branches covered (`100%`).

The broader catalog plus portable-macro lane after adding the writer completed
with `271 passed, 1 skipped in 44.26s`; the skip is unchanged.

After the inheritance refactor, that same broader lane completed with
`276 passed, 1 skipped in 32.15s`; the skip is unchanged.

### Factory verification

The factory-focused lane covers same-table source-column preference, all four
link cardinalities, raw numeric destination values, explicit resolved IDs,
ambiguous and missing targets, and real-database column/link round trips:
`40 passed in 58.42s`.

After the factory and concrete schema-backed leaves were added, the broader
catalog plus portable-macro lane completed with
`291 passed, 1 skipped in 78.69s`; the skip is unchanged.

### Specialized policy verification

Owned-row contract and factory tests cover immutable update values,
adapt/validate ordering, explicit unlink handling, invalid values and targets,
catalog facade delegation, one-to-one factory selection, real-database
create/link, stable-ID in-place update, and unlink-without-cleanup:
`24 passed in 107.26s` under branch instrumentation.

The additional real shared-value factory test passes for both database fixture
variants and proves find-only deletion leaves the destination-row count
unchanged: `2 passed in 57.55s`.

Portable macro tests cover policy-equivalent find-only matching, absence
without insertion, owned-row create/update/unlink behavior, destination-row
retention, and transaction rollback for both SQLite and the PostgreSQL-shaped
harness.

The focused base/link/owned/factory writer lane after adding `write_one`
completed with `47 passed in 272.14s`.

The complete catalog plus portable-macro regression lane after adding
`write_one` completed with `314 passed, 1 skipped in 334.89s`; the skip is the
unchanged `pg_temp` limitation of the PostgreSQL-shaped SQLite harness.

Compilation and targeted diff checks passed.

The focused type-guard lane completed with `315 passed, 1 skipped in
312.37s`; the skip remains the PostgreSQL-shaped `pg_temp` limitation.
