# Catalog Link Update Pipeline

Date: 2026-07-21

## Goal

Provide one normalized catalog/database link-update value object for this
pipeline:

1. accept the conventional compact forms used in `caches.write` and
   `catalog.write`;
2. match metadata values to secondary-table ids, creating rows under the
   caller's metadata policy when needed;
3. normalize the result to stable relation-id-to-relation-id instructions;
4. write the complete change through the portable database macro layer.

## Current contract

`src/LiuXin_alpha/catalog/write/link_update.py` owns the immutable
`LinkUpdate` model.

- Direct construction is the strict boundary: operation maps contain only
  `LinkValue` objects and are snapshotted into read-only mappings of tuples.
- `from_ids(...)` accepts already-matched compact ids.
- `from_values(...)` sends every raw value through a supplied resolver.
- `from_legacy(...)` matches the actual older writer convention: integer ids
  pass through, non-integer metadata values go through the resolver, and rich
  `LinkValue` objects retain type, priority, and link-column extras.
- Compact inputs may be scalar, iterable, `None`, an `UpdateDict`/other
  mapping, or a typed nested mapping. Repeated logical identities are removed
  in first-seen order, matching the old writer duplicate elimination.
- The three operation kinds are replacements, deletions, and additions, in
  that order. `as_replacement_update(...)` reads current rows only for ids
  which lack an authoritative replacement and composes incrementals into a
  replacement-only update.
- `write(...)` sends that replacement-only map to `replace_links_bulk(...)`.
  Thus the final database mutation is one all-or-nothing portable macro call;
  an empty update makes no database call.

### Inspection and per-id access

`LinkUpdate` also provides quality-of-life access without exposing mutable
state:

- `mentioned_primary_ids` lists every id present in an input operation map;
- `primary_ids`, iteration, `len(...)`, membership, `keys()`, `values()`, and
  `items()` cover ids with effective work in stable operation order;
- `for_primary_id(id)` always returns an immutable `LinkUpdateEntry`, while
  `update[id]` raises `KeyError` and `get(...)` supports a default for an id
  with no effective work;
- each entry exposes ordered `operations`, `operation_names`, and explicit
  `has_replacement`, `clears_scope`, and `is_incremental` predicates;
- `iter_links()` streams immutable per-link views without changing the
  update's established source-ID iteration contract, while `links()` returns
  the same views as a tuple;
- `to_dict()`, `pformat()`, and `str(...)` provide deterministic plain-data
  inspection of the route, type scope, ids, operations, and rich link values.

`LinkUpdateLink` is the frozen/slotted per-link inspection dataclass. It
records `src_id`, `dst_id`, operation, type, priority, and a read-only snapshot
of extras. It implements `Mapping[str, Any]` over those extras, so indexing,
`get`, membership, `keys`, `values`, `items`, length, and `dict(link)` work as
expected without mixing endpoint fields into the extra namespace.
`LinkUpdateEntry.iter_links()` and `LinkUpdate.iter_links()` stream views in
primary-id and database-operation order. Their `links()` counterparts and
`LinkUpdate.links_for_primary_id()` return materialized tuples.

A destination-value resolver can be bound through `links(dst_value_for=...)`
or supplied on the first `link.get_dst_value(resolver)` call. Construction,
`repr`, `to_dict`, `pformat`, and `str` never invoke it. A successful result,
including `None`, is cached per link view and exposed through the lazy
`dst_value` property; loader failures remain retryable.

An empty replacement remains effective because it clears links. Empty
addition/deletion entries remain visible through `mentioned_primary_ids` but
are excluded from effective collection access and now make no database call.

The resolver is intentionally injected. Existing catalog matching/create
helpers can be passed directly; portable callers can use
`db.macros.ensure_table_value(...)` so database-owned comparison and
normalization policies remain authoritative.

The concrete shared-value catalog writer is operation-aware: replacements and
additions ensure values, while deletions use `find_table_value(...)`. The latter
shares normalization, comparison-column, case, and identity-scope policy but
never inserts a row; deleting an unknown value is therefore a no-op.

### Catalog write entry point

`Catalog.write_link_update(update)` is the public facade method for applying a
normalized `LinkUpdate`. `CatalogAPI` declares the same method, and the minimal
catalog `DatabaseHandle` now declares its portable `macros` property.

The catalog method validates that its argument is a `LinkUpdate`, delegates to
`update.write(catalog.db.macros)`, and returns the complete `LinkRow` tuples by
source id. It therefore preserves the aggregate's scoped replacement,
incremental composition, atomic bulk-write, and empty no-op semantics. The
per-link `LinkUpdateLink` inspection view is deliberately not writable on its
own because it does not carry the complete link specification or update scope.

## Semantics worth preserving

- An omitted primary id is untouched; an empty replacement clears its links
  in scope.
- Deletions identify link rows and never delete the secondary-table row.
- Link type is part of identity only when `StorageLinkSpec` says it is.
- A type-scoped update is valid only for a typed link whose type is part of
  identity. Other link types remain untouched.
- Incremental additions use portable upsert behavior: omitted priority and
  extra columns are preserved from an existing link.
- Existing authoritative replacements do not require a database read.
- Explicit link types are rejected before destination resolution when the
  link is untyped, the value is not a non-blank string (apart from the valid
  SQL-null `None` type), or it is absent from a declared allowed set.
- `StorageLinkSpec.allowed_types` is the static restriction. An optional
  `allowed_types_table` is read live through the driver wrapper for each named
  typed write; both restrictions apply when both exist.
- The portable macro layer repeats type validation before persistence so a
  directly submitted `LinkUpdate` cannot bypass the catalog-writer guard.

## Verification

Focused command:

```text
.venv/bin/python -m pytest -q \
  tests/catalog/test_catalog_imports.py tests/catalog/test_link_update.py
```

Latest result on 2026-07-21 after adding the catalog entry point:
`237 passed in 31.99s` under branch instrumentation.

Latest focused result after adding per-link iteration and the read-only extra
mapping interface on 2026-07-22: `241 passed in 32.68s`.

Focused coverage was measured with Coverage.py 7.15.2 installed only under
`/tmp` (the project environment and dependency files were not changed):

```text
PYTHONPATH=/tmp/liuxin-coverage-tool \
COVERAGE_FILE=/tmp/liuxin-catalog-link-update.coverage \
.venv/bin/python -m coverage run --branch \
  --include='*/catalog/catalog.py,*/catalog/write/link_update.py' \
  -m pytest -q \
  tests/catalog/test_catalog_imports.py tests/catalog/test_link_update.py

PYTHONPATH=/tmp/liuxin-coverage-tool \
COVERAGE_FILE=/tmp/liuxin-catalog-link-update.coverage \
.venv/bin/python -m coverage report -m --fail-under=100 \
  src/LiuXin_alpha/catalog/catalog.py \
  src/LiuXin_alpha/catalog/write/link_update.py
```

Result: all `387` statements and all `120` branches across the changed runtime
modules are covered (`100%`). The catalog-specific contracts cover facade/API
shape, successful delegation and result propagation, scoped updates, invalid
arguments, empty no-ops, and the real-database replacement/incremental path.

Broader catalog plus portable-macro regression command:

```text
.venv/bin/python -m pytest -q tests/catalog tests/databases/api/test_portable_macros.py tests/databases/api/test_portable_macros_real_db.py
```

Latest result on 2026-07-22: `314 passed, 1 skipped in 334.89s`; the skip is
the existing PostgreSQL-shaped SQLite harness case which has no `pg_temp`
schema.

After adding static and live link-type guards, the focused catalog writer,
link update, factory, and portable-macro lane completed with
`315 passed, 1 skipped in 312.37s`. Coverage includes rich `LinkValue` types,
nested typed maps, update scopes, untyped links, blank/non-string types,
SQL-null types, one-shot iterables, fail-before-resolution behavior, direct
macro writes, and live registry extension on a real schema-discovered writer.

`py_compile` over the catalog implementation/API/common contract and focused
test modules also passed, as did targeted `git diff --check`.

This includes a real database round trip which accepts a mixed legacy map,
matches/creates string values with `ensure_table_value`, normalizes them to
ids, then applies replacement and incremental deletion/addition data through
`Catalog.write_link_update(...)`.

## Follow-up boundary

The incomplete catalog-only base/cardinality writer hierarchy has been retired;
the live cache writers remain untouched. The accepted replacement boundary and
rebuild sequence are documented in
`docs/development/catalog-link-writer-architecture.md` and the active handoff
is `working-memory/catalog-writer-reset-2026-07-21.md`.

An abstract `CatalogLinkWriter` now exists over this normalized seam with
separate build and apply phases inherited from a storage-neutral
`BaseCatalogWriter`. Concrete field writers override adaptation, validation,
and destination resolution. The link specialization accepts every declared
link cardinality and enforces the source-side multiplicity visible within a
request. Later migration can replace live cache-writer duplicate elimination,
id-map conversion, and specialized link SQL one field family at a time while
keeping metadata-specific matching in dedicated resolver strategies.

`create_catalog_writer(catalog, src_table, dst_column)` now resolves this link
specialization from schema metadata, or selects `CatalogColumnWriter` when the
destination column is stored directly on the source row. The factory-created
link writer resolves raw destination-column values with `ensure_table_value`;
already-resolved IDs are explicit `LinkValue` instances.

The factory now selects `CatalogOwnedRowOneToOneWriter` for exact one-to-one
routes. That path deliberately does not construct a `LinkUpdate`: its atomic
unit includes both the destination-row mutation and the link mutation, so it
uses `CatalogOwnedRowUpdate` and
`replace_owned_one_to_one_values_bulk(...)`. Existing destination IDs are
retained across value changes; `None` unlinks without deleting the row.

`StorageCacheAPI` now exposes `create_writer`, `write`, and `write_one` over
the same factory and catalog application seam. A cache-created writer keeps
all normalized update inspection and typed-link behavior while a private
catalog facade reconciles the cache after a successful application. Snapshot
backends reload only the affected main/destination and directed link tables;
the live database-backed cache requires no explicit refresh. Invalid or empty
writes leave cache state alone, and a writer rejects use after its cache is
detached or reattached. The public `cache.catalog` property aliases the
attached `cache.db` handle required by the established lifecycle contract.

Cache-writer coverage runs the same scalar, owned one-to-one, shared
many-to-many, typed-link, live-registry, build/apply, bulk/single, refresh, and
detach-safety contracts against the schema-backed, database-backed, and NumPy
backends. The focused writer slice passes `15` cases; the broader cache
import/plugin/schema/real-database/field regression lane passes `90` cases in
`86.80s`. Its real-database case covers schema discovery, scalar and typed-link
writes, fail-before-creation type validation, and refreshed cache reads.
