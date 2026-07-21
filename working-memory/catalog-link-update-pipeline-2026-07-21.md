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
- `to_dict()`, `pformat()`, and `str(...)` provide deterministic plain-data
  inspection of the route, type scope, ids, operations, and rich link values.

`LinkUpdateLink` is the frozen/slotted per-link inspection dataclass. It
records `src_id`, `dst_id`, operation, type, priority, and a read-only snapshot
of extras. `LinkUpdateEntry.links()`, `LinkUpdate.links_for_primary_id()`, and
`LinkUpdate.links()` return these views in primary-id and database-operation
order.

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

## Verification

Focused command:

```text
.venv/bin/python -m pytest -q \
  tests/catalog/test_catalog_imports.py tests/catalog/test_link_update.py
```

Latest result on 2026-07-21 after adding the catalog entry point:
`237 passed in 31.99s` under branch instrumentation.

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

Latest result: `256 passed, 1 skipped in 25.73s`; the skip is the existing
PostgreSQL-shaped SQLite harness case which has no `pg_temp` schema.

`py_compile` over the catalog implementation/API/common contract and focused
test modules also passed, as did targeted `git diff --check`.

This includes a real database round trip which accepts a mixed legacy map,
matches/creates string values with `ensure_table_value`, normalizes them to
ids, then applies replacement and incremental deletion/addition data through
`Catalog.write_link_update(...)`.

## Follow-up boundary

The normalized seam is now available without forcing an immediate rewrite of
all legacy writers. A later writer-migration slice can replace their local
duplicate elimination, id-map conversion, and specialized link SQL one field
family at a time, while retaining metadata-specific preflight and matching
policy in the resolver passed to `from_legacy(...)`.
