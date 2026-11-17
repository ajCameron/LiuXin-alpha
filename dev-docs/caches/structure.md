# Native caches and Calibre compatibility

Updated **2026-11-17**. This is the implemented package boundary following the
[compatibility review](calibre-compatibility-review-2026-11-16.md).

## Native owners

```text
LiuXin_alpha/
  caches/
    read_cache.py        lifecycle, reads, freshness and query indexes
    cache.py             Catalog writer bridge and reconciliation
    query.py             native queries, bounded plans/results and ID repair
    predicates.py        physical Boolean and relationship evaluation
    projection.py        explicit copied projection execution
    field_metadata.py    immutable native descriptor snapshots
    custom_fields.py     transaction-aware descriptor publication
    api/                generic cache/table/field/link contracts
    updates/            native update payloads
    cache_plugins/      registry and three storage backends
  catalog/search/
    database_search.py  WEMI search semantics and traversal
    physical_plan.py    mapping semantics to physical predicates
```

The `schema_backed`, `database_backed` and `numpy_vectorized` backends remain
native. Catalog owns bibliographic mutation policy; databases owns persistence
and physical SQL execution; caches owns derived state and recovery. Low-level
efficient SQL can remain in databases without moving field semantics there.

## Compatibility owners

```text
LiuXin_alpha/utils/calibre_compat/
  caches/
    base/               former plugins.cache contracts and implementations
    base_calibre/       shared Calibre field/view behavior
    calibre/            concrete cache, fields, tables and views
    write/              retained book-map writers and specialized field writers
    search/             retained legacy Search plus explicit metadata adapter
      adapter.py        projected CalibreBookRef search and template gate
      upstream*.py      attributed pinned parser/matcher/formatter
      provenance.json  source revision, licence and dated modification record
    categories.py       tag-browser categories and Tag records
    lazy.py             proxy book and format metadata
    utils.py            retained cache helpers
  metadata/
    field_adaptors.py   legacy coercion, defaults and custom-field conversion
    field_metadata.py   legacy built-ins, aliases, categories and result slots
    field_metadata_api.py
```

These are the real implementations at their new locations. Old imports from
`caches.write`, `plugins.cache`, `library.caches`, `library.lazy`,
`surfaces.categories`, `metadata.field_adaptors`, `catalog.field_metadata`,
`catalog.api.field_metadata_api` and `catalog.search.field_searches` are removed.
`catalog.search` now has a passive initializer and no legacy `Search` export.
The empty `library.tag_classes.BaseTagClass` placeholder is removed; legacy
field annotations use the actual compatibility `Tag` contract.

Callers must update imports. There are no forwarding modules. The compatibility
cache package initializer is passive; explicitly importing a concrete legacy
cache still assembles its dependencies. Compatibility field behavior is retained,
including the built-in factory actually used by both legacy metadata containers.

## Native field descriptors

[FieldMetadata](../../src/LiuXin_alpha/caches/field_metadata.py) is a read-only
mapping of canonical keys to immutable descriptors. Standard keys are physical
`table.column` names from main tables in `StorageSchemaSpec`; their datatype is
the SQL declared type or affinity. Custom keys are `#attachment.label`, with
datatype, collection mode, revision, editability and display metadata from native
definitions. Retired definitions are omitted. A short `#label` resolves only when
unambiguous through `search_term_to_field_key()`; direct indexing uses canonical
keys. Native Series fields do not create synthetic index companions.

`snapshot()` captures fields, aliases and generation together. Preparation builds
an immutable replacement while preserving standard fields. Publication swaps the
whole snapshot after commit, including when nested in an outer transaction;
rollback discards the callback. Registry-wide tickets prevent an older completion
from replacing a newer published snapshot, including reentrant receipt listeners
and different coordinators sharing the same registry. Preparation alone does not
advance generation. This addresses the reproduced CR01 ordering defect; it does
not establish general concurrent database snapshot ordering.

Core's default field metadata now uses this native registry. `catalog.fields.list`
and `catalog.fields.get` expose native descriptors, including qualified standard
keys. Consumers expecting `title`, `authors`, category maps, result slots or legacy
serialization must explicitly use compatibility metadata. Injected Core metadata
remains caller-owned until explicit refresh replaces it.

## Retained boundaries and next work

- Shared `utils.search_query_parser` grammar stays available to native search.
  Its saved-search support can be split separately after reviewing callers.
- Canonical metadata normalization stays in metadata; general conversion, text,
  locks and other primitives stay in their native utility owners.
- `library.library_metadata` transaction/write orchestration, the wider
  `metadata.book` family and `catalog.compatibility.calibre` conversion policy
  require separate reviews. Relocation does not turn them into cache helpers.
- CR02/CR03 are repaired; broader cache concurrency and registry-lifetime review remains open.
  This package boundary is not whole-cache sign-off or a performance migration.

Static ownership checks reject native cache/search/Core imports of the legacy
cache, descriptor and field-adaptor families, including deferred and type-only
imports. Fresh-process tests check initializer behavior. Source and wheel checks
reject the retired paths and require the replacement implementations. The broader
dependency ratchet still records existing cycles outside the protected acyclic
modules, including the native search/planner dependency.

Return to the [cache guide](README.md) or [compatibility guide](../calibre_compat/README.md).

The [metadata search contract](metadata-search.md) records field meaning, lifecycle
repairs, template permissions, cache bounds and remaining parity milestones.
