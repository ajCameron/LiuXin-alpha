# Cache compatibility boundary review — 2026-11-16

Status: **review and relocation implemented; validation below.** Original source
checkpoint: `2d2a9cc304b5394ab90003b0567c4d06130337f1`. This follows the
[publication/lifecycle review](../architectural_reviews/caches-review-2026-11-16.md)
and the user's request to identify what belongs in `utils.calibre_compat`.

## Boundary and relocation map

Use the existing `LiuXin_alpha.utils.calibre_compat` namespace, adding a `caches`
subpackage. Classify by the implemented contract: book/title maps, Calibre field
conventions, template/category behavior and legacy calling conventions belong
there. Provenance, generic names such as "field", or examples using a books table
do not alone determine ownership.

Compatibility adapters may consume native Catalog/database services and canonical
metadata normalization. Native cache behavior must not depend on the relocated
Calibre cache implementation. Keep compatibility package initializers passive.
This boundary is not a reason to place general cache or bibliographic policy in
utility modules.

The table preserves the original caller review. Its destinations are now
implemented, relative to `utils.calibre_compat`; source links follow the moves.
See [current structure](structure.md) for the native descriptor split and API changes.

| Current owner | Proposed destination | Evidence |
| --- | --- | --- |
| [caches/write](../../src/LiuXin_alpha/utils/calibre_compat/caches/write), 15 Python files | `caches.write` — first slice | Legacy `set_books`, title/book maps, case-change handling and specialized field writers. Only two external production importers: `library.caches.base_calibre.fields` and `plugins.cache.base_field`. Modern `Cache.create_writer()` uses Catalog writers. |
| [plugins/cache](../../src/LiuXin_alpha/utils/calibre_compat/caches/base), five files | `caches.base` | Legacy base cache, fields, tables and API decorators. All external production imports found are in `library.caches`. Separate from the modern backend registry and unified plugin host. |
| [library/caches](../../src/LiuXin_alpha/utils/calibre_compat/caches), 31 files | `caches.base_calibre`, `caches.calibre`, `caches.utils` | Concrete cache, views, fields and typed/priority table variants. Preserve internal structure initially. No external production imports found in this repository; tests instantiate it. That is insufficient evidence for deletion or absence of external users. |
| Legacy [catalog/search/__init__.py](../../src/LiuXin_alpha/utils/calibre_compat/caches/search/__init__.py) and `field_searches/` | `caches.search` | Cache-hosted `Search`, `Parser`, saved searches, LRU results, match helpers and typed operators. The old Calibre cache consumes this engine. Keep native search files in Catalog. |
| [metadata/field_adaptors.py](../../src/LiuXin_alpha/utils/calibre_compat/metadata/field_adaptors.py) | `metadata.field_adaptors` | Legacy field coercion/defaults. Three production consumers: the old base writer, Calibre cache and existing compatibility custom-column facade. Preserve delegation to canonical language/identifier normalization. |
| [surfaces/categories.py](../../src/LiuXin_alpha/utils/calibre_compat/caches/categories.py) | `caches.categories` | Legacy tag-browser items and category generation over book maps and preferences. Its sole production importer found is the old Calibre cache. Modern Core categories are separate. |
| [library/lazy.py](../../src/LiuXin_alpha/utils/calibre_compat/caches/lazy.py) | `caches.lazy` | `ProxyMetadata`, `FormatMetadata`, `FormatsList` and Calibre-cache getters. Its sole production importer found is the old Calibre cache. |

[library/tag_classes.py](../../src/LiuXin_alpha/utils/calibre_compat/caches/categories.py) contains an
empty `BaseTagClass`; its only production use found is an annotation in
`base_calibre.fields`. Prefer replacing it with the actual compatibility item
contract and deleting the placeholder. If its identity must be retained, keep it
inside the compatibility family.

`LRUCache` currently serves the legacy search engine and tests. Move it with that
engine; extracting a general utility needs a concrete additional consumer.

## Split before moving

**Native search stays in Catalog.**
[database_search.py](../../src/LiuXin_alpha/catalog/search/database_search.py)
implements WEMI matching/traversal and is imported directly by `Catalog`.
[physical_plan.py](../../src/LiuXin_alpha/catalog/search/physical_plan.py) maps
those semantics to physical predicates; databases executes the SQL. Move only
the legacy package-initializer engine and field operators, then make the native
initializer passive.

A fresh-process probe confirmed that importing native `CatalogSearch` currently
defines legacy `Search` and loads all three legacy field-operator modules through
the parent initializer. The shared
[search_query_parser.py](../../src/LiuXin_alpha/utils/search_query_parser.py)
also mixes grammar and saved-search support. Native search needs its `Parser` and
`ParseException`; moving that whole utility into cache compatibility would create
the wrong dependency. A later grammar/support split is a separate task.

**Field metadata has native consumers.** Do not relocate
[catalog/field_metadata.py](../../src/LiuXin_alpha/utils/calibre_compat/metadata/field_metadata.py)
wholesale. Core, native custom-field publication, book metadata/JSON conversion,
legacy tables and compatibility custom columns use it. `FieldMetadata` itself
contains legacy built-ins; `CalibreFieldMetadata` is not the only compatibility
surface.

Extract native descriptor/projection operations from book layouts, synthetic
Series companions, legacy aliases and category/serialization contracts. The latter
belong in compatibility metadata. Keep attachment-qualified identities, collection
modes, revisions and commit-only publication in a cache-owned native registry.
Review `catalog.api.field_metadata_api` with that split. Moving the implementation
and making native callers import it from `utils` would preserve the coupling.

Both current containers use `_builtin_field_metadata` and `_CustomFieldRefresh`;
their constructors do not use `_calibre_builtin_field_metadata`. Preserve observed
behavior during extraction rather than selecting a differently named factory.

[library/library_metadata.py](../../src/LiuXin_alpha/library/library_metadata.py)
needs a separate adapter review: its flat-book entry points serve the old cache,
but it coordinates transactions and Catalog writes, with aggregate/rating tests.
Do not move that persistence orchestration mechanically as a cache helper.

## Keep in native owners

- `caches.cache`, `read_cache`, `query`, `custom_fields`: composition, reads,
  query indexes, publication, freshness and recovery.
- All three `cache_plugins` backends and the registry, including generic schema,
  row, field and relationship handling.
- Generic cache APIs, table/field/link contracts and cardinalities. Older names
  and low-level mutation methods need their own API audit, not blanket relocation.
- `caches.updates`: field payloads are imported by current field APIs and table
  payloads are referenced by table contracts. Audit or retire unused pieces as
  native cache work; age or defects do not make them Calibre semantics.
- Canonical language, identifier, author/title and value rules; general conversion,
  locking and text primitives. Compatibility adapters should reuse these owners.

Existing `catalog.compatibility.calibre` conversion policy and the wider
`metadata.book` family are outside this first relocation slice; semantic writes
and shared consumers require their own review.

## Sequence and completion checks

1. Move the 15 writer modules, both production callers, internal imports and tests
   together. Update the writer-specific protected prefix and ownership rule in
   `scripts/check_modern_import_cycles.py` so relocation preserves enforcement.
2. Move field adapters and split legacy/native search in separately reviewable
   changes. Preserve normalization and native search results; update adapter
   ownership documentation and fresh-process import checks.
3. Move the legacy cache/base family together with category/lazy helpers. Preserve
   the unified plugin framework and modern backend registry. Update identity tests
   and measured layer-direction edges; moving category generation removes the old
   cache's upward import of `surfaces.categories`.
4. Split field metadata with dedicated native and compatibility contracts. Coordinate
   publication repair with CR01; relocation alone does not repair it.

Follow the earlier explicit-break preference: migrate callers and remove old
module paths without forwarding facades in `caches`, `library` or `plugins`.
Check wheel contents and string/dynamic references as well as imports. Preserve
existing attribution when moving files. Add guards preventing modern cache
facades/backends from importing the relocated compatibility cache family and
native Catalog search imports from initializing the legacy search engine.

## Executed evidence and limits

An AST inventory resolved absolute and relative imports across `src/LiuXin_alpha`,
with no source files skipped. Tests and likely string/import sites were inspected.
This identifies repository callers, not every third-party plugin or external user.

Focused baseline: **233 passed**.

```bash
.venv/bin/python -m pytest -q \
  tests/databases/caches/test_writer_dependencies.py \
  tests/databases/caches/test_custom_writer_adapters.py \
  tests/databases/write/test_write_import_smoke.py \
  tests/metadata/test_field_adaptors.py \
  tests/metadata/test_field_adaptor_boundaries.py \
  tests/catalog/test_field_metadata_and_search.py \
  tests/catalog/test_field_search_operators.py \
  tests/catalog/test_search_core.py \
  tests/plugins/test_package_structure.py
```

Native search/shared metadata baseline: **59 passed**.

```bash
.venv/bin/python -m pytest -q \
  tests/catalog/test_wemi_database_search.py \
  tests/catalog/test_field_metadata_contracts.py
```

Fresh-process imports of `caches.read_cache` and `caches.custom_fields` loaded no
old writer/library-cache/plugin-cache or legacy search-operator modules. Native
Catalog search loaded legacy operators through its parent, as described above.

These **292 passing cases** establish the pre-migration baseline. The sections
above preserve the original recommendations, not an outstanding move request.
The implementation and new API contracts are recorded in [structure](structure.md).
CR01 is repaired through registry-wide publication tickets; CR02/CR03 remain open.

## Implementation validation

All 61 source relocations are present; retired implementation paths and the empty
Tag placeholder are absent. The native descriptor registry, Core consumer and
cache publication coordinator are separate from compatibility metadata. Current
contracts and deliberate API breaks are in [structure](structure.md).

Executed after relocation:

- Compatibility/field/search suite: **291 passed**.
- Entire default cache test directory: **220 passed, 20 skipped**. Skips cover
  six opt-in legacy-schema modules, six backend capability cases and eight live
  PostgreSQL cases. The retained generated legacy-cache matrix was not enabled.
- Broader cache/native-field/compatibility/Catalog/book/Core selection:
  **803 passed, 20 skipped**, initially two failed assertions and one teardown
  error. The native ownership test was migrated to the native registry and its
  `id` descriptor; the corrected cases and Core integration then passed in a
  focused **four-case** run. The teardown error came from concurrent wheel
  construction and the test root-artifact cleanup, not search behavior; the
  complete cache rerun and final boundary run passed.
- Final boundary/import-checker/wheel-checker suite: **159 passed**. Layer/import
  checker regressions separately passed **66** cases. Documentation passed **22**
  checks using a temporary candidate index; the real index was unchanged.
- `bash scripts/run_type_checks.sh` passed formatting, lint, annotation coverage,
  protected cycles, layer direction, cold imports, complexity, basedpyright and
  mypy, including their internal contract rejection probes. Eleven new mandatory
  cases raise the database regression manifest from **2,462 to 2,473** without
  removing prior cases. This was not a full live database-ratchet run.
- Manual review probes now report **two passes, three expected failures and three
  skips**: CR01 is fixed on both SQLite drivers; CR02/CR03 remain reproducible.

A clean source snapshot built a wheel outside the repository. Both content
verifiers passed; a target installation loaded all selected native/compatibility
modules from that installation, rejected all ten retired module paths and passed
native publication plus default Core-registry checks. The earlier concurrent
build was discarded as evidence because test cleanup partially removed it.
Clean verification also exposed missing EBNF/kakasi data declarations; the package
manifest now includes those required existing resources.

No full project suite, live PostgreSQL, native Windows or performance run is
claimed. These results establish the relocation and reviewed contracts, not cache
sign-off or resolution of CR02/CR03.

Return to the [cache guide](README.md) or
[Calibre compatibility guide](../calibre_compat/README.md).
