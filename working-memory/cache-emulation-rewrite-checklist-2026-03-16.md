# Cache / Emulation Rewrite Checklist

Date: 2026-03-16

## Scope

This note narrows the old `database_caches` / `databases_legacy` manifest cluster into concrete outcomes instead of leaving every row as generic `rewrite`.

Manifest snapshot after the active custom-column and relation-field rewrites:
- cache/emulation rows reviewed: `35`
- `covered`: `33`
- `retire`: `2`
- `rewrite`: `0`

## Reclassified Now

### Covered

- `src/LiuXin_tests/databases/self_test.py`
  - covered by:
    - `tests/databases/database/database_contract`
    - `tests/databases/database_driver_plugins/database_driver_contract`
    - `tests/databases/api`
- `src/LiuXin_tests/databases/legacy/custom_columns/self_test.py`
  - covered by:
    - `tests/databases/database/database_contract/test_db_custom_columns_calibre_style.py`
    - `tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py`
    - `tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py`
    - `tests/databases/database_calibre_emultation/test_calibre_emulation_d1_custom_columns_introspection.py`
    - `tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py`

### Retire

- `src/LiuXin_tests/databases/caches/memory_sqlite/self_test.py`
- `src/LiuXin_tests/databases/caches/memory_sqlite/tables/generic_tables_test.py`

Rationale:
- alpha does not ship a live `memory_sqlite` cache implementation
- reviving the old in-memory cache harness would be fake compatibility, not product coverage

## Remaining Rewrite Batches

### Batch A: Custom-column field semantics

Original rows:
- all `calibre/fields/custom_columns/.../*self_test.py`
- `cc_*_full_test.py`
- `calibre/fields/custom_columns/self_test.py`
- `calibre/tables/custom_columns/self_test.py`

Current alpha target seams:
- `tests/databases/caches/test_calibre_cache_03_custom_columns_bootstrap.py`
- `tests/databases/caches/test_calibre_cache_04_categories.py`
- `tests/databases/database_calibre_emultation/test_calibre_emulation_d1_custom_columns_introspection.py`
- `tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py`
- `tests/databases/database/database_contract/test_db_custom_columns_calibre_style.py`
- `tests/databases/database_driver_plugins/database_driver_contract/test_contract_custom_columns.py`

Rewrite goal:
- collapse the old datatype-by-datatype unittest tree into a small parametrized alpha suite covering:
  - metadata/bootstrap
  - lookup for one book / many books
  - update semantics
  - failure behavior
  - category visibility where relevant

Current status:
- first slice landed on the active emulation seam:
  - [custom-column-field-matrix-2026-03-16.md](custom-column-field-matrix-2026-03-16.md)
- second slice landed on active direct cache/category seams:
  - [custom-column-cache-semantics-2026-03-16.md](custom-column-cache-semantics-2026-03-16.md)
- implemented now:
  - parameterized scalar/comment/composite round-trips
  - multi-text dedupe/order semantics
  - series input-shape handling
  - datetime normalization
  - direct category-visibility rules for custom/composite fields
  - one-to-one custom-column precheck/update failure semantics
- manifest effect:
  - all `15` legacy custom-column field/table rows are now reclassified from `rewrite` to `covered`
  - DB-writing cache updates are intentionally left out of that claim and deferred to a future live calibre-like cache seam
- still open inside Batch A:
  - any DB-writing cache update semantics that still matter after the direct precheck coverage
  - decide whether the remaining legacy gated bootstrap/category modules now have enough active replacements to start retiring rows

### Batch B: Relation-field semantics

Status: covered

Original rows:
- `many_to_many/*self_test.py`
- `many_to_one/*self_test.py`
- `one_to_many/*self_test.py`

Active alpha replacements:
- [relation-field-matrix-2026-03-16.md](relation-field-matrix-2026-03-16.md)
- `tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py`
- `tests/databases/database/database_contract/test_db_interlink_read.py`
- `tests/databases/database/database_contract/test_db_interlink_write_update_unlink.py`
- `tests/databases/database_driver_plugins/database_driver_contract/test_contract_links.py`
- `tests/databases/database_driver_plugins/database_driver_contract/test_contract_links_non_exclusive.py`

Covered effect:
- all `16` legacy relation-field cache rows are now `covered`
- the active cache suite now pins the adapter-layer shape semantics
- the DB/driver contract suites continue to pin the lower-level interlink/update semantics

## Recommended Work Order

1. Batch A: custom-column field semantics
2. Batch B: relation-field semantics
3. If more cache/emulation work appears, treat it as a fresh seam rather than reviving the old cache unittest tree

Reason:
- Batch A and Batch B now both have active replacements in the default suite.
- The old cache/emulation rewrite bucket is no longer a generic backlog item.
- Any remaining work should be justified by a live seam, not by the existence of old tests.
