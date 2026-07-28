# Custom-Column Cache Semantics

Date: 2026-03-16

## Scope

This is the second active Batch A slice in the cache/emulation rewrite stream.

It adds a small non-gated replacement test file instead of extending the legacy
`LIUXIN_ENABLE_LEGACY_CALIBRE_CACHE_TESTS` modules.

## Landed

- new active test file:
  - [test_calibre_cache_06_custom_column_semantics.py](../tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py)

## What It Covers

- category visibility rules at the direct `find_categories(...)` seam:
  - books-attached custom categories are exposed
  - non-books-attached custom fields are excluded
  - composite custom fields only appear when `display.make_category=True`
  - composite custom fields on non-books tables stay excluded
  - the returned category tuple correctly marks composite fields as composite

- one-to-one custom-column update validation at the direct table seam:
  - scalar values and `None` are accepted for known books
  - container-valued updates are rejected
  - unknown book ids are rejected
  - acceptance-function failures are surfaced as `InvalidCacheUpdate`

## Why This Seam

- keeps the replacement active in the default suite
- avoids inflating the old gated cache harness
- captures the behavior we still care about without reviving the old field-by-field unittest matrix

## Validation

- targeted file:
  - `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py`
  - `7 passed`

- combined Batch A slice:
  - `PYTHONPATH=src:. .venv/bin/python -m pytest -q tests/databases/database_calibre_emultation/test_calibre_emulation_d1_custom_columns_introspection.py tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py`
  - `24 passed`

## Manifest Effect

- the `15` legacy datatype-specific custom-column cache rows are now reclassified to `covered`
- rationale:
  - datatype/value semantics are covered by the active D1/D2 replacement tests
  - category exposure and direct precheck/update validation are covered by the new active cache-semantics file
  - legacy DB-writing cache update behavior is explicitly deferred, not silently claimed

## Remaining Batch A Gap

- if more legacy cache custom-column rows still matter after this, the next useful addition would be
  DB-writing update behavior beyond precheck-level validation
- do not revive the gated legacy cache modules unless a behavior cannot be expressed cleanly at the
  active emulation or direct table/category seams
