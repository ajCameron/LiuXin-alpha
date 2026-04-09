# Relation Field Matrix

Date: 2026-03-16

## Goal

Replace the old legacy cache field self-tests for relation-backed fields with active pytest coverage on the current cache seam.

## Implemented

New active test file:
- [test_calibre_cache_07_relation_field_semantics.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py)

It covers the cache-facing adapter behavior for:
- `CalibreOneToManyField`
  - default
  - priority
  - typed
  - priority-typed
  - unique and non-unique reverse lookups
- `CalibreManyToOneField`
  - default
  - typed
  - priority-typed
- `CalibreManyToManyField`
  - default
  - priority
  - typed
  - priority-typed

## What Is Pinned

- `for_book(...)` output shapes
- `ids_for_book(...)` output shapes
- `books_for(...)` reverse-relation shapes
- priority ordering vs unordered set semantics
- typed relation partitioning
- default-value behavior for known-but-unlinked records
- `NotInCache` behavior for unknown books/items

## Validation

Targeted file:
- `15 passed`

Active Batch A + Batch B bundle:
- [test_calibre_cache_06_custom_column_semantics.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/caches/test_calibre_cache_06_custom_column_semantics.py)
- [test_calibre_cache_07_relation_field_semantics.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/caches/test_calibre_cache_07_relation_field_semantics.py)
- [test_calibre_emulation_d1_custom_columns_introspection.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/database_calibre_emultation/test_calibre_emulation_d1_custom_columns_introspection.py)
- [test_calibre_emulation_d2_custom_values.py](/home/blackjane/LiuXin-alpha-wsl/tests/databases/database_calibre_emultation/test_calibre_emulation_d2_custom_values.py)
- result: `39 passed`

## Manifest Effect

The `16` legacy relation-field cache rows are now treated as `covered`.

Reason:
- field-adapter semantics are now pinned in the active cache suite
- lower-level link/update semantics are already covered by the DB and driver contract suites
- there is no need to revive the old unittest field-table matrix

## Remaining Boundary

Cache DB-writing/update semantics should still be claimed only where there is a live seam.

For relation fields, the active claim is:
- adapter behavior: covered
- lower-level interlink semantics: covered
- old unittest cache harness: retired as a migration target, not revived
