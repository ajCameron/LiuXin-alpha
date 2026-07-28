# Legacy Duplicate Cleanup Wave

Date: 2026-03-16

## Scope

- Alpha-side duplicate cleanup after the DB-property salvage bucket closed.
- Focused on the safest duplicate support cluster first:
  - legacy DB-property files under `src/LiuXin_tests/test_databases/test_db_properties`

## Wave 1

Deleted the covered duplicate legacy property files from `src/LiuXin_tests/test_databases/test_db_properties`:

- `test_db_0_properties.py`
- `test_db_2_properties.py`
- `test_db_3_properties.py`
- `test_db_5_properties.py`
- `test_db_6_properties.py`
- `test_db_7_properties.py`
- `test_db_8_properties.py`
- `test_db_9_properties.py`
- `test_db_11_properties.py`
- `test_db_12_properties.py`
- `test_db_13_properties.py`
- `test_db_22_properties.py`
- `test_db_23_properties.py`
- `test_db_24_properties.py`
- `test_db_25_properties.py`

Updated the legacy package shim in:
- [__init__.py](../src/LiuXin_tests/test_databases/test_db_properties/__init__.py)

That package now:
- delegates covered rows to the authoritative alpha support copies under `tests/support/test_databases/test_db_properties`
- keeps only the remaining rewrite rows local

## Wave 2

Removed more duplicate support plumbing from the same subtree:

- deleted duplicate base file:
  - `src/LiuXin_tests/test_databases/test_db_properties/common_db_properties.py`

Rewired the remaining local rewrite property modules to use the authoritative shared base in:
- [common_db_properties.py](../tests/support/test_databases/test_db_properties/common_db_properties.py)

Updated stale builder helper imports in:
- [test_db_4/__init__.py](../src/LiuXin_tests/test_databases/test_db_4/__init__.py)
- [test_db_11/__init__.py](../src/LiuXin_tests/test_databases/test_db_11/__init__.py)

Those now point at:
- [objects.py](../tests/support/test_databases/_legacy/objects.py)

## Why This Slice

This is safe cleanup because:
- the authoritative alpha copies already exist and are collected
- the covered rows are no longer the source of truth
- the rewrite rows can share the same support base without preserving a duplicate copy

## Validation

- legacy compatibility import still works:
  - covered property classes now resolve from `tests.support...`
  - rewrite property classes still resolve locally from `LiuXin_tests...`
- focused DB-property support slice still passes
- the remaining builder import failure is still the known `folder_stores` rewrite boundary, not the cleaned-up support helpers

## Deliberate Non-Action

Did not mass-delete the rest of `src/LiuXin_tests`.

Reason:
- the broader legacy tree still has internal import edges
- the remaining rows there are either explicit `rewrite` seams or need package-level cleanup by subtree, not random file-by-file deletion

## Practical Effect

- duplicate alpha-side legacy files are now being removed in real waves, not just marked in notes
- the remaining legacy DB-property files in `src/LiuXin_tests/test_db_properties` are exactly the rewrite family
- the local rewrite family now depends on shared alpha support helpers instead of duplicate local bases where that cleanup is safe
