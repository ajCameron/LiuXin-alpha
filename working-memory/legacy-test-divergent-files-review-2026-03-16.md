# Legacy Test Divergent Files Review

Date: 2026-03-16

## Scope

- Reviewed the `13` previously divergent file pairs between:
  - [src/LiuXin_tests/test_databases](../src/LiuXin_tests/test_databases)
  - [tests/support/test_databases](../tests/support/test_databases)
- Goal: decide whether the remaining drift is accidental, intentional alpha normalization, or a real semantic adaptation that must be preserved.

## Result

- The divergence set is narrower than it first looked.
- [src/LiuXin_tests/test_databases/__init__.py](../src/LiuXin_tests/test_databases/__init__.py) and [tests/support/test_databases/__init__.py](../tests/support/test_databases/__init__.py) are now identical again.
- The support tree should remain authoritative for all remaining divergent files.

## Divergence Classes

### 1. Intentional alpha normalization

These are support-tree variants that should be kept as-is.

- [tests/support/test_databases/test_db_4/__init__.py](../tests/support/test_databases/test_db_4/__init__.py)
  - imports rewritten away from `LiuXin_tests`
  - now uses local `_legacy` helper shims
  - now uses [liuxin_tqdm.py](../src/LiuXin_alpha/utils/libraries/liuxin_tqdm.py)
- [tests/support/test_databases/test_db_properties/__init__.py](../tests/support/test_databases/test_db_properties/__init__.py)
  - support tree now uses same-package relative imports
  - older source copy still points back into `LiuXin_tests`
- [tests/support/test_databases/test_db_properties/test_db_17_properties.py](../tests/support/test_databases/test_db_properties/test_db_17_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_19_properties.py](../tests/support/test_databases/test_db_properties/test_db_19_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_1_properties.py](../tests/support/test_databases/test_db_properties/test_db_1_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_20_properties.py](../tests/support/test_databases/test_db_properties/test_db_20_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_2_properties.py](../tests/support/test_databases/test_db_properties/test_db_2_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_3_properties.py](../tests/support/test_databases/test_db_properties/test_db_3_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_4_properties.py](../tests/support/test_databases/test_db_properties/test_db_4_properties.py)
  - these are all mostly import cleanup, docstring cleanup, or support-tree wiring changes
  - no meaningful regression signal was found in the old source variants

### 2. Real alpha-side driver adaptation

These are not just import rewrites. The support-tree copies have a substantive adjustment that should be preserved.

- [tests/support/test_databases/test_db_properties/test_db_0_properties.py](../tests/support/test_databases/test_db_properties/test_db_0_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_11_properties.py](../tests/support/test_databases/test_db_properties/test_db_11_properties.py)
- [tests/support/test_databases/test_db_properties/test_db_12_properties.py](../tests/support/test_databases/test_db_properties/test_db_12_properties.py)

Reason:
- the older source copies still import `LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw`
- the support-tree copies import `LiuXin_alpha.databases.database_driver_plugins.SQLite`
- that is real alpha driver drift, not a cosmetic difference

## Recommendation

- Keep [tests/support/test_databases](../tests/support/test_databases) authoritative.
- Do not try to re-sync these files back toward the `src/LiuXin_tests` copies.
- If the duplicate `src/LiuXin_tests/test_databases` tree is later archived or removed, these divergences are not blockers anymore.

## Practical Consequence

- The DB-property salvage stream is now clearer:
  - import decoupling is complete
  - utility dependency shims are in place
  - remaining divergence is understood
  - the next blocker is not support-tree confusion
  - the next blocker is the separate `folder_stores` rewrite boundary
