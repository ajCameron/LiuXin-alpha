# Legacy Test Source Of Truth

Date: 2026-03-16

## Decision

- The only living test tree should be inside [LiuXin-alpha-wsl](..).
- Legacy tests from the original LiuXin repo should be:
  - ported or rewritten into alpha if they still matter
  - then deleted from the duplicate in-repo legacy copy once the alpha version exists
- The original LiuXin repo remains the archival reference in Git if the historical source is needed later.

## Practical Policy

- Do not keep long-lived duplicate test files under both:
  - `src/LiuXin_tests`
  - `tests/...`
- If a legacy test is worth keeping, move its behavior into the modern alpha suite.
- If a legacy support/helper file is still needed temporarily, keep it only long enough to finish the port or rewrite wave that depends on it.

## What This Means For Current Work

### DB property salvage

- [tests/support/test_databases](../tests/support/test_databases) is the authoritative alpha-side support tree.
- Once the DB-property corpus is promoted into collected alpha contracts or otherwise stabilized, the duplicate `src/LiuXin_tests/test_databases` copy should be deleted.

### `folder_stores`

- Legacy `folder_stores` tests are not salvage targets.
- Their behavior should be rewritten into current storage tests under:
  - [tests/storage/store_backend_plugins](../tests/storage/store_backend_plugins)
  - [tests/storage/api](../tests/storage/api)
  - [tests/storage/reconcile](../tests/storage/reconcile)
  - [tests/library](../tests/library)
- After rewrite or retirement decisions are made, the duplicate legacy copies should be deleted from alpha.

## Non-Goal

- This is not a commitment to preserve every historical test file name or layout.
- The goal is behavioral preservation inside alpha, not mirror preservation inside alpha.
