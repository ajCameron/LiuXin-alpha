# folder_stores Rewrite Boundary

Date: 2026-03-16

## Decision

- `folder_stores` is a hard rewrite boundary.
- It is not a salvage target.
- It is not a shim target.

## Why

- The original `folder_stores` layer is one of the main areas that alpha intentionally redesigned.
- Utility wrappers made sense for:
  - `clint`
  - `tqdm`
- They do not make sense for:
  - `folder_stores`

Reason:
- the behavior surface changed, not just the import path
- store semantics, reconcile flows, and storage boundaries have moved substantially
- a compatibility alias would hide real architectural drift and produce low-value tests

## Practical Consequence

Anything still blocked on legacy `folder_stores` imports should now be treated as:
- `rewrite`

Not:
- `salvage_existing`
- `covered`
- `vendor_frozen`
- `integration_frozen`

## Rewrite Targets

Legacy `folder_stores` tests and fixtures should be rewritten against the current alpha seams:

- [tests/storage/store_backend_plugins](/home/blackjane/LiuXin-alpha-wsl/tests/storage/store_backend_plugins)
- [tests/storage/api](/home/blackjane/LiuXin-alpha-wsl/tests/storage/api)
- [tests/storage/reconcile](/home/blackjane/LiuXin-alpha-wsl/tests/storage/reconcile)
- [tests/library](/home/blackjane/LiuXin-alpha-wsl/tests/library)

Use those targets depending on the behavior under test:
- storage-backend mechanics
- store API contracts
- reconcile/discovery/registration flows
- library-facing integration behavior

## Effect On The Current Salvage Batch

- The import normalization work for [tests/support/test_databases](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases) is still valid and useful.
- But once the first visible import-time blocker becomes `LiuXin_alpha.folder_stores`, the salvage path stops there.
- That blocker is not “another missing dependency”.
- It marks the point where the old builder/fixture corpus crosses into rewritten product architecture.

## Current Recommendation

1. Stop adding wrappers once the blocker is `folder_stores`.
2. Finish the duplicate-tree review for [src/LiuXin_tests/test_databases](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests/test_databases) versus [tests/support/test_databases](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases).
3. Create a separate rewrite plan for the legacy store-backed DB builders and tests.
