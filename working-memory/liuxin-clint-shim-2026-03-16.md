# liuxin_clint Shim

Date: 2026-03-16

## What Changed

- Added [liuxin_clint.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/utils/libraries/liuxin_clint.py)
  - exports:
    - `puts`
    - `colored`
  - behavior:
    - uses real `clint.textui` if installed
    - otherwise falls back to plain-text output and passthrough coloring

- Repointed direct `clint.textui` imports in:
  - [utils/terminal.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/utils/terminal.py)
  - [tests/support/test_databases](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases)
  - [src/LiuXin_tests](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_tests)

## Why

- The DB-support salvage work had removed `LiuXin_tests` import coupling, but package import was still blocked by the old direct `clint` dependency.
- A thin library-layer shim is the right place for this:
  - one compatibility point
  - no need for test-only monkeypatching to make core support modules import

## Validation

- `python3 -m py_compile` passed for:
  - [liuxin_clint.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/utils/libraries/liuxin_clint.py)
  - [utils/terminal.py](/home/blackjane/LiuXin-alpha-wsl/src/LiuXin_alpha/utils/terminal.py)
- full `py_compile` over [tests/support/test_databases](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases) passed
- package import check:
  - `PYTHONPATH=src:. python3 -c 'import tests.support.test_databases'`
  - now succeeds

## New Current Blocker

- The next import-time blocker in the legacy DB-support tree is `tqdm`, not `clint`.
- Example:
  - importing [test_db_4](/home/blackjane/LiuXin-alpha-wsl/tests/support/test_databases/test_db_4/__init__.py) now fails on missing `tqdm`

## Practical Meaning

- `clint` is no longer the reason the salvaged DB-support package needs test-time stubs.
- The support tree is now importable at the package root with only the current `src` path on `PYTHONPATH`.
