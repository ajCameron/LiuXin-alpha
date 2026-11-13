# Native custom-column package boundary — 2026-11-12

The user asked to remove the compatibility facade from the module entirely and
fix broken callers instead of retaining aliases. The previous native-only storage
work is complete; this follow-up removes source/API import compatibility from
`databases.custom_columns`. Keep the actual Calibre adapter in the explicit
`utils.calibre_compat.custom_columns` package as previously requested.

## Changes

- Removed root lazy exports and `__getattr__`; root exports native services/types.
- Deleted the old facade, manager, five mixin and writer module paths (eight files).
- Moved manager to `utils/calibre_compat/custom_columns/manager.py`.
- Moved compatibility-only error out of native types to `errors.py` in that package.
- Updated callers, fixtures, type contracts and the lint scope; no forwarding aliases.
- Strengthened the existing mandatory cold-import test to require all old imports
  to fail and no Calibre package to load through native imports.
- Updated current contracts and historical review source links.

## State and validation

Main at `a88a5230e819c8de40518228b34e70584283f4cb`; no commit/publication requested.
Preserve all pre-existing work, real index and dirty data submodule. No AGENTS.md
applies; no subagents used. Prior complete gate: 2,242 passed, 2,131 required,
117 live PostgreSQL cases, full quality and docs. PostgreSQL remains stopped.

Evidence/baseline `.tmp/custom-columns-native-boundary-2026-11-12/` contains
pre-task changed files and hashes, HEAD/index/submodule status, changed import
paths and removed modules. Run with `.venv/bin/python`, `PYTHONPATH="$PWD/src"`.

Completed validation:
- `boundary-tests.log`: 127 passed, including cold native imports, removed import
  paths, manager behavior and facade compatibility boundaries.
- `callers.json/log`: 454 passed; eight older tests used raw values without native
  identity keys or injected failure into the removed SQL macro path. Their fixtures
  and failure injection now use the native service; corrupt-display checks expect
  the native validation errors and retain the no-mutation assertions.
- `extra-contracts-final.json/log`: all 16 fixture/write/refresh contracts passed,
  including all eight corrections. `validation-summary.json` verifies every caller
  case has passing evidence: **589 distinct affected database cases** across these
  runs. This is combined validation, not a claim of one green full-gate run.
- `quality-final.log`: full quality passed (355 formatted files, 463 annotations,
  import cycles, layer direction, 25 cold-import budgets, lint/complexity,
  basedpyright 0 errors, mypy 472 source files, internal contracts 95/93 invalid
  examples rejected). First `quality.log` caught import ordering, corrected before
  the successful final run.
- `tooling.log`: 50 passed; `docs.log`: 22 passed.
- `collection.json`: every **2,131 required case** still collects; with additional
  affected modules, 2,268 nodes collected. The regression manifest is unchanged.
- `preservation.json`: real index, HEAD and submodule status preserved; the seven
  native storage implementation modules are byte-for-byte unchanged.
- `final-code-fingerprints.json` records final source/tests/docs/scripts. Whitespace
  checks passed. Documentation used the disposable alternate index at
  `/tmp/liuxin-unified-doc-index-_6f701h3`, never the real index.

No required work remains. No storage behavior changed, so this follow-up used
focused affected tests, collection and full quality rather than repeating the
previous 38-minute live PostgreSQL storage gate. Private PostgreSQL stayed stopped.
The changes remain uncommitted; no publication or production data changes occurred.
