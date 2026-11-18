# Native cache typing pass — 2026-11-18

Complete. User asked for a thorough cache typing pass. No commit, staging, push
or PR was requested or performed. Prior uncommitted cache work is preserved.

## Changes

Both checker configurations now include the entire native cache package, instead
of two selected modules. Baseline: 189 basedpyright errors/one warning and 227
mypy errors across 52 files. Final: all 53 native modules checked, all functions
annotated, no new blanket ignores/exclusions or weaker settings.

Typed storage attachments, cardinality getters/ordinary RowAPI results, shared
field/mixin state, public mapping registries, query/predicate/projection protocols
and writer receipts. Both relation mixins inherit FieldBasicInterfaceAPI so
nullable attachment state has one shared definition. SQL-dependent table reads
require an attachment; detached snapshot reads remain available.

Moved `_CacheCatalogFacade` to cache.py and replaced string method dispatch with
typed callbacks. The facade directly satisfies the writer host protocol.
Cache.create_writer returns SchemaCatalogWriter; the abstract facade returns
CacheWriter. Catalog construction retains one localized DatabaseHandle cast for
its read protocol's metadata_sql optionality mismatch; the prepared writer path
uses native driver/schema capabilities. SQL values, optional NumPy buffers and
plugin dispatch remain explicitly dynamic boundaries.

Core exception classification now uses try/except/else for its optional cache
exception import, preserving receipt behavior. Namespace-package locations with
no file raise CachePluginError, covered by a new mandatory runtime case. Manifest
now 2,827, with previous requirements retained. Nine new negative static examples
per checker plus positive contracts and a package-scope tooling regression.

Documentation: `dev-docs/caches/typing-2026-11-18.md`, caches README, regression
ratchet note. The native package was formatted; no Calibre upstream code copied.

## Validation

- Full quality runner exited 0: basedpyright 0 errors/warnings; mypy 527 files
  clean; 104/102 negative examples rejected; valid examples accepted.
- Final runtime: 615 cache/Catalog cases + 36 real SQLite/APSW write-recovery
  cases = 651 passed, 16 expected capability skips. All 359 mandatory cases
  passed setup/call/teardown (323 + 36).
- Final gated synthetic smoke: 4 passed/4 mandatory audits, 14.86s.
- All 448 recorded source hashes match both final runtime runs and final smoke.
- 233 tooling tests; 6 documentation boundary tests; 15 navigation tests passed.
  The tracked-content doc-link test was deselected because new guides are
  deliberately unstaged. Changed guides' working-tree links checked separately.
- RPC receipt-transport regression passed after authorized sandbox escalation
  for its temporary localhost server (the restricted attempt could not bind).
- Earlier 152-case smoke, 80-case backend/MRO checks, and first broad 651-case
  run also passed; final runs above supersede their source snapshots.
- Quality: 421 formatted files, 463 annotation targets, 256 protected import
  modules, 1,934 layer modules/10 reviewed backward edges/28 existing dynamic
  allowances, 25 import budgets. Only existing registry import line allowances
  moved; no new architectural exception.
- PostgreSQL live tests and full-duration soaks were not rerun. No performance
  claim; this is a typing/contract pass.

## Artifacts

- `/tmp/liuxin-cache-typing-start/`: pre-task snapshot of cache sources/config/
  typing fixture, separating this pass from earlier uncommitted work.
- `/tmp/liuxin-cache-typing-before-pyright.json`, `...before-mypy.log`: baseline.
- `/tmp/liuxin-cache-typing-quality-final.log`: complete passing quality runner.
- `/tmp/liuxin-cache-typing-final-runtime/` and corresponding `.log`: final
  cache/Catalog run, JUnit and mandatory phase audit.
- `/tmp/liuxin-cache-typing-final-write-routes/` and `.log`: final real recovery.
- `/tmp/liuxin-cache-typing-final-smoke/`: gated smoke audit/timings/source hashes.
- `/tmp/liuxin-cache-typing-tooling.log`, `...doc.log`, `...doclinks.log`,
  `...rpc.log`: supporting checks.
- `/tmp/liuxin_run_cache_typing_final_runtime.py` and
  `/tmp/liuxin_run_cache_typing_final_write_routes.py`: audited runners.

Branch remains caches-calibre-compat-boundary, HEAD c1d2cb74. Preserve unrelated
LiuXin_alpha_data submodule dirt, architectural review README/file-formats review,
Claude outputs and all stashes. No further implementation work remains for this
request; any commit/PR must include the earlier uncommitted cache work deliberately.
