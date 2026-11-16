# LiuXin-first custom-column ownership — started 2026-11-12

Status: complete on 2026-11-13. Implementation, dev-docs and validation are
complete. Changes remain uncommitted; earlier work is preserved.

## User intent

The user accepted the ownership recommendation: remove custom-column behaviour
from driver/wrapper/database surfaces, put native rules in the feature package,
keep general SQL mechanics with drivers, and make Calibre adapters explicit.
They explicitly requested dev-docs. Native custom columns must be LiuXin-first.
The previous cache ownership work remains in force; facade cache/update coupling
may stay local to the Calibre adapter. No commit/push/publication requested.

## Implemented

- Moved wrapper custom-column implementation to
  `utils/calibre_compat/custom_columns/adapter.py::CustomColumnAdapter` and made
  `CustomColumns` inherit it there. DriverWrapper retains only generic connection
  resolution and its table-category registry; no custom CRUD methods.
- Removed Database's compatibility row mixin. Explicit
  `CustomColumnRows(db)` in `calibre_compat/custom_columns/database.py` supplies
  the old row projection when needed.
- Detached `CustomColumnMacros` from the SQL macro provider. It now requires an
  explicit database argument. Removed custom methods from `MacrosAPI` and wrapper
  API declarations, as well as old custom-macro forwarding modules.
- Moved Calibre contracts from `databases/api/custom_columns_api.py` to compat
  `api.py`; removed root database API exports. Moved notification hooks to compat
  `notify.py`. Moved the unused legacy read adapter into compat `reads.py`.
- Renamed general temporary-table helpers to `create_temp_id_tables` and
  `destroy_temp_tables`; no old aliases. Bulk temporary IDs remain SQL mechanics.
- Native repository no longer creates/drops Calibre tag-browser views. Explicit
  compat `schema.refresh_category_views(db)` replaces owned SQLite views and removes
  stale ones after native schema changes. Native storage marker remains 2.
- Native definitions no longer silently default missing attachments to `books`.
- Native Series do not reserve an `_index` label. Calibre adapter owns that check.
  Native `CustomFieldCache` requests metadata without synthetic Series companions;
  Catalog's preparation method supports that while keeping compatibility behaviour
  available to the Calibre facade.
- Updated production/fixture/test callers to explicit adapters. Older Core custom
  commands now instantiate the facade rather than finding wrapper methods. Modern
  `custom-fields.native.*` routes remain native. Protocol docstrings synchronized.
- Cache writers explicitly select the borrowed Database from a raw Database or
  Catalog host in `caches/write/base_writer.py::custom_column_macros`. Real storage
  regressions cover updates and deletions through both hosts; generic database
  and native service APIs gain no compatibility forwarding.
- Added ownership tests (cold imports, removed methods/modules, real native writes
  without loading custom-column compatibility, independent Series labels, explicit
  attachments and explicit category views) and PostgreSQL parity for native rules.
- Dev-docs: new `databases/custom-column-ownership.md`, linked from the database
  index and native contract; updated compatibility, transaction and Core docs and
  historical source links.

## State and evidence

Repo `/mnt/c/dev/LiuXin-alpha`, HEAD `a88a5230e819c8de40518228b34e70584283f4cb`.
Use `.venv/bin/python`, `PYTHONPATH="$PWD/src"`. No agents/skills used. No AGENTS.md
found. Real index and dirty `LiuXin_alpha_data` must remain unchanged.

Evidence directory `.tmp/custom-columns-liuxin-first-2026-11-12/`:
- `before/`, `baseline.json`, `state.json`: incoming dirty files and index/HEAD/data
  submodule baseline. Foundation scripts `/tmp/liuxin_cc_move.py`,
  `/tmp/liuxin_cc_callers.py`, `/tmp/liuxin_cc_views.py` were already applied; do not
  rerun them.
- `moved-methods.json`, `caller-files.json`: migration inventory.
- `collection.json`: all **2,162 required cases** collect, 2,273 total. All prior
  2,153 requirements retained; nine new cases in `native-custom-field-ownership`.
- `initial.log`: 173 passed, five failures corrected (missing adapter import, Core
  lookup, fake wrapper macro binding).
- `followup.log`: 312 passed, two failures corrected (accidental adapter import
  inside cold-import subprocess string and stale protocol docstring).
- `corrective.log`: 44 passed; remaining protocol docstring mismatch subsequently
  corrected in endpoint handlers. `protocol-docs.log`: that case passes.
- `ownership.log`: 18 passed before strengthening cold-import coverage to include
  native database creation and value writes. `ownership-final.log`: all seven strengthened ownership cases passed.
- `quality-complete.log`: full quality passed (364 formatted files, architecture,
  25 import budgets, basedpyright 0, mypy 470 files, internal contracts 95/93).
  `quality-verified.log` also passes after final adapter transaction tightening.
- `docs.log`: 22 passed using the disposable alternate index under
  `/tmp/liuxin-unified-doc-index-_6f701h3`; never export that Git environment to
  tooling tests that create repositories.

## Final validation

- Full database gate: **2,273 passed**, **2,162/2,162 required**, zero failures or
  skips; two existing `logger.warn` deprecation warnings. Runtime 40m32s.
  `ratchet.log`; machine report and telemetry at
  `/tmp/liuxin-custom-columns-liuxin-first-2026-11-12/ratchet.json`.
- Final affected adapter/Core/tooling callers: `final-callers.log`, **310 passed**.
- Strengthened native cold-import/ownership tests: `ownership-final.log`, **7 passed**.
- Final writer-host integration and isolated writer imports:
  `writer-hosts-final.log`, **28 passed**. The two new real-storage cases live in
  `tests/databases/caches/test_custom_writer_adapters.py` and run separately from
  the required database manifest.
- `cache-callers.log`: 26 passed, four legacy Calibre cache modules skipped by their
  existing FRBR-first opt-in gate. This was not evidence that the obsolete whole
  cache fixtures run; the final writer-host tests exercise the changed host boundary.
- `quality-final.log`: full quality passed: 364 formatted files, annotation coverage
  463, 25 cold-import budgets, basedpyright zero diagnostics, mypy 470 files,
  internal contracts 95/93 invalid cases rejected. Final writer imports also passed
  fresh architecture/direction checks (`architecture-final.log`, `layers-final.log`).
- `docs.log`: 22 passed. Final working-memory link updates checked separately in
  `docs-final.log`.
- `preservation.json`: HEAD, real Git index and pre-existing dirty data submodule
  unchanged. Final file hashes and validation summary are saved beside it.

Validation is combined evidence: the full gate loaded adapter code before the
metadata-operation transaction was tightened and before the final cache-writer
host selection. The affected adapter/Core paths and writer paths were rerun against
final code, as was the strengthened cold-import test. Do not describe this as one
full-suite run against every final byte.

The private PostgreSQL test cluster at `/tmp/liuxin-cc-followup-pg/data`, port 55439,
was stopped after all live tests finished. The first automatic shutdown review timed
out; its permitted retry succeeded. No approval or cleanup remains pending.

No commit, push, PR, production migration, or publication was requested or performed.
