# Custom columns: remove v1 entirely — 2026-11-12

Follow-up: native-package compatibility exports and module paths were subsequently
removed; see [the completed boundary handoff](custom-columns-native-boundary-2026-11-12.md).

## User decision and scope

The user requested removal of v1 custom-column storage and a Calibre-shaped
compatibility facade over native storage. Explicit clarification:
**“Remove v1 format support entirely.”** There is no converter/importer/upgrade
exception. Persisted marker `2` is retained for the sole supported native layout.
External Calibre database generation/emulation is a separate existing feature.

Implementation and final validation are complete. No commit, push, PR or
production catalogue migration/deletion was requested or performed. No subagents
were used. No AGENTS.md applies. Preserve the real index and dirty data submodule.

Checkout: `/mnt/c/dev/LiuXin-alpha`, main at
`a88a5230e819c8de40518228b34e70584283f4cb`.
The prior native implementation and six review fixes are uncommitted baseline
work, not changes to discard. Their completed gate was 2,101 passed / 1,990 required.

## Implementation

- Native service/repository owns creation, values, identity, entry order,
  revisions, definition migration, retirement, audit, repair and explicit purge.
  Old schema headers or marker 1 raise `UnsupportedStorage` without conversion.
  V1 read/write branches, upgrade plans and raw custom-table driver builders
  are removed. Fresh schema defaults to marker 2.
- `utils/calibre_compat/custom_columns` contains the Calibre calling facade,
  value/cache adapters, transaction wrapper, macro adapters and Database row
  projection. Old import paths are thin re-exports. Native imports do not load
  the facade. There is no standalone legacy conversion module.
- Facade, wrapper and Core creation all create native fields. Supported scalar
  and delimiter-free text-set shapes preserve existing calling conventions;
  richer collections use the native API. Legacy Series bulk extras remain
  indexed by input position. Macros use native identity and managed transactions.
- Facade writes require an owned transaction and journal optional cache state.
  SQL/cache/projection preparation fails atomically; maps and notifications
  publish after commit. Native operations compose inside managed transactions.
- Constructor/refresh/old cleanup entry points do not mutate schemas or purge.
  Retirement preserves data until explicit native purge. Generic raw SQL remains
  a privileged database facility; it is not an alternative custom-field API.
- Native definition publication updates DB/wrapper custom-table registries after
  commit. Database row lookup projects native ordinal order, including reordered
  entries whose persistent IDs do not follow the new order.
- Docs, ownership/layer allowance, lint/format/type scopes and real native fixtures
  were updated. Historical review findings/signoff remain historical.

## Validation and evidence

Evidence: `.tmp/custom-columns-unified-2026-11-12/`. `before/` and baseline JSON
preserve all 67 pre-existing changed files; `working-memory-checkpoints.md` records
intermediate work. Exploratory failure logs are retained and are not signoffs.

Completed checks before the last registry/row projection additions:
- Full quality passed: format, annotation coverage, import cycles, layer direction,
  cold-import budgets, lint/complexity, basedpyright, mypy, internal contracts.
- SQLite/APSW facade/lifecycle/wrapper 269 passed; native transaction injection
  78 passed; focused single-format checks 53 passed.
- Live UTF8 PostgreSQL focus 61 passed (795.85s).
- Core/tooling 141 passed; documentation 22 passed with an alternate Git index.

Latest integration checks:
- `registry-types.log`: basedpyright 0 errors/warnings.
- `registry-commit.log`: final commit/rollback registry checks 2 passed.
- Additional 100-case database contract migrated from v1 raw fixtures to native
  identity/ordinal fixtures. The final full gate covers it and the new row tests.
- `manifest-changes.json`: 2,131 required / 2,242 collected. All 1,622 committed
  HEAD requirements retained verbatim; earlier 1,990 requirements retained with
  recorded renames for deliberately removed upgrade/implicit-purge behavior.
- HEAD, real index hash and data-submodule status match `baseline-state.json`.

Final validation:
- Full gate PASSED: **2,242 tests**, **2,131/2,131 required**, no skips or
  failures, **117 live PostgreSQL cases**, 2,279.73 seconds. Log `ratchet-run.log`;
  durable result copy `ratchet-final.json`; full telemetry in
  `/tmp/liuxin-custom-columns-unified-final-2026-11-12`. Two pre-existing logging
  deprecation warnings in `search_mixin.py` are unrelated to this change.
- Final full quality PASSED: `quality-complete.log` (session 57645).
  basedpyright 0 errors; mypy 478 files; internal contracts 95/93 invalid cases.
  Prior `quality-registry.log` caught two declaration mismatches, now fixed.
- Documentation PASSED again after API docstring corrections:
  `docs-api-final.log`, 22 tests. Earlier `docs-final.log` also passed.
- Final adapter suite PASSED: `unification-final.log`, 21 tests after the
  declaration fixes. Subsequent API docstring corrections do not change behavior.
- Earlier full gate was deliberately interrupted at 121 passes to fix registry
  and row-order gaps. Artifacts moved to
  `/tmp/liuxin-custom-columns-unified-interrupted-2026-11-12`; not a signoff.
- Two exploratory SQLite runs (sessions 25063 and 73394) overlapped the same
  `registry-final.log/json` destination; do NOT treat those artifacts as a clean
  result. Final gate and `registry-commit.log` are the authoritative reruns.

## Environment and completion

Use `.venv/bin/python` and `PYTHONPATH="$PWD/src"`. SQLite and APSW are installed.
Private PostgreSQL16 was used for the gate, data `/tmp/liuxin-cc-followup-pg/data`, port 55439,
socket `/tmp/liuxin-cc-followup-pg/socket`, UTF8 database `liuxin_followup_utf8`.
URL `postgresql://blackjane@127.0.0.1:55439/liuxin_followup_utf8`.
Do not use database `postgres` (SQL_ASCII). Live socket tools require escalation.
The private server was stopped after successful final validation; see
`postgres-stop.log`.

Docs checks use disposable index/object store
`/tmp/liuxin-unified-doc-index-_6f701h3`, with real `.git/objects` as alternates.
Never stage in the real index. Do not export those variables to tooling tests
that create nested repositories.

No required work remains. `workspace-preservation.json` verifies the real Git
index, HEAD and data-submodule status are unchanged. `preservation-and-coverage.json`
verifies all 1,990 pre-task requirements (with recorded intentional renames) and
all 1,622 committed requirements remain; 25 unrelated baseline files are unchanged.
`final-code-fingerprints.json` records the final changed source/tests/docs/scripts.
`git diff --check` passed. The changes remain uncommitted for the user's review.
