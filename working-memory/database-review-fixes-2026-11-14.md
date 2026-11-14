# Database review repairs — 2026-11-14 (complete)

Completed objective: fix all reasonable findings from
`dev-docs/architectural_reviews/databases-review-2026-11-14.md` and commit
the finished work, including the earlier uncommitted DR01–DR03 repairs. The exact
checkpoint is the local commit containing this note, whose parent is
`7780b7eb62020d8b1cf6891b3bb95a47d9f7fcfb`. No push or production migration.
The independently dirty `LiuXin_alpha_data` submodule is excluded.

The review-of-review evidence is in
`.tmp/database-review-assessment-2026-11-14/assessment.json`. Its corrections
are retained in the dated review follow-up: view_reads is also affected by DBR05;
five of nine default identities use NFKC; duplicate preference resolution must
be explicit; deferred SQLite lock promotion is a conflict, not permission to
replay arbitrary transaction bodies; moving adapter registration to import time
does not remove process-wide interference. Expression-index drift was injected,
not demonstrated on a particular interpreter upgrade.

Implementation and local validation are complete. DBR01–DBR13 are resolved at the
documented boundaries; DBR14 is partial because broad indexing and automatic dirty
scheduling remain explicit follow-ups. Independent re-review is the next step.

Implemented DBR01–DBR14:

- Explicit `target_profiles` audit/migration on macros and Database, lossy identity
  write/ensure guard, single default identity registry and declared key roles.
  Profile migrations require other catalogue writers to be stopped.
- Orphan pruning inventories actual references, takes an owned serialized unit,
  and locks PostgreSQL target/referring tables. Polymorphic IDs conservatively
  protect candidates; external schema references refuse deletion.
- Maintained script connection commands refused before any statements; empty
  SQLite type declarations preserve native values; owned-BEGIN idle guard.
- Expression indexes retained with policy/Unicode stamp, open-time validation
  and explicit offline `rebuild_sqlite_search_indexes` utility. Generic-tool
  restrictions are deliberate and documented.
- Owned SQLite macro writes reserve `BEGIN IMMEDIATE`; read snapshots remain
  deferred. No arbitrary body replay or silent WAL switch.
- Unique preferences, explicit duplicate choice migration, upserts and
  after-commit cache publication/deletion, tagged bytes, atomic backup.
- Draft Row reads preserve defaults; missing identified updates raise on both
  backends; explicit table hints survive insertion, reload and later updates.
- Both SQLite driver names avoid global adapter/converter/traceback changes;
  container binding is local JSON, typed reads or explicit decode restore values.
  PYSET aggregate/codec now JSON. Updated one legacy raw-SQL test to the explicit
  decode contract, preserving exact type/value assertions.
- Directory fsync; dump/restore retains verified unique sibling backups and uses
  full configured callbacks for backup integrity validation. Existing mandatory
  restore regressions additionally verify retained original backup contents.
- PostgreSQL exact index direction/collation/opclass checks and TEMPORARY grant;
  incomplete storage bootstrap reports true unknown counts and restores startup
  policy; dirty persistence failures requeue and raise.
- Broad comparison-column additions and automatic dirty-queue scheduling remain
  explicit policy/follow-up decisions, with reasons in the repair contracts.

New files: `databases/preferences.py`, `databases/sqlite_values.py`, three
`tests/databases/test_database_review_{contracts,concurrency,postgres_live}.py`,
`dev-docs/databases/review-repairs-2026-11-14.md`, and this handoff.
Source contracts/docs and API declarations were updated; source from earlier
DR01–DR03 remains included. No Calibre facade or v1 storage was reintroduced.

Validation (overlapping selections, do not sum blindly):

- `/tmp/liuxin-dbr-new-tests3.log`: 46 passed, including four two-process cases.
- `/tmp/liuxin-dbr-pg-new.log`: eight passed; `liuxin-dbr-pg-race.log`: one passed.
- `/tmp/liuxin-dbr-existing-tests.log`: 103 passed, one pre-existing harness skip.
- `/tmp/liuxin-dbr-pg-existing.log`: 27 passed.
- `/tmp/liuxin-dbr-metadata.log`: 64 passed.
- `/tmp/liuxin-dbr-rebuild3.log`: 18 passed, including full callback backups.
- `/tmp/liuxin-dbr-quality3.log`: full quality passed (376 formatted files,
  import/layer checks, 25 cold imports, lint/complexity, both type checkers;
  mypy 472 sources; invalid-contract checks 95/93).
- `/tmp/liuxin-dbr-tooling-final.log`: 96 tooling, storage integration/docstring
  and documentation-link/boundary tests passed. This includes a small repair of
  a pre-existing missing `:return:` field on the storage repository's `macros`
  property; its runtime AST is unchanged, and its format check passes.
- All previous 2,259 requirements retained; 55 new => **2,314 mandatory cases**.
  `.tmp/database-review-fixes-2026-11-14/prior-manifest.json` is the prior 2,259
  snapshot; `new-collection.log` records the 55 additions.
- `/tmp/liuxin-dbr-full-gate.log`: **2,425 passed** in 2,312.91 seconds, with two
  existing logging deprecation warnings. **2,314/2,314 required cases** passed
  setup, execution and teardown, with no skips, failures or xfails. No source
  repairs were needed during this full run.

Final full-gate artifact:
`/tmp/liuxin-database-review-repairs-final-2026-11-14/ratchet.json`.
Command: `LIUXIN_TEST_POSTGRES_URL=postgresql://blackjane@127.0.0.1:55439/liuxin_followup_utf8
PYTHONPATH="$PWD/src:$PWD" .venv/bin/python scripts/run_database_regression_ratchet.py
--baseline-ref 7780b7eb62020d8b1cf6891b3bb95a47d9f7fcfb
--artifacts /tmp/liuxin-database-review-repairs-final-2026-11-14`.
Session 27536 completed with exit zero. No validation job remains running.

Private PostgreSQL **stopped**, data `/tmp/liuxin-cc-followup-pg/data`, reserved
port 55439, socket directory `/tmp/liuxin-cc-followup-pg`.
`postgres-stop-proof.json` records successful shutdown and subsequent status exit
three, “no server running”. Other servers were untouched.

`.tmp/database-review-fixes-2026-11-14/gate-source-fingerprints.json` freezes 78
source/test/config files; `checkpoint.json` records HEAD and required count.
All 78 frozen inputs remain unchanged; all previous 2,259 requirements remain in
their original invariants. `final-gate-validation.json` records the result and
input verification; `post-freeze-docstring.json` proves the storage correction
does not change runtime code. Tooling/docs checks and final diff review passed.

The user authorized the combined local commit. Git identity was absent after
restart; the command reuses the per-command identity documented for the previous
commit, without changing local or global settings. The dirty data submodule
remains outside the commit. No push was requested or performed.
