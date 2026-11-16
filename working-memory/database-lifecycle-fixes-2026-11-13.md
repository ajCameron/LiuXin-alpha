# Database lifecycle repairs — 2026-11-13

Status: implementation and validation complete. LC01–LC04 resolved.
User authorized fixing the four findings from the preceding lifecycle review.
No subagents or skills used. No commit/push/publication requested or performed.

## Implemented contracts

Read the [review and repaired contract](../dev-docs/databases/module-boundary-and-lifecycle-review-2026-11-13.md).

- LC01: constructor cleanup covers escaping failures after acquisition, including
  cleanup of an internally created driver rejected during wrapper preparation.
  Rejected caller-supplied drivers remain caller-owned; successful binding adopts
  them. Original construction exceptions remain primary, with cleanup-error notes.
- LC02: lifecycle states are initializing/open/closing/closed. Shutdown joins owned
  workers before closing storage or database handles. Failures propagate and retain
  dependencies; another close can finish cleanup. Service startup and driver
  replacement reject closing/closed facades. Context-body exceptions remain primary
  if close also raises. Destructor remains best effort.
- LC03: bootstrap records every Database-created StorageManager independently of
  the mutable storage attachment; close shuts those managers down before database
  handles. Detaching an owned manager cannot bypass the catalogue-switch guard.
  Retained failed-startup workers also block catalogue switches. Injected managers remain caller-owned. Real Store close failure now resets
  StorageManager's completion flag so a retry actually invokes Store shutdown.
- LC04: removed Database/DatabaseAPI set_macros, set_driver_wrapper and
  set_metadata_sql. Read-only collaborator properties and guarded complete
  set_driver remain; no forwarding aliases. Migrated tests and negative typing
  contract. DriverWrapper's own assembly setter is separate and remains.

Construction/open/create/repair SQL semantics are unchanged. Cleanup is not a
transaction or rollback of already committed bootstrap work. Lifecycle callers
must serialize their own use; closing retains worker access so in-flight owned
work can complete. Failed service shutdown is not permission to resume normal use.

## Validation and evidence

Evidence root: `.tmp/database-lifecycle-fixes-2026-11-13/`.

- `sqlite-final.log`: 82 passed, including the final 28 new SQLite cases and the
  existing lifecycle/rebinding tests.
- `postgres-final.log`: final 14 PostgreSQL cases passed. Earlier
  `postgres-tests.log`: 13 passed before the existing-driver constructor case
  was parameterized.
- `storage-tests.log`: 60 storage-manager reload/contracts and Library lifecycle
  integration cases passed.
- `integration-tests.log`: 172 creation safety, transactions, Row state,
  metadata SQL and facade surface/lifecycle contracts passed.
- `rebinding-final.log`: all 24 final rebinding cases passed, including the two
  detached-storage cases (22 overlap the earlier SQLite selection).
- `quality-final.log`: full runner passed on the final implementation (earlier
  `quality.log` also passed). Scope: 366 formatted files, 463 annotation checks,
  architecture/layer checks, 25 import budgets, basedpyright zero errors/warnings,
  mypy 470 sources, negative contracts 95/93.
- `docs-tests.log`: all 22 docs/navigation tests passed using the disposable index.
- `ratchet-tooling.log`: 21 gate-tooling tests passed.
- `collected-final.txt`: 42 new lifecycle IDs collect (28 SQLite and
  14 PostgreSQL); the final rebinding audit adds two detached-storage cases.
  Manifest adds all 44 under database-lifecycle-failure-ownership;
  all 2,162 previous IDs retain invariant membership, for 2,206 required cases.
  No new full 2,206-case gate run is claimed; validation targets this lifecycle
  change and affected integrations. The prior full gate is earlier evidence.

Initial test repair notes: copied probe used an unsupported get_rows(limit=...)
argument; removed it so resumed work performs a valid real read. One existing
failure-injection test left its simulated worker permanently failing at fixture
teardown; it now restores that flag. Initial PostgreSQL tests inside the sandbox
could not connect to loopback; rerun outside it passed. These initial logs remain
as evidence, not final validation.

## Workspace and process state

Repo `/mnt/c/dev/LiuXin-alpha`, branch main, HEAD
`a88a5230e819c8de40518228b34e70584283f4cb`. Use `.venv/bin/python` and
`PYTHONPATH="$PWD/src"`. Preserve the earlier uncommitted custom-column work,
real index and dirty `LiuXin_alpha_data` submodule. Incoming state/fingerprints are
in `state.json`, `baseline.json`, `database-before.py`, and `ratchet-before.json`.

Private PostgreSQL16 test cluster is **stopped**, verified by pg_ctl status:
`/tmp/liuxin-cc-followup-pg/data`, loopback port 55439, writable socket directory
`/tmp/liuxin-cc-followup-pg`, database `liuxin_followup_utf8`. Startup requires sandbox escalation for loopback sockets; the first
escalated attempt needed the writable socket directory added. No approval denied.

Docs index only:
`GIT_INDEX_FILE=/tmp/liuxin-unified-doc-index-_6f701h3/index`,
`GIT_OBJECT_DIRECTORY=/tmp/liuxin-unified-doc-index-_6f701h3/objects`,
`GIT_ALTERNATE_OBJECT_DIRECTORIES=/mnt/c/dev/LiuXin-alpha/.git/objects`.
Do not apply these variables to tests that create nested Git repositories.

Final preservation audit and fingerprints are recorded in `preservation-final.json`
and `final-fingerprints.json`; `validation.json` maps LC01–LC04 to the final test
and implementation evidence. Real index/HEAD and data-submodule status are unchanged.
All incoming files outside the scoped lifecycle changes retain their fingerprints.
No running test jobs or PostgreSQL server remain. The active goal can be marked
complete after inspecting those final artifacts. No full database-gate rerun or
whole-package architectural completion is claimed.
