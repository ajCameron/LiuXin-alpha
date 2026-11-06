# Database driver plugins review — 2026-11-05

User confirmed PR #155 merged and requested a review of
`databases.database_driver_plugins`. GitHub confirms merge commit `07fc3326`
on `database-review`. Fetched that branch and created
`database-driver-plugins-review-2026-11-05` in `/tmp/liuxin-databases-typing`.
The original checkout's accumulated changes, preferences and data were preserved.

Review: `dev-docs/databases/database-driver-plugins-review-2026-11-05.md`.
The database index and next-task list link it. The original review was read-only;
the requested fixes and final validation are recorded below.

Six findings from the original review (now repaired):

1. High: file-copy SQLite backup excludes committed WAL rows (both driver names).
2. High: PG primary connection probe treats aborted transactions as dead handles,
   reconnecting and losing pending writes and recovery savepoints.
3. Medium: max/min/value/direct SQL helpers retain live handles; the tracking list
   also retains closed connections for the driver's full lifetime.
4. Medium: PG SQL translation rewrites dollar literals/comments/operators, fails
   percent literals with bindings, and script splitting breaks quoted semicolons.
5. Medium: SQL dump iteration commits primary work or rolls it back on early close.
6. Medium: SQLite restore enables FKs before parents exist. A populated current
   WEMI Item/Manifestation fixture fails; APSW shell restore succeeds.

Tests: broad plugin/registry/adapter selection 320 passed, seven skipped, one
teardown error from a concurrent standalone probe creating LiuXin_scratch.
The probe was isolated explicitly in ptempfiles. Affected lifecycle cases plus
live PG adapter/row-write/transaction tests passed: 32, zero skips. Counts overlap.
Standalone reproductions and fresh/populated WEMI round trips provide the finding
evidence. Logs and scripts are `/tmp/liuxin-driver-review-*`.

All 16 documentation checks pass and the staged diff has no whitespace errors.
Private PostgreSQL 16 cluster `/tmp/liuxin-driver-review-pg-5h1gec9u` is stopped
and removed. Its server log and status receipt remain under `/tmp`.
The review was followed by the user’s request to fix all six findings and add
permanent regression coverage.

## Fix follow-up — complete locally

All six findings are fixed, with updated API/concrete docstrings and 110 new
regression cases in three test modules. Backup uses a validated, create-only
committed SQLite snapshot, including WAL. Probe failures retain primary recovery
state. Owned handles close and deregister; failed closes stay owned for retry.
PostgreSQL SQL adaptation is lexical and native scripts preserve transaction
ownership. SQL dump no longer commits/rolls back the caller. Both restore paths
validate complete populated graphs before replacement and refuse pending writes.
Validation disables language reseeding so it cannot silently repair its subject.

Final verification:
- Expanded mandatory ratchet: **350 passed, zero skips/xfails**, preserving all
  240 old cases and invariant groups. Baseline `07fc3326`; final 103.72 s.
- Backend, creation, portable API, all maintenance, Core/Catalog consumers,
  wrappers and tooling/docs: **1,275 passed, 37 expected backend/platform/fixture
  skips, no failures** (1657.36 s). Exact reasons are in the log.
- Native driver/adapter doctests: **121 examples passed**. Final documentation
  navigation/boundary selection: **22 passed**.
- Full quality runner passed: formatting/lint, import boundaries/budgets,
  annotation coverage, 455 mypy sources, zero basedpyright errors/warnings, and
  internal negative contracts (95/93 rejected examples).
- Final concurrent-backup regression forces the writer to commit between pages;
  the earlier tiny fixture could have written after copying. The strengthened
  case passed the final mandatory gate and Ruff/format checks.

Evidence: `/tmp/liuxin-driver-fixes-ratchet-final/ratchet.json`,
`/tmp/liuxin-driver-fixes-ratchet-final.log`,
`/tmp/liuxin-driver-fixes-integration.log`, integration selection/telemetry,
`/tmp/liuxin-driver-fixes-doctests.log` and
`/tmp/liuxin-driver-fixes-quality.log`. Counts overlap; no full-repository run or
production import is claimed. Integration peak process-tree RSS ~229 MiB; OOM
counter unchanged. Its full PostgreSQL table/view walk passed but took 140.2 s;
recorded as a profiling follow-up alongside connection-setup consolidation and
splitting the largest implementation modules.

The private PG16 server is stopped and cluster removed; receipt
`/tmp/liuxin-driver-fixes-pg.json` and server log remain. Changes are staged but
uncommitted on `database-driver-plugins-review-2026-11-05` in
`/tmp/liuxin-databases-typing`, based on merged integration `07fc3326`. No new PR
publication was requested. Preserve the original checkout’s accumulated changes,
preferences and data; only its working-memory pointer was updated.
