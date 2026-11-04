# Maintenance and driver-wrapper delivery

Date: **2026-11-04**. Branch: `maintenance-driver-wrapper-upgrades`.
This packages the maintenance overhaul and driver-wrapper review/upgrades together.
It supersedes the uncommitted-delivery notes in the earlier reviews and handoffs;
their dated test results remain historical evidence.

The branch starts at `b2343096` (`database-batch-overheads`, PR #153), which follows
`database-review` (PR #152). The PR targets `database-batch-overheads` so its diff
contains this work. Review and merge those prerequisites before this branch.

## Scope and review order

1. The wrapper commit repairs transaction ownership, WEMI custom-column handling,
   schema/cache inspection, view resources and hierarchy cycles. It adds native
   schema facts, bounded/resumable reads, read-only snapshots, capability reports,
   optional observations and checked API contracts. See the
   [review](driver-wrapper-review-2026-11-04.md) and
   [usage guide](driver-wrapper-features-2026-11-04.md).
2. The maintenance commit supplies transactional prompt effects and durable lazy
   delivery through existing jobs, including retries, pause/resume, priority,
   rebuilds, versioned upgrades and bounded history retirement. Catalog owns the
   replacement semantic repair policies; obsolete maintenance is retired with a
   capability inventory. See [modes](maintenance-modes.md),
   [operator usage](../../docs/maintenance.md),
   [repair policies](../catalog/maintenance-repair-policies.md) and the
   [retirement inventory](legacy-maintenance-retirement-2026-11-02.md).

Local preference edits and the dirty data submodule are excluded. The obsolete
TA-update preference may remain in defaults/configuration; it has no live caller.

## Validation on the PR branch

Before adding maintenance, the isolated wrapper commit passed **224** focused
SQLite/APSW/schema/API checks, **45** live PostgreSQL wrapper/transaction checks,
**22** documentation checks and the complete scoped quality runner. The runner
checked 279 formatting targets and 253 mypy source files, with zero basedpyright
diagnostics, both 47-invalid-call probes and all 25 cold-import budgets passing.
Logs: `/tmp/liuxin-wrapper-pr-{tests,pg,docs,quality}.log`.

The combined checkout passed **371 tests**, with **13 backend-specific skips**,
in 1,024.41 seconds. The selection covers SQLite/APSW/live PostgreSQL maintenance
runtime, production writers, operator controls, journal/history retirement,
upgrades, read-transaction hooks, Catalog repairs and Agent changes, Core/jobs,
CLI routing and API signatures. Log: `/tmp/liuxin-review-upgrade-pr-tests.log`.

The complete scoped quality runner passed with **303** formatted targets,
**254** clean mypy source files, zero basedpyright diagnostics, both
**47-invalid-call** probes, all **25** cold-import budgets and the lint,
complexity, annotation, import-cycle and layer checks. Documentation and tooling
contracts passed **44 tests**. The man page renders and diff whitespace passes.
Logs: `/tmp/liuxin-review-upgrade-pr-{quality,docs}.log`.

Scratch, caches, configuration and telemetry stayed under `/tmp`, with per-test
scratch discarded. Telemetry recorded **132.8 MiB** peak test-process-tree RSS,
at least **3.99 GiB** available memory and **zero OOM kills**. The summary is
`/tmp/liuxin-review-upgrade-pr-summary.json`. No test schemas remained in the
private PostgreSQL instance; it is stopped and removed, with its server log
retained separately. The original 199-path source snapshot matched before
delivery, including protected preferences.

The [maintenance signoff](maintenance-review-fixes-2026-11-03.md) and wrapper
guides retain earlier broader test evidence. These selections overlap; do not
add their counts as distinct tests. No new full-repository run or real-file import
is claimed by this delivery checkpoint.

## Remaining work

The monitored SquashFS import still awaits physical server access. Plugins remain
trusted synchronous Python; database rollback does not cover external effects or
establish host power-loss behavior. Historical explicit-commit helpers,
SQLite-specific compatibility custom-column DDL and remaining placeholders retain
their documented limits. Continuation IDs do not recreate a snapshot or provide
exactly-once processing. Follow-ups remain in [next tasks](next_tasks.md).

Return to [Databases](README.md).
