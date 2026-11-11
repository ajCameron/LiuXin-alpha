# Maintenance review fixes and signoff

Completed goal work: **fix the latest review findings, then establish whether this
tranche can be signed off**. Include generic retry coordination, pause/retry accounting,
lazy priority and bounded maintenance job-history retirement. Preserve the dirty
data submodule, preference files and user TODOs. No commit or PR requested.

## Completed implementation

- Generic Core job retries refuse maintenance runs before mutation and direct
  operators to maintenance retry. The bridge can reuse an independently created
  retry successor; low-level API use cannot wedge publication indefinitely.
- Unavailable queued deliveries retire without acknowledging obligations or
  consuming attempts. A fresh generation is available after resume. Direct calls
  raise MaintenanceUnavailable without recording a failed handler attempt.
- Publication uses plugin priority; job claims use definition priority and FIFO
  ties. Captured handoffs from older priority configuration remain recoverable.
- `maintenance prune-jobs` offers explicit, bounded, age-based retirement. Core
  proves current-library/current-journal completion under catalogue serialization;
  jobs atomically rechecks terminal state, active definitions and retry ancestry.
  It removes approved events, request keys, runs and orphaned definitions. Defaults:
  seven days, 128 inspected rows; descending pagination via before-run-id. No
  automatic pruning or VACUUM; freed pages are reusable.

New modules: core/maintenance_history.py, jobs/history.py.
New tests: databases/test_maintenance_operator_controls.py and
databases/test_maintenance_history.py. Existing jobs/Core/CLI tests extended.
Operator docs, contract and man page updated. All applicable quality checks pass.

Final evidence: **637 passed, 28 backend skips** in 614.74s for the affected
SQLite/APSW/live PostgreSQL maintenance/jobs/Core/CLI matrix. The final late-receipt
fencing correction passed **29 claim checks** separately. Full quality runner:
271 formatted files, lint/complexity/import/layer checks, 25 cold-import budgets,
both type checkers (228 mypy files) and both 38-invalid-call probes pass. Initial
docs/contracts selection had 59 passes and one untracked-link failure; new files
were registered with git add -N, then all 16 link checks passed. Man rendering and
diff whitespace pass. All test/quality processes are finished.

Matrix peak tree RSS **163.7 MiB**, minimum available memory **4.03 GiB**, OOM counter
zero. Pre-matrix preference hashes unchanged. No test schemas remained; the private
PostgreSQL server was stopped and its root removed. C: about 3 GiB free. Logs use
`/tmp/liuxin-maintenance-signoff-{matrix,claims,quality,docs,docs-final}.log`;
resource summary is `/tmp/liuxin-maintenance-signoff-matrix-summary.json`.
The durable [signoff report](../dev-docs/databases/maintenance-review-fixes-2026-11-03.md)
records scope and limits. Changes remain uncommitted; no new full-repository run.

## Evidence so far

- Initial SQLite runtime/operator/jobs selection: **117 passed, 55 deselected**.
  `/tmp/liuxin-maintenance-fixes-initial.log`.
- Initial SQLite retention/operator/submission selection: **55 passed, 16
  deselected**. `/tmp/liuxin-maintenance-retention-initial.log`.
- First basedpyright found only an inferred literal-key dictionary type in the
  new history module; an explicit dict[str, int] annotation has been applied.
- Maintenance dispatcher exceeded its complexity budget after adding prune-jobs;
  journal/jobs retirement now delegates through one small helper.

## Historical validation setup (no longer live)

Private PostgreSQL 16 server was initialized and started at
`/tmp/liuxin-maintenance-signoff-pg.kGgKLA`, socket `socket/`, port **55437**, user
blackjane, database postgres; no TCP listener. It is now stopped and removed. URL used:
`postgresql://blackjane@/postgres?host=/tmp/liuxin-maintenance-signoff-pg.kGgKLA/socket&port=55437`.
Tests using the socket require the sandbox escalation used in previous matrices.

The final matrix covered process-loss and hung-handler cases before starting
import-heavy quality work. Types/contracts/format/lint/architecture/docs checks
completed afterward. The requirements and their regression evidence are mapped
in the signoff report.

C: had **2.9 GiB free** at the latest check, versus about 8.4 GiB after the previous
task. All new scratch, caches, PostgreSQL data and telemetry are under `/tmp`
(about 922 GiB available). Keep tests serial and temporary-file retention disabled.
These figures describe this test run; they do not establish production import
throughput or host power-loss guarantees.
