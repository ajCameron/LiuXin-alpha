# Maintenance review fixes and signoff evidence

Date: **2026-11-03**. Status: **complete; maintenance tranche ready for signoff**.

This follows the [adversarial validation](maintenance-adversarial-validation-2026-11-03.md).
The subsequent review reproduced three operator/scheduling defects and identified
maintenance job-history retention as an outstanding storage requirement.

## Changes and acceptance evidence

| Requirement | Resulting behavior | Regression owner |
| --- | --- | --- |
| Generic job retries must not wedge maintenance publication | Core refuses generic maintenance retries before creating work. The publisher adopts an existing lower-level successor and continues publishing independent obligations. | [Operator controls](../../tests/databases/test_maintenance_operator_controls.py), [jobs submissions](../../tests/jobs/test_job_submissions.py) |
| Pause must preserve the remaining retry budget | Queued paused deliveries retire with an explicit deferred receipt; obligations remain pending under a new generation. Resume republishes, including when no handler retries remain. | [Operator controls](../../tests/databases/test_maintenance_operator_controls.py) |
| Lazy priority must govern ready work | Publication sorts eligible plugins by descending priority; jobs claims use definition priority with FIFO ties. Previously captured handoffs remain recoverable. | Operator controls and jobs submissions |
| Completed maintenance job history needs safe reclamation | Explicit age-based, bounded retirement checks library/journal identity and catalogue completion, then rechecks terminal jobs and retry ancestry atomically. Pending/active/foreign work survives. | [History retirement](../../tests/databases/test_maintenance_history.py), [Core](../../tests/core/test_durable_maintenance.py), [CLI](../../tests/surfaces/test_cli_operational_families.py) |

`maintenance prune-jobs` defaults to seven days and 128 inspected runs. It removes
approved runs, events, request keys and unused definitions; retired job IDs become
unavailable. Descending cursor pagination avoids repeatedly examining protected
entries at the front of a page. Catalogue and ledger identities/monotonic IDs are
preserved. SQLite pages become reusable; automatic scheduling, VACUUM and retention
of unrelated storage/audit histories remain outside this maintenance command.
See [operator usage](../../docs/maintenance.md#pause-retry-and-reclaim-journal-space).

Retirement explicitly ends delivery-key history only after the domain owner proves
completion or prior safe journal pruning. It does not weaken ordinary jobs request
deduplication. A stale worker whose terminal run has since been retired receives
JobLeaseLost rather than an unhandled missing-row error.

## Validation

Initial SQLite runtime/operator/jobs checks passed **117 tests**. The subsequent
retention/operator/submission selection passed **55 tests**. These selections
overlap; do not add their counts as distinct cases.

The affected SQLite/APSW/live PostgreSQL matrix passed **637 tests**, with **28
backend-specific skips**, in **614.74 seconds**. It includes every maintenance
test module, all jobs tests, durable maintenance/storage-audit Core integration,
polling failures, Core service contracts and operational CLI routing. Process-loss,
externally killed hung handlers, malformed plugins, upgrades, rebuilds and real
database writers all passed. Log: `/tmp/liuxin-maintenance-signoff-matrix.log`.

The last retirement/claim correction has an additional **29-pass** selection,
covering the late worker receipt on SQLite/PostgreSQL and all durable claim tests:
`/tmp/liuxin-maintenance-signoff-claims.log`. This correction was made while the
broader matrix was running, so the separate follow-up verifies its final state.

The complete quality runner passed: **271 formatted files**, scoped lint,
complexity, **255 protected-module** import checks, **1,880-module** layer checks,
all **25 cold-import budgets**, basedpyright with **zero errors/warnings/notes**,
and mypy with **228 clean source files**. Both contract probes accepted valid calls
and rejected all **38 invalid examples**. Log:
`/tmp/liuxin-maintenance-signoff-quality.log`.

Documentation/CI/quality-runner contracts initially passed 59 checks and failed one
link check because the new files were not yet registered with Git. After adding
only those files as intent-to-add, all **16 documentation-link checks** passed.
Logs: `/tmp/liuxin-maintenance-signoff-docs.log` and
`/tmp/liuxin-maintenance-signoff-docs-final.log`. The man page renders successfully;
diff whitespace checks pass. Nothing has been committed or published.

The matrix used one test worker with scratch/caches/telemetry under `/tmp`. It recorded
**163.7 MiB peak process-tree RSS**, at least **4.03 GiB available memory**, and
**no OOM kills**. Summary: `/tmp/liuxin-maintenance-signoff-matrix-summary.json`.
The three preference-file hashes match their pre-matrix values. C: had about
3 GiB free at completion; test scratch stayed off that filesystem. The private
PostgreSQL server had no leftover test schemas and was stopped and removed.

All four review requirements above are implemented and verified. No new
full-repository run is claimed; the preceding full-run evidence and resolved
failures remain in the adversarial validation record.

## Remaining deployment limits

Plugins remain trusted synchronous Python. Process-loss and externally killed
hung-handler tests establish database recovery, not in-process preemption,
filesystem/network rollback or host power-loss guarantees. Positive backoff and
leases use persisted wall time. Prompt work remains bounded and per-catalogue
serialization is deliberate. Real-file throughput still needs a monitored import.
