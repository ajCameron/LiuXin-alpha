# Database batch overheads — 2026-11-01

Branch `database-batch-overheads`, based on `database-review` at `8203b737`.
Parent [PR #152](https://github.com/ajCameron/LiuXin-alpha/pull/152) remains open
at the latest check. Publish the follow-up against that branch while it is
unmerged. Do not merge either PR without the user's instruction.

## Authorized scope and steering

Implement the measured follow-ups: avoid the default per-instruction Python
progress handler and resolve normalization metadata once per batch. The user
explicitly clarified that progress machinery must remain available through hooks,
with nothing installed when there are no callbacks. Keep the existing diagnostic
observer and counter; enable it only by registration.

## Changes

- Shared `SQLiteProgressMixin` supplies `set_progress_callbacks` on SQLite and
  SQLite_apsw drivers. It replaces hooks on existing/future connections, removes
  the native handler when cleared, skips closed handles, and supports cancellation.
  New connections complete initialization before installing hooks.
- `NormalizedValuePreparer` captures one table's declarations/headings for one
  operation. Portable inserts/updates and direct writers share it within batches;
  PostgreSQL mixed-table batches keep one snapshot per table. Scalar calls still
  resolve fresh policy. Key-only updates retain reads on the writer connection.
- Benchmark reports now count opened connections alongside SQL trace events.
- API/driver docstrings and developer guides describe opt-in progress and batch
  snapshots. No schema, integrity-preflight or stored-normalization rule changed.

## Validation

- Initial new regressions: 16 passed. Broader batch/search/write/backend selection:
  176 passed in 254 seconds, including those 16 cases and 32 PostgreSQL unit tests.
- Live PostgreSQL 16: seven passed in 105 seconds, covering new portable/direct
  normalization cases, mixed-table batches, existing efficiency checks and direct
  writer rollback/mapping behavior. The isolated Unix-socket cluster is stopped;
  only its disposable data directory was removed. Logs/telemetry remain under
  `/tmp/liuxin-overheads-pg*`.
- Full quality runner passed: formatting, annotation coverage, import cycles,
  layers, 25 import budgets, Ruff, basedpyright, mypy (227 files), and both sets of
  38 negative type-contract examples. Log: `/tmp/liuxin-overheads-quality.log`.
- Final progress/documentation/hygiene selection passed 17 cases (ten progress
  cases, overlapping the broader selection); documentation links passed all 16
  cases. Targeted Ruff, new-file formatting and staged whitespace checks pass.
  Logs are `/tmp/liuxin-overheads-final-checks.log` and
  `/tmp/liuxin-overheads-links.log`. The 50,000-Work, 12-scenario
  benchmark passed with all result digests matching the original report. Batch
  insert median: 67.7 ms, 31 SQL events, two connection opens; current scalar
  reference: 23.93 s, 6,016 events, 1,001 opens. Shared WEMI: 0.965 s, 46 events,
  unchanged integrity checks. Timings vary on this host; do not claim a controlled
  historical speedup ratio. The raw report is versioned under `dev-docs/databases/`
  as `sql-efficiency-overheads-benchmark-2026-11-01.json`, with analysis in the
  existing SQL efficiency guide.
- Parent PR #152 completed all CI gates at `8203b737`: full suite 8,065 passed,
  363 skipped, 17 xfailed in 34m02s. Run `41195661329`; full-job `124521194377`.
  Telemetry: 1.06 GiB peak process-tree RSS, 14.1 GiB minimum available RAM,
  84.7 GiB minimum disk free, no OOM or swap. This is parent evidence, not a full
  run of the follow-up branch.

## Preserve unrelated work

The dirty `LiuXin_alpha_data` submodule and the user's concurrent comments in
`src/LiuXin_alpha/databases/maintenance/builtin_plugins.py` are outside this task.
Do not stage, format or remove them. The latter includes existing trailing spaces;
use scoped or staged whitespace checks for this delivery.

Full-suite validation belongs on CI because the Windows volume remains low on
space. Local validation uses `/tmp` with telemetry and scratch cleanup.
Local implementation and checks are complete. The PR for `database-batch-overheads`
tracks publication and subsequent CI; do not attribute the parent's full-suite
result to this follow-up.
