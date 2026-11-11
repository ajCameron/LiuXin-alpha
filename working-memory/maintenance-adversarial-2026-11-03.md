# Maintenance adversarial testing handoff

User task completed: thoroughly test maintenance, including breaking/malformed
plugins. Resumed and validated 2026-11-03. The goal tool reported `usageLimited`,
with zero usage, despite the user's explicit resume; no accounting reset attempted.

Scope and acceptance are recorded in
`dev-docs/databases/maintenance-adversarial-validation-2026-11-03.md`.
No agents requested/allowed. No AGENTS.md found. Preserve existing uncommitted
work on `maintenance-plugin-modes`, dirty data submodule, preferences and TODOs.
There are no live old test sessions or PostgreSQL cluster from the preceding goal.
Scratch belongs in /tmp; C: had about8.4GiB free after the full run.

## Completion checkpoint (2026-11-03)

No test/type sessions remain running. Full51820 finished with **9,757passed,
403skipped,21xfailed,three failed**,1,583warnings,8281.34s. Its final tracebacks
confirmed both macro API signature mismatches and the stale-worker test's
`Maintenance delivery is waiting for retry or exhausted` after a backward clock
step. All three are fixed; no unresolved failure from that invocation remains.

Final regression43923: **83passed,four backend-specific skips,450.16s**. It includes
all three failures plus clocks, retry/backoff/expired jobs, read snapshots,
SQLite/APSW/livePG Catalog repairs/inheritance, Core delivery/polling and doc links.
Log `/tmp/liuxin-maintenance-final-regression-20261103.log`; matching telemetry dir
and `/tmp/liuxin-maintenance-final-regression-summary-20261103.json`. No full rerun
after the limited fixes; don't call the original full invocation green.

Final quality9433/69647/70686 all finished successfully: basedpyright0errors,
mypy227files, both internal contracts38invalid examples rejected, format264files.
Changed runtime/bridge/test lint and macro syntax/undefined checks pass, whitespace
clean. Logs `/tmp/liuxin-maintenance-final-{basedpyright,mypy,format}-20261103.log`.
Earlier complete architecture/import/complexity evidence is below; final fixes
introduced no imports or ownership changes.

Full resources `/tmp/liuxin-maintenance-full-summary-20261103.json`: peak treeRSS
1,154,023,424bytes (1.07GiB), minimum available3,848,171,520bytes (3.58GiB), OOM0.
The three hashes in `/tmp/liuxin-maintenance-pre-full-file-hashes.json` are unchanged.
No leftover private PG schemas; pg_ctl stopped cleanly and
`/tmp/liuxin-maintenance-adversarial-pg` was removed after asserting version16 and
no postmaster.pid. No functional work remains. Documentation evidence updated;
final whitespace/status audit passed and the goal was marked complete. No commit/PR
requested. Preserve existing user edits and all current uncommitted work.

All checkpoints below are historical, including references to running sessions.

## Latest checkpoint (03:17 UTC)

Full-suite **session51820** is still running, **9,165 passes/three known failures**.
Current family: tests/surfaces/test_text_browser.py, tag/link operations. This is
a large fresh-catalogue integration file (175 test functions, mostly two drivers),
so it advances about12–16cases/minute. Keep waiting; no restart or abandonment.
No additional failures since the ones below; all have source fixes and focused
proof, but the full run still uses previously imported code. No other test/quality
jobs are active. The private PG cluster remains idle and running. No OOM kills;
last process-tree RSS about475MiB. C: space stayed stable around8.8GiB.

After full completion: inspect exact traceback for third failure (expected clock
step delaying zero-backoff publication), summarize telemetry; run the three failed
cases through pytest plus affected regression coverage on livePG/SQLite. Suggested
bounded final selection: API parity file, read_transaction file, Catalog repairs
and inherited metadata, Core durable maintenance/polling, and runtime retry tests
(`test_job_handoff_restart_and_failure_retry`, `test_repeated_expired_workers_consume_retry_budget`,
`test_lazy_backoff_and_cancelled_jobs_require_explicit_retry`), all stale-worker
variants and the four new clock cases. No need to repeat unaffected 535-case matrix.
Run final formatting/scoped lint and both type checkers/internal contracts for the
latest source changes. Preserve prefs hashes recorded below. Update docs status,
final evidence/handoff/index; confirm private PG schema cleanup and stop/remove it.
No commit/PR requested. Only mark goal complete when these remaining checks finish.

## Earlier checkpoint (01:49 UTC, updated through02:14)

**02:14 update (current):** session51820 still runs,3,473passes/three failures.
The original two API failures are NOW FIXED: four repair wrapper annotations
match PortableMacrosAPI, and public read_transaction returns a private unchanged
contextmanager generator. Both AST parity tests pass via direct runpy invocation.
Still run them under pytest with read_transaction/Catalog repair tests after full.

Third failure is stale-worker `[sqlite-False-ledger]`. Telemetry proves a backward
wall-clock jump during its call:02:08:21.905349 ->02:08:21.163256, while monotonic
advanced0.148s. Zero-backoff failure receipts stored `now`, so a subsequent clock
step could hide them from publication until the clock caught up. Added four cases
to test_maintenance_broken_callbacks.py, both failure owners (runtime/jobs) and
SQLite/PG, with injected backward clocks. Initial2241:4failed11.52s; fixed61277:
4passed7.23s. Logs `/tmp/liuxin-maintenance-clock-initial.log` and `-fixed.log`.
Runtime `_record_failure` and core/maintenance_jobs reconciliation now store0 when
the millisecond delay is zero; positive backoff unchanged. No active extra testjob.

These implementation changes were made after full-suite imports; its three
original failures remain in that run. Inspect final tracebacks and rerun ALL three
failed cases plus expanded affected maintenance/Core/jobs/read-snapshot/repair/API
checks against final source, livePG as appropriate. Also rerun affected type, lint,
format gates and docs. Do not claim the original full invocation was green.

**01:55 update:** full session51820 remains live,1,887passes and **two failures**.
Both are `tests/databases/api/test_macros_api_signature_parity.py`. AST-only direct
reproduction confirms four new concrete repair methods lack the declared parameter
and return annotations, and `read_transaction` has Iterator[Any] rather than the
public API's AbstractContextManager[Any]. Plan after the full run: annotate the
four wrappers to match PortableMacrosAPI; give read_transaction the same public
context-manager-returning wrapper pattern as transaction(), moving its unchanged
generator body to private `_read_transaction`. Keep parity checks strict. Rerun
this API file, read snapshot and Catalog repair checks (livePG too), applicable
type/lint gates. No runtime failure observed so far; do inspect all final failures.

Resumed the existing **session51820**, which remains live. At the last sample the
full suite had **1,305 passes, no failures**, in Core storage ownership tests.
Do not restart it. Its log/telemetry/scratch paths and command are below. C: still
has about8.8GiB free; /tmp922GiB. No OOM kills, test tree around300MiB. The private
adversarial PG cluster is still running and idle; it must be cleaned up at the end.

No implementation changes on this resume. Refreshed docs/maintenance.md's date,
replaced the stale maintenance-modes.md note about future semantic repairs with a
link to the implemented Catalog policies, and added reproducible serial matrix/full
commands to the current validation report. Final checks should cover these docs.
The goal still reports usageLimited; continue the user's explicit resume, without
resetting accounting or marking complete before the full-suite result is known.

## Earlier checkpoint (01:18 UTC)

All preceding test/type sessions finished. The full suite is NOW RUNNING in
**exec session51820**. Command:

```bash
env -u LIUXIN_TEST_POSTGRES_URL PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q tests -n0 --durations=30 --telemetry-dir=/tmp/liuxin-maintenance-full-20261103-telemetry --basetemp=/tmp/liuxin-maintenance-full-20261103-tmp -o cache_dir=/tmp/liuxin-malformed-cache -o tmp_path_retention_policy=none -o tmp_path_retention_count=0 > /tmp/liuxin-maintenance-full-20261103.log 2>&1
```

Run it alone; don't restart on observation timeouts. Inspect its specific process
handle and telemetry events/resources. The private PG cluster remains running but
the full suite deliberately omits its URL; live PG was covered separately.

Completed evidence:
- Broad matrix3894: **535 passed,32 backend skips,one failure**,1625.08s. Failure
  traceback is subprocess.TimeoutExpired at30s for `[postgres-effects]`, empty
  stdout/stderr. That case passed in isolated session42155: **1passed9.32s**, log
  `/tmp/liuxin-maintenance-recovery-isolated-20261103.log`, and in the separate
  **14passed225.46s** hung/process selection. No timeout changed.
- Matrix resource summary `/tmp/liuxin-maintenance-adversarial-matrix-summary-20261103.json`:
  peak treeRSS152240128bytes (~145.2MiB), available memory minimum3554430976bytes,
  OOM counter unchanged0. Concurrent child imports were observed in D state.
- Added unsupported-subscription8cases passed13.75s (SQLite/PG).
- Core follow-up5896: **35passed1docfailure**,167.79s. All functional/polling/Core
  tests passed, including livePG. Documentation inventory was stale: six new
  methods missing owner links/fields, one duplicate-discovery summary mismatched.
  Updated service/protocol docs and inventory85->91. Follow-up3631:
  **17passed99.01s** in protocol contract + all developer doc links.
- Type sessions26504/5686: both green; basedpyright0errors, mypy227files, and both
  reject38invalid contract examples. Format264, lint, C901 including storage-manager
  gate, cycles255, layer1878 and all25 import budgets pass. Whitespace clean.

Full-suite source is now stable. New runtime source corrections and Core helper
refactor are verified by targeted tests; no more planned implementation changes.
Do fix genuine full-suite failures and rerun affected checks. Preference file
hashes before full run stored in `/tmp/liuxin-maintenance-pre-full-file-hashes.json`
for comparison; do not revert pre-existing changes. Final requirements still:
complete full run, inspect/resource-summary evidence, update docs/handoff, final
quality checks for subsequent changes, confirm schemas cleaned and stop/remove
only private PG cluster. Work uncommitted, all new files intent-to-add only.

## Earlier checkpoint (01:11 UTC)

Broad matrix3894 is still running (~89%, Catalog repair cases); known failure is
the 30-second PG subprocess timeout, already passed in the separate14-case process
selection. Eight extra subscription rollback cases passed in13.75s (log
`/tmp/liuxin-maintenance-subscriptions-20261103.log`); session38882 is finished.

The quality rerun13542 finished: format263, annotations463, cycle255, layer1878,
all25 import budgets and ratcheted lint passed; C901 found `maintenance_run`
complexity15>10. Split rebuild/upgrade/drain into private helpers, retaining
validation and result shape. Protocol annotations now use Mapping[str,object].
The initial type checks40496/76979 then found a missing worker annotation, missing
required log_exception level, nullable drain budget and possibly unbound rebuild
receipt. These were corrected, including actual failure-path regressions:

- `_poll_work(worker: JobWorker)` now uses default_log.error with the safe bounded
  exception message. The old call raised TypeError after publication errors and
  stopped existing queued delivery. New test uses an actual JobRepository/Worker
  and verifies successful delivery after ordinary/unprintable publication errors.
- `_drain_jobs` refuses None before starting jobs; `_rebuild_work_pages` executes
  a definite first page then bounded successors. Rebuild no longer has a possibly
  unbound receipt. Existing declaration / diagnostic fixes are unchanged.
- New `tests/core/test_maintenance_polling_failures.py` (3 initial failures
  reproduced), intent-to-add and format ratchet (now264). Scoped lint/complexity pass.

**Current additional sessions**:
- **5896**: Core polling/durable maintenance/service-contract regressions, with
  livePG, `/tmp/liuxin-maintenance-core-final-20261103.log` + telemetry/scratch.
- **26504**: basedpyright + its internal contracts,
  `/tmp/liuxin-maintenance-adversarial-basedpyright-final-20261103.log`.
- **5686**: mypy + its internal contracts,
  `/tmp/liuxin-maintenance-adversarial-mypy-final-20261103.log`.

Full repository suite is still NOT started. Let matrix/Core/type checks finish,
inspect failures, then run full -n0 with telemetry alone. Check the latest source
before relying on older completed logs. Finish quality gates remaining after C901
(storage-manager complexity, finalformat264, type/internal contracts), final docs
links, telemetry summary and cluster cleanup. Do not mark completion early.

## Earlier checkpoint (01:02 UTC)

The broad matrix **session3894** is still running (~77%, production writer cases).
It has one failed case: PG process-recovery `postgres-effects`, duration30.45s,
apparently subprocess timeout during simultaneous quality/import work. Inspect
the final traceback before concluding. The separate hung/process recovery run
**session98869 finished: 14 passed in225.46s**, including that same existing case
and all four new externally killed hung-handler cases. No OOM events observed.

Initial quality **session54987 finished with four lint failures**: newly added
Core handler protocols used undefined `Any`. Corrected all four return annotations
to existing `Mapping[str, object]` style. The rerun is **session13542**, log
`/tmp/liuxin-maintenance-adversarial-quality-final-20261103.log`; formatting263,
cycle255 and layer1878 have passed. Do not restart it. Earlier scoped format/lint
session88021 has finished successfully.

Added eight new unsupported-subscription install rollback cases to
`test_maintenance_malformed_plugins.py` after broad matrix collection. They need
their own SQLite/livePG run after current heavy work finishes (combine with the
isolated PG process timeout rerun if useful). New `test_maintenance_hung_plugin.py`
has intent-to-add and format-ratchet coverage. Docs now describe exact booleans,
sync callback contracts, safe diagnostics and external supervision of stuck code.

Full repository suite has NOT started yet. Wait for current matrix/quality work
to finish first to avoid competing import I/O and flaky subprocess timeouts. Full
run should omit livePG URL (already covered in matrix), use -n0, durations30,
telemetry and /tmp scratch, retaining no per-test temp data. No source logic
changes are pending beyond failures identified by the running checks.

## Earlier live progress (00:55 UTC)

- Private PostgreSQL 16 cluster now RUNNING at
  `/tmp/liuxin-maintenance-adversarial-pg/data`, socket sibling `socket`, port55436.
  URL: `postgresql://blackjane@/postgres?host=/tmp/liuxin-maintenance-adversarial-pg/socket&port=55436`.
  No TCP listener. Stop/remove this cluster only after all live tests finish.
- **session3894**: broad maintenance/Catalog/Core/jobs/CLI matrix, log
  `/tmp/liuxin-maintenance-adversarial-matrix-20261103.log`, same prefix telemetry
  `-telemetry`/scratch `-tmp`. Around 63% with no failures at last check. All new
  malformed/callback tests precede the old matrix and passed so far. Full selection
  includes 18 files, see the log telemetry for node IDs. Do not restart on timeout.
- **session98869**: hung-plugin plus existing process recovery tests, log
  `/tmp/liuxin-maintenance-hung-20261103.log`, same telemetry/scratch convention.
  Four new cases externally kill a child after its handler signals uncommitted
  effects. No runtime preemption claim. The existing process helper now accepts
  optional mode/blocked phase; old behavior remains unchanged.
- **session54987**: complete offline quality script (`bash scripts/run_type_checks.sh`),
  log `/tmp/liuxin-maintenance-adversarial-quality-20261103.log`. Formatting262 and
  import-cycle255 passed; layer/import/type steps still running. New hung test
  made the format scope263 afterward, so run the format gate again at completion.
- All earlier small runs are finished. Initial malformed selection33failed/27pass;
  corrected malformed+upgrades110passed on SQLite/PG. Callback/control initial
  113pass/4fail demonstrated broken exception formatting. Worker diagnostics initial
  had a missing request_key fixture bug; fixed then reproduced all three intended
  failures. Their corrected cases are included in session3894.

Implementation changes this goal:

- `declarations.py`: strict NUL/default-enabled/numeric storage limits, reserved
  maintenance-table refusal, synchronous callable hooks (including callable
  objects), valid unique upgrade policy declarations. `require_sync_callable` and
  `reject_deferred_result` cover native async/generator functions and hidden
  deferred results; native coroutine/generator results close before refusal.
- runtime `_handle` requires exact bool selection and synchronous key/result;
  upgrades `_migrate` requires synchronous completion/None result. Errors roll back.
- New leaf `utils/errors.py::exception_message` makes diagnostics printable,
  bounded2000 and resilient to broken __str__. Maintenance failure recording and
  jobs worker now use it, preserving original exceptions and durable retry state.
- New tests: malformed plugins, broken callbacks, hung plugins, worker diagnostics.
  Format scope additions (five files). Docs contract and plugin docstrings updated.
  New files have intent-to-add only. Scoped lint passes; no commit requested.

Next: finish current live checks, fix any failures; run full repository suite with
telemetry, `-n0`, tmp_path_retention_policy=none under /tmp. The full run can omit
the live PG URL because session3894 already covers that backend extensively.
Inspect full-suite failures and resource summary; finish doc links/format/whitespace,
update dated evidence and this handoff, confirm schema cleanup, stop/remove private
PG cluster. Only mark goal complete when scope is actually verified. Current
runtime cannot preempt an infinite handler or sandbox arbitrary trusted Python;
the kill test proves transaction recovery under external process supervision.

Initial inspection (now fixed): `wants_event` accepted non-booleans, so None silently skipped and
acknowledges events. Upgrade callbacks only validate callable, allowing async or
generator functions to be accepted without executing their body. Declaration
validation does not check hook callability/synchronous behavior or exact default
enabled type; internal journal subscriptions can be requested. Add failing tests,
fix reproduced boundaries, run expanded SQLite/live PG matrix, production APSW
writers and full suite with telemetry. Record handles and results here as needed.
