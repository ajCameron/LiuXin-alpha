# Maintenance failure and malformed-plugin validation

Completed: **2026-11-03**. Status: **validated; all reproduced failures resolved**.

This pass thoroughly tested the maintenance changes, including broken and
malformed plugins. Fresh checks cover the current uncommitted implementation;
the final results below distinguish the original full run from verification of
its fixes. This review includes the durable prompt/lazy
runtime, rebuilds and upgrades, database writers, Core/jobs delivery, and Catalog
operations that depend on maintenance transaction guarantees.

## Required evidence

1. Invalid declarations, non-callable or asynchronous hooks, malformed upgrade
   policies and unsupported subscriptions fail before persistent installation or
   migration. A failed operation leaves existing source data and configuration
   intact and permits recovery with a corrected implementation.
2. Selection, coalescing and handler exceptions or malformed return values never
   silently acknowledge unfinished work. Prompt failures roll back source/effects;
   lazy failures preserve the committed source, pending work and retry evidence.
3. Borrowed connections and cursors reject transaction/schema control and expire
   after both success and failure. Multi-statement SQL cannot publish partial work.
   Failed migrations preserve the old version, subscriptions, projections and jobs.
4. Retry exhaustion, dependencies, downstream prompt failures, generated cycles,
   process termination and stale workers retain the documented atomicity and
   recovery behavior. One failing lazy plugin must not starve independent work.
5. Run the expanded failure matrix on SQLite and live PostgreSQL, production writer
   cases on SQLite/APSW/PostgreSQL, then the broader affected regressions and full
   repository suite with resource telemetry and scratch under `/tmp`. Record exact
   outcomes, investigate failures and run applicable quality/documentation gates.

Plugins are trusted in-process Python. These checks establish API and database
transaction behavior, not isolation from deliberate reflection, arbitrary file
writes or a handler which never returns. Declaring success with semantically wrong
SQL also requires plugin-specific correctness tests. Those limits must remain
explicit in the final assessment.

## Coverage map

| Boundary | Regression evidence |
| --- | --- |
| Invalid declarations, hooks, return values and physical subscriptions | [Malformed plugin tests](../../tests/databases/test_maintenance_malformed_plugins.py) cover attachment refusal, installation rollback, exact booleans, complete receipts and recovery after correction. |
| Exceptions, deferred wrappers, transaction/schema control and expired cursors | [Callback failure tests](../../tests/databases/test_maintenance_broken_callbacks.py) exercise both consistency modes, migration rollback, independent jobs and safe retry diagnostics. |
| Process interruption and handlers which never return | [Process-loss tests](../../tests/databases/test_maintenance_process_recovery.py) and [externally killed hung handlers](../../tests/databases/test_maintenance_hung_plugin.py) verify durable recovery on SQLite/PostgreSQL. |
| Jobs and continued Core polling | [Worker diagnostics](../../tests/jobs/test_worker_error_diagnostics.py) and [polling failure tests](../../tests/core/test_maintenance_polling_failures.py) verify failure receipts and continued queued delivery. |
| Existing runtime, upgrades, rebuilds and production writers | The broad matrix includes all maintenance modules, real SQLite/APSW/PostgreSQL writers, Core/jobs, CLI, Catalog repair and read-only inheritance checks. |

## Outcomes

The initial malformed-plugin selection reproduced **33 failing cases**, with 27
passes. Fixes reject non-boolean selection results, invalid/default declarations,
internal-table subscriptions, non-callable/async/generator hooks and malformed
upgrade policies. Deferred work returned by synchronous wrappers is also refused.
The first corrected SQLite/live PostgreSQL selection passed **110 tests**.

An additional callback/control matrix passed **113 tests** and exposed four
diagnostic failures across both backends: a plugin exception whose `__str__`
raises could replace the original failure and prevent retry evidence. Further
checks cover null/surrogate text and oversized diagnostics in maintenance and the
real jobs worker; these cases are included in the corrected broad matrix below.

The expanded SQLite/APSW/live PostgreSQL matrix finished with **535 passes, 32
backend-specific skips and one failure** in 1,625.08 seconds. The failure was the
30-second subprocess deadline in PostgreSQL process recovery; the same case passed
in isolation in 9.32 seconds and in the separate **14-pass** process-loss/hung-handler
selection. No timeout was widened. The matrix recorded 145.2 MiB peak test-process
tree RSS and no OOM events. Resource telemetry does not establish the exact cause
of a transient timeout; concurrent imports were observed waiting on filesystem I/O.

Eight further invalid subscription cases passed on SQLite/PostgreSQL: views,
missing keys, composite keys and text keys all roll installation back. The Core
follow-up passed 35 checks and exposed stale protocol documentation; the corrected
documentation/link selection passed **17 tests**. Both type checkers and their
38-invalid-call contract probes now pass. Quality checks also identified and
corrected an over-complex command dispatcher and undefined protocol annotations.

The type-check pass exposed a runtime polling defect: an invalid logging call
could stop polling after publication failed. Real queued-job regressions now show
that ordinary and unprintable publication errors leave delivery running. A null
drain budget is rejected before worker startup. Formatting covers 264 files;
scoped lint, complexity, import-cycle, layer-direction and 25 cold-import budgets
pass. The full repository suite ran serially with `/tmp` scratch and telemetry,
without the optional PostgreSQL URL (its live matrix ran separately). It completed
in **8,281.34 seconds**: **9,757 passed, three failed, 403 skipped, 21 expected
failures and 1,583 warnings**. The warnings concern existing markers, collection
helper classes and deprecated APIs; no maintenance coroutine or thread-failure
warnings were reported.

The full run exposed API signature mismatches in the new repair/snapshot methods,
now aligned with the portable contract without weakening the parity assertions.
It also exposed a zero-backoff scheduling defect when the VM clock stepped
backwards. During the failing stale-worker test, wall time moved backwards by
0.742 seconds while monotonic time advanced by 0.148 seconds. Failure receipts
stored the current wall time even for immediate retries; publication could then
temporarily treat them as future work, and delivery could reject them as waiting
for retry. The full-suite traceback confirms that delivery rejection. Four
deterministic regressions reproduced
the defect on SQLite/PostgreSQL through runtime and jobs failure recording; all
four pass after storing zero for a zero-millisecond delay. Positive backoff retains
its persisted wall-clock deadline.

The final regression run passed **83 tests**, with **four backend-specific skips**,
in **450.16 seconds**. It includes all three full-suite failures, the four new clock
cases, ordinary/backoff/expired-worker retries, real SQLite/APSW/PostgreSQL repair
operations, concurrent PostgreSQL snapshots, Core delivery/polling and developer
documentation links. The full invocation was not repeated after these focused
fixes; its three failures are all resolved by this final run.

Final basedpyright reports zero errors/warnings/notes; mypy reports **227 clean
source files**. Both accept the valid internal contracts and reject all **38 invalid
examples**. The final **264-file** formatting gate, changed-code lint and diff
whitespace checks pass. Earlier architecture/import/complexity gates remain green;
the final corrections added no imports or changed module ownership.

The full run recorded **1.07 GiB peak process-tree RSS**, at least **3.58 GiB
available memory**, and **no OOM kills**. C: had about **8.4 GiB free** afterward;
scratch remained on `/tmp`. All three pre-run preference-file hashes match their
post-run values. These measurements describe this run, not a host power-loss or
filesystem-corruption guarantee.

The private PostgreSQL 16 cluster had no leftover disposable schemas and was
stopped and removed after the final backend tests. Work remains uncommitted on
`maintenance-plugin-modes`; existing data-submodule and preference edits are
preserved.

The five required-evidence areas above are covered. In-process plugins remain
trusted Python: externally killed hung-handler tests prove database recovery,
not in-process preemption or rollback of filesystem/network effects. Positive
retry and jobs-lease deadlines still depend on wall time; this pass specifically
fixes an immediate retry being delayed by a backward clock step.

Related: [maintenance contract](maintenance-modes.md),
[previous completion evidence](maintenance-completion-plan-2026-11-02.md).

## Reproduce the checks

Set `LIUXIN_TEST_POSTGRES_URL` to a disposable PostgreSQL test server for the live
backend cases. Fixtures create and remove their own schemas. Run this selection
serially, including the additional hung-handler and polling regressions:

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -n0 \
  tests/databases/test_maintenance_*.py tests/databases/test_read_transaction.py \
  tests/core/test_durable_maintenance.py tests/core/test_maintenance_polling_failures.py \
  tests/jobs/test_worker_error_diagnostics.py tests/jobs/test_durable_job_claims.py \
  tests/jobs/test_jobs_repository.py tests/catalog/test_catalog_repairs.py \
  tests/catalog/test_inherited_metadata.py tests/surfaces/test_cli_operational_families.py \
  --telemetry-dir=/tmp/liuxin-maintenance-regression-telemetry \
  --basetemp=/tmp/liuxin-maintenance-regression-tmp \
  -o cache_dir=/tmp/liuxin-maintenance-pytest-cache \
  -o tmp_path_retention_policy=none -o tmp_path_retention_count=0
```

The full repository run uses the same serial/scratch settings and a separate
telemetry directory. It omits the optional live PostgreSQL URL, whose maintenance
coverage is supplied by the selection above:

```bash
env -u LIUXIN_TEST_POSTGRES_URL PYTHONDONTWRITEBYTECODE=1 \
  .venv/bin/python -m pytest -q tests -n0 --durations=30 \
  --telemetry-dir=/tmp/liuxin-maintenance-full-20261103-telemetry \
  --basetemp=/tmp/liuxin-maintenance-full-20261103-tmp \
  -o cache_dir=/tmp/liuxin-maintenance-pytest-cache \
  -o tmp_path_retention_policy=none -o tmp_path_retention_count=0
```

Keep other import-heavy checks out of the subprocess recovery tests' execution
window. Summarize either telemetry directory with
`scripts/test_resource_sampler.py summary DIRECTORY`. These test selections overlap;
their pass counts must not be summed as distinct cases.
