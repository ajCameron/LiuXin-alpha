# Test VM telemetry — 2026-10-17

PR #118 (`catalog-write-consolidation`) was promoted from draft to ready for
review at the user's request. Its existing PostgreSQL verification gaps remain
recorded in the PR and the Catalog handoff; promotion does not resolve them.

The separate `test-vm-telemetry` branch starts at `af2cebd0` and adds opt-in
diagnostics. Work happened in `/tmp/liuxin-test-vm-telemetry`, leaving the main
checkout's ongoing documentation reorganization and storage edits untouched.

Use `--telemetry-dir=PATH` on any pytest selection. The plugin records collection,
test setup/call/teardown and outcomes in flushed, fsynced JSONL files. Each xdist
worker has a separate event log. One independent Linux sampler records the pytest
process tree and system memory, swap, memory pressure, OOM counters, shared-memory
and disk usage. It stops when pytest disappears or closes its control pipe.
Other platforms retain the event logs without Linux resource sampling.

The [diagnostic guide](test-results/README.md) has serial commands, recovery
instructions and limitations. Start with a small selection and `-n 0`: the
existing full-suite runners default to `auto` workers. Do not run concurrent
quality/build jobs in the same checkout because the test root-artifact guard can
delete their newly created directories. No full suite was launched for this task.

Read-only VM investigation found an unclean previous shutdown but no recorded
OOM kill or kernel panic. At idle, the VM had about 7.6 GiB RAM and 2 GiB swap;
this says nothing about usage at the time of the crash. Historical sysstat samples
were unavailable. The legacy RAM-disk helper can allocate under `/dev/shm`, but we
have not established that it ran at the time of the crash. The cause remains
unproven; the last active test is an investigative lead, not a causal conclusion.

Validation uses only tiny isolated subprocesses and existing harness contracts.
Coverage includes normal pass/fail/skip outcomes, relative paths after cwd changes,
SIGKILL during collection and all three test phases, continued sampling while
pytest is SIGSTOPped, monitor cleanup, two-worker logs, invalid intervals,
truncated final records and process descendants. Logs from the final validation
are written to the Windows-mounted checkout under
`working-memory/test-results/telemetry-validation-final-20261017` (ignored by Git).

Final result: **47 passed in 19.08 seconds** across telemetry, test-stream and CI
workflow contracts. Ruff lint/format and the full-suite runner's serial telemetry
dry run passed. The standalone summary command confirmed a clean session and
sampler exit, no stale collectors and no invalid records in the Windows-mounted
logs. Peak sampled process-tree RSS was about 145 MiB during this small validation;
this is not evidence about the original VM crash or full-suite resource usage.
