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
delete their newly created directories. The initial implementation validation did
not run the full suite; the subsequent diagnostic is recorded below.

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

## Full-suite diagnostic and scratch-storage follow-up

The subsequent serial run collected 8,323 tests and was stopped after about
25 minutes by a host-free-space guard: C: dropped from approximately 1.7 GiB free
to 738 MiB. WSL's virtual disk is on that volume. The pytest JSON report records
1,659 passed, six failed, 56 skipped and six xfailed before interruption. The six
failures are `database is locked` errors in creator/organisation sidecar writes
for both SQLite and APSW. They remain separate follow-up work.

The run accumulated about 3.1 GiB in its per-test temporary tree, predominantly
database copies; its durable telemetry artifacts occupied about 15 MiB. Peak
sampled process-tree RSS was about 685 MiB, with no swap activity or OOM kills.
The abrupt host-space drop is consistent with VHD allocation growth, but VHD
allocation was not measured before/after and other host writes are not excluded.
The VM stayed up. Pytest 9.0.2 raised a tmp_path stash KeyError during interrupted
teardown, after recording the report; the resource sampler shut down cleanly.

Artifacts and the detailed report remain under the Windows checkout's
`working-memory/test-results/vm-full-20261017T150349Z`.

Scratch-storage changes use pytest's native retention policy: passing cases
release their temporary directories during the run, failed-call artifacts remain,
and only two recent retained sessions are kept. `minversion` now matches the
project's existing pytest >=7.4 dependency. The root fixture restores cwd before
tmp_path cleanup, and the bootstrap preferences directory now has process-scoped
TemporaryDirectory ownership. Shared fixture templates already live under the
session temporary tree; no new global cache cleanup is needed.

The diagnostic guide documents `-o tmp_path_retention_policy=all` for fixture-error
investigations, because pytest bases per-test retention on the call outcome, plus
explicit-basetemp ownership and relocating scratch files to a larger volume.

Validation: 72 tests passed in 84.10 seconds across
`tests/scripts/test_test_scratch_cleanup.py`,
`tests/scripts/test_pytest_telemetry.py`, and
`tests/databases/database/database_contract/test_db_row_roundtrips.py`, using
`-n 0`, telemetry, a JSON report, and a fresh explicit `--basetemp` to avoid
pruning the earlier interrupted run's evidence. The resulting scratch tree held
only the shared test_db_13 template (about 4 MiB); per-test copies were gone.
The sampler and pytest exited cleanly with no invalid telemetry records.
The new subprocess regressions cover cleanup between real database cases,
failed-call preservation, the keep-all override, two-session history retention,
bootstrap preferences cleanup, durable logs, and leaving cwd before deletion.
Ruff lint/format checks for the new regression module and whitespace checks pass.
Host C: still had only about 519 MiB free, so another full run awaits more host
space; the earlier six database-lock failures remain unresolved.
