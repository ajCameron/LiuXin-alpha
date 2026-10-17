# Diagnosing interrupted test runs

## Keeping scratch storage bounded

The project defaults to `tmp_path_retention_policy=failed`. Pytest removes a
passing test's temporary directory during teardown, including its writable
database copies, instead of accumulating every copy until the run ends. The
session template cache remains reusable within that run. At most two recent
retained pytest sessions are kept (`tmp_path_retention_count=2`). This applies to
direct pytest commands and all suite runners, without enabling telemetry.

Python's default `tempfile` directory and child-process `TMPDIR`/`TEMP`/`TMP`
environment variables also point inside each test's scratch directory. This
contains legacy helpers that allocate temporary files without an explicit
destination. Explicit destinations remain the caller's responsibility; reusable
session fixtures should use `tmp_path_factory` rather than a test's default temp
directory.

The 2026-10-17 serial diagnostic accumulated about 3.1 GiB of temporary files in
the first 1,727 tests; nearly all of it was fixture databases. The host C: drive
then fell below 1 GiB free, triggering a controlled stop. Test-process memory
peaked near 685 MiB, with no swap use or OOM kills. The WSL virtual disk was stored
on C:, so `/tmp`'s large reported filesystem capacity did not mean the host had
room for further virtual-disk allocation. This is evidence of a storage problem,
not proof of the cause of previous VM crashes.

The later run with aggressive cleanup covered all collected cases across two
serial segments: 8,196 passed, 29 failed, 87 skipped and 23 xfailed. The first
segment's sampled visible scratch peaked at 162 MiB; including deleted files
still held open, its combined peak was 483 MiB. The continuation peaked at
120 MiB including open deleted files and left no scratch after exit. The VM
stayed up, but host free space still changed by several GiB without comparable
changes in these measured trees. See [the diagnostic record](../test-vm-telemetry-2026-10-17.md)
for the telemetry fix required to resume, failure groups and measurement limits.

That run exposed auxiliary database connections surviving facade shutdown.
`Database.close()` and driver replacement now release all driver-owned handles.
A 14-case comparison reduced deleted-open database storage from 54.7 MiB to zero
at every test boundary, before pytest exited; 80 lifecycle checks also passed
with two Windows-only skips. The full-suite results above predate this fix.
Fixtures must still close their databases explicitly, before discarding ownership
references with `break_cycles()`. These changes release file allocations without
forcing garbage collection; they do not compact the Windows VHD.

To keep all scratch files while debugging a small selection:

```bash
.venv/bin/python -m pytest tests/path/to/test_file.py \
  -o tmp_path_retention_policy=all
```

Pytest's per-test retention decision uses the test **call** outcome. Use that
override when diagnosing setup/teardown failures or interrupts: it does not
guarantee that their directories survive under `failed`. Temporary-directory
removal is best effort on platforms with open-file restrictions. An abrupt
process/VM exit can also bypass cleanup; normal session-history pruning applies
on later runs. Explicit `--basetemp` directories are owned and cleared by pytest
on reuse, so never point that option at existing data you want to keep.

Keep diagnostic logs outside per-test `tmp_path` directories, such as under this
directory, so passing-test cleanup does not discard them. The telemetry artifacts
from the interrupted run occupied only about 15 MiB; their retention is separate
from pytest scratch storage. For a genuinely larger temporary volume, create a
directory there and set `PYTEST_DEBUG_TEMPROOT` to it before running pytest.
Check host free space as well as the Linux filesystem before another large run.

### Aggressive cleanup during the run

For runs where reports and telemetry are sufficient evidence, select `none`:

```bash
.venv/bin/python -m pytest -n 0 tests/path/to/test_file.py \
  -o tmp_path_retention_policy=none \
  --telemetry-dir=working-memory/test-results/low-space-probe-001
```

The project scratch plugin removes each completed test's directory after all
fixture finalizers, before the next case. This includes setup, call and teardown
failures: **failed tests' scratch files are discarded in this mode**. Logs and
reports saved outside per-test directories survive. Native pytest alone defers
`none` cleanup to session-history pruning; the plugin makes it happen during the
run. The full-suite runners accept the same `-o` option after `--`.

The per-test hook is confined to the current test's directory under its worker's
pytest base. It does not sweep other runs or shared templates, follow directory
symlinks, or delete explicitly allocated files elsewhere. Separately, native
pytest session-history cleanup keeps zero sessions under `none`; use a dedicated
`PYTEST_DEBUG_TEMPROOT` if older diagnostic sessions must remain untouched.
Removal failures produce a pytest warning, so open handles or permission errors
are visible. An abrupt process exit
can bypass cleanup, and deleting a file cannot release storage still held by an
open handle. This limits accumulation between cases; it cannot prevent one active
test from filling the disk. Start with host disk headroom and a small worker count.

## Recording test activity and resource use

Enable telemetry with `--telemetry-dir`. The pytest plugin writes test activity
before collection and before each setup, call and teardown phase. Every record is
flushed and `fsync`ed; Linux also syncs the directory entry for each new log file.
Ordinary runs do not start a sampler or write telemetry.

Start with a small, serial selection on the unstable VM:

```bash
.venv/bin/python -m pytest -n 0 -x tests/scripts/test_test_streams.py \
  --telemetry-dir=working-memory/test-results/vm-probe-001
```

Choose a new directory for each run, then replace the test path with a small
suspect selection. The default sample interval is one second; change it with
`--telemetry-interval=0.5` (minimum 0.1 seconds). For WSL, choose a directory on a
durable Windows mount if the WSL filesystem may be discarded. This checkout's
`working-memory/test-results` is on `/mnt/c` when run from the Windows checkout;
the same relative path in a `/tmp` worktree is **not** on that mount. Paths are
resolved before fixtures change the working directory. Logs are ignored by Git.

The full-suite shell and Python runners pass through pytest options after `--`:

```bash
scripts/run_full_test_suite.sh --skip-install --workers 0 -- \
  --telemetry-dir=working-memory/test-results/vm-full-001
```

That command runs the full suite. Use it only when ready for that diagnostic;
the existing runners otherwise default to `--workers auto`, which scales by CPU
count and may put much more pressure on a memory-limited VM. Telemetry itself does
not cap memory, stop a runaway test, or change worker counts. Avoid concurrent
quality/build jobs in the same checkout: the autouse root-artifact guard can
remove their newly created root directories during test teardown.

After a normal exit, interruption, or VM restart:

```bash
.venv/bin/python scripts/test_resource_sampler.py summary \
  working-memory/test-results/vm-probe-001
```

The summary streams the files and reports each worker's unfinished test/phase or
collector, last completed test, session exit status, latest resource sample,
maximum sampled process-tree RSS and minimum available system memory. It tolerates
a truncated final JSON record and counts invalid records. A missing
`session_finish` means pytest did not record a normal session finish; an ordinary
assertion failure has a finish record with a nonzero status.

Files in each directory:

- `events-<worker>-<pid>-<token>.jsonl`: collection, test/phase start and finish,
  outcomes, UTC and monotonic timestamps. Each worker writes its own file, so
  parallel failures retain separate active-test records. Reusing a directory
  retains older runs, which the summary lists separately.
- `resources-controller-<pid>-<token>.jsonl`: on Linux/WSL, an independent process
  samples available RAM, swap, shared memory, memory pressure, kernel OOM/swap
  counters, disk usage for the initial temporary directory, `/dev/shm` and log
  directory, and pytest plus its descendants (PIDs, names, RSS, swap, CPU ticks and
  thread counts). The sampler can run while pytest's Python interpreter is stuck.
  It exits when pytest closes its control pipe or disappears, without signalling
  pytest. Short-lived children between samples and reparented children may be
  missed. Summed RSS can double-count shared pages.
- `sampler-<token>.stderr`: sampler errors. Events also record the sampler exit
  code. Check this file if resource samples are absent or stop before test events.
  Other platforms retain activity logs and explicitly record that Linux resource
  sampling is unavailable.

Everything stays in local files. No environment variables, command lines, test
output or traceback bodies are captured; test IDs and paths can still contain
project data. Logging adds disk I/O and can change timing. Sampling can miss a
short allocation spike, and `fsync` cannot guarantee recovery after host or disk
failure. The last active test is a candidate for investigation, not proof of
causation. Compare its timestamps and resource trend with kernel/Windows logs.

For the current VM, the previous boot's journal showed an unclean shutdown but no
recorded OOM kill or kernel panic. Historical resource samples were unavailable.
The cause of the restart remains unproven.
