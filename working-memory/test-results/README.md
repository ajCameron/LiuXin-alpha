# Diagnosing interrupted test runs

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
