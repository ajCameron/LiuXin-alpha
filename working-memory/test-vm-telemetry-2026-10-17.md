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

## Aggressive cleanup during active runs

A follow-up audit found two remaining gaps: ordinary Python/child-process temp
allocations escaped per-test ownership, and pytest's native `none` policy only
pruned directories at session cleanup. The shared `pytest_scratch` plugin now
redirects `tempfile.tempdir` plus `TMPDIR`/`TEMP`/`TMP` into each test's directory.
This also contains the legacy `_EphemeralScratchFolderManager` fallback. Explicit
destinations and session caches retain their existing ownership.

With `-o tmp_path_retention_policy=none`, a teardown wrapper removes only the
current test's directory after fixture finalizers, even for setup/call/teardown
failures. Registration is limited to descendants of the worker's pytest base;
symlink targets and unrelated paths are not swept. Removal failures emit warnings.
The default `failed` policy still preserves failed-call data; `all` retains scratch
for debugging. The guide makes the aggressive mode's loss of failed-test scratch
explicit. Logs outside per-test directories survive all policies.

Eleven subprocess regressions passed in 52.05 seconds, including real database
copies in serial and two-worker runs, deletion before the next case, all three
retention policies, standard/legacy/child-process temp allocations, explicit
destination and symlink-target preservation, fixture-error cleanup after
finalizers, and durable failure telemetry.

A further 84 existing tests passed in 98.91 seconds under `none`: telemetry,
database row round trips, temporary-directory database initialization, pure SQLite
driver behavior, legacy object helpers, and storage error messages. This run used
`-n 0`, a fresh explicit basetemp, telemetry and a JSON report. A mid-run spot check
found only the shared template plus the active test's files; after exit only the
4,108,288-byte shared database template remained. Session and sampler exits were
clean, with no invalid records. Ruff lint/format and whitespace checks passed.
This is 95 focused passing checks, not a full-suite result or a measurement of
Windows VHD compaction. No full suite was repeated on the nearly full host.

## Full run after host space was freed

The run from commit `819b0a67` started 2026-10-17 at 18:41:05 UTC.
The user restored about 9.5 GiB of host C: headroom and requested the full suite.
The existing environment collected 8,334 cases and ran them serially with
`tmp_path_retention_policy=none`, a fresh explicit basetemp, JSON reporting,
durable telemetry, and `faulthandler_timeout=120`. The separate launcher stops
below 2 GiB of host free space or below 512 MiB available RAM for five samples.
It also samples owned scratch size and allocated bytes in deleted files still
held open by suite processes. Earlier diagnostic scratch/history is untouched.

Artifacts are in the Windows checkout under
`working-memory/test-results/vm-full-aggressive-20261017T184105Z`, including the
exact command/commit, launcher, resource guard samples and test/resource logs.
The report contains **7,226 passed, 11 failed, 86 skipped and 12 xfailed** after
65 minutes 52 seconds. It is incomplete: pytest stopped with internal-error
status 3 at `test_directory_sync_ignores_only_open_failure`, leaving 999 cases
untouched and that test without completed teardown. The shell runner returned 1.
The VM remained up and neither resource guard fired.

The interrupted test patches the shared `os.fsync` function to raise. Telemetry
used that mock in its call-finish/report hooks before monkeypatch teardown, which
turned a successful application assertion into a telemetry failure and aborted
pytest. Commit `ea316ad5` binds telemetry's original open/fsync/close primitives
independently of application mocks. A new subprocess regression exercises mocks
across setup, call and teardown reporting, a newly created log, and the next test.
All 41 telemetry and shared-surface documentation checks passed in 19.87 seconds;
session and sampler exits were clean with no invalid telemetry records. Ruff
lint/format and whitespace checks passed.

The continuation from `ea316ad5` ran from 19:50:35 to 20:31:25 UTC, under
`working-memory/test-results/vm-full-aggressive-resume-20261017T195035Z`.
It collected the full suite, then deselected exactly the 7,334 node IDs with durable
`test_finish` records. It selected 1,001 cases: 999 untouched cases, the interrupted
test and the new regression. Both telemetry-related cases passed. The
selection plugin, manifests, exact launcher and reports are retained alongside
the telemetry. The report contains **970 passed, 19 failed, one skipped and 11
xfailed**, with normal pytest failure exit status 1. Session and sampler exits
were clean, with no invalid records, unfinished collectors, cleanup warnings or
resource stops. Its scratch directory was empty after process exit.

The checked union covers **every original collected node ID**, plus the new
regression: **8,196 passed, 29 failed, 87 skipped and 23 xfailed** across 8,335
unique cases. JSON-reported durations total 106 minutes 26 seconds. The earlier
telemetry-induced failure is replaced by its successful rerun, not counted twice.
This is coverage across two processes and commits, not a single uninterrupted
run or a passing full suite. Seven additional collection skips cover six gated
legacy CalibreCache modules and the missing Hypothesis dependency; the resumed
case skip was missing tkinter. PostgreSQL and parallel full-suite behavior remain
unverified. `full-suite-coverage.json` and `combine-coverage.py` in the first
segment's artifact directory preserve and verify the complete node mapping.

The 29 remaining failures group as follows:

- Six creator/organisation sidecar writes raise `database is locked`, matching
  the earlier diagnostic for both SQLite and APSW.
- Two direct-insert identity-key assertions retain spaces in `tag_phash`.
- Ten browser tag/genre cases fail searches or normalization assertions; the
  genre wizard stores `science fiction` where the test expects `science_fiction`.
  A common normalization cause is plausible but has not been fully isolated.
- Three stale surface contracts reference the removed `surfaces/cli/storage.py`,
  renderer `__getattr__`, or surface-root `__all__`. The relevant files/tests
  already had this mismatch before telemetry (`af2cebd0`).
- Eight image fallback cases: five import `img.py` and fail on missing PyQt5;
  two imageops cases require an unavailable ImageMagick backend; one propagates
  the injected CLI `RuntimeError('boom')` instead of falling back to Pillow.
  These implementation/test files are unchanged by the telemetry branch.

During the first segment, visible allocated scratch peaked at 161.8 MiB. Deleted
files still held open by suite processes reached 327.1 MiB; the simultaneous
combined peak was 483.2 MiB. Snapshots identify database handles retained from
much earlier cases. After process exit, those open allocations were released;
about 156 MiB of session templates and the interrupted test's directory remain.
This is much smaller than the earlier 3.1 GiB accumulation, but deletion alone
does not close database connections or compact the Windows VHD. The descriptor
scan excludes mappings without an open descriptor and processes outside the
runner's process group.

The continuation peaked at 3.9 MiB visible scratch and 116.0 MiB deleted-open
allocation, with a simultaneous combined peak of 119.9 MiB. The first segment's
retained 156 MiB was still present separately. After continuation exit its owned
scratch and deleted-open allocations were zero. The two segments' durable
artifacts occupy about 113 MiB. Counts cover their owned basetemp trees, not
arbitrary paths elsewhere or physical VHD allocation.

The open-handle follow-up required tracing ownership, not only missing fixture
finalizers: the aggregate/API tests already use `with Database(...)`, and
`database_contract.open_db` calls `db.close()` in `finally`. At this checkpoint,
`Database.close` did not call `StorageManager.close` or the driver-wide close
method. The driver connection leak is now reproduced and fixed below.
The continuation's text-browser helper uses `LibraryServices.open` with
maintenance disabled; that owner explicitly closes owned storage. Its sampled
deleted-open allocation has remained zero through the first 362 resumed cases.
This is a lead for a controlled lifecycle comparison, not proof that one change
explains every surviving handle.

Peak sampled summed process RSS was 2.21 GiB with pytest and a Python child alive
during a squashfs case; shared pages may be counted twice. WSL available memory
never fell below 5.32 GiB, and OOM/swap counters remained zero. The sampler exited
cleanly despite pytest's internal error; all JSONL records were valid.

At 19:19:32 UTC (20:19:32 BST), adjacent host-space samples dropped from
10,095,546,368 to 4,179,902,464 bytes, about 5.51 GiB. Linux filesystem usage
changed by only about 300 KiB over nearby samples. The active case was the
test_db_19 profile smoke test; that timing is context, not evidence of causation.
The user reported no known large Windows or other VM activity. C: subsequently
stayed near 3.9 GiB free, with a sampled minimum of 3.86 GiB.

Read-only Windows CIM snapshots found only about 811 MiB free physical RAM at
19:21 UTC, improving to 2.2 GiB at 19:29. Windows reported an 11,152 MiB pagefile
then and 12,182 MiB at 19:47. The later Windows/WSL free-space cross-check agreed
exactly. There is no pagefile or physical VHD allocation baseline before the
5.51 GiB drop, so neither mechanism is established as its cause. The snapshots
and `host-space-drop.json` preserve the evidence. Future crash diagnostics need
continuous host memory/pagefile/VHD measurements as well as guest telemetry.

At 20:12:39 UTC (21:12:39 BST), during the continuation, adjacent C: samples
increased by 3,823,505,408 bytes (3.56 GiB), from about 3.9 to 7.4 GiB free.
Nearby Linux filesystem usage again showed no comparable change. A Windows
snapshot at 20:13 reported a 12,117 MiB pagefile, only 65 MiB below the prior
snapshot, so that difference alone does not explain the recovery. Windows and
WSL free-space readings agreed. The continuation's
`windows-memory-free-space-recovery.json` records the evidence. Final C: free
space was about 7.37 GiB; neither large host-space change has been attributed.

The first attempt at 18:29 UTC was deliberately interrupted after 982 cases:
959 passed, eight skipped and 15 failed. Local HTTP/RPC tests could not create
sockets inside the tool sandbox (`PermissionError: Operation not permitted`).
A direct socket probe reproduced the restriction; the permitted execution
profile allowed loopback binding. The suite was restarted from the beginning
with that profile and the same cleanup/telemetry settings. The first attempt's
artifacts remain under `vm-full-aggressive-20261017T182943Z`, including its
manual-stop reason and JSON report. This was an environment correction, not a
resource stop or VM crash.

## Database connection cleanup follow-up

The facade closed the wrapper lock and primary connection, but never called
`driver.close()`. Both SQLite backends track auxiliary `get_connection()` handles
for that method to release. Those handles remained usable after facade shutdown
and kept unlinked database files allocated. Driver replacement had the same gap.
This confirms a driver ownership issue without changing StorageManager ownership.

`Database.close()` and replacement with a different driver now invoke the
driver-wide cleanup after the existing primary commit/rollback/close sequence.
Auxiliary transactions are rolled back by connection closure, not committed.
Reinstalling the same driver keeps its primary and auxiliary connections usable.
The `break_cycles` robustness test also now closes resources before clearing
ownership references, honoring that method's documented precondition.

Six regressions failed before the production fix across SQLite and SQLite_apsw:
auxiliary handles still accepted SQL after close or replacement, and Linux inode
inspection found descriptors for deleted databases after explicit shutdown. The
new tests keep Python references alive and never force garbage collection.
Additional transaction regressions verify primary commit, auxiliary rollback and
rollback after an injected primary commit failure.

Post-fix validation: **80 passed, two Windows-only skips** across facade lifecycle,
connection cleanup, driver lifecycle and LibraryServices ownership tests. The
same 14 representative aggregate, rating-helper, API and cycle-cleanup cases
passed before and after the fix. A diagnostic plugin sampled `/proc/self/fd`
after every test, deduplicating unlinked scratch files by device/inode and
counting allocated blocks. Before: 14 retained files, **57,401,344 bytes
(54.7 MiB)** at peak and session finish. After: **zero retained files/bytes at
every test boundary and session finish**, while pytest was still alive.

All runs used serial execution, fresh explicit basetemps, aggressive per-test
cleanup, durable telemetry and JSON reports. Samplers stopped normally and all
JSONL records were valid. Ruff lint/format checks passed for the new test module;
the patch passed whitespace checks. Raw evidence, the descriptor probe and a
validation summary are saved in the Windows checkout under
`working-memory/test-results/db-connection-cleanup-20261017` (ignored by Git).

The earlier 8,335-case coverage predates this fix; the complete suite was not
repeated. Its 29 failures remain tracked separately. This focused measurement
establishes release of these database allocations, not Windows VHD compaction or
the cause of the unexplained host free-space changes.

## Exact failed-test rerun — 2026-10-18

At commit `152991fa`, reran exactly the 29 failed node IDs from the checked
full-suite coverage manifest, with the six database-lock cases first. All
**29 failed again**, at the same source lines and with the same failure kinds;
none moved to setup or teardown. Pytest completed normally with exit status 1
in 111.48 seconds (112.65 seconds including JSON-reported overhead).

- Six creator/organisation sidecar writes still raise `database is locked`.
- Two direct-insert tag identity assertions retain spaces.
- Ten browser tag/genre lookup or normalization assertions still fail.
- Three surface contracts still expect removed paths or attributes.
- Eight image fallback cases still fail. These tests intentionally exercise
  operation without PyQt5 or external image backends; installing those backends
  would not establish the promised Pillow fallback behavior.

The shutdown fix did not resolve the locks during writes. Transaction ownership
in the creator/organisation path is the next investigation; it has not yet been
changed. No production or test source was modified for this rerun.

Serial execution used a fresh basetemp, aggressive cleanup, faulthandler,
durable telemetry, JSON reporting and the established disk/memory guards. Peak
sampled pytest-tree RSS was 120.3 MiB; WSL available RAM stayed above 6.59 GiB,
swap remained unused, and OOM counters stayed at zero. C: free space stayed near
15.43 GiB. Neither resource guard fired and WSL remained up.

Visible allocated scratch peaked at 7.9 MiB. All 29 post-test descriptor
snapshots and the session-finish snapshot recorded **zero deleted files held
open**. The 3.93 MiB reusable session template left after pytest exited was
removed from this run's owned basetemp; prior diagnostics were preserved.
Telemetry and sampler exits were clean and every JSONL record parsed.

Artifacts in the Windows checkout (ignored by Git):
`working-memory/test-results/failed-tests-rerun-20261018T002755Z`. This contains
the exact selected IDs, baseline failures, launcher, report, test log, telemetry,
descriptor probe, comparison summary, analysis script and cleanup record.
