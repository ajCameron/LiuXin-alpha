# Import local files and SquashFS collections

Updated **2026-10-23**. Local ingest registers source files and archive members
in place. Nested archives can require copies in the configured materialization
cache. It does not yet create bibliographic Works, Expressions or Manifestations.

## Set up your default system

Start the interactive wizard:

```bash
liuxin init
# Explicitly request the same wizard:
liuxin init --wizard
```

Choose SQLite for the local SquashFS workflow, a system-root directory, and
whether to configure a managed primary Store. The wizard asks **“Use this system
by default for future LiuXin commands for this user?”**, with Yes as the default.
Review the plan and confirm it. The default is saved after successful setup.
Declining that choice keeps your previous default; cancelling the plan creates
nothing. Setup failures can leave created files; saving the default is the final
setup step, after initialization and manifest publication succeed.

For scripted setup, request saving explicitly:

```bash
liuxin init "$HOME/LiuXin" --make-default
```

To select an already configured installation, use `liuxin connect /path/to/system`.
Later commands for the same operating-system user can omit the selector:

```bash
liuxin core health
liuxin storage status
liuxin ingest runs watch RUN_UUID
liuxin connect status
```

The saved connection is a private pointer to `liuxin-system.json` in
`$XDG_CONFIG_HOME/liuxin/active-connection.json`, normally
`~/.config/liuxin/active-connection.json`. It is shared by the existing command
selection mechanism. Explicit selectors override environment selectors, which
override this saved default. Setup reports an active environment override.
`liuxin disconnect` forgets the selection without deleting the installation.

Defaults belong to the current account. A command run through `sudo` normally
uses root's configuration, so the protected-ingest examples below retain an
explicit `--system-root` to select your intended installation.

## Preflight and run

With an initialized system root, preflight the same protected source path that
the real run will use:

```bash
sudo liuxin storage ingest "/media/book archives" \
  --system-root /srv/liuxin --preflight-only --min-free-mib 8192
```

The positional source selects a verified read-only view on Linux. Supply either
a collection directory or a standalone SquashFS image. Preflight checks access,
required readers and free-space headroom without opening the catalogue or
creating the cache. It still writes logs and a report. Install `squashfs-tools`
for collections containing SquashFS archives.

To ingest, repeat the command without `--preflight-only`. Keep catalogue, cache,
temporary storage, logs and reports outside the source. The
[developer runbook](../dev-docs/storage_and_backup/mixed_ingest_operations.md)
explains mount requirements, archive budgets and source-protection boundaries.

## Watch a long import

Interactive imports show a **Rich status panel on stderr** automatically. It
shows the phase and current source/archive/member, elapsed time, adopted sources
and members, containers processed, average member rate, cache bytes, process RSS,
and free space against required headroom. Counts describe the current attempt:
resuming visits existing records too. Nested discovery can reveal more work, so
there is no overall percentage or ETA.

Open another terminal and attach with the run ID printed at startup:

```bash
liuxin ingest runs watch RUN_UUID --system-root /srv/liuxin
# A single snapshot, suitable for redirected output:
liuxin ingest runs watch RUN_UUID --system-root /srv/liuxin --once --progress plain
# Without a system profile:
liuxin ingest runs watch RUN_UUID --log-directory /path/to/ingest/logs
```

The watcher needs read access to the original CLI host's logs. It selects the
newest attempt once, exits when that attempt finishes, and never opens the
catalogue or signals the importer. **Ctrl-C in the watcher only detaches**;
Ctrl-C in the import terminal requests the existing cooperative stop. After
resuming, attach again to select the new attempt. Watching does not make the
import survive a closed terminal; use your normal process supervisor for that.
The watcher returns zero when observation finishes or is detached; read the
displayed status or the report for the import's outcome.

`--progress auto|rich|plain` selects presentation for either command. Redirected
stderr uses plain output in auto mode. `--no-console-progress` disables the
import display, including an explicit Rich selection. JSON reports remain on
stdout; panels and watcher output stay on stderr. `--no-stdout-report` still
leaves stdout empty.

Both displays read the same JSONL telemetry, including at `--log-level ERROR`.
Resource values and current activity refresh at `--resource-interval-seconds`
(default 5 seconds); watcher redraws default to 1 second and can be changed with
`--refresh-seconds`. Sample age is visible. Silence longer than 15 seconds or
three sample intervals, whichever is greater, is labelled **STALE; liveness
unknown**. A process killed without a final event leaves the watcher waiting
until detached. Older logs without samples show unknown metrics.

Selection scans event-log names and modification times without loading historical
reports. Readers retain only the latest state and read at most the most recent
MiB per refresh, skipping malformed/oversized records with a notice. This allows
attaching to large DEBUG logs without replaying their full history. Recent
counter snapshots remain authoritative; a latest-issue message may omit older
issues, which remain available through `ingest runs issues` and the report.

## Space protection

Local ingest requires **2,048 MiB free by default** on each filesystem containing
the catalogue (including WAL/journal), cache, temporary files, logs, report and
run lock. `--min-free-mib` changes that positive reserve; the example uses 8 GiB.
Paths not yet created are checked through their nearest existing parent.
Unavailable capacity is a failed check, not permission to continue.

The first check runs before catalogue startup. Periodic observations and safe
workflow boundaries then check for external disk consumption. Before reading a
member, admission also allows for its declared size in temporary storage:
SquashFS readers spool individual members there. New nested-container copies
conservatively allow two cache copies and one temporary copy, adding demands
when they share a device. These checks can refuse an allocation before free
space reaches the reserve itself.

Under **WSL**, automatic detection reads the current distribution's registered
Windows location and checks its backing drive as well as Linux free space. It
does not assume that the virtual disk lives on C:. If Windows interop or the
distribution identity is unavailable, supply the actual backing drive explicitly:

```bash
sudo liuxin storage ingest "/media/book archives" \
  --system-root /srv/liuxin --host-space-path /mnt/c
```

Use the drive containing your distribution's virtual disk; repeat the option
for additional host volumes. Explicit paths replace automatic host detection.
Unknown host capacity stops the run rather than silently omitting protection.

Checks are cooperative observations, **not disk reservations or hard quotas**.
An in-flight extractor, database operation or unrelated writer can consume space
before the next safe boundary. Size checks cannot predict every backend scratch
allocation. Keep additional headroom for large objects and other activity.

## Resource telemetry and log volume

`--resource-interval-seconds` defaults to **5 seconds** (minimum 0.1). A sampler
continues while the ingest thread hashes or waits for a reader. Its
`resource_sample` JSONL events record:

- Each monitored path's free/total bytes and required headroom.
- Linux process RSS, peak RSS and swap, plus system available memory and swap.
  Unsupported counters are null; process RSS excludes extractor children.
- The current source, archive or member when available.
- Per-attempt source, container and member counters, and adopted member bytes.
- Cumulative cache reservations and confirmed newly materialized cache bytes
  for this attempt. These are logical byte counters, not a recursive measurement
  of existing cache contents or filesystem allocation.

Initial and final samples are retained too. The final report's `resources` field
contains the policy, sample count, stop reason and latest observation. The full
time series stays in the existing correlated JSONL log; there is no separate
growing in-memory sample list.

The default log level is now **INFO**: lifecycle, container and aggregate
checkpoints plus resource samples. Choose `--log-level DEBUG` for per-file and
per-member audit events. The human log rotates; JSONL remains append-only and
unbounded, with its filesystem protected by the same reserve. Even at WARNING or
ERROR, JSONL retains start/terminal events and resource samples needed for
telemetry and resumable history; the human log respects the selected threshold.

## Resume a resource stop

Low or unknown space produces exit **1**, status **`resource_stopped`**, and a
report explaining the observed path and required capacity. A stop during ingest
retains partial catalogue progress. The run ID and effective options remain in
the start event. Release space, then resume:

```bash
liuxin ingest runs list --system-root /srv/liuxin
liuxin ingest runs show RUN_UUID --system-root /srv/liuxin
liuxin ingest runs resume RUN_UUID --system-root /srv/liuxin
```

Use the same account/privileges and selectors needed for the original mount.
Resume reconstructs the original limits and resource controls, rechecks capacity
and deduplicates existing records/copies. Discovery and preflight attempts are
not resumable. The guard never deletes source files, cache replicas or old logs.
