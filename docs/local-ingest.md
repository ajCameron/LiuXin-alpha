# Import local files and SquashFS collections

Updated **2026-10-23**. Local ingest registers source files and archive members
in place. Nested archives can require copies in the configured materialization
cache. It does not yet create bibliographic Works, Expressions or Manifestations.

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
