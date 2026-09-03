# Mixed-format ingest operations

Updated: 2026-08-25

## Purpose and data model

`MixedFormatIngestCoordinator` is the run-level layer above LiuXin's hardened
container Stores. It is intended for an untidy local tree containing loose
ebooks and nested SquashFS, ISO/UDF, ZIP, TAR, RAR, or 7z files.

A real run:

1. registers the source directory as a read-only unmanaged Store;
2. adopts every regular source file in place as a Digital Asset and UNMANAGED
   Replica;
3. exposes each recognized container as an immutable Asset-backed Store;
4. adopts regular members as Digital Assets and ARCHIVE Replicas; and
5. queues recognized nested members, materializing their exact Asset bytes into
   a managed CACHE Store when a local file is required by the next driver.

No member is extracted into the source tree or a caller-selected member path.
Symlinks are skipped. Existing container bytes are not described as
derivations: a derivation is recorded only when LiuXin knows a producing
process. The workflow records technical filename, media, container, depth,
parent-Asset, and member-path metadata; bibliographic Item matching and
enrichment remain a later ingest stage.
Applications can supply `source_metadata_factory` and
`member_metadata_factory`; the latter receives a `ContainerMemberContext` with
format, depth, parent Asset, and full container chain plus the Store's native
inventory hints. This is the supported seam for richer technical or future
bibliographic observations without putting implementation in the API models.

EPUB, CBZ, CBR, MOBI, AZW, PDF, FB2/FBZ, LIT, PDB, DOCX, ODT, and HTMLZ are
terminal ebook Assets by default. `--expand-ebook-containers` deliberately
opts ZIP/RAR-like ebook containers into recursive expansion.

## Preflight without writes

```bash
python examples/storage/ingest_mixed_tree_example.py \
  --source-root /media/archive-drives/disk-01 \
  --discover-only
```

This bounded pass classifies top-level candidates by suffix or a small magic
probe. It creates no Store, Asset, Replica, database, or cache file. A named but
corrupt archive is still reported as a candidate; validity is established only
by the real backend. Discovery-only does not claim to inventory members or find
nested containers.

## Durable run and resume

```bash
python examples/storage/ingest_mixed_tree_example.py \
  --source-root /media/archive-drives/disk-01 \
  --database /srv/liuxin/catalogue.sqlite \
  --materialization-root /srv/liuxin/ingest-materialized
```

The materialization directory must be outside the source root so cache output
cannot become new input on a later scan. It is created lazily: top-level
containers that already have a local unmanaged Replica do not get copied. If a
nested container is found without a configured cache, its bytes remain safely
catalogued and the report contains an actionable issue. Use
`--no-nested-containers` when top-level inventory is the intended boundary.

Resume by rerunning the same command with the same database, source, and cache.
Operation UUIDs are stable across surrogate-escaped local filenames, Asset
identity deduplicates equal bytes, backed Store UUIDs derive from content and
durable safety options, and existing CACHE Replicas are reused. The JSON report
distinguishes newly created catalogue records and materialized bytes from
already-known ones. An isolated corrupt or unsupported container does not stop
later work unless `--strict` is selected.

## Unattended logging and run correlation

Every invocation creates two LiuXin-native logs before it opens the catalogue
or scans the source. The command prints their absolute paths and a UUID run ID
to stderr immediately; stdout remains reserved for the final JSON report. The
same `run_id` appears in the report and every workflow event, making concurrent
or repeated runs distinguishable.

- `mixed-ingest-<UTC timestamp>-<run ID>.jsonl` is the authoritative,
  append-only forensic stream. It is not rotated or sampled. At the default
  `DEBUG` level it includes every classified/adopted source and member as well
  as Store identities, nested-container transitions, deduplication, verification
  results, safety decisions, issues, and tracebacks. Each event is flushed
  before ingest continues.
- The matching `.log` is the human-readable view. It rotates at 100 MiB and
  retains ten backups by default. It includes UTC time, severity, process,
  thread, logger, event name, and structured context.

By default logs are stored in `<database>.ingest-logs`; discovery-only logs go
in a hidden sibling of the source root. An explicit log directory must also be
outside the source tree so log output can never become ingest input. For the
first unattended run, make the destination a monitored filesystem with room
for an unbounded JSONL audit:

```bash
python examples/storage/ingest_mixed_tree_example.py \
  --source-root /media/archive-drives/disk-01 \
  --database /srv/liuxin/catalogue.sqlite \
  --materialization-root /srv/liuxin/ingest-materialized \
  --log-directory /srv/liuxin/ingest-logs \
  --log-level DEBUG \
  --log-checkpoint-every 1000 \
  --log-max-mib 100 \
  --log-backup-count 10
```

`cli_started` records the resolved paths, safe command options, complete safety
budget, LiuXin/Python/platform versions, host, process, and working directory.
Legacy database-construction output is captured as `captured_output` rather
than contaminating stdout. Existing Store reload produces an aggregate
`store_bootstrap_complete` event plus one `store_bootstrap_issue` per problem.
Ordinary isolated damage produces an exception event with a traceback and an
`ingest_issue`, then work continues. `source_checkpoint` and
`member_checkpoint` provide aggregate recovery markers. A normal workflow ends
with `complete`, followed by `cli_complete`; a process-level exception ends
with `cli_failed`, including its traceback. Absence of either CLI terminal
event indicates termination outside normal Python handling (for example power
loss or an unconditional process kill).

Each JSONL line has stable top-level event-log fields (`id`, `ts`, `level`,
`level_name`, `message`) and a `context` object. The LiuXin event name is
`context.event`; workflow fields are under `context.details`. Useful first
checks include:

```bash
# Terminal state and last durable events
tail -n 20 /srv/liuxin/ingest-logs/mixed-ingest-*.jsonl | jq .

# All warnings/errors with their event and workflow details
jq -c 'select(.level >= 30) | {ts, level_name, event: .context.event,
  details: .context.details, traceback: .context.traceback}' RUN.jsonl

# Latest aggregate checkpoints
jq -c 'select(.context.event == "source_checkpoint" or
  .context.event == "member_checkpoint") | [.ts, .context.event,
  .context.details]' RUN.jsonl
```

The logger deliberately records filenames and resolved Store/catalogue paths
because they are required to diagnose a collection run. It does not dump the
process environment, file contents, database rows, or credential values.
`--log-level INFO` suppresses per-object DEBUG events when storage pressure is
more important than a complete object audit; checkpoints and lifecycle events
remain. `--no-console-progress` affects stderr only and never durable logs.

## Safety budgets

`MixedIngestBudget` owns ceilings across the entire traversal as well as per
container. Defaults are:

| Budget | Default |
| --- | ---: |
| Source files | 1,000,000 |
| Containers | 10,000 |
| Nested container depth | 8 |
| Members across the run | 1,000,000 |
| Members per container | 100,000 |
| One member | 4 GiB |
| Expanded bytes per container | 64 GiB |
| Expanded bytes across the run | 256 GiB |
| Logical/container ratio | 200:1 |
| CACHE materialization across the run | 64 GiB |
| One spooled/materialized object | 4 GiB |
| Wall time | 24 hours |
| Recorded issues | 10,000 |

The coordinator passes compatible per-container ceilings into every new Store,
so hostile metadata and expansion are bounded while a backend indexes. The
driver layer additionally rejects unsafe paths, links, special entries,
duplicates, overwrite topology, encryption/multi-volume forms it cannot safely
represent, and format-specific parser excess. The coordinator adds cumulative
member/byte accounting, ancestry-cycle detection, identical-container
suppression, depth control, cancellation, and a wall clock.

Limits are fail-closed but scoped where possible. A per-container member,
ratio, or byte ceiling truncates that branch; a run-wide member, expanded-byte,
time, cancellation, or issue ceiling halts further work and leaves a resumable
report. Already committed Asset and Replica records remain valid.

## Dependencies and operational caveats

ZIP, TAR, and ISO 9660 support is dependency-free. SquashFS requires
`unsquashfs`; 7z uses optional `py7zr`; UDF uses optional `pycdlib`; and some RAR
members require maintained `rarfile` or an operator-configured `unrar`/`rar`
extractor. Missing capabilities appear as contextual container issues rather
than fabricated empty inventories.

External SquashFS and RAR work receives a durable per-call timeout. The global
wall clock is checked between backend operations. Python-hosted parser calls,
notably optional 7z parsing, cannot be safely killed mid-call; their parser,
header, entry, member, ratio, and byte budgets still apply, but one active call
may return after the global deadline. Run untrusted collections under normal
OS process/resource supervision as well as these application limits.

The CACHE Replica is durable and restartable. Automatic eviction must not run
concurrently with this coordinator until the cache layer has a live Store
pin/lease contract. This is the remaining cache-composition follow-up; it is not
worked around with an in-memory byte cache.
