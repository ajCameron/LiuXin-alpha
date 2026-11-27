# Core recovery and completion programme

Accepted 2026-11-25, based on `e76ebe74`. Implementation and the recorded
acceptance selections are complete for review. Full-duration and platform limits
remain below; this document does not claim whole-Core operational sign-off.

Core owns short command dispatch, authority, application adapters and outcome
reporting. The top-level jobs package owns durable execution, attempts, claims,
cancellation and history. Database and domain owners retain SQL transactions,
publication decisions and recovery checkpoints. There is no second Core job engine.

The accepted delivery order is:

1. Durable command identity, transactional outcomes, replay and outcome lookup.
2. Catalogue outbox to the existing durable job ledger; lifecycle supervision.
3. Ingestion/synchronization, conversion/metadata retrieval, backup/restore and
   maintenance/repair workflow migrations, with domain recovery contracts.
4. Bounded direct/HTTP execution, paging, content ranges and mixed-load evidence.
5. Restart-aware event cursors, explicit gaps, bounded delivery and readiness.
6. PostgreSQL language seeding and native disk ingestion, guarded Catalog cleanup,
   storage repair/recreation/restore and adapter consolidation.
7. Backend/cache/direct-HTTP acceptance, fault injection, gated long soaks,
   quality gates and operator runbooks.

Commands already generate UUID4 identifiers. An identifier names a logical action:
resending it must resolve the original action, and changed input must conflict.
Correlation IDs do not participate. Clients retain the identity before sending.
Transactional mutations and authoritative outcomes must commit together in the
catalogue. Cache publication is separate. External effects use domain evidence;
neither a missing response nor an expired lease proves rollback or executor death.

On restart, accepted queued work and provably safe checkpoint continuations run
automatically. Uncertain effects remain held for explicit recovery. API changes
and repository caller migrations are authorized; compatibility facades are not
required. Preserve existing settings ownership and borrowed transactions.

Initial execution limits: JSON requests 4 MiB, responses 8 MiB, depth 48,
100,000 values, pages 1,000, metadata batches 100, 16 admitted HTTP requests,
32 waiting application requests, 64 MiB aggregate buffers, 10s header/30s total
body deadlines, and four content streams with 1 MiB chunks. Work outside bounded
synchronous contracts goes to jobs. Ordinary shared-connection dispatch remains
serialized; independent status/cancellation must stay responsive.

Acceptance includes duplicate processes, post-commit response loss, cancellation
and publication races, restart/lease recovery, caller rollback, result pruning,
backup/restore, schema upgrades and explicit event resynchronization. Required
SQLite/APSW/PostgreSQL and cache/transport cases must run rather than pass by skip.
Measure 10,000/100,000-Work workloads, gated million-Work workloads and eight-hour
soaks. At four clients on the recorded reference environment, warm 10,000-Work
targets are p95 <=1s indexed reads, <=5s single-record writes and <=250ms
health/cancellation acknowledgement with one background workflow. Functional
timeouts do not establish these performance targets.

Per-user identity, distributed writable catalogue clones and new optional external
tool implementations are outside this programme. Missing baseline functionality
remains a sign-off blocker.

## Implementation and validation checkpoint

Implemented contracts are documented in the [operator runbook](recovery-operations.md).
The evidence below records completed acceptance, including the eight-hour soak,
and the remaining platform limits.

- Native metadata UUID/key journal, transactional outcomes, replay/conflict,
  passive `commands.outcome`, and result pruning that retains identity tombstones.
  Cache publication remains a separate observation. First mutating use installs
  the internal journal in the owning transaction; read-only lookup never installs it.
- Catalogue outbox and deduplicated delivery to the existing jobs ledger. Accepted
  receipts can contain `command_id` with `job_id=null`; clients resolve delivery
  through `commands.outcome` and never resubmit under a new identity on timeout.
- Normal factory startup resumes queued work. `start_jobs=False` explicitly opens
  an inspection/manual-drain session. Worker graphs are independent; outbox polling
  opens without storage startup. Missing durable composition refuses long workflow
  submission instead of selecting the utility executor. Recovery registration
  inspects the existing database handle without constructing the lazy Catalog;
  cold-process job inspection does not require metadata preference initialization.
- OS execution locks across a workflow's retry lineage. Expired leases cannot
  release them. Guarded automatic continuation permits native disk registration,
  metadata retrieval and pinned, owned backup staging, for at most three automatic
  successors. Other interrupted workflows remain held for explicit recovery.
- Shared wire depth/value/byte budgets; HTTP admission, aggregate raw-buffer
  accounting, total header/body deadlines, exact framing and early-EOF checks.
  Independent `health.live` and cancellation dispatch; ordinary calls are capped
  at 32 admitted/waiting calls. `/health` reports liveness, not catalogue readiness.
- Event stream identities and explicit overflow/restart gaps. Local delivery is
  asynchronous, isolated and bounded; blocked retired callbacks retain their slots.
  Readiness probes the catalogue without repair and distinguishes cache readiness.
- PostgreSQL language constants on creation and explicit repair, preserving IDs
  and references and refusing stale/conflicting upgrade plans.
- Native disk registration through StorageManager adoption, with SHA-256 identity,
  repeat-scan reuse and explicit errors for changed claimed locations. Conversion
  stages output and publishes without overwriting an existing destination.
- Removed the unavailable `maintenance.clean` Core route. The CLI uses
  `catalog.repair.cleanup`, a retained reason, and the reviewed `expected_state`.
- Long workflow adapters use the outbox/ledger: ingest/sync, metadata retrieval,
  conversion, backup, verification, restoration/recreation, repair, evacuation and
  remaining database maintenance. CLI and terminal callers resolve acceptance and
  wait for completion according to their existing wait/detach policy.
- Captured preferences preserve literal container types, bytes, paths and defaults;
  cyclic/opaque values are rejected. Workers keep independent library graphs and
  cooperative execution deadlines. Storage copy/hash/staging loops check cancellation.
- Verified restoration retains its source. Exact recreation verifies pinned inputs,
  executors and outputs before publication. Uncertain external effects remain held;
  cancellation does not claim rollback of already-published artifacts.
- Discovery distinguishes transactional, submitted, observed and non-journaled
  operations. Ordinary supported synchronous commands reserve identity before
  effects and retain uncertain outcomes on interruption. Generic invocation and
  job controls retain their explicit owner contracts.
- Versioned acquisition ranges enforce four readers and one-MiB chunks. Small
  legacy reads reject large content. Legacy global/database scans cap row hydration;
  native indexed metadata queries remain the large-catalogue read path. Job history
  reads a bounded SQL page instead of hydrating every attempt.
- Scalar cache edits refresh known row dependencies; relationship and schema edits
  retain broader reconciliation. Full cache reads share the managed database
  snapshot. Failed nested batches discard their child publication flags.

## Focused recovery review — 2026-11-25

The pre-PR review traced mutation/receipt commits, lost responses, outbox
acknowledgement, retry exclusion/cancellation, filesystem publication and cache
rollback/publication. It found and corrected two gaps:

1. The generic jobs worker could mark a handler successful after its execution
   deadline if the handler returned without a final checkpoint. The worker now
   checks after result serialization and retains the completed-effect receipt with
   timed-out state. Four return-boundary cases cover deadline/cancellation
   combinations; both expired cases failed before the correction.
2. An outer metadata batch could catch a nested rollback and return
   `cache="current"` while its conservative dirty guard remained. All three cache
   backends reproduced that disagreement. An unchanged outer batch now reconciles
   remaining dirty state before claiming readiness; refresh failure retains an
   unchanged, committed result with `cache="dirty"`. Tests cover uncached use,
   all three caches and failed refresh recovery.

Two additional conversion tests exercise a competing publisher between initial
inspection and publication, and directory-sync failure after publication. The
first preserves the competing output; the second retains the published file and
refuses to overwrite it on retry. Existing process-exit, lease and outbox tests
provide the other failure-boundary evidence. Jobs documentation now reflects the
current deadline, supervision and guarded recovery contracts.

The review follow-up passed 101 jobs/workflow/publication tests in 43.74s,
90 Core/HTTP/subprocess/documentation checks in 300.58s, and all 85 cache
extension/transaction/concurrency regressions in 1,360.89s. Five command-journal
rollback/refresh-failure checks passed again after the cache correction. The full
quality gate also passed after both corrections, and the final documentation
selection passed 22 checks. Cache and scale evidence is recorded below. This
review's platform and duration coverage remains bounded by that evidence.

## Evidence and limits

Validation selections overlap and are not additive totals. Named JUnit results
were audited against all **1,032 collected Core/jobs cases**: **1,028 passed** in
the default runs, with **four PostgreSQL-only skips separately covered by passing
live runs**. No collected case is missing and no failure remains unresolved.
This is combined evidence across partitions and focused corrections, not one
uninterrupted green run. The final lifecycle/recovery selection passed **85 tests**
in 171.73s after correcting cold-process recovery registration to preserve the
lazy Catalog. The other correction supplied an explicit count in a mock reader
and synchronized protocol documentation with the owners.

The live PostgreSQL acceptance selection passed **37 tests** in 3,746.83s,
including native metadata/cache/transport, command recovery, language upgrades,
snapshot ownership and restoration coverage. That selection also contains shared
helper/SQLite checks. Three additional profile-reopening/maintenance checks passed
in 98.40s, and the final native-ingest/restore/bounded-read check passed in 49.40s.
No required live case skipped. The disposable PostgreSQL server is stopped.

Additional passing selections include 96 cache publication/workflow checks,
74 cache regressions, 77 storage/backup checks, eight restoration/configuration/
history/recreation checks, 98 legacy/surface/docs checks, 23 final resource/event/
readiness checks, 22 documentation checks and 43 quality-runner checks. The real
SQLite/APSW bounded acquisition/restoration selection passed three checks.
Real SQLite/APSW subprocess tests exercise duplicate delivery and abrupt exit.
The full quality gate passed again after the final production correction, with
all new lint/format targets included: 537
formatted files, 483 annotation targets, 268 protected import modules, 2,120
direction modules, 31 cold-import budgets, lint/complexity checks, zero
basedpyright errors/warnings, mypy across 603 files and 127/123 rejected negative
type examples. The existing ISO-639 `codecs.open` deprecation remains a warning.

The per-case audit is `/tmp/liuxin-core-review-acceptance-audit28.json`. It records
status, source report and JUnit SHA-256 for the original combined results from
`/tmp/liuxin-core-{broad-final20,remaining23,construction-final25}.xml` and the
focused review follow-ups. The original 1,026-case audit is retained separately.
Backend evidence is in `/tmp/liuxin-core-{live-pg10,live-extra20,postgres-read-final24}.log`;
the final quality gate is `/tmp/liuxin-core-review-quality28.log`. Review logs and
JUnit reports use `/tmp/liuxin-core-review-{focused26,boundaries26,rejections28,cache27}`;
the final documentation selection passed 22 checks in 19.08s
(`/tmp/liuxin-core-review-docs28.log`). These are local development artifacts,
not permanent CI records.

All **24 smoke cases passed** in 485.35s: SQLite/APSW/PostgreSQL, uncached and all
three cache modes, local and HTTP. Required cases did not pass by skipping.

The reference measurements used four clients and an actual background ledger
worker doing controlled read/hash work. They ran on Python 3.14.10, Linux/WSL2,
eight reported CPUs, alongside the regression suites. Values below are bounded
histogram p95 upper bounds, not claimed universal service guarantees:

| Works / SQLite schema-backed HTTP | Read | Write | Health | Cancel | Peak process RSS |
| --- | --- | --- | --- | --- | --- |
| 10,000 / 60s | 0.837s | 1.149s | 0.061s | 0.155s | 186 MiB |
| 100,000 / 120s | 0.686s | 1.061s | 0.051s | 0.144s | 559 MiB |
| 1,000,000 / 300s | 0.531s | 0.743s | 0.048s | 0.109s | 4,335 MiB |
| 10,000 / eight hours | 0.337s | 0.481s | 0.039s | 0.083s | 195 MiB |

The warm reference passed all four declared latency targets. The larger cases
passed their correctness checks and record latency without extending that SLO.
Host load averages for the first two cases ranged roughly 6.4–8.2. The million-Work
case ran with lower host load (one-minute average 3.44–3.94); its lower measured
latencies do not establish improved scaling. It passed in 471.48s including setup,
with 3,008 reads, 752 writes and 252 replays during the measured interval.
Source hashes and complete reports,
including the earlier failed measurement, remain under
`/tmp/liuxin-core-load-{smoke13,warm17,large15,warm-first}`. These are development
measurements. The million-Work result and source hashes are in
`/tmp/liuxin-core-review-million28`. An earlier million-Work invocation was
interrupted during construction to apply the cache correction and is not counted.
Long-duration results for other backend/cache/transport combinations remain unclaimed.
The reference workload measures indexed `read_for_edit` and scalar metadata updates.
It does not establish relationship/schema-publication latency. Some complete
workflow cases take minutes; stack samples show schema inspection during full
NumPy cache refreshes. Keep that cost visible when choosing further optimizations.

The gated runner supports all 24 backend/cache/transport combinations, 100,000 and
million-Work workloads, and eight-hour soaks. The **eight-hour soak passed** on
**2026-11-26 at 18:09:57 UTC**, with exit code 0. It exercised 10,000 Works using
SQLite, the schema-backed cache, HTTP, four clients and the background ledger
workflow. The measured interval was **28,800.63 seconds**, with 437,280 reads,
109,320 writes, 36,440 replay checks, 156,596 health checks and 156,596 cancellation
checks. All four declared latency targets passed. Peak process RSS was
204,685,312 bytes (195 MiB). Pytest reported one passing case in 28,819.05s and
only the existing ISO-639 deprecation warning.

The [retained soak result](evidence/soak-2026-11-26.json) records the measurements,
supervisor outcome and source provenance at commit
`b190042ae91e55b21a1f03061fa504e56a47348a`. All 1,462 recorded source hashes were
verified against that commit. The full manifest and log remain local artifacts
under `/tmp/liuxin-core-review-soak29` and `/tmp/liuxin-core-review-soak29.log`.
The start wall timestamp was 2026-11-25 22:56:29 UTC; wall time advanced farther
than the measured runtime. Host/VM suspension is a possible explanation, not a
confirmed diagnosis. This is a local Linux/WSL2 result for the stated configuration.

Protocol buffers and adapter scan budgets do not cap
the entire Python heap, every backend's internal buffering or blocking third-party
I/O. Windows execution locks and process termination have not been exercised here.
