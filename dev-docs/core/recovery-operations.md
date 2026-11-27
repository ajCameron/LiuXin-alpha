# Command recovery and durable execution

Implemented on `core-review`, 2026-11-25. The [completion programme](recovery-completion-2026-11-25.md)
records validation separately from the contracts below.

## Owners and identities

Core owns command dispatch, permission checks, application adapters and wire
receipts. `jobs` owns durable requests, attempts, claims, cancellation and history.
The catalogue owns its transactional command journal and submission outbox.
Storage, ingestion and conversion owners decide when their effects can safely
continue. There is no second job engine inside Core.

A command ID identifies a logical action. Construct and retain the envelope before
sending it. Reuse that envelope after a lost response; changing the operation or
payload under the same ID returns `command_conflict`. Correlation IDs are tracing
context and do not change the identity. UUID4 is the generated default; existing
nonblank caller keys up to 200 characters remain accepted.

```python
from LiuXin_alpha.core.commands import CoreCommand

command = CoreCommand("metadata.update", {
    "entity_id": work_id, "data": {"title": "Revised title"},
})
# Retain command.command_id and its original payload before sending.
receipt = client.execute_command(command).result
outcome = client.query("commands.outcome", {"command_id": command.command_id})
```

Endpoint discovery exposes its recovery policy:

| Policy | Durable guarantee |
| --- | --- |
| `transactional` | Native metadata effects and outcome commit together. Rejection rolls back the mutation and can retain a failed outcome. |
| `submission` | The accepted receipt and catalogue outbox intent commit together. Execution and attempts are recorded in the jobs ledger. |
| `observed` | Other supported catalogue/system commands reserve identity before effects. A completed receipt can be replayed; an interrupted effect remains uncertain and is not rerun automatically. |
| `none` | No catalogue journal guarantee. This includes generic invocation, dynamically registered handlers unless opted in, utility jobs and job control/status operations with their own owner contracts. |

Outcome lookup requires the original command's authority. Unknown IDs report
`not_found` with `effects=unknown`; this is not evidence that nothing happened.
Pruning successful results retains an identity tombstone, so an old command cannot
execute again simply because its detailed result was removed.

Caller-owned managed transactions retain their commit/rollback ownership. A native
metadata receipt inside one is `pending`, including outcome lookup from that same
connection. Raw borrowed transactions with unobservable completion are refused.
Workflow acceptance requires an independently owned outer transaction.

## Acceptance, completion and restart

Long commands now return acceptance, not the final domain result. This includes
ingestion/synchronization, metadata retrieval, conversion, backup/publication,
database backup/vacuum/migrations, verification, restoration/recreation, repair,
evacuation and ingest recovery. The existing `storage.file.copy` route shares the
streaming verified restoration owner. `maintenance.clean` is removed; use the
reviewed `catalog.repair.cleanup` plan, reason and expected-state token.

An accepted receipt may have `job_id=null` before outbox delivery. Query
`commands.outcome` or use `core.submissions.wait_for_submission` to resolve delivery.
Never create a new command ID merely because delivery has not yet appeared. Once delivered,
use `jobs.get`, `jobs.wait`, `jobs.result` and `jobs.events`; command outcome lookup
also reports the latest linked attempt and its execution receipt.

`create_core` starts queued-work supervision by default when it owns a persistent
connection and durable ledger. `start_jobs=False` explicitly selects inspection or
manual draining. SQLite/APSW default to a sibling `.jobs.sqlite` ledger; PostgreSQL
needs an explicit durable ledger path. Worker graphs are independently opened with
the captured backend, connection metadata, schema and library UUID. Outbox polling
does not start storage. Shutdown stops polling without discarding accepted work or
claiming an active workflow has been undone.

Keep the job ledger and its `.execution-locks` directory on a local filesystem.
OS locks exclude cooperating executors across retry attempts; backup workflow
IDs also have resource locks across distinct commands. Lease expiry alone cannot
prove an executor has stopped. A surviving executor keeps recovery waiting; a dead
process releases its OS lock. Distributed writable catalogue copies and distributed
filesystem locking are outside this contract.

Automatic continuation is limited to native disk adoption, metadata retrieval,
and owned backup staging with pinned source sizes/digests and verified staged
members. At most three automatic successors are created. Unknown publication,
conversion, restoration, recreation, finalization and other uncertain effects are
held. A held decision stays held until explicit recovery; polling does not
continually reclassify it. Retry requires an explicit possible-effects
acknowledgement. It creates an attempt linked to the original action.

Application preference values/defaults and fixed component paths are captured by
the configuration owner. Literal tuples, sets, bytes and paths retain their types;
cycles, opaque objects and nonfinite numbers are rejected. Workers do not reload or
rewrite the application settings file. Independent component files retain their
existing owners and process-scoped limitations.

## Effects and cancellation

SQL commit and cache publication are separate. A committed metadata result with
`cache=dirty` must not be repeated as a new mutation. Refresh/read back or reopen
the derived cache. Historical outcome receipts are labelled as historical cache
observations. Scalar metadata edits repair known rows and projections; relationship
and schema edits retain the broader refresh required by their dependencies.

`jobs.cancel` acknowledges a request, not rollback. It follows the latest attempt
under the ledger's write lock. Copy/hash loops, staging and cooperating providers
check cancellation; execution deadlines use the same cooperative boundary.
Blocking third-party I/O cannot always be interrupted immediately. A late successful
effect remains recorded even if its job subsequently acknowledges cancellation or
timeout. The generic worker also checks the deadline after a handler returns and
retains its result; expiry takes precedence over concurrent cancellation at that
boundary. Never interpret a non-success job state as proof that no output exists.

Restoration streams an active copy from an explicit source or a readable active,
backup, archive, unmanaged, cache or transient replica, verifies the destination,
and retains the source. Exact recreation requires the reviewed plan fingerprint,
pinned inputs and executor/dependency identities, bounded materialization and a
verified output matching the registered Asset. Recipe execution is privileged
local tool execution, not an untrusted-code sandbox. Incomplete or unavailable
recipes remain unavailable; uncertain publication is held for inspection.

Conversion publishes a completed staged file with create-only semantics. An
existing destination is never replaced by this path. Failure after publication
may leave output and requires inspection. Directory-producing conversion and
external tool availability retain their domain limitations.

## Bounds, readiness and events

Stable dispatch enforces 4 MiB requests, 8 MiB responses, depth 48 and 100,000
values. HTTP admits 16 requests and accounts for at most 64 MiB of raw buffers;
headers/body have total 10s/30s deadlines. These are protocol buffer limits, not a
bound on the entire Python heap or on every legacy scan's execution time. Ordinary
application calls admit at most 32 concurrent/waiting callers. Liveness and job
cancellation bypass the shared application queue. Native pages and durable job
history pages cap at 1,000; custom in-memory job managers own their paging policy.
Legacy global search shares a 10,000-row budget across selected tables. Legacy
database and browse hydration and file enumeration refuse larger scans with
`scan_limit_exceeded` rather than publishing a misleading partial count. Their
upstream owners retain responsibility for buffering; use native indexed metadata
search for large catalogues. Bounded output is not a backend execution-time bound.

`health.live` and HTTP `/health` report process liveness. `health.ready` probes the
catalogue under its normal dispatch lock and reports starting/ready/degraded state,
cache readiness and event delivery statistics. Database failure reports unavailable
without exposing driver error text. It does not repair a catalogue or dirty cache.

Use `acquisition.read-range` for large content: at most four simultaneous readers,
1 MiB per chunk, and a version token retained across chunks. Changed versions,
short reads and oversized replies fail. `core.content.acquisition_chunks` checks
progress and completion. Small legacy `acquisition.read` refuses resources over
one MiB, as does `storage.file.read`. Surface helpers that assemble all chunks still
own their final buffer.

Local event delivery isolates subscribers with bounded queues: 32 subscriber
threads, 128 events/1 MiB each, and 64 KiB per event. A blocked callback still
occupies its slot after unsubscribe until it returns. HTTP events carry a stream
identity and sequence; restarts, overflow and invalid cursors produce explicit
resynchronization signals. Events are notifications, not an authoritative durable
outcome ledger. On a gap, refresh views and reconcile retained command/job IDs.

## Operator recovery sequence

1. Keep the original command ID, catalogue, ledger, source files and produced artifacts.
2. Inspect `commands.outcome` and the latest job attempt/events. A missing response
   or a missing outcome does not establish rollback.
3. For committed metadata, recover cache publication without repeating the edit.
4. For accepted work, allow normal delivery/restart or inspect with `start_jobs=False`.
5. For held or uncertain effects, compare domain checkpoints and output identities.
   Resolve publication evidence before explicitly retrying with acknowledgement.
6. Retain source copies until the domain workflow has verified the required outputs.

## Reproducible long tests

The explicit runner refuses skipped required cases and saves source hashes and
bounded latency histograms, including failures:

```bash
PYTHONPATH=src .venv/bin/python scripts/run_core_long_tests.py \
  --size warm --backend SQLite --cache schema_backed --transport http \
  --artifacts /tmp/core-warm
```

Sizes are `smoke` (128 Works), `warm` (10,000), `large` (100,000),
`million` (1,000,000) and `soak` (eight hours at 10,000 Works).
`--backend all --cache all --transport all` runs all 24 combinations; configure
`LIUXIN_TEST_POSTGRES_URL` for the disposable PostgreSQL schemas. Each case has four
clients and a real background ledger worker doing controlled reads/hashing, not an
optional converter benchmark. Warm p95 targets are read <=1s, write <=5s, and
health/cancellation <=250ms. Larger workloads record latency without silently
extending the warm-size target. Artifacts capture load averages and, on POSIX, the
process peak RSS through that case (including earlier cases in the same process).
A gated
test definition does not establish that its full-duration run has passed.

Return to [Core](README.md).
