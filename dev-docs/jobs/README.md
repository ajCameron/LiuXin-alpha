# Jobs

Reviewed **2026-10-20**. This folder owns the map of execution state and its
consolidation work. See [next tasks](next_tasks.md) for the proposed sequence.

## Current owners

| Owner | Current responsibility | Limit |
| --- | --- | --- |
| [jobs.repository](../../src/LiuXin_alpha/jobs/repository.py) | SQLite-backed definitions, captured inputs, atomic claims/events, fenced worker updates and interrupted-run recovery. | Separate job database/schema; not the default Core job manager. |
| [jobs.scheduler](../../src/LiuXin_alpha/jobs/scheduler.py) | Queue due definitions. | Scheduling does not make a domain workflow idempotent. |
| [jobs.worker](../../src/LiuXin_alpha/jobs/worker.py) and handler registry | Lease and renew runs, invoke captured handlers/inputs, persist owned progress/results. | Each handler must cooperate with its effect/recovery contract; definition timeouts and retries are not enforced. |
| [utils.jobs.manager](../../src/LiuXin_alpha/utils/jobs/manager.py) | In-memory supervision of thread/process execution, cancellation, timeout and retry. | Process-local job state does not survive restart as a durable ledger. |
| [CoreRuntime](../../src/LiuXin_alpha/core/runtime.py) | Named submit/list/get/wait/result/cancel/retry operations and application receipts. | Defaults to the process-shared utility manager; it is not automatically wired to JobRepository. |
| [workflow jobs](../../src/LiuXin_alpha/core/workflow_jobs.py) | Adapters that invoke owning conversion, ingest and storage workflows. | The job wrapper does not add all-run rollback or missing result fields. |

Domain-specific journals still exist. Storage ingest records publication and
catalogue finalization; backup workflows retain their own sources/checkpoints;
mixed ingest keeps run/resume state. These are recovery evidence for their
operations, not interchangeable universal job records.

The [durable claim contract](durable-claims.md) describes the A09 prerequisite:
atomic transitions, claim tokens, lease renewal, explicit interruption,
cancellation receipts and migration. It does not yet wire a production Core
workflow into this ledger.

## Lifetime, waiting and outcomes

Core borrows its default process-shared job manager and does not normally shut it
down when one library runtime closes. Explicitly injected managers and
`close_job_manager_on_shutdown` need deliberate ownership at composition.
See [Core lifecycle](../core/core-api.md).

Core serializes shared-service handlers, but its built-in `jobs.wait` and
`jobs.result` registrations wait on the manager outside that lock. Other
synchronous operations can still block dispatch. The
[waiting contract](core-wait-dispatch.md) documents permissions, timeouts,
replacement, shutdown and the remaining durable-state work.

Cancellation is a request with backend/workflow-specific effects. A client
timeout is not proof of server cancellation; a retry can repeat effects unless
the domain operation has an idempotency/recovery contract. Job success also
does not prove semantic completeness: conversion workers currently omit the
structured loss report described in [conversion](../conversion_pipeline/README.md).

## How to extend this area

Add domain work behind its owner, then expose a handler with bounded input,
observable progress, cancellation checkpoints and a structured result.
Choose a persistent operation identity before claiming restart safety. Keep
execution primitives separate from Catalog validation and storage placement.
Migrate one live workflow to the agreed durable route before retiring a manager.

Relevant tests are [durable jobs](../../tests/jobs) and
[execution helpers](../../tests/utils/jobs), plus the consuming Core/workflow
tests. Use small deterministic failure/restart fixtures; live external services
are not required to establish state transitions.

Return to the [developer index](../README.md).
