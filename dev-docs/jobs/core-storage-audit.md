# Durable Core storage audits

Implemented for PR review **2026-10-20**, as A09's first production workflow.

`storage.audit.start` accepts a bounded page selection and returns a durable job
ID promptly. A worker opens its own LibraryServices graph and verifies Replicas
through storage. `jobs.get/list/wait/result/cancel/retry` route the returned ID to
the durable ledger; other jobs retain their utility manager and existing IDs.
`jobs.events` exposes bounded event cursors and completed per-Replica receipts.

## Composition and identity

For a path-opened SQLite/APSW library with storage enabled, `create_core` configures
a lazy `<library filename>.jobs.sqlite` sidecar. Construction creates neither a
ledger nor a worker thread. Pass `durable_job_path` to choose another local path;
CLI local composition accepts `--job-ledger`. PostgreSQL requires an explicit
local ledger path. A borrowed library/database without a database path does not
automatically acquire a worker capable of reopening it.

The ledger has a stable UUID and binds once to the library UUID. Core rejects
cross-library use, a populated ledger without an owner, and a ledger path that
aliases the SQLite catalogue. It does not adopt another application's queued work. Returned
IDs have the form `durable:<ledger UUID>:<run ID>`. Preserve the ledger alongside
the catalogue when retaining job evidence; it is a separate SQLite database.
The worker also checks the reopened library UUID before verification.

The worker owns its database connections, storage graph and selected preferences.
It does not use Core's foreground connection concurrently. Core closes its
foreground graph independently and stops future worker polling on shutdown.
An active worker can finish using its own graph while the process remains alive;
its daemon thread does not keep a terminating process running.

## Submission, inspection and events

```python
accepted = core.command("storage.audit.start", {
    "limit": 100,
    "offset": 0,
    "calculate_digests": True,
    "request_key": "audit-2026-10-20-page-0",
})
job_id = accepted["job_id"]
snapshot = core.query("jobs.wait", {"job_id": job_id, "timeout_s": 1.0})
page = core.query("jobs.events", {
    "job_id": job_id,
    "after_event_id": 0,
    "limit": 100,
})
```

Keep the request key when retrying delivery after a lost response. An identical
delivery returns the same run, including after completion; changed intent under
the same key is rejected. If omitted, the command ID is the key, so a newly
constructed command normally submits new work. See [submission identity](submission-identity.md).

Limit is 1–10,000 and offset is nonnegative. Selection is applied to the current,
ID-ordered, nondeleted Replica inventory **at execution**, not frozen at
submission. A retry can therefore inspect a different current page.

The normal job view adds `durable.version`, ledger/run identity, attempt number,
durable state, cancellation request and progress fields. `queued`/`leased` map
to the existing public `pending` state. Events accept a nonnegative cursor and
a limit of 1–1,000; use `next_cursor` for the next page. `has_more` describes the
current read, not whether future events can appear. Per-Replica event payloads
are JSON text in `event_json`.

Both waits and results run outside Core's shared-service handler lock.
Other queries and cancellation remain responsive while a client waits or
disconnects. Wait timeout returns the current snapshot; result timeout raises
if execution is still incomplete. Neither cancels the job.

Submission and job-control/event routes require the existing admin authority.
Read-only authority cannot start an audit: verification persists observations.
The synchronous `storage.audit` command remains available for compatibility.
The CLI `storage audit` uses submission followed by one-second waits and accepts
`--request-key` for delivery recovery. Its final JSON includes the job ID/state;
exit zero requires both successful execution and a healthy audit receipt.

## Cancellation, interruption and retry

Cancellation before handler entry prevents verification. During execution it is
cooperative: the worker checks between Replicas and after the final verification.
An in-flight backend read cannot be preempted. Completed observations remain
committed; the terminal cancelled receipt retains completed results and counts.
Per-Replica events provide incremental evidence if the process dies before a
final receipt is stored.

Expired running attempts become `aborted`, with uncertain effects recorded.
Inspection after reopening recovers that status; no interrupted audit is
automatically replayed. Queued work starts when its submission is repeated or
another audit starts polling. This is an on-demand worker, not an installed
background service.

Explicit `jobs.retry` preserves captured intent and ancestry. Supply a new
`request_key`, and `acknowledge_possible_effects=True` for an attempt that entered
execution. Inspect retained results/events first. Acknowledgement is not proof
that an old process stopped or that repeating effects is harmless.

Audit version 1 receipts retain `ok`, counts, pagination and per-Replica results,
and add `result_version` and `cancelled`. A completed audit can have successful
execution but `ok=False` because some Replicas are unhealthy. Verification uses
the [storage record owner](../storage_and_backup/record-mutations.md), including
revision checks. Byte publication and ingest recovery remain storage concerns.

## Scope

Only this workflow is migrated. Utility jobs, maintenance scheduling, conversion,
ingest and checkpointed backup retain their existing execution/recovery owners.
Definition timeouts and automatic retries are not implemented by this worker.
Audit results/events are retained; operator retention/rotation is future work.

[Integration tests](../../tests/core/test_durable_storage_audit.py) cover local/HTTP
parity, responsive cancellation, lost clients, process interruption, real
SQLite/APSW observations, reopening/delivery recovery, ownership, authority and
retry acknowledgement.

Return to [jobs](README.md).
