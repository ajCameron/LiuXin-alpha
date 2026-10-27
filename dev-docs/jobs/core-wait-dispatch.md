# Core job waiting and dispatch

Implemented **2026-10-20** for JOB01, the first part of architecture A09.
Utility work retains `utils.jobs.manager.InMemoryJobManager`. This dispatch
change does not make those records persistent. The subsequent
[durable audit](core-storage-audit.md) integrates JOB02's selected workflow;
broader migration remains open.

## Registration and resource ownership

Commands and queries share Core's reentrant handler lock by default. This
serializes access to the composed database connection for local and HTTP callers.
The built-in `jobs.wait` and `jobs.result` queries explicitly register with
`serialized=False`: both wait on the independently synchronized job manager and
do not use the shared database connection.
Dispatch resolves the preference context under the shared lock before releasing
it for either wait. An injected `JobManagerAPI` must also support concurrent
wait/result/state calls; the maintained in-memory manager provides that contract.

`register_query_handler(..., serialized=False)` is a trusted composition choice,
not a client-controlled option or a promise implied by calling something a query.
An opted-out handler must use resources that are safe for concurrent access and
must not access the shared database or services being closed. Existing custom
registrations remain serialized. Replacing either job route without the keyword
restores the serialized default.

A query captures its handler, permissions, execution policy and result conversion
policy in one immutable registration. Replacement affects later requests; an
in-flight request finishes under its captured registration. Remote authorization
still precedes handler entry, and both job wait routes still require `admin`.

Other synchronous operations may still occupy the handler lock. This exception
does not turn an audit, reconciliation, ingest or storage mutation into background
work automatically. A serialized handler that calls a wait itself retains its
outer lock; it should submit work and return to its client instead.

## Waiting, cancellation and shutdown

While one client waits, another can inspect `health` or `jobs.get` and request
`jobs.cancel`, subject to its permissions. These calls retain their ordinary
serialized dispatch; the waiting client no longer occupies that lock.

The existing manager outcomes are preserved:

- `jobs.wait` with an expired wait timeout returns the current snapshot, which
  may still be running.
- `jobs.result` with an expired wait timeout raises an error if no execution
  payload exists yet.
- Neither timeout cancels the job. A disconnected HTTP client also does not
  cancel server work or its server-side wait.
- Cancellation acknowledges a request, not guaranteed interruption. In particular,
  the serial execution backend cannot preempt a running function.

Shutdown marks the runtime closed and acquires the shared handler lock before
closing owned services. Requests queued on that lock check the flag again and
refuse to enter their handlers. A queued command that already emitted
`command.started` emits `command.failed` when refused. New requests are rejected.

An already admitted concurrent wait may finish after Core shutdown. Core borrows
its default process-shared manager, and normally also an explicitly supplied
manager. Closing one runtime therefore does not close that manager or cancel its
jobs. Explicit `close_job_manager_on_shutdown=True` retains the existing
non-waiting manager shutdown policy; backend-specific cancellation still applies.

## Evidence and next slice

[Concurrent dispatch tests](../../tests/core/test_job_wait_dispatch.py) use
event-gated real jobs, local clients and authenticated HTTP clients. They also
exercise zero-timeout outcomes, permission refusal, manager survival after
shutdown, queued-handler refusal, registration replacement and serialized
transactions on SQLite and APSW.

The subsequent [durable Core audit](core-storage-audit.md) now uses a stable
identity, progress/event cursor, terminal receipt and recovery contract. See
[job tasks](next_tasks.md) and the [owner map](README.md) for remaining migration.
This dispatch change alone does not provide durable execution.
