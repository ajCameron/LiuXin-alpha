# Durable claims and interrupted runs

Implemented **2026-10-20** as the next A09 prerequisite, after
[responsive Core waiting](core-wait-dispatch.md). The durable repository is still
separate from Core's default utility manager. The following prerequisite adds
[submission identity and retry ancestry](submission-identity.md); a real workflow's
Core integration remains JOB02 work.

## One owner for each transition

`JobRepository` owns the SQLite ledger. Each operation closes its connection.
Enqueue checks and insertion share an immediate transaction, so concurrent
submitters cannot bypass `QUEUE_ONE` or `SKIP_IF_RUNNING`. Claim selection and
assignment also share an immediate transaction, so two workers cannot acquire
the same queued run.

Every claim has a fresh `lease_token`. A worker name alone grants no ownership:
reusing that name after expiry does not revive an earlier claim. Starting,
heartbeat, progress, logs and terminal receipts require the current worker/token
pair and an unexpired lease. State transitions and their events commit together.
A failed event write rolls back the corresponding state change.

| Observed state | Allowed action |
| --- | --- |
| Queued | Claim it, or cancel it before execution. |
| Leased, unexpired | The current claim may start or renew; cancellation revokes it. |
| Leased, expired | A worker may reclaim it with a new token; the old token cannot start. |
| Running, unexpired | Its claim may report progress, renew, log and finish. |
| Running, expired | Recovery records `aborted` with an uncertain-effects explanation. |
| Terminal | Late worker updates and further cancellation requests cannot change it. |

`recover_expired_runs()` is also available without running a worker. Normal
worker polling performs this recovery before claiming more work. Recovery
does not replay a handler. A stale process may still be executing external code;
fencing prevents it from overwriting the ledger, not from touching an external
system. A workflow must inspect its own publication/checkpoint evidence before
an explicit retry.

## Worker and handler contract

`JobWorker` renews its lease on a separate thread while a handler executes.
Handler lookup, payload validation, renewal-thread startup and result
serialization failures become durable failures while the worker still owns
the claim. Results must be JSON-compatible; unsupported objects are not silently
converted to strings.

Runs capture their kind, label and JSON payload when enqueued. Editing a definition
affects later submissions, not the handler/input selected for an already queued
run. The worker does not currently enforce the definition's execution timeout,
heartbeat-timeout or retry declarations; its configured lease duration governs
claim renewal. Automatic retries are not provided.

`JobRunContext` supplies the current token to progress, log and heartbeat calls.
Handlers can call `raise_if_cancel_requested()` at appropriate domain checkpoints.
Cancellation while queued or leased prevents handler entry. Once running it remains
a request: a handler that returns after the request is acknowledged as cancelled,
with its returned receipt retained. That receipt may describe completed effects.
Neither cancellation nor lease loss implies rollback.

## Inspection and retention

`list_events(run_id, after_event_id=cursor, limit=count)` provides bounded,
ordered event pages. Event IDs provide the cursor; timestamps need not be unique.
Result retention keeps run identities, status, error text, logs and event cursors.
It prunes old terminal result payloads and their terminal event payloads.
`KEEP_LATEST_ONLY` retains the terminal payload with the highest run ID;
`KEEP_FAILURES` retains failure/cancellation receipts and the newest success.
Active runs are never removed by result retention.

## Upgrade and callers

Stop workers before upgrading a ledger and its callers. Old binaries do not
honor claim fencing. Existing ledgers gain `lease_token` and `payload_json`
columns. Legacy runs capture their current definition payload once during
upgrade; historical inputs cannot be reconstructed if that definition was
already edited.

Worker-owned repository methods now require both `worker_id` and `lease_token`.
Use the token returned by `lease_next_run`, or let `JobWorker` build a
`JobRunContext`. Hand-built contexts must first acquire and start a real claim.
The retained backup handler test follows this path.

The [durable claim tests](../../tests/jobs/test_durable_job_claims.py) cover
concurrent claims/enqueue, reused worker names, stale callbacks, process restart,
atomic event failure, long-handler renewal, cancellation receipts, retention,
captured inputs and a fixed legacy schema fixture. This prerequisite does not
yet give Core a durable submission or retry API. Repository-level delivery keys
and explicit successors are described in the [submission contract](submission-identity.md).
