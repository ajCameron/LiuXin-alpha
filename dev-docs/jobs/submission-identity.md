# Durable submission identity and explicit retries

Implemented for review **2026-10-20**, after [claim ownership](durable-claims.md).
This is an A09 prerequisite, now used by the
[Core storage audit](core-storage-audit.md). Other utility jobs do not acquire
these guarantees automatically.

## Delivery identity

`JobRepository.submit_job(definition, request_key=...)` creates a new definition,
its first run, the queue event and the delivery mapping in one immediate
transaction. Repeating the same request returns that run's current state,
including after completion or process restart. A failed transaction leaves no
partially accepted definition, run or consumed key.

Keys are exact, nonblank strings of at most 200 characters, shared by submit,
enqueue and retry across one ledger. Reusing a key for different input or a
different operation raises `JobRequestConflict` without accepting work.
JSON object order and whitespace do not matter; array order and numeric
representations do. Non-finite JSON values are refused. Configuration fields,
including the label and scheduling/retention declarations, participate in the
comparison; generated IDs and bookkeeping timestamps do not.

```python
from LiuXin_alpha.jobs.api import JobDefinition, JobRepository

repository = JobRepository("jobs.sqlite")
definition = JobDefinition(
    job_kind="registered.workflow",
    job_name="Inspect selected files",
    payload_json='{"file_ids": [12, 18]}',
)
accepted = repository.submit_job(definition, request_key="client-operation-42")
# After a lost response, send the same key and intent:
repeated = repository.submit_job(definition, request_key="client-operation-42")
assert repeated.job_run_id == accepted.job_run_id
```

The example queues work; a registered handler and worker are still required.
Different keys intentionally create different operations, even for identical
payloads. Each `submit_job` creates its own definition; concurrency policies apply
within a definition, not across independent definitions or matching payloads.
Domain workflows must supply their own stable operation identity and effect rules.

For an existing definition, `enqueue_run(..., request_key=...)` provides the same
delivery protection. It compares the current definition plus trigger and deferred
start inputs. An edited definition requires a new key. Without a key, each call
requests new work, preserving existing scheduler/manual callers. Deduplication
happens before concurrency policy, so repeating a replacement request does not
cancel its own previously accepted run.

Each ledger gains a persistent `ledger_uuid`; pair it with the run ID when
designing a transport identity. Copying or restoring a ledger preserves its
identity. Independent writable copies are not a distributed execution system.

## Explicit execution retries

`retry_run(parent_id, request_key=...)` accepts failed, timed-out, cancelled or
aborted attempts. Active and successful runs cannot be retried through this API.
It creates one successor with the same captured kind, label and payload, an
incremented `attempt_number` and `retry_of_job_run_id`. Editing the definition
does not retarget a retry. Current concurrency and retention policy still apply.

A parent has at most one successor. Repeating the same retry key resolves that
successor, including after it finishes. A different request to retry the same
parent fails; to make a later attempt, retry the successor. Neither the worker
nor the scheduler automatically invokes this API.

If the parent started, the caller must pass `acknowledge_possible_effects=True`
after inspecting domain recovery evidence and dealing with any stale worker.
The queue event records that acknowledgement. This is a deliberate request to
retry despite possible effects, not proof of rollback, publication idempotency or
exactly-once execution. Lease fencing prevents stale ledger callbacks; it cannot
stop an old process from changing an external system.

Cancellation before start can be retried without that acknowledgement. A
cancelled run that started may retain a completed-effect receipt and still
requires it. A timeout declaration alone does not stop a worker; deadline
enforcement remains separate work.

## Retention and upgrade

Result pruning retains runs, ancestry and request mappings. Repeated delivery
still resolves the original run when its result payload has been pruned; it does
not silently run the operation again to reconstruct that result. No key expiry
is implemented, so operators must account for the retained ledger history.

Stop old workers before upgrading. Existing ledgers gain the identity/request
tables and nullable label/parent columns. Legacy labels are captured once from
the current definition, just like legacy payloads. Historical edits and parent
relationships cannot be reconstructed; existing attempt numbers are retained.

[Submission tests](../../tests/jobs/test_job_submissions.py) cover fresh-process
delivery, concurrent duplicates, a real publishing handler, changed inputs,
atomic failures, concurrency policies, retry chains, uncertain effects, stale
claims, retention and a fixed pre-migration schema.
