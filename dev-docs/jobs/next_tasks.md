# Next job tasks

Date: **2026-10-20**. JOB01 and JOB02's audit workflow are implemented for PR review;
broader migration remains open.

Core routes the selected storage audit to durable state. Other jobs retain the
utility manager. Architecture A09 is convergence work, not a package rename.

## JOB01. Release long waits from Core dispatch

**Implemented for PR review.** `jobs.wait` and `jobs.result` explicitly bypass
the shared handler lock using the selected job manager. Other queries
and all commands remain serialized by default. Registration replacement captures
authority and execution/result policy together; queued handlers recheck shutdown.
See the [waiting contract](core-wait-dispatch.md).

Concurrent local/HTTP clients exercise health/status/cancellation before the
blocked job finishes. SQLite/APSW controls retain serialized transactions.
This closes the wait-lock defect, not A09's durable workflow requirement.

## JOB02. Move one production workflow onto durable job state

**Prerequisite implemented for review:** [durable claims](durable-claims.md)
now enforce exclusive claiming, atomic state/events, stale-owner refusal and
explicit recovery after lease expiry. Workers renew long-running claims and
preserve completed-effect receipts when cancellation is acknowledged.
The following [submission prerequisite](submission-identity.md) adds atomic
delivery deduplication, stable ledger identity and explicit retry ancestry.
Started attempts require possible-effect acknowledgement before retry.
The [Core storage audit](core-storage-audit.md) now uses these contracts:
namespaced run IDs, independently owned library services, bounded progress/events,
reopen inspection and explicit retry with possible-effect acknowledgement.

**Selected workflow implemented for PR review; architecture A09.** Local and
HTTP clients share durable audit status and partial receipts. Process interruption
becomes explicit `aborted` state without automatic replay. Utility IDs remain
unchanged. Continue broader workflow migration only with its domain recovery
contract; this slice does not migrate conversion/ingest or install a scheduler.

Done when restart after submit, lease and side effect gives a deterministic recoverable state; duplicate submissions and retries do not silently repeat publication.

## JOB03. Unify observable cancellation and results

**Audit contract implemented; other workflows open.** Audit cancellation is
cooperative between verifications and retains completed observations/results.
Execution success and audit health remain separate. Preserve equivalent domain
warnings/loss reports and log references when migrating other workflows.

Done when direct and remote callers receive the same versioned outcome and know whether work may still be running after their request times out.

## JOB04. Retire duplicate registries by live caller migration

**Following.** Map durable handlers, utility executors, Core wrappers and domain journals. Keep execution backends and domain recovery evidence where they have distinct roles.

Done when one migrated workflow has one authoritative run state, obsolete callers are gone, and compatibility removal does not discard useful domain checkpoints.

## Validation and routing

Use repository/worker tests plus Core concurrent-client tests. Record manager ownership explicitly and keep all test databases, logs and worker outputs disposable.

Return to [job](README.md).
